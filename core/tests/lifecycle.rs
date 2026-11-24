use rzmq::socket::options::SNDTIMEO;
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Msg, SocketType, ZmqError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify; // For signalling
use tokio::task::{self, JoinHandle};
use tokio::time::timeout;
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(250);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);
const MONITOR_EVENT_TIMEOUT: Duration = Duration::from_secs(3);

/// `term()` with a PUSH whose connecter may still be in flight completes
/// promptly: the connecter stops with its socket instead of holding the
/// context's wait group to its 10 s timeout.
#[tokio::test]
async fn test_context_term_with_connecter_in_flight_is_prompt() -> Result<(), ZmqError> {
  for _ in 0..20 {
    let ctx = common::test_context();
    let bound = ctx.socket(SocketType::Pull)?;
    let connected = ctx.socket(SocketType::Push)?;
    let endpoint = common::bind_and_resolve_tcp(&bound).await?;
    connected.connect(&endpoint).await?;
    let started = std::time::Instant::now();
    ctx.term().await?;
    let took = started.elapsed();
    assert!(took < Duration::from_secs(1), "term took {took:?}");
  }
  Ok(())
}

/// Runs `scenario` on a runtime whose blocking pool is one thread and holds
/// that thread until `scenario` returns. tokio resolves a hostname on the
/// blocking pool, so a connecter's lookup stays pending for the whole scenario.
fn run_with_dns_lookups_held<F>(scenario: impl FnOnce() -> F) -> (Duration, Result<(), ZmqError>)
where
  F: std::future::Future<Output = Result<(), ZmqError>>,
{
  let runtime = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .max_blocking_threads(1)
    .build()
    .expect("runtime");
  runtime.block_on(async {
    let (release, held) = std::sync::mpsc::channel::<()>();
    let (holding_tx, holding) = tokio::sync::oneshot::channel::<()>();
    let holder = task::spawn_blocking(move || {
      let _ = holding_tx.send(());
      let _ = held.recv();
    });
    holding.await.expect("blocking thread held");

    let started = std::time::Instant::now();
    let outcome = timeout(Duration::from_secs(30), scenario()).await;
    let took = started.elapsed();

    drop(release);
    holder.await.expect("holder");
    (took, outcome.expect("scenario finished"))
  })
}

#[test]
fn test_context_term_during_a_connecters_dns_lookup_is_prompt() -> Result<(), ZmqError> {
  let (took, outcome) = run_with_dns_lookups_held(|| async {
    let ctx = common::test_context();
    let connected = ctx.socket(SocketType::Push)?;
    connected.connect("tcp://localhost:5870").await?;
    ctx.term().await
  });
  assert!(took < Duration::from_secs(1), "term took {took:?}");
  outcome
}

#[test]
fn test_socket_close_during_a_connecters_dns_lookup_is_prompt() -> Result<(), ZmqError> {
  let (took, outcome) = run_with_dns_lookups_held(|| async {
    let ctx = common::test_context();
    let connected = ctx.socket(SocketType::Push)?;
    connected.connect("tcp://localhost:5871").await?;
    connected.close().await?;
    ctx.term().await
  });
  assert!(took < Duration::from_secs(1), "close and term took {took:?}");
  outcome
}

/// Every event still queued on `monitor` once its socket has stopped.
async fn events_until_monitor_closes(monitor: &MonitorReceiver) -> Vec<SocketEvent> {
  let mut events = Vec::new();
  while let Ok(Ok(event)) = timeout(SHORT_TIMEOUT, monitor.recv()).await {
    events.push(event);
  }
  events
}

fn disconnected_count(events: &[SocketEvent]) -> usize {
  events.iter().filter(|e| matches!(e, SocketEvent::Disconnected { .. })).count()
}

/// libzmq raises `ZMQ_EVENT_DISCONNECTED` from the engine of a connection
/// (`stream_engine_base_t::error`). An endpoint that never connected has none.
#[tokio::test]
async fn test_closing_a_socket_that_never_connected_emits_no_disconnected_event() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let unused_port = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port").local_addr().expect("local addr").port();
  let endpoint = format!("tcp://127.0.0.1:{unused_port}");
  let push = ctx.socket(SocketType::Push)?;
  let monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;
  common::wait_for_monitor_event(&monitor, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::ConnectDelayed { .. })
  })
  .await
  .expect("first connect attempt fails");

  push.close().await?;
  ctx.term().await?;

  let events = events_until_monitor_closes(&monitor).await;
  assert_eq!(disconnected_count(&events), 0, "events after close: {events:#?}");
  Ok(())
}

#[tokio::test]
async fn test_a_peer_that_left_before_close_is_reported_disconnected_once() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  let endpoint = common::bind_and_resolve_tcp(&pull).await?;
  let push = ctx.socket(SocketType::Push)?;
  let monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;
  common::wait_for_monitor_event(&monitor, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .expect("handshake");

  pull.close().await?;
  common::wait_for_monitor_event(&monitor, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::Disconnected { .. })
  })
  .await
  .expect("the peer leaving is reported");

  push.close().await?;
  ctx.term().await?;

  let events = events_until_monitor_closes(&monitor).await;
  assert_eq!(disconnected_count(&events), 0, "events after close: {events:#?}");
  Ok(())
}

// --- Test: Context termination closes sockets and allows exit ---
#[tokio::test]
async fn test_context_term_closes_sockets() -> Result<(), ZmqError> {
  println!("Starting test_context_term_closes_sockets...");
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  println!("Setting up monitor for PUSH socket...");
  let push_monitor_rx = push.monitor_default().await?;
  println!("PUSH monitor setup.");

  let endpoint = "inproc://term-test";

  println!("Binding PULL...");
  pull.bind(endpoint).await?;
  println!("Connecting PUSH...");
  push.connect(endpoint).await?;

  println!("Main test: Waiting for PUSH monitor Connected event...");
  common::wait_for_monitor_event(
    &push_monitor_rx,
    MONITOR_EVENT_TIMEOUT,
    SHORT_TIMEOUT,
    |e| matches!(e, SocketEvent::Connected { endpoint: ep, .. } if *ep == endpoint),
  )
  .await
  .expect("Monitor did not receive Connected event");
  println!("Main test: PUSH monitor received Connected event.");
  tokio::time::sleep(Duration::from_millis(20)).await;

  println!("PUSH sending message...");
  push.send(Msg::from_static(b"Before Term")).await?;
  println!("PUSH sent.");

  println!("PULL receiving message...");
  let msg1 = common::recv_timeout(&pull, SHORT_TIMEOUT).await?;
  assert_eq!(msg1.data().unwrap(), b"Before Term");
  println!("PULL received.");

  println!("Initiating context termination...");
  // Initiate termination - this returns quickly
  let shutdown_task = ctx.shutdown(); // Use shutdown() to just initiate

  println!("Waiting for Disconnected event for {}...", endpoint);
  // Now wait for the event AFTER initiating shutdown
  let event_wait_task = common::wait_for_monitor_event(
    &push_monitor_rx,
    MONITOR_EVENT_TIMEOUT, // Use a reasonable timeout
    SHORT_TIMEOUT,
    |e| matches!(e, SocketEvent::Disconnected { endpoint: ep } if *ep == endpoint),
  );

  let (_, event_wait_result) = futures::future::join(shutdown_task, event_wait_task).await;

  match event_wait_result {
    Ok(_) => println!("PUSH monitor received Disconnected event as expected."),
    Err(e) => panic!("PUSH monitor wait failed: {}", e),
  }

  println!("Event received or timed out. Now waiting for term() to complete...");
  // Now await the full termination process
  let term_result = ctx.term().await; // This waits on the WaitGroup
  println!("ctx.term() completed with: {:?}", term_result);
  assert!(term_result.is_ok(), "Context termination failed");
  println!("Context terminated.");

  // Try using sockets after termination - should fail
  println!("Attempting PUSH set_option_raw after term (should fail)...");
  let setopt_res = push
    .set_option_raw(rzmq::socket::options::SNDTIMEO, &(0i32).to_ne_bytes())
    .await;
  println!("PUSH set_option_raw result: {:?}", setopt_res);
  assert!(
    setopt_res.is_err(),
    "Expected error setting option after term, got {:?}",
    setopt_res
  );
  println!("PUSH set_option_raw correctly failed: {:?}", setopt_res.err().unwrap());

  println!("Attempting PUSH send after term (should fail)...");
  let send_res = push.send(Msg::from_static(b"After Term")).await;
  assert!(
    send_res.is_err(),
    "Expected error sending after term and disconnect, got {:?}",
    send_res
  );
  println!("PUSH send correctly failed after term: {:?}", send_res.err().unwrap());

  println!("Attempting PULL recv after term (should fail)...");
  let recv_res = pull.recv().await;
  assert!(
    recv_res.is_err(),
    "Expected error receiving after term, got {:?}",
    recv_res
  );
  println!("PULL recv correctly failed: {:?}", recv_res.err().unwrap());

  println!("Test test_context_term_closes_sockets finished.");
  Ok(())
}

// --- Test: Explicit socket close stops activity ---
#[tokio::test]
async fn test_socket_close_stops_connection() -> Result<(), ZmqError> {
  println!("Starting test_socket_close_stops_connection...");
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?; // Keep pull alive

  let endpoint = "tcp://127.0.0.1:5640"; // Unique port

  println!("Binding PUSH...");
  push.bind(endpoint).await?; // Let PUSH be the listener this time
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Connecting PULL...");
  pull.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await; // Connect + handshake

  // PUSH sends a message, PULL receives it
  println!("PUSH sending Message 1...");
  push.send(Msg::from_static(b"Message 1")).await?;
  let msg1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(msg1.data().unwrap(), b"Message 1");
  println!("PULL received Message 1.");

  // Close the PULL socket explicitly
  println!("Closing PULL socket...");
  pull.close().await?;
  println!("PULL socket closed.");

  // Allow time for close to propagate and detach pipe
  tokio::time::sleep(Duration::from_millis(200)).await;

  // PUSH attempts to send another message
  println!("PUSH sending Message 2 (after PULL closed)...");
  // Use non-blocking send to check if peer is gone
  push
    .set_option_raw(rzmq::socket::options::SNDTIMEO, &(0i32).to_ne_bytes())
    .await?;
  let send_res = push.send(Msg::from_static(b"Message 2")).await;
  println!("PUSH send result: {:?}", send_res);

  // Since the only peer (PULL) is closed, the PUSH load balancer has no targets.
  // SNDTIMEO=0 should result in ResourceLimitReached.
  assert!(
    matches!(send_res, Err(ZmqError::ResourceLimitReached)),
    "Expected ResourceLimitReached sending after peer close, got {:?}",
    send_res
  );
  println!("PUSH correctly failed sending after PULL closed.");

  println!("Terminating context...");
  ctx.term().await?; // Should clean up the remaining PUSH socket
  println!("Test test_socket_close_stops_connection finished.");
  Ok(())
}

// --- Test: Closing socket handle triggers cleanup ---
#[tokio::test]
async fn test_socket_explicit_close_triggers_disconnect_event() -> anyhow::Result<()> {
  println!("Starting test_socket_explicit_close_triggers_disconnect_event...");
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?; // Keep as owned initially

  // ... (setup push_monitor, bind push, check Listening) ...
  println!("Setting up PUSH monitor...");
  let push_monitor = push.monitor_default().await?;

  let endpoint = "tcp://127.0.0.1:5641"; // Keep unique port

  println!("Binding PUSH...");
  push.bind(endpoint).await?;

  println!("Expecting Listening event...");
  common::wait_for_monitor_event(
    &push_monitor,
    MONITOR_EVENT_TIMEOUT,
    SHORT_TIMEOUT,
    |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == endpoint),
  )
  .await
  .map_err(|e| anyhow::anyhow!("Listening event wait failed: {}", e))?;
  println!("PUSH Monitor: Received Listening event.");

  tokio::time::sleep(Duration::from_millis(50)).await;

  let disconnected_endpoint_uri: String;

  {
    // Scope for PULL socket
    let pull = ctx.socket(SocketType::Pull)?;
    println!("Connecting PULL...");
    pull.connect(endpoint).await?;

    println!("Expecting Accepted/Handshake event...");
    let event2 = common::wait_for_monitor_event(&push_monitor, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
      matches!(e, SocketEvent::HandshakeSucceeded { .. })
    })
    .await
    .map_err(|e| anyhow::anyhow!("Accepted/Handshake event wait failed: {}", e))?;
    println!("PUSH Monitor: Received connection event: {:?}", event2);

    disconnected_endpoint_uri = match event2 {
      SocketEvent::Accepted { endpoint: _, peer_addr } => format!("tcp://{}", peer_addr),
      SocketEvent::HandshakeSucceeded { endpoint: ep } => ep,
      _ => panic!("Unexpected event type received: {:?}", event2),
    };
    println!("Determined peer endpoint URI: {}", disconnected_endpoint_uri);

    tokio::time::sleep(Duration::from_millis(150)).await;

    println!("PUSH sending Message 1...");
    push.send(Msg::from_static(b"Message 1")).await?;
    let msg1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    assert_eq!(msg1.data().unwrap(), b"Message 1");
    println!("PULL received Message 1.");

    println!("PULL socket closing...");
    pull.close().await?;
    println!("PULL socket closed (explicitly).");
  } // pull socket scope ends, handle is dropped

  println!("Waiting for Disconnected event for {}...", disconnected_endpoint_uri);
  common::wait_for_monitor_event(
    &push_monitor,
    MONITOR_EVENT_TIMEOUT,
    SHORT_TIMEOUT,
    |e| matches!(e, SocketEvent::Disconnected { endpoint: ep } if *ep == disconnected_endpoint_uri),
  )
  .await
  .map_err(|e| anyhow::anyhow!("Disconnected event wait failed: {}", e))?;
  println!(
    "PUSH Monitor: Received Disconnected event for {}.",
    disconnected_endpoint_uri
  );

  println!("PUSH setting SNDTIMEO=0...");
  push.set_option_raw(SNDTIMEO, &(0i32).to_ne_bytes()).await?;
  println!("PUSH sending Message 2 (after PULL disconnected)...");
  let send_res = push.send(Msg::from_static(b"Message 2")).await;
  println!("PUSH send result: {:?}", send_res);

  assert!(
    matches!(send_res, Err(ZmqError::ResourceLimitReached)),
    "Expected ResourceLimitReached sending after peer disconnect, got {:?}",
    send_res
  );
  println!("PUSH correctly failed sending after PULL disconnected.");

  println!("Closing PUSH socket explicitly before context term...");
  let push_close_res = push.close().await;
  println!("PUSH close result: {:?}", push_close_res);
  // We expect Ok(()) here as the socket should still be operational enough to process close
  assert!(push_close_res.is_ok(), "PUSH close failed unexpectedly");
  // Allow a moment for the close command to be processed and shutdown to initiate
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Terminating context...");
  ctx.term().await?; // Terminate context gracefully

  println!("Checking if monitor channel is closed...");
  match timeout(SHORT_TIMEOUT, push_monitor.recv()).await {
    Ok(Ok(event)) => {
      // It's possible to receive a final 'Closed' or 'Disconnected' related to the *listener*
      // being shut down as part of the PUSH socket closing, *before* the monitor channel itself closes.
      // However, receiving ConnectDelayed is definitely wrong.
      match event {
        SocketEvent::ConnectDelayed { .. } => {
          anyhow::bail!(
            "Received unexpected ConnectDelayed event after context term: {:?}",
            event
          );
        }
        // Allow other close-related events like Closed or potentially another Disconnected
        // if the timing is very specific, but log them as warnings maybe.
        _ => {
          println!(
            "PUSH Monitor received final event {:?} (expected closed/empty). Tolerating.",
            event
          );
        }
      }
    }
    Ok(Err(_recv_err)) => {
      println!("PUSH Monitor channel correctly closed after context term.");
    }
    Err(_) => {
      println!("PUSH Monitor channel timed out (likely closed and empty) after context term.");
    }
  }

  println!("Test test_socket_explicit_close_triggers_disconnect_event finished.");
  Ok(())
}

// --- Test: Concurrent Context Termination and Socket Operation ---
// This test is a bit more complex, aiming to catch race conditions during shutdown.
#[tokio::test]
async fn test_concurrent_term_and_op() -> Result<(), ZmqError> {
  println!("Starting test_concurrent_term_and_op...");
  let ctx = common::test_context();
  let push = Arc::new(ctx.socket(SocketType::Push)?);
  let pull = ctx.socket(SocketType::Pull)?;
  let endpoint = "inproc://concurrent-term";

  println!("Binding PULL...");
  pull.bind(endpoint).await?;
  println!("Connecting PUSH...");
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  let push_clone = push.clone();
  let finished_sending = Arc::new(Notify::new());
  let finished_sending_clone = finished_sending.clone();

  // Task that continuously sends messages
  let send_task: JoinHandle<()> = task::spawn(async move {
    // Type annotation for clarity
    let mut count = 0;
    loop {
      let msg = Msg::from_vec(format!("Msg {}", count).into_bytes());
      match common::send_timeout(&*push_clone, msg, SHORT_TIMEOUT).await {
        Ok(()) => {
          count += 1;
          tokio::task::yield_now().await;
        }
        Err(ZmqError::Timeout) => {
          println!("Send task: Send timed out.");
          break;
        }
        Err(e) => {
          println!("Send task: Send failed: {}", e);
          break;
        }
      }
      if count % 1000 == 0 {
        println!("Send task: Sent {} messages", count);
      }
    }
    println!("Send task finished.");
    finished_sending_clone.notify_one(); // Signal completion
  });

  // Main task receives some messages, then terminates context
  println!("Main task receiving first message...");
  let _ = common::recv_timeout(&pull, LONG_TIMEOUT).await?; // Receive at least one
  println!("Main task received one message.");
  tokio::time::sleep(Duration::from_millis(10)).await; // Allow sender to send a bit more

  println!("Main task initiating context termination...");

  let termination_complete = Arc::new(Notify::new());
  let termination_complete_clone = termination_complete.clone();

  // Spawn termination task
  let term_task: JoinHandle<Result<(), ZmqError>> = task::spawn(async move {
    let result = ctx.term().await; // ctx is MOVED here
    println!("Termination task: ctx.term() finished with result: {:?}", result);
    termination_complete_clone.notify_waiters(); // Signal completion
    result // Return the result
  });

  // Wait for EITHER termination to signal completion OR the send task to signal completion
  tokio::select! {
      // Branch 1: Termination task signals completion first
      _ = termination_complete.notified() => {
          println!("Termination completed signal received first.");
          // Termination is done, no need to wait further here in select.
          // We'll check term_task result outside the select.
      }
      // Branch 2: Send task signals completion first
      _ = finished_sending.notified() => {
          println!("Send task finished notification received first.");
          // Do NOT re-await termination_complete here: the term task signals via
          // notify_waiters(), which stores no permit — if it fired between the select
          // race and a fresh notified() registration, the signal is lost and the test
          // hangs forever. The post-select `term_task.await` below is the authoritative
          // completion join.
      }
      // Branch 3: Overall timeout
      _ = tokio::time::sleep(Duration::from_secs(5)) => {
           panic!("Test timed out waiting for termination or send task completion");
      }
  }

  // --- Post-select checks ---

  // Check the result of the termination task *after* select ensures it was signalled.
  // Watchdog: a genuine ctx.term() hang must fail the test loudly, not stall the runner.
  let term_final_result = tokio::time::timeout(Duration::from_secs(10), term_task)
    .await
    .expect("Timed out waiting for termination task to join — ctx.term() hung");
  match term_final_result {
    Ok(Ok(())) => {
      println!("Termination task joined successfully.");
    }
    Ok(Err(e)) => return Err(e), // Termination itself failed with ZmqError
    Err(join_err) => panic!("Term task panicked: {:?}", join_err), // Term task panicked
  }

  // Ensure send task didn't panic (outside the select)
  if let Err(e) = send_task.await {
    match e.try_into_panic() {
      Ok(payload) => std::panic::resume_unwind(payload),
      Err(join_err) if !join_err.is_cancelled() => {
        // Only panic if it wasn't just cancelled by the runtime/termination
        panic!("Send task failed to join normally: {:?}", join_err);
      }
      _ => {
        println!("Send task was cancelled (expected outcome possible).");
      }
    }
  }

  println!("Test test_concurrent_term_and_op finished.");
  Ok(())
}

/// Dropping the last `Socket` handle without explicitly calling `close().await`
/// must still stop its `SocketCore` actor and disconnect peers cleanly.
#[tokio::test]
async fn test_socket_drop_without_close_stops_actor_and_disconnects_peer() -> anyhow::Result<()> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let push_monitor = push.monitor_default().await?;
  let endpoint = common::bind_and_resolve_tcp(&push).await?;

  {
    let pull = ctx.socket(SocketType::Pull)?;
    pull.connect(&endpoint).await?;

    common::wait_for_monitor_event(&push_monitor, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
      matches!(e, SocketEvent::HandshakeSucceeded { .. })
    })
    .await
    .map_err(|e| anyhow::anyhow!("HandshakeSucceeded wait failed: {}", e))?;

    push.send(Msg::from_static(b"before-drop")).await?;
    let msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    assert_eq!(msg.data().unwrap(), b"before-drop");
  }

  common::wait_for_monitor_event(&push_monitor, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::Disconnected { .. })
  })
  .await
  .map_err(|e| anyhow::anyhow!("Peer did not observe Disconnected after Socket drop: {}", e))?;

  push.set_option_raw(SNDTIMEO, &(0i32).to_ne_bytes()).await?;
  let send_res = push.send(Msg::from_static(b"after-drop")).await;
  assert!(
    matches!(send_res, Err(ZmqError::ResourceLimitReached)),
    "Expected ResourceLimitReached after dropped peer disconnected, got {:?}",
    send_res
  );

  ctx.term().await?;
  Ok(())
}

/// Dropping a cloned `Socket` handle keeps the socket alive while another
/// clone still exists. The actor stops once the last clone is dropped.
#[tokio::test]
async fn test_socket_clone_drop_keeps_socket_alive_until_last_handle_drops() -> anyhow::Result<()> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let push_monitor = push.monitor_default().await?;
  let endpoint = common::bind_and_resolve_tcp(&push).await?;

  let pull = ctx.socket(SocketType::Pull)?;
  pull.connect(&endpoint).await?;

  common::wait_for_monitor_event(&push_monitor, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .map_err(|e| anyhow::anyhow!("HandshakeSucceeded wait failed: {}", e))?;

  let pull_clone = pull.clone();
  drop(pull_clone);
  tokio::time::sleep(Duration::from_millis(50)).await;

  push.send(Msg::from_static(b"still-alive")).await?;
  let msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(msg.data().unwrap(), b"still-alive");

  drop(pull);
  common::wait_for_monitor_event(&push_monitor, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::Disconnected { .. })
  })
  .await
  .map_err(|e| {
    anyhow::anyhow!(
      "Peer did not observe Disconnected after final Socket clone dropped: {}",
      e
    )
  })?;

  ctx.term().await?;
  Ok(())
}
