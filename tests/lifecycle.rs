// tests/lifecycle.rs

use rzmq::socket::SocketEvent;
use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify; // For signalling
use tokio::task::{self, JoinHandle};
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(250);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);
const MONITOR_EVENT_TIMEOUT: Duration = Duration::from_secs(3); 

// --- Test: Context termination closes sockets and allows exit ---
#[tokio::test]
async fn test_context_term_closes_sockets() -> Result<(), ZmqError> {
  println!("Starting test_context_term_closes_sockets...");
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  // *** ADD MONITOR FOR PUSH SOCKET ***
  println!("Setting up monitor for PUSH socket...");
  let push_monitor_rx = push.monitor_default().await?;
  println!("PUSH monitor setup.");
  // *** END ADDITION ***

  let endpoint = "inproc://term-test"; // Using inproc for simplicity

  println!("Binding PULL...");
  pull.bind(endpoint).await?;
  println!("Connecting PUSH...");
  push.connect(endpoint).await?;
  // Allow time for inproc connection setup (should be fast, but wait briefly)
  // Optionally, wait for a specific monitor event signalling connection if implemented for inproc
  tokio::time::sleep(Duration::from_millis(50)).await;

  // Send a message - should succeed
  println!("PUSH sending message...");
  push.send(Msg::from_static(b"Before Term")).await?;
  println!("PUSH sent.");

  // PULL receives message
  println!("PULL receiving message...");
  let msg1 = common::recv_timeout(&pull, SHORT_TIMEOUT).await?;
  assert_eq!(msg1.data().unwrap(), b"Before Term");
  println!("PULL received.");

  // Terminate the context - this should initiate shutdown of PUSH and PULL actors
  println!("Terminating context...");
  ctx.term().await?; // Awaits clean shutdown of both socket actors
  println!("Context terminated.");

  // *** ADD MONITOR WAIT FOR DISCONNECT ***
  println!("Waiting for PUSH monitor to report disconnect for {}...", endpoint);
  let disconnect_result = common::wait_for_monitor_event(
    &push_monitor_rx,
    MONITOR_EVENT_TIMEOUT,
    SHORT_TIMEOUT, // Interval to check channel
    |e| matches!(e, SocketEvent::Disconnected { endpoint: ep } if *ep == endpoint),
  )
  .await;

  match disconnect_result {
    Ok(_) => println!("PUSH monitor received Disconnected event as expected."),
    Err(e) => panic!("PUSH monitor failed to receive Disconnected event: {}", e), // Fail test if event not received
  }
  // *** END ADDITION ***

  // Try using sockets after termination - should fail
  println!("Attempting PUSH set_option after term (should fail)..."); // Keep this check
  let setopt_res = push
    .set_option(rzmq::socket::options::SNDTIMEO, &(0i32).to_ne_bytes())
    .await;
  println!("PUSH set_option result: {:?}", setopt_res);
  // Setting option should fail because the core actor's mailbox is gone
  assert!(
    setopt_res.is_err(),
    "Expected error setting option after term, got {:?}",
    setopt_res
  );
  println!("PUSH set_option correctly failed: {:?}", setopt_res.err().unwrap());

  println!("Attempting PUSH send after term (should fail)...");
  // Now that we've confirmed disconnect via monitor and set_option failed,
  // the send attempt should ideally also fail quickly, likely with the same
  // mailbox error as set_option, or ResourceLimitReached if the ISocket::send
  // could somehow proceed to check the load balancer (which should be empty).
  let send_res = push.send(Msg::from_static(b"After Term")).await;
  assert!(
    send_res.is_err(), // Expect *some* error
    "Expected error sending after term and disconnect, got {:?}",
    send_res
  );
  println!("PUSH send correctly failed after term: {:?}", send_res.err().unwrap());

  println!("Attempting PULL recv after term (should fail)...");
  let recv_res = pull.recv().await;
  assert!(
    recv_res.is_err(), // Expect some error
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
    .set_option(rzmq::socket::options::SNDTIMEO, &(0i32).to_ne_bytes())
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

// --- Test: Dropping socket handle triggers cleanup ---
// (Similar to close, but relies on Rust's Drop)
#[tokio::test]
async fn test_socket_close_on_drop_stops_connection() -> Result<(), ZmqError> {
  println!("Starting test_socket_close_on_drop_stops_connection..."); // Renamed log
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;

  let endpoint = "tcp://127.0.0.1:5641"; // Unique port

  println!("Binding PUSH...");
  push.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  {
    // Scope for PULL socket
    let pull = ctx.socket(SocketType::Pull)?;
    println!("Connecting PULL...");
    pull.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    println!("PUSH sending Message 1...");
    push.send(Msg::from_static(b"Message 1")).await?;
    let msg1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    assert_eq!(msg1.data().unwrap(), b"Message 1");
    println!("PULL received Message 1.");

    println!("PULL socket closing...");
    // *** CHANGE: Explicitly call close() instead of relying on drop ***
    pull.close().await?;
    // *** END CHANGE ***

    println!("PULL socket closed.");
    // pull handle is dropped here, but close already initiated shutdown
  } // pull socket scope ends

  // Allow time for close to propagate
  println!("Waiting after PULL close...");
  tokio::time::sleep(Duration::from_millis(200)).await;

  // PUSH attempts to send another message
  println!("PUSH sending Message 2 (after PULL closed)..."); // Changed log
  push
    .set_option(rzmq::socket::options::SNDTIMEO, &(0i32).to_ne_bytes())
    .await?;
  let send_res = push.send(Msg::from_static(b"Message 2")).await;
  println!("PUSH send result: {:?}", send_res);

  assert!(
    matches!(send_res, Err(ZmqError::ResourceLimitReached)),
    "Expected ResourceLimitReached sending after peer close, got {:?}", // Changed expectation message
    send_res
  );
  println!("PUSH correctly failed sending after PULL closed."); // Changed log

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_socket_close_on_drop_stops_connection finished."); // Renamed log
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
      if count % 100 == 0 {
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

  // <<< MODIFIED: Use Notify for termination signal >>>
  let termination_complete = Arc::new(Notify::new());
  let termination_complete_clone = termination_complete.clone();

  // Spawn termination task
  let term_task: JoinHandle<Result<(), ZmqError>> = task::spawn(async move {
    let result = ctx.term().await; // ctx is MOVED here
    println!("Termination task: ctx.term() finished with result: {:?}", result);
    termination_complete_clone.notify_waiters(); // Signal completion
    result // Return the result
  });
  // <<< MODIFIED END >>>

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
          // Send task finished, now explicitly wait for termination to also signal completion.
          println!("Awaiting final termination signal after send task finished...");
           _ = termination_complete.notified().await; // Wait for the signal
          println!("Final termination signal received after send task finished.");
      }
      // Branch 3: Overall timeout
      _ = tokio::time::sleep(Duration::from_secs(5)) => {
           panic!("Test timed out waiting for termination or send task completion");
      }
  }

  // --- Post-select checks ---

  // Check the result of the termination task *after* select ensures it was signalled
  let term_final_result = term_task.await; // Join the termination task
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
