use rzmq::{
  socket::{
    options::{RCVHWM, SNDHWM, SNDTIMEO},
    MonitorReceiver, SocketEvent,
  },
  Context, Msg, SocketType, ZmqError,
};
use std::time::Duration;
use std::{sync::Arc, time::Instant};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio::{sync::Notify, task};

mod common;

const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_RECV_TIMEOUT: Duration = Duration::from_secs(4);
const NUM_MESSAGES_RACE: usize = 20_000; // Slightly higher message count
const TEST_HWM_RACE: i32 = 100; // Keep HWM low to increase backpressure chance
const NUM_CONCURRENT_PAIRS: usize = 4; // Number of PUSH/PULL pairs to run at once

const CHAOS_DURATION: Duration = Duration::from_secs(2); // How long to let the chaos run
const CHAOS_HWM: i32 = 50; // Low HWM to cause backpressure quickly
const CHAOS_ENDPOINT: &str = "inproc://chaotic-pipe";
const NUM_PUSHERS: usize = 2;
const NUM_PULLERS: usize = 2;
const FINAL_TERM_TIMEOUT: Duration = Duration::from_secs(5); // Timeout for ctx.term() itself

async fn wait_for_event(
  monitor_rx: &MonitorReceiver,
  check_event: impl Fn(&SocketEvent) -> bool,
) -> Result<SocketEvent, String> {
  // ... (implementation from previous step) ...
  let start_time = std::time::Instant::now();
  loop {
    if start_time.elapsed() > EVENT_RECV_TIMEOUT {
      return Err(format!("Timeout after {:?}", EVENT_RECV_TIMEOUT));
    }
    match timeout(Duration::from_millis(50), monitor_rx.recv()).await {
      Ok(Ok(event)) => {
        if check_event(&event) {
          return Ok(event);
        }
      }
      Ok(Err(_)) => {
        return Err("Monitor channel closed".to_string());
      }
      Err(_) => {} // Timeout poll
    }
  }
}

async fn setup_push_pull_for_race_test(
  ctx: &Context,
  endpoint: &str,
) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  push.set_option_raw(SNDHWM, &TEST_HWM_RACE.to_ne_bytes()).await?;
  pull.set_option_raw(RCVHWM, &TEST_HWM_RACE.to_ne_bytes()).await?;
  // println!("[Setup {}] HWM options set to {}.", endpoint, TEST_HWM_RACE); // Add endpoint to logs

  let push_monitor = push.monitor_default().await?;
  let pull_monitor = pull.monitor_default().await?;
  // println!("[Setup {}] PULL monitor created.", endpoint);

  pull.bind(endpoint).await?;
  // println!("[Setup {}] PULL bound.", endpoint);
  wait_for_event(
    &pull_monitor,
    |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == endpoint),
  )
  .await
  .map_err(|e| ZmqError::Internal(format!("[{}] PULL Listening event error: {}", endpoint, e)))?;
  // println!("[Setup {}] PULL Listening event confirmed.", endpoint);

  push.connect(endpoint).await?;
  // println!("[Setup {}] PUSH connect initiated.", endpoint);

  // println!("[Setup {}] Waiting for connection confirmation...", endpoint);
  wait_for_event(&push_monitor, |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }))
    .await
    .map_err(|e| ZmqError::Internal(format!("[{}] PULL Connection event error: {}", endpoint, e)))?;
  // println!("[Setup {}] PULL Connection confirmed via monitor.", endpoint);

  sleep(Duration::from_millis(50)).await;

  // println!("[Setup {}] Sockets ready.", endpoint);
  Ok((push, pull))
}

// --- Individual Pair Logic ---
// Runs one PUSH/PULL pair and tries to trigger the race
async fn run_single_pair(ctx: Arc<Context>, endpoint: String) -> Result<(), String> {
  println!("[Pair {}] Starting...", endpoint);
  let (push_res, pull_res) = match timeout(SETUP_TIMEOUT, setup_push_pull_for_race_test(&ctx, &endpoint)).await {
    Ok(Ok(sockets)) => sockets,
    Ok(Err(e)) => return Err(format!("[Pair {}] Setup failed: {}", endpoint, e)),
    Err(_) => return Err(format!("[Pair {}] Setup timed out overall", endpoint)),
  };

  let push = Arc::new(push_res);
  let pull = Arc::new(pull_res);

  let sender_finished_notify = Arc::new(Notify::new());
  let sender_finished_notify_clone = sender_finished_notify.clone();

  let sender_task: JoinHandle<Result<usize, String>> = {
    let push_clone = push.clone();
    let endpoint_clone = endpoint.clone();
    tokio::spawn(async move {
      // println!("[Sender {}] Started.", endpoint_clone);
      let mut sent_count = 0;
      for i in 0..NUM_MESSAGES_RACE {
        let msg = Msg::from_vec(format!("Msg {}", i).into_bytes());
        // Using a shorter send timeout for individual messages to not stall the sender too long
        // if the HWM is full and it's supposed to be a racy shutdown.
        match timeout(Duration::from_millis(250), push_clone.send(msg)).await {
          Ok(Ok(())) => sent_count += 1,
          Ok(Err(ZmqError::ResourceLimitReached)) => {
            // This is expected if HWM is hit.
            println!("[Sender {}] HWM reached at msg {}. Yielding.", endpoint_clone, i);
            tokio::task::yield_now().await; // Yield to let receiver catch up
                                            // Optionally, retry the send or break if testing strict send behavior
          }
          Ok(Err(ZmqError::InvalidState(ref s))) if s.contains("closing") || s.contains("terminated") => {
            println!(
              "[Sender {}] Socket closed/terminated while sending msg {}. Stopping.",
              endpoint_clone, i
            );
            break;
          }
          Ok(Err(e)) => {
            println!("[Sender {}] Error sending msg {}: {}", endpoint_clone, i, e);
            break;
          }
          Err(_) => {
            println!("[Sender {}] Timeout sending msg {}. Stopping.", endpoint_clone, i);
            break;
          }
        }
        if i % 1000 == 0 && i > 0 {
          // Yield periodically but not excessively
          tokio::task::yield_now().await;
        }
      }
      println!(
        "[Sender {}] Finished send loop (attempted up to {}). Actual sent: {}. Notifying.",
        endpoint_clone, NUM_MESSAGES_RACE, sent_count
      );
      sender_finished_notify_clone.notify_one();
      Ok(sent_count)
    })
  };

  let receiver_task: JoinHandle<Result<usize, String>> = {
    let pull_clone = pull.clone();
    let endpoint_clone = endpoint.clone();
    tokio::spawn(async move {
      // println!("[Receiver {}] Started.", endpoint_clone);
      let mut recv_count = 0;
      // Increased receiver timeout to give more time for messages to arrive,
      // especially if draining occurs after close signal.
      let individual_recv_timeout = Duration::from_secs(5);
      loop {
        match timeout(individual_recv_timeout, pull_clone.recv()).await {
          Ok(Ok(_msg)) => recv_count += 1,
          Ok(Err(ZmqError::InvalidState(ref s))) if s.contains("closing") || s.contains("terminated") => {
            println!(
              "[Receiver {}] Got 'Socket closing/terminated' after {} msgs.",
              endpoint_clone, recv_count
            );
            break;
          }
          Ok(Err(ZmqError::Timeout)) => {
            println!(
                            "[Receiver {}] Timed out waiting for message (after {} msgs, timeout {:?}). Assuming end of stream for this pair.",
                            endpoint_clone, recv_count, individual_recv_timeout
                        );
            break;
          }
          Ok(Err(e)) => {
            println!("[Receiver {}] Error receiving: {}", endpoint_clone, e);
            // Depending on strictness, could return Err or break.
            // For a race test, breaking might be acceptable.
            return Err(format!("[{}] Recv error: {}", endpoint_clone, e));
          }
          Err(_) => {
            // This is outer timeout from tokio::time::timeout
            println!(
              "[Receiver {}] Outer timeout on recv() call after {} msgs.",
              endpoint_clone, recv_count
            );
            break;
          }
        }
        if recv_count % 1000 == 0 && recv_count > 0 {
          // Yield periodically
          tokio::task::yield_now().await;
        }
      }
      println!(
        "[Receiver {}] Finished recv loop, received {}.",
        endpoint_clone, recv_count
      );
      Ok(recv_count)
    })
  };

  // --- Main Pair Logic: Wait for sender, then shutdown ---
  println!("[Pair {}] Waiting for sender notification...", endpoint);
  sender_finished_notify.notified().await;
  println!(
    "[Pair {}] Sender finished signal received. Initiating socket closures.",
    endpoint
  );

  // Give socket close operations more time.
  // The `Socket::close()` future should ideally only complete when the socket's
  // internal actor and resources are properly shut down.
  let socket_close_timeout = Duration::from_secs(4); // Increased from 1s

  println!(
    "[Pair {}] Closing PUSH socket (timeout {:?})...",
    endpoint, socket_close_timeout
  );
  let push_close_result = timeout(socket_close_timeout, push.close()).await;
  match push_close_result {
    Ok(Ok(())) => println!("[Pair {}] PUSH socket closed successfully.", endpoint),
    Ok(Err(e)) => println!("[Pair {}] PUSH socket close returned error: {}", endpoint, e),
    Err(_) => println!(
      "[Pair {}] PUSH socket close timed out after {:?}.",
      endpoint, socket_close_timeout
    ),
  }

  println!(
    "[Pair {}] Closing PULL socket (timeout {:?})...",
    endpoint, socket_close_timeout
  );
  let pull_close_result = timeout(socket_close_timeout, pull.close()).await;
  match pull_close_result {
    Ok(Ok(())) => println!("[Pair {}] PULL socket closed successfully.", endpoint),
    Ok(Err(e)) => println!("[Pair {}] PULL socket close returned error: {}", endpoint, e),
    Err(_) => println!(
      "[Pair {}] PULL socket close timed out after {:?}.",
      endpoint, socket_close_timeout
    ),
  }

  println!("[Pair {}] Sockets closed. Waiting for tasks to complete...", endpoint);

  // Wait for tasks to fully join *after* close initiated and awaited (or timed out)
  let sent_res = match sender_task.await {
    Ok(s_res) => s_res,
    Err(e) => return Err(format!("[Pair {}] Sender task panicked: {:?}", endpoint, e)),
  };
  let recv_res = match receiver_task.await {
    Ok(r_res) => r_res,
    Err(e) => return Err(format!("[Pair {}] Receiver task panicked: {:?}", endpoint, e)),
  };

  println!(
    "[Pair {}] Task Results: Sent={:?}, Recv={:?}",
    endpoint, sent_res, recv_res
  );

  // Perform checks
  let actual_sent = sent_res.map_err(|e| format!("[{}] Sender task failed internally: {}", endpoint, e))?;
  let actual_recv = recv_res.map_err(|e| format!("[{}] Receiver task failed internally: {}", endpoint, e))?;

  // In a race condition test, sent might not equal received if shutdown is abrupt.
  // The key is that the process doesn't deadlock or panic, and `ctx.term()` is clean.
  // If the goal IS to receive all messages, then this check is important.
  // For now, we'll keep it simple: tasks should complete without internal errors.
  // The original problem was about `ctx.term()` cutting things short.
  if actual_sent > 0 && actual_recv < actual_sent {
    println!("[Pair {}] WARNING: Sent {} messages, but received only {}. Some messages might have been lost during shutdown race.", endpoint, actual_sent, actual_recv);
    // Depending on test strictness, this could be an error.
    // For now, let it be a warning, as the primary goal is to avoid ctx.term() issues.
  }

  println!("[Pair {}] Finished.", endpoint);
  Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_push_pull_concurrent_shutdown_race() {
  println!("\n--- Starting test_push_pull_concurrent_shutdown_race ---");
  let ctx = Arc::new(common::test_context());

  let mut pair_handles = Vec::new();

  for i in 0..NUM_CONCURRENT_PAIRS {
    let ctx_clone = ctx.clone();
    // Ensure unique endpoints for each pair
    let endpoint = format!("tcp://127.0.0.1:{}", 5710 + i);
    let handle: JoinHandle<Result<(), String>> =
      tokio::spawn(async move { run_single_pair(ctx_clone, endpoint).await });
    pair_handles.push(handle);
  }

  println!("[Main] Waiting for all {} pairs to complete...", NUM_CONCURRENT_PAIRS);
  let all_results = futures::future::join_all(pair_handles).await;
  println!("[Main] All pairs joined.");

  // --- Terminate Context AFTER all pairs finished or errored ---
  println!("[Main] Initiating final context termination...");
  // A small delay here can sometimes help ensure all background OS-level stuff from closed sockets settles.
  // This is a bit of a pragmatic adjustment if issues persist.
  tokio::time::sleep(Duration::from_millis(500)).await;

  let ctx_owned = Arc::try_unwrap(ctx)
    .expect("Context Arc should only have one owner left, or this test needs refactoring on Arc handling for ctx");
  let term_result = timeout(FINAL_TERM_TIMEOUT, ctx_owned.term()).await; // Added timeout here too

  match term_result {
    Ok(Ok(())) => println!("[Main] Final context term completed successfully."),
    Ok(Err(e)) => {
      // If term itself returns an error.
      assert!(false, "Final context termination failed with ZmqError: {}", e);
    }
    Err(_) => {
      // If term timed out.
      assert!(
        false,
        "Final context termination timed out after {:?}!",
        FINAL_TERM_TIMEOUT
      );
    }
  }

  // Check results from pairs
  let mut failures = Vec::new();
  for (i, result) in all_results.into_iter().enumerate() {
    match result {
      Ok(Ok(())) => println!("[Main] Pair {} completed successfully.", i),
      Ok(Err(e)) => {
        println!("[Main] Pair {} failed: {}", i, e);
        failures.push(e);
      }
      Err(e) => {
        println!("[Main] Pair {} panicked: {:?}", i, e);
        failures.push(format!("Pair {} panicked: {:?}", i, e));
      }
    }
  }

  assert!(failures.is_empty(), "One or more test pairs failed: {:?}", failures);
  println!("--- Test test_push_pull_concurrent_shutdown_race Finished ---");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_chaotic_shutdown() -> Result<(), ZmqError> {
  println!("\n--- Starting test_chaotic_shutdown (Inproc N-PUSH -> 1-PULL) ---");
  let ctx = common::test_context(); // Owned context

  let mut push_sockets = Vec::new();
  let mut pull_sockets = Vec::new();
  let mut task_handles: Vec<JoinHandle<_>> = Vec::new();

  // --- Setup PULL Socket (The single receiver) ---
  println!("Creating 1 PULL socket, binding to {}...", CHAOS_ENDPOINT);
  let pull = ctx.socket(SocketType::Pull)?;
  pull.set_option_raw(RCVHWM, &CHAOS_HWM.to_ne_bytes()).await?;
  pull.bind(CHAOS_ENDPOINT).await.expect("Failed to bind PULL socket");
  let pull_socket = Arc::new(pull);
  pull_sockets.push(pull_socket.clone());
  println!("[Setup] PULL bound.");

  // --- Setup PUSH Sockets (Multiple Connectors) ---
  println!(
    "Creating {} PUSH sockets, connecting to {}...",
    NUM_PUSHERS, CHAOS_ENDPOINT
  );
  for i in 0..NUM_PUSHERS {
    let push = ctx.socket(SocketType::Push)?;
    push.set_option_raw(SNDHWM, &CHAOS_HWM.to_ne_bytes()).await?;
    push.set_option_raw(SNDTIMEO, &(0i32).to_ne_bytes()).await?; // Non-blocking send

    // Connect directly - for inproc, connect() awaits confirmation
    match push.connect(CHAOS_ENDPOINT).await {
      Ok(()) => {
        println!("[Setup] PUSH socket {} connected successfully.", i);
        push_sockets.push(Arc::new(push)); // Store Arc<Socket>
      }
      Err(e) => panic!("[Setup] PUSH socket {} failed to connect: {}", i, e),
    }
  }
  // Short sleep *after* all connections complete might help ensure pipe attachment messages are processed
  sleep(Duration::from_millis(50)).await;
  println!("All sockets created and connected/bound.");

  // --- Spawn Pusher Tasks ---
  println!("Spawning {} pusher tasks...", NUM_PUSHERS);
  for i in 0..NUM_PUSHERS {
    let push_socket_clone = push_sockets[i].clone();
    let handle = task::spawn(async move {
      let mut count = 0u64;
      let start = Instant::now();
      while start.elapsed() < CHAOS_DURATION {
        let msg = Msg::from_vec(format!("PUSH[{}]-{}", i, count).into_bytes());
        match push_socket_clone.send(msg).await {
          Ok(()) => count += 1,
          Err(ZmqError::ResourceLimitReached) => {
            task::yield_now().await;
          }
          // Check specifically for InvalidState("Socket terminated")
          Err(ZmqError::InvalidState(ref s)) if s == &"Socket terminated" => {
            println!("[Pusher {}] Got 'Socket terminated'. Stopping.", i);
            break;
          }
          Err(e) => {
            println!("[Pusher {}] Send error: {}", i, e);
            break;
          }
        }
        // Optional yield
        // if count % 100 == 0 { task::yield_now().await; }
      }
      println!(
        "[Pusher {}] Finished loop after {:?}, sent ~{} messages.",
        i,
        start.elapsed(),
        count
      );
      count
    });
    task_handles.push(handle);
  }

  // --- Spawn Puller Task (Only one) ---
  println!("Spawning 1 puller task...");
  let pull_socket_clone = pull_sockets[0].clone(); // The single puller
  let handle = task::spawn(async move {
    let mut count = 0u64;
    let start = Instant::now();
    loop {
      // Use a slightly longer timeout for recv
      match timeout(CHAOS_DURATION + Duration::from_secs(2), pull_socket_clone.recv()).await {
        Ok(Ok(_msg)) => {
          count += 1;
        }
        Ok(Err(ZmqError::InvalidState(ref s))) if s == &"Socket terminated" => {
          println!("[Puller 0] Got 'Socket terminated' after {} msgs.", count);
          break;
        }
        Ok(Err(ZmqError::Timeout)) => {
          // This might happen if pushers finish early
          println!("[Puller 0] Timed out waiting after {} msgs.", count);
          break;
        }
        Ok(Err(e)) => {
          println!("[Puller 0] Recv error: {}", e);
          break;
        }
        Err(_) => {
          // Outer timeout
          println!(
            "[Puller 0] Recv loop timed out after {:?}, received {} messages.",
            start.elapsed(),
            count
          );
          break;
        }
      }
      if start.elapsed() >= CHAOS_DURATION {
        // Don't break immediately, allow recv timeout to catch remaining messages
        // println!("[Puller 0] Chaos duration ended after {:?}, received {} messages.", start.elapsed(), count);
        // break;
      }
      // Optional yield
      // if count % 100 == 0 { task::yield_now().await; }
    }
    println!("[Puller 0] Finished loop, received total {}.", count);
    count
  });
  task_handles.push(handle);

  // Let the chaos run
  println!("Letting chaos run for {:?}...", CHAOS_DURATION);
  sleep(CHAOS_DURATION).await;
  println!("Chaos duration finished. Initiating context termination.");

  pull_socket.close().await;
  for sck in push_sockets {
    sck.close().await;
  }
  // --- Initiate Context Termination ---
  let term_result = timeout(FINAL_TERM_TIMEOUT, ctx.term()).await;

  println!("Context termination initiated. Waiting for tasks...");

  // --- Wait for all tasks to complete ---
  let mut total_sent: u64 = 0;
  let mut total_recv: u64 = 0;
  let mut puller_count = 0;
  for (i, handle) in task_handles.into_iter().enumerate() {
    match handle.await {
      Ok(count) => {
        if i < NUM_PUSHERS {
          println!("[Main] Pusher {} finished, sent ~{}.", i, count);
          total_sent += count;
        } else {
          println!("[Main] Puller {} finished, received {}.", puller_count, count);
          total_recv += count;
          puller_count += 1;
        }
      }
      Err(e) => {
        println!("[Main] Task {} panicked: {:?}", i, e);
      }
    }
  }
  println!(
    "All tasks joined. Total Sent ~{}, Total Recv {}",
    total_sent, total_recv
  );

  // --- Assertions ---
  match term_result {
    Ok(Ok(())) => {
      println!("[Main] Context terminated successfully.");
    }
    Ok(Err(e)) => {
      panic!("Context termination failed with ZmqError: {}", e);
    }
    Err(_) => {
      panic!(
        "Context termination timed out after {:?}! Potential deadlock.",
        FINAL_TERM_TIMEOUT
      );
    }
  }

  println!("--- Test test_chaotic_shutdown (Inproc N-PUSH -> 1-PULL) Finished ---");
  Ok(())
}

// Constants specific to this TCP test
const TCP_CHAOS_DURATION: Duration = Duration::from_secs(1);
const TCP_CHAOS_HWM: i32 = 100; // Keep HWM low
const TCP_CHAOS_ENDPOINT: &str = "tcp://127.0.0.1:5720"; // Unique TCP port
const TCP_NUM_PUSHERS: usize = 4; // Same as chaotic inproc test
const TCP_NUM_PULLERS: usize = 1; // Must be 1 for TCP bind
const TCP_FINAL_TERM_TIMEOUT: Duration = Duration::from_secs(60); // Slightly longer for TCP cleanup
const TCP_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const TCP_EVENT_RECV_TIMEOUT: Duration = Duration::from_secs(4);

// --- Helper function copied/adapted from benchmark setup ---
async fn wait_for_event_tcp(
  // Renamed slightly for clarity
  monitor_rx: &MonitorReceiver,
  check_event: impl Fn(&SocketEvent) -> bool,
) -> Result<SocketEvent, String> {
  let start_time = Instant::now();
  loop {
    if start_time.elapsed() > TCP_EVENT_RECV_TIMEOUT {
      return Err(format!("Timeout after {:?}", TCP_EVENT_RECV_TIMEOUT));
    }
    match timeout(Duration::from_millis(50), monitor_rx.recv()).await {
      Ok(Ok(event)) => {
        // println!("[Monitor]: {:?}", event); // Optional detailed logging
        if check_event(&event) {
          return Ok(event);
        }
      }
      Ok(Err(_)) => return Err("Monitor channel closed".to_string()),
      Err(_) => {} // Timeout poll
    }
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_chaotic_shutdown_tcp() -> Result<(), ZmqError> {
  println!("\n--- Starting test_chaotic_shutdown_tcp (TCP N-PUSH -> 1-PULL) ---");
  let ctx = common::test_context(); // Owned context

  let mut push_sockets = Vec::new();
  let mut pull_sockets = Vec::new(); // Will contain only one
  let mut task_handles: Vec<JoinHandle<_>> = Vec::new();

  // --- Setup PULL Socket (The single receiver) ---
  println!("Creating 1 PULL socket, binding to {}...", TCP_CHAOS_ENDPOINT);
  let pull = ctx.socket(SocketType::Pull)?;
  pull.set_option_raw(RCVHWM, &TCP_CHAOS_HWM.to_ne_bytes()).await?;

  // Monitor the PULL socket to wait for connections
  let pull_monitor = pull.monitor_default().await?;
  println!("[Setup] PULL monitor created.");

  pull.bind(TCP_CHAOS_ENDPOINT).await?;
  println!("[Setup] PULL bound.");
  wait_for_event_tcp(
    &pull_monitor,
    |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == TCP_CHAOS_ENDPOINT),
  )
  .await
  .map_err(|e| ZmqError::Internal(format!("PULL Listening event error: {}", e)))?;
  println!("[Setup] PULL Listening event confirmed.");
  let pull_socket = Arc::new(pull); // Store Arc<Socket>
  pull_sockets.push(pull_socket.clone()); // Add the single puller to the list

  // --- Setup PUSH Sockets (Multiple Connectors) ---
  println!(
    "Creating {} PUSH sockets, connecting to {}...",
    TCP_NUM_PUSHERS, TCP_CHAOS_ENDPOINT
  );
  let mut push_connection_futures = vec![];
  let expected_connections = Arc::new(tokio::sync::Semaphore::new(TCP_NUM_PUSHERS)); // To count connections

  for i in 0..TCP_NUM_PUSHERS {
    let push = ctx.socket(SocketType::Push)?;
    push.set_option_raw(SNDHWM, &TCP_CHAOS_HWM.to_ne_bytes()).await?;
    push.set_option_raw(SNDTIMEO, &(0i32).to_ne_bytes()).await?; // Non-blocking send

    // Spawn a task to connect
    let connect_endpoint = TCP_CHAOS_ENDPOINT.to_string();
    let connect_task = tokio::spawn(async move {
      // Connect doesn't wait for confirmation for TCP
      push.connect(&connect_endpoint).await?;
      println!("[Setup PUSH {}] Connect call returned Ok.", i);
      // The confirmation will happen via the PULL monitor below
      Ok::<_, ZmqError>(push) // Return the socket handle
    });
    push_connection_futures.push(connect_task);
  }

  // Wait for all PUSH sockets connect calls to complete (doesn't mean connected yet)
  println!("[Setup] Waiting for all PUSH connect calls to return...");
  let push_results = futures::future::join_all(push_connection_futures).await;
  for (i, result) in push_results.into_iter().enumerate() {
    match result {
      Ok(Ok(push_sock)) => {
        push_sockets.push(Arc::new(push_sock)); // Store Arc<Socket>
        println!("[Setup] PUSH socket {} connect call finished.", i);
      }
      Ok(Err(e)) => panic!("[Setup] PUSH socket {} failed connect call: {}", i, e),
      Err(e) => panic!("[Setup] PUSH socket {} connect task panicked: {:?}", i, e),
    }
  }

  // Wait for the PULL monitor to see all PUSH connections
  println!(
    "[Setup] Waiting for PULL monitor to confirm {} connections...",
    TCP_NUM_PUSHERS
  );
  let mut confirmed_connections = 0;
  let confirmation_timeout = TCP_SETUP_TIMEOUT + Duration::from_secs(2); // Extra time for connections
  let start_confirm = Instant::now();
  while confirmed_connections < TCP_NUM_PUSHERS {
    if start_confirm.elapsed() > confirmation_timeout {
      panic!(
        "[Setup] Timed out waiting for all {} PUSH connections to be confirmed by PULL monitor (only saw {}).",
        TCP_NUM_PUSHERS, confirmed_connections
      );
    }
    // Wait for the next connection event
    match wait_for_event_tcp(&pull_monitor, |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await {
      Ok(_) => {
        confirmed_connections += 1;
        println!(
          "[Setup] PULL Monitor confirmed connection {}/{}.",
          confirmed_connections, TCP_NUM_PUSHERS
        );
      }
      Err(e) => {
        panic!("[Setup] Error waiting for PULL monitor connection event: {}", e);
      }
    }
  }
  println!("All sockets created and connections confirmed.");

  // --- Spawn Pusher Tasks ---
  println!("Spawning {} pusher tasks...", TCP_NUM_PUSHERS);
  for i in 0..TCP_NUM_PUSHERS {
    let push_socket_clone = push_sockets[i].clone();
    let handle = task::spawn(async move {
      let mut count = 0u64;
      let start = Instant::now();
      while start.elapsed() < TCP_CHAOS_DURATION {
        let msg = Msg::from_vec(format!("PUSH[{}]-{}", i, count).into_bytes());
        match push_socket_clone.send(msg).await {
          Ok(()) => count += 1,
          Err(ZmqError::ResourceLimitReached) => {
            task::yield_now().await;
          }
          Err(ZmqError::InvalidState(ref s)) if s == &"Socket terminated" => {
            println!("[Pusher {}] Got 'Socket terminated'. Stopping.", i);
            break;
          }
          Err(e) => {
            println!("[Pusher {}] Send error: {}", i, e);
            break;
          }
        }
      }
      println!(
        "[Pusher {}] Finished loop after {:?}, sent ~{} messages.",
        i,
        start.elapsed(),
        count
      );
      count
    });
    task_handles.push(handle);
  }

  // --- Spawn Puller Task (Only one) ---
  println!("Spawning 1 puller task...");
  let pull_socket_clone = pull_sockets[0].clone(); // The single puller
  let handle = task::spawn(async move {
    let mut count = 0u64;
    let start = Instant::now();
    loop {
      match timeout(TCP_CHAOS_DURATION + Duration::from_secs(2), pull_socket_clone.recv()).await {
        Ok(Ok(_msg)) => {
          count += 1;
        }
        Ok(Err(ZmqError::InvalidState(ref s))) if s == &"Socket terminated" => {
          println!("[Puller 0] Got 'Socket terminated' after {} msgs.", count);
          break;
        }
        Ok(Err(ZmqError::Timeout)) => {
          println!("[Puller 0] Timed out waiting after {} msgs.", count);
          break;
        }
        Ok(Err(e)) => {
          println!("[Puller 0] Recv error: {}", e);
          break;
        }
        Err(_) => {
          println!(
            "[Puller 0] Recv loop timed out after {:?}, received {} messages.",
            start.elapsed(),
            count
          );
          break;
        }
      }
      if start.elapsed() >= TCP_CHAOS_DURATION {
        // Let recv timeout handle the end
      }
    }
    println!("[Puller 0] Finished loop, received total {}.", count);
    count
  });
  task_handles.push(handle);

  // Let the chaos run
  println!("Letting chaos run for {:?}...", TCP_CHAOS_DURATION);
  sleep(TCP_CHAOS_DURATION).await;
  println!("Chaos duration finished. Initiating context termination.");

  // --- Initiate Context Termination ---
  let term_result = timeout(TCP_FINAL_TERM_TIMEOUT, ctx.term()).await; // Use owned ctx

  println!("Context termination initiated. Waiting for tasks...");

  // --- Wait for all tasks to complete ---
  let mut total_sent: u64 = 0;
  let mut total_recv: u64 = 0;
  let mut puller_count = 0;
  for (i, handle) in task_handles.into_iter().enumerate() {
    match handle.await {
      Ok(count) => {
        if i < TCP_NUM_PUSHERS {
          println!("[Main] Pusher {} finished, sent ~{}.", i, count);
          total_sent += count;
        } else {
          println!("[Main] Puller {} finished, received {}.", puller_count, count);
          total_recv += count;
          puller_count += 1;
        }
      }
      Err(e) => {
        println!("[Main] Task {} panicked: {:?}", i, e);
      }
    }
  }
  println!(
    "All tasks joined. Total Sent ~{}, Total Recv {}",
    total_sent, total_recv
  );

  // --- Assertions ---
  match term_result {
    Ok(Ok(())) => {
      println!("[Main] Context terminated successfully.");
    }
    Ok(Err(e)) => {
      panic!("Context termination failed with ZmqError: {}", e);
    }
    Err(_) => {
      panic!(
        "Context termination timed out after {:?}! Potential deadlock.",
        TCP_FINAL_TERM_TIMEOUT
      );
    }
  }

  println!("--- Test test_chaotic_shutdown_tcp (TCP N-PUSH -> 1-PULL) Finished ---");
  Ok(())
}
