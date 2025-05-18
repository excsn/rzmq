// tests/push_pull.rs

use rzmq::socket::options::{RCVHWM, SNDHWM, SNDTIMEO};
use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use std::time::Duration;
mod common; // Import common helpers

const SHORT_TIMEOUT: Duration = Duration::from_millis(250);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);
const SEND_PEER_WAIT_TIMEOUT: Duration = Duration::from_millis(150);

// --- TCP Tests ---

#[rzmq::main]
async fn test_push_pull_tcp_basic_messaging() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  let endpoint = "tcp://127.0.0.1:5555"; // Use a fixed port for simplicity

  pull.bind(endpoint).await?;
  // Give bind a moment
  tokio::time::sleep(Duration::from_millis(50)).await;

  push.connect(endpoint).await?;
  // Give connect/handshake a moment
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Send a message
  let msg_data = b"Hello PULL from PUSH";
  push.send(Msg::from_static(msg_data)).await?;

  // Receive the message
  let received_msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;

  assert_eq!(received_msg.data().unwrap(), msg_data);
  ctx.term().await?;

  Ok(())
}

#[rzmq::main]
async fn test_push_pull_tcp_multiple_messages() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;
  let endpoint = "tcp://127.0.0.1:5556";

  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  let count = 5;
  for i in 0..count {
    let msg = Msg::from_vec(format!("Message {}", i).into_bytes());
    push.send(msg).await?;
  }

  for i in 0..count {
    let expected_data = format!("Message {}", i).into_bytes();
    let received_msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    assert_eq!(received_msg.data().unwrap(), expected_data.as_slice());
  }

  // Check no more messages immediately available
  let result = common::recv_timeout(&pull, SHORT_TIMEOUT).await;
  assert!(matches!(result, Err(ZmqError::Timeout)));

  ctx.term().await?;

  Ok(())
}

#[rzmq::main]
async fn test_push_pull_tcp_connect_before_bind() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;
  let endpoint = "tcp://127.0.0.1:5557";

  // Connect first
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect attempt

  // Bind later
  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await; // Allow bind & connection establishment

  // Send/Recv should still work
  let msg_data = b"Late bind";
  push.send(Msg::from_static(msg_data)).await?;
  let received_msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(received_msg.data().unwrap(), msg_data);

  Ok(())
}

// --- IPC Tests (Feature Gated) ---

#[cfg(feature = "ipc")]
#[rzmq::main]
async fn test_push_pull_ipc_basic_messaging() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  let endpoint = common::unique_ipc_endpoint();
  println!("Using IPC endpoint: {}", endpoint); // Useful for debugging permissions etc.

  println!("ITS ALL OVER1");
  pull.bind(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  println!("ITS ALL OVER2");
  push.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;
  println!("ITS ALL OVER3");

  let msg_data = b"Hello IPC PULL from PUSH";
  println!("ITS ALL OVER4");
  push.send(Msg::from_static(msg_data)).await?;
  println!("ITS ALL OVER5");
  let received_msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(received_msg.data().unwrap(), msg_data);

  // Cleanup is handled by IpcListener Drop

  Ok(())
}

// --- Inproc Tests (Feature Gated) ---

#[cfg(feature = "inproc")]
#[rzmq::main]
async fn test_push_pull_inproc_basic_messaging() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  let endpoint = common::unique_inproc_endpoint();

  // Order matters more for inproc usually (bind first)
  pull.bind(&endpoint).await?;
  // No sleep needed for inproc usually, happens via context registry
  push.connect(&endpoint).await?;
  // Give a tiny moment for pipe attachment maybe?
  tokio::time::sleep(Duration::from_millis(10)).await;

  let msg_data = b"Hello Inproc PULL from PUSH";
  push.send(Msg::from_static(msg_data)).await?;
  let received_msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(received_msg.data().unwrap(), msg_data);

  Ok(())
}

#[cfg(feature = "inproc")]
#[rzmq::main]
async fn test_push_pull_inproc_multiple_clients() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  let endpoint = common::unique_inproc_endpoint();

  pull.bind(&endpoint).await?;

  let push1 = ctx.socket(SocketType::Push)?;
  let push2 = ctx.socket(SocketType::Push)?;

  push1.connect(&endpoint).await?;
  push2.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(20)).await; // Allow connections

  push1.send(Msg::from_static(b"From Push 1")).await?;
  push2.send(Msg::from_static(b"From Push 2")).await?;

  let mut received = std::collections::HashSet::new();
  received.insert(
    common::recv_timeout(&pull, LONG_TIMEOUT)
      .await?
      .data()
      .unwrap()
      .to_vec(),
  );
  received.insert(
    common::recv_timeout(&pull, LONG_TIMEOUT)
      .await?
      .data()
      .unwrap()
      .to_vec(),
  );

  assert!(received.contains("From Push 1".as_bytes()));
  assert!(received.contains("From Push 2".as_bytes()));
  assert_eq!(received.len(), 2);

  Ok(())
}

// --- Test: PUSH HWM and SNDTIMEO=0 ---

// Keep the SNDTIMEO=0 test as it works correctly
#[rzmq::main]
async fn test_push_pull_hwm_sndtimeo_0() -> Result<(), ZmqError> {
  println!("Starting test_push_pull_hwm_sndtimeo_0...");
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  let endpoint = "tcp://127.0.0.1:5630"; // Unique port

  println!("Setting HWM=1 on PUSH and PULL, SNDTIMEO=0 on PUSH...");
  push.set_option(SNDHWM, &(1i32).to_ne_bytes()).await?;
  pull.set_option(RCVHWM, &(1i32).to_ne_bytes()).await?;
  push.set_option(SNDTIMEO, &(0i32).to_ne_bytes()).await?;
  println!("Options set.");

  println!("Binding PULL to {}...", endpoint);
  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Connecting PUSH to {}...", endpoint);
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // Send msg1 - should succeed
  println!("PUSH sending Message 1 (should succeed)...");
  push.send(Msg::from_static(b"Message 1")).await?;
  println!("PUSH sent Message 1.");

  // Send msg2 - should fail immediately as HWM=1 on the Core->Session pipe is full
  // because PULL hasn't called recv() yet.
  println!("PUSH sending Message 2 (should fail EAGAIN)...");
  let send2_result = push.send(Msg::from_static(b"Message 2")).await;
  assert!(
    matches!(send2_result, Err(ZmqError::ResourceLimitReached)),
    "Expected ResourceLimitReached error, got {:?}",
    send2_result
  );
  println!("PUSH correctly failed with ResourceLimitReached.");

  // PULL receives msg1, freeing up space in the pipe/PUSH buffer
  println!("PULL receiving Message 1...");
  let rec1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec1.data().unwrap(), b"Message 1");
  println!("PULL received Message 1.");

  // Allow time for buffer to clear
  tokio::time::sleep(Duration::from_millis(50)).await;

  // PUSH sends msg2 again - should succeed now
  println!("PUSH sending Message 2 again (should succeed)...");
  push.send(Msg::from_static(b"Message 2")).await?;
  println!("PUSH sent Message 2 again.");

  // PULL receives msg2
  println!("PULL receiving Message 2...");
  let rec2 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec2.data().unwrap(), b"Message 2");
  println!("PULL received Message 2.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_push_pull_hwm_sndtimeo_0 finished.");
  Ok(())
}

// --- Test: PUSH times out with SNDTIMEO > 0 when no peer is available ---
#[rzmq::main]
async fn test_push_sndtimeo_positive_no_peer() -> Result<(), ZmqError> {
  println!("Starting test_push_sndtimeo_positive_no_peer...");
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  // Intentionally NO PULL socket created or bound initially

  let endpoint = "tcp://127.0.0.1:5631"; // Use the same port, doesn't matter if unused initially

  // Set a short positive send timeout on PUSH
  let snd_timeout_val = SEND_PEER_WAIT_TIMEOUT.as_millis() as i32;
  println!("Setting SNDTIMEO={}ms on PUSH...", snd_timeout_val);
  push.set_option(SNDTIMEO, &snd_timeout_val.to_ne_bytes()).await?;
  println!("Option set.");

  // Connect PUSH - this setup succeeds asynchronously, but no listener exists yet
  println!("Connecting PUSH to {} (no listener)...", endpoint);
  push.connect(endpoint).await?;
  println!("PUSH connect call returned Ok.");

  // Allow some time for potential background tasks (though none should connect)
  tokio::time::sleep(Duration::from_millis(50)).await;

  // Attempt to send msg1 - PUSH has no peers in its LoadBalancer.
  // It should call load_balancer.wait_for_pipe() which blocks.
  // The outer tokio::time::timeout in PushSocket::send should expire.
  println!("PUSH sending Message 1 (should timeout waiting for peer)...");
  let start_time = std::time::Instant::now();
  let send1_result = push.send(Msg::from_static(b"Message 1")).await;
  let elapsed = start_time.elapsed();
  println!("PUSH send result: {:?}, elapsed: {:?}", send1_result, elapsed);

  assert!(
    matches!(send1_result, Err(ZmqError::Timeout)), // Expect Timeout because SNDTIMEO > 0 and no peer available
    "Expected Timeout error, got {:?}",
    send1_result
  );
  // Optional: Check if elapsed time is roughly the timeout duration
  assert!(
    elapsed >= SEND_PEER_WAIT_TIMEOUT && elapsed < SEND_PEER_WAIT_TIMEOUT + Duration::from_millis(100),
    "Elapsed time {:?} not close to timeout {:?}",
    elapsed,
    SEND_PEER_WAIT_TIMEOUT
  );
  println!(
    "PUSH correctly failed with Timeout after ~{}ms.",
    SEND_PEER_WAIT_TIMEOUT.as_millis()
  );

  // Bind a PULL socket *after* the PUSH send timed out
  println!("Binding PULL socket...");
  let pull = ctx.socket(SocketType::Pull)?;
  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await; // Allow bind and connection

  // Send msg2 - should succeed now as a peer is available
  println!("PUSH sending Message 2 (should succeed)...");
  push.send(Msg::from_static(b"Message 2")).await?;
  println!("PUSH sent Message 2.");

  // PULL receives msg2
  println!("PULL receiving Message 2...");
  let rec2 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec2.data().unwrap(), b"Message 2");
  println!("PULL received Message 2.");

  // PULL attempts recv again - should timeout as msg1 was never successfully sent
  println!("PULL attempting recv again (should timeout)...");
  let rec1_result = common::recv_timeout(&pull, SHORT_TIMEOUT).await;
  assert!(
    matches!(rec1_result, Err(ZmqError::Timeout)),
    "Expected Timeout error for Message 1, got {:?}",
    rec1_result
  );
  println!("PULL correctly timed out waiting for Message 1.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_push_sndtimeo_positive_no_peer finished.");
  Ok(())
}

// --- Test: PULL RCVTIMEO ---
#[rzmq::main]
async fn test_pull_rcvtimeo() -> Result<(), ZmqError> {
  println!("Starting test_pull_rcvtimeo...");
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  let endpoint = "tcp://127.0.0.1:5632"; // Unique port

  // Set short RCVTIMEO on PULL
  let rcv_timeout_ms = 150i32;
  println!("Setting RCVTIMEO={}ms on PULL...", rcv_timeout_ms);
  pull
    .set_option(rzmq::socket::options::RCVTIMEO, &rcv_timeout_ms.to_ne_bytes())
    .await?;
  println!("Option set.");

  println!("Binding PULL to {}...", endpoint);
  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Connecting PUSH to {}...", endpoint);
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // PULL attempts recv first - should fail with Timeout
  println!("PULL attempting recv (should timeout)...");
  let recv1_result = pull.recv().await; // Use direct recv to test timeout option
  assert!(
    matches!(recv1_result, Err(ZmqError::Timeout)),
    "Expected Timeout error on first recv, got {:?}",
    recv1_result
  );
  println!("PULL correctly timed out on first recv.");

  // PUSH sends a message
  println!("PUSH sending message...");
  push.send(Msg::from_static(b"Timed Message")).await?;
  println!("PUSH sent message.");

  // PULL attempts recv again - should succeed now
  println!("PULL attempting recv again (should succeed)...");
  let recv2_result = pull.recv().await; // Use direct recv again
  match recv2_result {
    Ok(msg) => {
      assert_eq!(msg.data().unwrap(), b"Timed Message");
      println!("PULL received message successfully.");
    }
    Err(e) => {
      return Err(e); // Fail test on unexpected error
    }
  }

  // PULL attempts recv again - should timeout again
  println!("PULL attempting recv again (should timeout)...");
  let recv3_result = pull.recv().await;
  assert!(
    matches!(recv3_result, Err(ZmqError::Timeout)),
    "Expected Timeout error on third recv, got {:?}",
    recv3_result
  );
  println!("PULL correctly timed out on third recv.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_pull_rcvtimeo finished.");
  Ok(())
}
