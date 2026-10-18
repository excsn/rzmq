// tests/push_pull.rs

use rzmq::socket::options::{RCVHWM, SNDHWM, SNDTIMEO};
use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use std::time::Duration;
mod common; // Import common helpers

const SHORT_TIMEOUT: Duration = Duration::from_millis(250);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);
const SEND_PEER_WAIT_TIMEOUT: Duration = Duration::from_millis(150);

// --- TCP Tests ---

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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
#[tokio::test]
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
#[tokio::test]
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
#[tokio::test]
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

const HWM_PAYLOAD_LEN: usize = 64 * 1024;
const HWM_MAX_SENDS: u64 = 10_000;

fn hwm_numbered(n: u64) -> Msg {
  let mut payload = vec![0u8; HWM_PAYLOAD_LEN];
  payload[..8].copy_from_slice(&n.to_be_bytes());
  Msg::from_vec(payload)
}

fn hwm_seq(msg: &Msg) -> u64 {
  u64::from_be_bytes(msg.data().expect("payload")[..8].try_into().expect("sequence prefix"))
}

#[tokio::test]
async fn test_push_pull_hwm_sndtimeo_0() -> Result<(), ZmqError> {
  println!("Starting test_push_pull_hwm_sndtimeo_0...");
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  let endpoint = "tcp://127.0.0.1:5630"; // Unique port

  println!("Setting HWM=1 on PUSH and PULL, SNDTIMEO=0 on PUSH...");
  push.set_option_raw(SNDHWM, &(1i32).to_ne_bytes()).await?;
  pull.set_option_raw(RCVHWM, &(1i32).to_ne_bytes()).await?;
  push.set_option_raw(SNDTIMEO, &(0i32).to_ne_bytes()).await?;
  println!("Options set.");

  println!("Binding PULL to {}...", endpoint);
  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Connecting PUSH to {}...", endpoint);
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // The pipe holds one message, but whatever drains it moves messages on into the
  // socket buffers, so how many sends fit depends on drain timing. Large payloads
  // fill those buffers within a bounded number of sends.
  println!("PUSH sending until a send is refused...");
  let mut accepted: u64 = 0;
  loop {
    match push.send(hwm_numbered(accepted)).await {
      Ok(()) => {
        accepted += 1;
        assert!(accepted < HWM_MAX_SENDS, "PUSH accepted {} sends without refusing one", accepted);
      }
      Err(ZmqError::ResourceLimitReached) => break,
      Err(e) => return Err(e),
    }
  }
  assert!(accepted >= 1, "the first send should be accepted");
  println!("PUSH refused send {} with ResourceLimitReached.", accepted);

  for n in 0..accepted {
    let msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    assert_eq!(hwm_seq(&msg), n, "messages arrive in send order");
  }
  println!("PULL received all {} accepted messages.", accepted);

  // Allow time for buffer to clear
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("PUSH sending the refused message again (should succeed)...");
  push.send(hwm_numbered(accepted)).await?;
  let msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(hwm_seq(&msg), accepted);
  println!("PULL received the resent message.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_push_pull_hwm_sndtimeo_0 finished.");
  Ok(())
}

// --- Test: PUSH times out with SNDTIMEO > 0 when no peer is available ---
#[tokio::test]
async fn test_push_sndtimeo_positive_no_peer() -> Result<(), ZmqError> {
  println!("Starting test_push_sndtimeo_positive_no_peer...");
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  // Intentionally NO PULL socket created or bound initially

  let endpoint = "tcp://127.0.0.1:5631"; // Use the same port, doesn't matter if unused initially

  // Set a short positive send timeout on PUSH
  let snd_timeout_val = SEND_PEER_WAIT_TIMEOUT.as_millis() as i32;
  println!("Setting SNDTIMEO={}ms on PUSH...", snd_timeout_val);
  push.set_option_raw(SNDTIMEO, &snd_timeout_val.to_ne_bytes()).await?;
  println!("Option set.");

  // The connect pipe holds one message; the next send has nowhere to go.
  push.set_option_raw(SNDHWM, &1i32.to_ne_bytes()).await?;
  println!("Connecting PUSH to {} (no listener)...", endpoint);
  push.connect(endpoint).await?;
  println!("PUSH connect call returned Ok.");

  tokio::time::sleep(Duration::from_millis(50)).await;

  push.send(Msg::from_static(b"Message 0")).await?;
  println!("PUSH sending Message 1 (should timeout: pipe full, no peer)...");
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

  // The queued Message 0 arrives first, then Message 2; Message 1 was never accepted.
  let rec0 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec0.data().unwrap(), b"Message 0");
  let rec2 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec2.data().unwrap(), b"Message 2");
  let rec1_result = common::recv_timeout(&pull, SHORT_TIMEOUT).await;
  assert!(
    matches!(rec1_result, Err(ZmqError::Timeout)),
    "Expected Timeout error for Message 1, got {:?}",
    rec1_result
  );

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_push_sndtimeo_positive_no_peer finished.");
  Ok(())
}

// --- Test: PULL RCVTIMEO ---
#[tokio::test]
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
    .set_option_raw(rzmq::socket::options::RCVTIMEO, &rcv_timeout_ms.to_ne_bytes())
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
