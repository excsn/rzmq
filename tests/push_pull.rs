// tests/push_pull.rs

use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use std::time::Duration;
mod common; // Import common helpers

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);

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

// --- Tests for HWM / Timeouts (TODO) ---

// #[tokio::test]
// async fn test_push_pull_hwm_sndtimeo_0() -> Result<(), ZmqError> {
//   // Set SNDHWM=1 on PUSH, RCVHWM=1 on PULL
//   // Set SNDTIMEO=0 on PUSH
//   // Send msg1 (should succeed)
//   // Send msg2 (should fail immediately with ResourceLimitReached)
//   // Recv msg1 on PULL
//   // Send msg2 again (should succeed now)
//   // Recv msg2 on PULL
//   todo!();
// }

// #[tokio::test]
// async fn test_pull_rcvtimeo() -> Result<(), ZmqError> {
//   // Set RCVTIMEO=100 on PULL
//   // Bind PULL, Connect PUSH
//   // Attempt recv on PULL (should fail with Timeout)
//   // Send message from PUSH
//   // Attempt recv on PULL (should succeed)
//   todo!();
// }

// --- Test Disconnect/Unbind (TODO) ---
