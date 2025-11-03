mod common;

use anyhow::Result;
use rzmq::socket::SUBSCRIBE;
use rzmq::{Msg, SocketType, context::context};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct ChildProcessGuard {
  child: Child,
}
impl Drop for ChildProcessGuard {
  fn drop(&mut self) {
    let _ = self.child.kill();
    let _ = self.child.wait();
  }
}

#[tokio::test]
async fn test_pub_sub_with_pyzmq() -> Result<()> {
  common::setup_logging();
  let endpoint = "tcp://127.0.0.1:5573"; // New port

  let mut cmd = Command::new("python3")
    .arg("python_scripts/pub_server.py")
    .arg(endpoint)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;

  let stdout = cmd.stdout.take().expect("Failed to open stdout");
  let _guard = ChildProcessGuard { child: cmd };

  let mut reader = BufReader::new(stdout);
  let mut line = String::new();
  let mut is_ready = false;

  let start = std::time::Instant::now();
  while start.elapsed() < Duration::from_secs(5) {
    if reader.read_line(&mut line)? == 0 {
      break;
    }
    println!("[py-stdout] {}", line.trim());
    if line.contains("READY") {
      is_ready = true;
      break;
    }
    line.clear();
  }
  anyhow::ensure!(is_ready, "Python PUB server did not signal READY.");

  let ctx = context()?;
  let sub_socket = ctx.socket(SocketType::Sub)?;
  sub_socket.connect(endpoint).await?;

  // Subscribe to "TOPIC_A"
  tracing::info!("Subscribing to 'TOPIC_A'");
  sub_socket
    .set_option(SUBSCRIBE, "TOPIC_A".as_bytes())
    .await?;

  // VERY IMPORTANT: Give the subscription message time to propagate to the publisher.
  // This is a classic ZMQ "slow joiner" consideration.
  tokio::time::sleep(Duration::from_millis(100)).await;

  // We expect two messages on TOPIC_A
  // 1. The data message
  let msg1_parts = sub_socket.recv_multipart().await?;
  assert_eq!(msg1_parts.len(), 2);
  assert_eq!(msg1_parts[0].data().unwrap(), b"TOPIC_A");
  assert_eq!(msg1_parts[1].data().unwrap(), b"hello from topic A");
  tracing::info!("Correctly received message on TOPIC_A");

  // 2. The shutdown message
  let msg2_parts = sub_socket.recv_multipart().await?;
  assert_eq!(msg2_parts.len(), 2);
  assert_eq!(msg2_parts[0].data().unwrap(), b"TOPIC_A");
  assert_eq!(msg2_parts[1].data().unwrap(), b"shutdown");
  tracing::info!("Correctly received shutdown message");

  // We can add a small timeout here to prove we DON'T receive the TOPIC_B message.
  let res = tokio::time::timeout(Duration::from_millis(200), sub_socket.recv()).await;
  assert!(
    res.is_err(),
    "Should not have received any more messages (especially not from TOPIC_B)"
  );

  Ok(())
}
