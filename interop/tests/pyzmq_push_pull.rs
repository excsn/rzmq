mod common;

use anyhow::Result;
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
async fn test_push_pull_with_pyzmq() -> Result<()> {
  common::setup_logging();
  let endpoint = "tcp://127.0.0.1:5572"; // Use a new port

  let mut cmd = Command::new("python3")
    .arg("python_scripts/push_server.py")
    .arg(endpoint)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;

  let stdout = cmd.stdout.take().expect("Failed to open stdout");
  let _guard = ChildProcessGuard { child: cmd };

  let mut reader = BufReader::new(stdout);
  let mut line = String::new();
  let mut is_ready = false;

  // Wait for the server to be ready
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
  anyhow::ensure!(is_ready, "Python PUSH server did not signal READY.");

  // Setup Rust PULL client
  let ctx = context()?;
  let pull_socket = ctx.socket(SocketType::Pull)?;
  pull_socket.connect(endpoint).await?;

  // Receive the 5 messages
  for i in 0..5 {
    let reply = pull_socket.recv().await?;
    let expected = format!("message-{}", i);
    assert_eq!(reply.data().unwrap(), expected.as_bytes());
    tracing::info!(message = %expected, "Correctly received message from PUSH server.");
  }

  Ok(())
}
