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
async fn test_dealer_router_with_pyzmq() -> Result<()> {
  common::setup_logging();
  let endpoint = "tcp://127.0.0.1:65240";

  let mut cmd = Command::new("python3")
    .arg("python_scripts/router_server.py")
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
  anyhow::ensure!(is_ready, "Python ROUTER server did not signal READY.");

  let ctx = context()?;
  let dealer_socket = ctx.socket(SocketType::Dealer)?;
  dealer_socket.connect(endpoint).await?;

  // Give time for the connection to establish fully.
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Send a request. The rzmq DEALER impl should automatically prepend an empty delimiter.
  tracing::info!("Sending 'request-1'...");
  dealer_socket.send(Msg::from_static(b"request-1")).await?;

  // Receive the reply. The rzmq DEALER impl should strip the empty delimiter.
  let reply = dealer_socket.recv().await?;
  assert_eq!(reply.data().unwrap(), b"request-1-reply");
  tracing::info!("Correctly received reply for request-1");

  // Send another one to be sure
  tracing::info!("Sending 'request-2'...");
  dealer_socket
    .send_multipart(vec![Msg::from_static(b"request-2")])
    .await?;
  let reply_parts = dealer_socket.recv_multipart().await?;
  assert_eq!(reply_parts.len(), 1);
  assert_eq!(reply_parts[0].data().unwrap(), b"request-2-reply");
  tracing::info!("Correctly received reply for request-2");

  // Shut down the server
  dealer_socket.send(Msg::from_static(b"shutdown")).await?;

  Ok(())
}
