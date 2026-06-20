mod common;

use anyhow::Result;
use rzmq::{SocketType, context::context};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_push_pull_with_pyzmq() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  let (_guard, _reader) =
    common::spawn_and_wait_ready("push_server.py", &[endpoint.as_str()], Duration::from_secs(5))?;

  // Setup Rust PULL client
  let ctx = context()?;
  let pull_socket = ctx.socket(SocketType::Pull)?;
  pull_socket.connect(&endpoint).await?;

  // Receive the 5 messages
  for i in 0..5 {
    let reply = pull_socket.recv().await?;
    let expected = format!("message-{}", i);
    assert_eq!(reply.data().unwrap(), expected.as_bytes());
    tracing::info!(message = %expected, "Correctly received message from PUSH server.");
  }

  Ok(())
}
