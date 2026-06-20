mod common;

use anyhow::Result;
use rzmq::{Msg, SocketType, context::context};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_req_rep_with_pyzmq() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  // 1. Start Python REP server and wait for it to bind.
  let (_guard, _reader) =
    common::spawn_and_wait_ready("rep_server.py", &[endpoint.as_str()], Duration::from_secs(5))?;
  tracing::info!("Python server is READY. Proceeding with Rust client.");

  // 2. Run Rust Client
  let ctx = context()?;
  let req_socket = ctx.socket(SocketType::Req)?;
  req_socket.connect(&endpoint).await?;

  // 3. Test Communication
  tracing::info!("Sending 'hello' to Python server...");
  req_socket.send(Msg::from_static(b"hello")).await?;
  let reply = req_socket.recv().await?;
  tracing::info!("Received reply from Python server.");

  assert_eq!(reply.data().unwrap(), b"hello-reply");

  // 4. Shutdown
  tracing::info!("Sending 'shutdown' command to Python server.");
  req_socket.send(Msg::from_static(b"shutdown")).await?;

  Ok(())
}
