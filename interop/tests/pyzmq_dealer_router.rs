mod common;

use anyhow::Result;
use rzmq::{Msg, SocketType, context::context};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dealer_router_with_pyzmq() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  let (_guard, _reader) =
    common::spawn_and_wait_ready("router_server.py", &[endpoint.as_str()], Duration::from_secs(5))?;

  let ctx = context()?;
  let dealer_socket = ctx.socket(SocketType::Dealer)?;
  dealer_socket.connect(&endpoint).await?;

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
