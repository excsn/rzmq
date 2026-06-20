mod common;

use anyhow::Result;
use rzmq::socket::SUBSCRIBE;
use rzmq::{SocketType, context::context};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pub_sub_with_pyzmq() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  let (_guard, _reader) =
    common::spawn_and_wait_ready("pub_server.py", &[endpoint.as_str()], Duration::from_secs(5))?;

  let ctx = context()?;
  let sub_socket = ctx.socket(SocketType::Sub)?;
  sub_socket.connect(&endpoint).await?;

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
