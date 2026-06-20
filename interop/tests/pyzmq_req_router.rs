mod common;

use anyhow::Result;
use rzmq::{Msg, SocketType, ZmqError, context::context};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_req_to_pyzmq_router() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  // Start the Python pyzmq ROUTER server. It echoes one request then holds the
  // connection open until this test kills it (so REQ's recv() can't race a peer teardown).
  let (_guard, _reader) = common::spawn_and_wait_ready(
    "router_server_for_req.py",
    &[endpoint.as_str()],
    Duration::from_secs(5),
  )?;

  let ctx = context()?;
  let req_socket = ctx.socket(SocketType::Req)?;
  req_socket.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  let request_payload = b"hello world";
  tracing::info!(
    "rzmq REQ: Sending '{}'...",
    String::from_utf8_lossy(request_payload)
  );

  req_socket.send(Msg::from_static(request_payload)).await?;

  tracing::info!("rzmq REQ: Receiving reply...");

  let reply = req_socket.recv().await?;

  // The test asserts that the application receives only the payload.
  assert_eq!(
    reply.data().unwrap(),
    request_payload,
    "The received payload should match the sent payload."
  );
  assert!(
    !reply.is_more(),
    "The single reply frame should not have the MORE flag."
  );

  tracing::info!("SUCCESS: rzmq REQ socket correctly received only the payload from pyzmq ROUTER.");

  tracing::info!("rzmq REQ: Verifying that calling recv() again fails...");
  let second_recv_result = req_socket.recv().await;

  assert!(
    matches!(second_recv_result, Err(ZmqError::InvalidState(_))),
    "Expected InvalidState error on second recv(), but got {:?}",
    second_recv_result
  );

  tracing::info!("SUCCESS: recv() correctly returned InvalidState as expected.");

  Ok(())
}

// This test creates an rzmq ROUTER server and a pyzmq REQ client.
// It validates that the rzmq ROUTER can correctly receive from and reply to a libzmq REQ socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pyzmq_req_to_rzmq_router() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  // 1. Setup Rust rzmq ROUTER server
  let ctx = context()?;
  let router_socket = ctx.socket(SocketType::Router)?;
  router_socket.bind(&endpoint).await?;
  tracing::info!("rzmq ROUTER: Bound to {}", endpoint);
  tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind to settle

  // 2. Start Python pyzmq REQ client and wait until it has sent its request.
  let (_guard, mut reader) =
    common::spawn_script("req_client_for_router.py", &[endpoint.as_str()])?;
  common::wait_for_line(&mut reader, "Request sent.", Duration::from_secs(5))?;

  // 3. rzmq ROUTER receives the request
  tracing::info!("rzmq ROUTER: Waiting for request from pyzmq REQ...");
  let frames = router_socket.recv_multipart().await?;

  assert_eq!(
    frames.len(),
    2,
    "ROUTER should receive 2 frames (identity, payload)"
  );
  let identity = &frames[0];
  let payload = &frames[1];

  assert!(
    !identity.data().unwrap_or(&[1]).is_empty(),
    "Identity frame from ROUTER should not be empty"
  );
  assert_eq!(payload.data().unwrap(), b"hello from pyzmq req");
  tracing::info!("rzmq ROUTER: Correctly received identity and payload.");

  // 4. rzmq ROUTER sends a reply
  let reply_payload = b"reply from rzmq router";
  tracing::info!("rzmq ROUTER: Sending reply to pyzmq REQ...");

  // The application must now provide the full envelope for the ROUTER to send.
  let reply_frames = vec![
    identity.clone(),                // The routing frame (identity)
    Msg::from_static(reply_payload), // The payload
  ];
  router_socket.send_multipart(reply_frames).await?;

  tracing::info!("rzmq ROUTER: Reply sent. Waiting for Python client to confirm receipt...");

  common::wait_for_line(
    &mut reader,
    "SUCCESS - Received the expected reply.",
    Duration::from_secs(5),
  )?;
  tracing::info!("SUCCESS: rzmq ROUTER successfully communicated with pyzmq REQ.");

  Ok(())
}
