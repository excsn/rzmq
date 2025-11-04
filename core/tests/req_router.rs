// core/tests/req_router.rs

mod common;

use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn test_req_router_rzmq_to_rzmq() -> Result<(), ZmqError> {
  println!("\n--- Starting test_req_router_rzmq_to_rzmq ---");
  let ctx = common::test_context();

  let router = ctx.socket(SocketType::Router)?;
  let req = ctx.socket(SocketType::Req)?;

  let endpoint = "tcp://127.0.0.1:5585"; // Unique port
  println!("Binding ROUTER to {}...", endpoint);
  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Connecting REQ to {}...", endpoint);
  req.connect(endpoint).await?;
  // Increased sleep to ensure connection and handshake are fully complete before sending.
  tokio::time::sleep(Duration::from_millis(200)).await;

  // REQ sends its message
  let request_payload = vec![0, 1, 2];
  println!("REQ sending payload: {:?}", request_payload);
  req.send(Msg::from_vec(request_payload.clone())).await?;

  // ROUTER receives
  // It should receive [identity, empty_delimiter, payload].
  println!("ROUTER receiving multipart message...");
  let frames = router.recv_multipart().await?;
  println!("[ROUTER] received {} frames", frames.len());

  // Assert what the ROUTER application should see
  assert_eq!(
    frames.len(),
    2,
    "ROUTER should receive 2 frames (identity, payload)"
  );
  let identity = &frames[0];
  let payload = &frames[1];
  assert!(
    !identity.data().unwrap_or(&[1]).is_empty(),
    "Identity frame should not be empty"
  );
  assert_eq!(
    payload.data().unwrap(),
    request_payload.as_slice(),
    "Payload received by ROUTER should match sent payload"
  );

  // ROUTER echoes back exactly what it received (the full envelope)
  println!("ROUTER echoing back received frames...");
  router.send_multipart(frames).await?;

  // REQ receives the reply
  // The REQ socket implementation is responsible for stripping the routing envelope
  // ([identity, empty_delimiter]) and returning ONLY the application payload.
  println!("REQ receiving reply multipart message...");
  let reply_frames = req.recv_multipart().await?;

  println!("[REQ] received {} payload frames", reply_frames.len());

  // Assertions based on the CORRECT behavior of a REQ socket
  assert_eq!(
    reply_frames.len(),
    1,
    "REQ socket should only receive the payload, not the envelope"
  );
  assert_eq!(
    reply_frames[0].data().unwrap(),
    request_payload.as_slice(),
    "The received payload should match the original sent payload"
  );

  println!("SUCCESS: rzmq REQ socket correctly received only the payload from rzmq ROUTER.");

  // Verify that the REQ socket has correctly transitioned back to the ReadyToSend state
  println!("REQ sending a second request to confirm state...");
  req.send(Msg::from_static(b"second request")).await?;
  println!("SUCCESS: Second send call did not fail with InvalidState.");

  ctx.term().await?;
  Ok(())
}
