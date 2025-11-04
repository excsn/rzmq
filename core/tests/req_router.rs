mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rzmq::{Context, Msg, SocketType, ZmqError};
use tokio::sync::Barrier;
use tokio::task;

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

#[tokio::test]
async fn test_req_router_multiple_req_clients() -> Result<(), ZmqError> {
  println!("\n--- Starting test_req_router_multiple_req_clients ---");
  let ctx = common::test_context();
  {
    let router = ctx.socket(SocketType::Router)?;
    let endpoint = "tcp://127.0.0.1:5586"; // New unique port

    println!("[ROUTER] Binding to {}...", endpoint);
    router.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let num_reqs = 3;
    let mut req_handles = Vec::new();
    let barrier = Arc::new(Barrier::new(num_reqs));

    for i in 0..num_reqs {
      let req_socket = ctx.socket(SocketType::Req)?;
      let endpoint_clone = endpoint.to_string();
      let barrier_clone = barrier.clone();

      let handle = task::spawn(async move {
        req_socket.connect(&endpoint_clone).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        barrier_clone.wait().await; // Synchronize clients

        let request_payload = format!("Request from REQ {}", i);
        println!("[REQ {}] Sending: '{}'", i, request_payload);
        req_socket
          .send(Msg::from_vec(request_payload.into_bytes()))
          .await
          .unwrap();

        println!("[REQ {}] Receiving reply...", i);
        let reply = common::recv_timeout(&req_socket, LONG_TIMEOUT)
          .await
          .unwrap();

        let expected_reply = format!("Reply to REQ {}", i);
        assert_eq!(reply.data().unwrap(), expected_reply.as_bytes());
        println!("[REQ {}] Received correct reply.", i);
      });
      req_handles.push(handle);
    }

    // ROUTER logic
    let mut identities = HashMap::new();
    println!("[ROUTER] Receiving {} requests...", num_reqs);
    for _ in 0..num_reqs {
      let frames = router.recv_multipart().await?;
      assert_eq!(frames.len(), 2);
      let identity = frames[0].data().unwrap().to_vec();
      let payload = String::from_utf8(frames[1].data().unwrap().to_vec()).unwrap();
      println!("[ROUTER] Received '{}' from ID {:?}", payload, identity);

      // Extract the client number from the payload
      let client_num_str = payload.split_whitespace().last().unwrap();
      let client_num: usize = client_num_str.parse().unwrap();
      identities.insert(client_num, identity);
    }
    assert_eq!(identities.len(), num_reqs);
    println!("[ROUTER] Received all requests. Now sending replies.");

    // ROUTER sends targeted replies
    for i in 0..num_reqs {
      let identity_bytes = identities.get(&i).unwrap();
      let reply_payload = format!("Reply to REQ {}", i);

      let mut id_frame = Msg::from_vec(identity_bytes.clone());
      id_frame.set_flags(rzmq::MsgFlags::MORE);
      let payload_frame = Msg::from_vec(reply_payload.into_bytes());

      println!(
        "[ROUTER] Sending reply to REQ {} (ID {:?})",
        i, identity_bytes
      );
      router.send_multipart(vec![id_frame, payload_frame]).await?;
    }
    println!("[ROUTER] Sent all replies.");

    // Wait for all REQ tasks to finish
    for handle in req_handles {
      handle.await.expect("REQ client task panicked");
    }
    println!("[SYS] All REQ clients finished successfully.");
  }

  println!("[SYS] Terminating context...");
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

#[tokio::test]
async fn test_req_router_req_disconnects_before_reply() -> Result<(), ZmqError> {
  println!("\n--- Starting test_req_router_req_disconnects_before_reply ---");
  let ctx = common::test_context();
  {
    let router = ctx.socket(SocketType::Router)?;
    let endpoint = "tcp://127.0.0.1:5587"; // New unique port

    println!("[ROUTER] Setting ROUTER_MANDATORY=true");
    router
      .set_option(rzmq::socket::options::ROUTER_MANDATORY, true)
      .await?;

    println!("[ROUTER] Binding to {}...", endpoint);
    router.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let req_identity: Vec<u8>;

    {
      // Scope for the REQ socket
      let req = ctx.socket(SocketType::Req)?;
      req.connect(endpoint).await?;
      tokio::time::sleep(Duration::from_millis(100)).await;

      req
        .send(Msg::from_static(b"Request to be orphaned"))
        .await?;
      println!("[REQ] Sent request.");

      let frames = router.recv_multipart().await?;
      assert_eq!(frames.len(), 2);
      req_identity = frames[0].data().unwrap().to_vec();
      println!("[ROUTER] Received request from ID {:?}", req_identity);

      println!("[REQ] Closing socket before reply is sent...");
      req.close().await?;
    } // REQ socket is dropped here

    // Allow time for the disconnect to be processed by the ROUTER's core
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ROUTER attempts to reply
    let mut id_frame = Msg::from_vec(req_identity.clone());
    id_frame.set_flags(rzmq::MsgFlags::MORE);
    let payload_frame = Msg::from_static(b"This reply will fail");

    println!(
      "[ROUTER] Attempting to send reply to disconnected REQ (ID {:?})...",
      req_identity
    );
    let send_result = router.send_multipart(vec![id_frame, payload_frame]).await;

    println!("[ROUTER] Send result: {:?}", send_result);
    assert!(
      matches!(send_result, Err(ZmqError::HostUnreachable(_))),
      "Expected HostUnreachable error when sending to a disconnected peer with ROUTER_MANDATORY=true, got {:?}",
      send_result
    );
    println!("[ROUTER] Correctly failed to send with HostUnreachable.");
  }

  println!("[SYS] Terminating context...");
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}
