#![cfg(feature = "curve")]

use rzmq::{
  Context, Msg, MsgFlags, SocketType, ZmqError,
  socket::SocketEvent,
  socket::options::{
    CURVE_SECRET_KEY, CURVE_SERVER, CURVE_SERVER_KEY, RCVTIMEO, ROUTING_ID, SNDTIMEO, SUBSCRIBE,
  },
};
use serial_test::serial;
use std::time::Duration;

// For key generation
use dryoc::{keypair::StackKeyPair as Keypair, types::Bytes};

mod common; // Include your test helpers

const SHORT_TIMEOUT: Duration = Duration::from_millis(500);
const LONG_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECT_DELAY: Duration = Duration::from_millis(200);

#[tokio::test]
#[serial]
async fn test_curve_req_rep_basic_encrypted_exchange() -> Result<(), ZmqError> {
  println!("\n--- Starting test_curve_req_rep_basic_encrypted_exchange ---");
  let ctx = common::test_context();

  let server_keys = Keypair::r#gen();
  let client_keys = Keypair::r#gen();
  let endpoint = "tcp://127.0.0.1:5760";

  // --- REP Server Setup ---
  let rep_server = ctx.socket(SocketType::Rep)?;
  println!("[REP Server {}] Configuring CURVE...", endpoint);
  rep_server.set_option(CURVE_SERVER, true).await?;
  rep_server
    .set_option_raw(CURVE_SECRET_KEY, server_keys.secret_key.as_slice())
    .await?;
  println!("[REP Server {}] Binding...", endpoint);
  rep_server.bind(endpoint).await?;
  println!("[REP Server {}] Bound and listening with CURVE.", endpoint);

  // --- REQ Client Setup ---
  let req_client = ctx.socket(SocketType::Req)?;
  println!("[REQ Client {}] Configuring CURVE...", endpoint);
  req_client
    .set_option_raw(CURVE_SECRET_KEY, client_keys.secret_key.as_slice())
    .await?;
  req_client
    .set_option_raw(CURVE_SERVER_KEY, server_keys.public_key.as_slice())
    .await?;

  let mut client_monitor = req_client.monitor_default().await?;
  println!("[REQ Client {}] Monitor created.", endpoint);

  println!("[REQ Client {}] Connecting...", endpoint);
  req_client.connect(endpoint).await?;
  println!(
    "[REQ Client {}] Connect call returned. Waiting for HANDSHAKE SUCCEEDED event...",
    endpoint
  );

  // Wait for HandshakeSucceeded event
  common::wait_for_monitor_event(&client_monitor, LONG_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .map_err(|e| ZmqError::Internal(format!("HandshakeSucceeded event wait failed: {}", e)))?;
  println!(
    "[REQ Client {}] Handshake Succeeded event received!",
    endpoint
  );

  // --- Message Exchange ---
  let request_data = b"Secure Hello via CURVE!";
  println!(
    "[REQ Client {}] Sending: \"{}\"",
    endpoint,
    String::from_utf8_lossy(request_data)
  );
  req_client.send(Msg::from_static(request_data)).await?;

  println!("[REP Server {}] Receiving request...", endpoint);
  let received_req = common::recv_timeout(&rep_server, LONG_TIMEOUT).await?;
  assert_eq!(received_req.data().unwrap(), request_data);

  let reply_data = b"Secure World via CURVE!";
  println!(
    "[REP Server {}] Sending reply: \"{}\"",
    endpoint,
    String::from_utf8_lossy(reply_data)
  );
  rep_server.send(Msg::from_static(reply_data)).await?;

  println!("[REQ Client {}] Receiving reply...", endpoint);
  let received_reply = common::recv_timeout(&req_client, LONG_TIMEOUT).await?;
  assert_eq!(received_reply.data().unwrap(), reply_data);

  println!("[SYSTEM {}] Secure message exchange successful!", endpoint);

  // --- Teardown ---
  println!("[SYSTEM {}] Terminating context...", endpoint);
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

#[tokio::test]
#[serial]
async fn test_curve_client_auth_server_pk_mismatch() -> Result<(), ZmqError> {
  println!("\n--- Starting test_curve_client_auth_server_pk_mismatch ---");
  let ctx = common::test_context();

  let server_keys = Keypair::r#gen();
  let client_keys = Keypair::r#gen();
  let fake_server_keys = Keypair::r#gen();
  let endpoint = "tcp://127.0.0.1:5761";

  // --- REP Server Setup (Genuine Server) ---
  let rep_server = ctx.socket(SocketType::Rep)?;
  rep_server.set_option(CURVE_SERVER, true).await?;
  rep_server
    .set_option_raw(CURVE_SECRET_KEY, server_keys.secret_key.as_slice())
    .await?;
  rep_server.bind(endpoint).await?;
  println!("[REP Server {}] Bound with genuine PK.", endpoint);

  // --- REQ Client Setup (Expects FAKE Server PK) ---
  let req_client = ctx.socket(SocketType::Req)?;
  req_client
    .set_option_raw(CURVE_SECRET_KEY, client_keys.secret_key.as_slice())
    .await?;
  req_client
    .set_option_raw(CURVE_SERVER_KEY, fake_server_keys.public_key.as_slice())
    .await?;

  let mut client_monitor = req_client.monitor_default().await?;
  println!(
    "[REQ Client {}] Connecting (expecting server PK mismatch)...",
    endpoint
  );
  req_client.connect(endpoint).await?;

  println!("[REQ Client {}] Waiting for handshake outcome...", endpoint);
  let event = common::wait_for_monitor_event(&client_monitor, LONG_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(
      e,
      SocketEvent::HandshakeFailed { .. } | SocketEvent::ConnectFailed { .. }
    )
  })
  .await;

  assert!(event.is_ok(), "Client should have emitted a failure event");

  let error_msg = match event.unwrap() {
    SocketEvent::HandshakeFailed { error_msg, .. } => error_msg,
    SocketEvent::ConnectFailed { error_msg, .. } => error_msg,
    _ => panic!("Unexpected event type"),
  };

  println!(
    "[REQ Client {}] Received expected failure event: {}",
    endpoint, error_msg
  );
  assert!(
    error_msg.contains("AuthenticationFailure")
      || error_msg.contains("Security error")
      || error_msg.contains("decryption error"),
    "Error message did not indicate an auth/security failure. Got: {}",
    error_msg
  );
  println!(
    "[REQ Client {}] Correctly observed handshake failure.",
    endpoint
  );

  // --- Teardown ---
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

#[tokio::test]
#[serial]
async fn test_curve_dealer_router_encrypted_exchange() -> Result<(), ZmqError> {
  println!("\n--- Starting test_curve_dealer_router_encrypted_exchange ---");
  let ctx = common::test_context();

  let server_keys = Keypair::r#gen();
  let client_keys = Keypair::r#gen();
  let endpoint = "tcp://127.0.0.1:5762";
  let dealer_id = "DEALER-A";

  // --- ROUTER Server Setup ---
  let router = ctx.socket(SocketType::Router)?;
  router.set_option(CURVE_SERVER, true).await?;
  router
    .set_option_raw(CURVE_SECRET_KEY, server_keys.secret_key.as_slice())
    .await?;
  router.bind(endpoint).await?;
  println!("[ROUTER Server {}] Bound with CURVE.", endpoint);

  // --- DEALER Client Setup ---
  let dealer = ctx.socket(SocketType::Dealer)?;
  dealer
    .set_option_raw(ROUTING_ID, dealer_id.as_bytes())
    .await?;
  dealer
    .set_option_raw(CURVE_SECRET_KEY, client_keys.secret_key.as_slice())
    .await?;
  dealer
    .set_option_raw(CURVE_SERVER_KEY, server_keys.public_key.as_slice())
    .await?;

  let mut client_monitor = dealer.monitor_default().await?;
  dealer.connect(endpoint).await?;
  common::wait_for_monitor_event(&client_monitor, LONG_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .map_err(|e| ZmqError::Internal(format!("Handshake event wait failed: {}", e)))?;
  println!("[DEALER Client {}] Handshake Succeeded.", endpoint);

  // --- Message Exchange: DEALER -> ROUTER ---
  let request_payload = b"Request from DEALER";
  println!("[DEALER Client] Sending request...");
  dealer.send(Msg::from_static(request_payload)).await?;

  println!("[ROUTER Server] Receiving message...");
  let received_id = common::recv_timeout(&router, LONG_TIMEOUT).await?;
  let received_payload = common::recv_timeout(&router, SHORT_TIMEOUT).await?;

  assert_eq!(
    received_id.data().unwrap(),
    dealer_id.as_bytes(),
    "ROUTER received wrong identity"
  );
  assert!(
    received_id.is_more(),
    "Identity frame should have MORE flag"
  );
  assert_eq!(
    received_payload.data().unwrap(),
    request_payload,
    "ROUTER received wrong payload"
  );
  assert!(!received_payload.is_more(), "Payload should be last frame");
  println!("[ROUTER Server] Received message correctly.");

  // --- Message Exchange: ROUTER -> DEALER ---
  let reply_payload = b"Reply from ROUTER";
  println!("[ROUTER Server] Sending reply to DEALER...");
  let mut id_frame = Msg::from_vec(received_id.data().unwrap().to_vec());
  id_frame.set_flags(MsgFlags::MORE);

  router.send(id_frame).await?;
  router.send(Msg::from_static(reply_payload)).await?;

  println!("[DEALER Client] Receiving reply...");
  let reply = common::recv_timeout(&dealer, LONG_TIMEOUT).await?;
  assert_eq!(
    reply.data().unwrap(),
    reply_payload,
    "DEALER received wrong reply"
  );
  assert!(!reply.is_more(), "Reply payload should be last frame");
  println!("[DEALER Client] Received reply correctly.");

  // --- Teardown ---
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

#[tokio::test]
#[serial]
async fn test_curve_pub_sub_encrypted_exchange() -> Result<(), ZmqError> {
  println!("\n--- Starting test_curve_pub_sub_encrypted_exchange ---");
  let ctx = common::test_context();

  let server_keys = Keypair::r#gen();
  let client_keys = Keypair::r#gen();
  let endpoint = "tcp://127.0.0.1:5763";
  let topic = b"WEATHER";

  // --- PUB Server Setup ---
  let publisher = ctx.socket(SocketType::Pub)?;
  publisher.set_option(CURVE_SERVER, true).await?;
  publisher
    .set_option_raw(CURVE_SECRET_KEY, server_keys.secret_key.as_slice())
    .await?;
  publisher.bind(endpoint).await?;
  println!("[PUB Server {}] Bound with CURVE.", endpoint);

  // --- SUB Client Setup ---
  let subscriber = ctx.socket(SocketType::Sub)?;
  subscriber
    .set_option_raw(CURVE_SECRET_KEY, client_keys.secret_key.as_slice())
    .await?;
  subscriber
    .set_option_raw(CURVE_SERVER_KEY, server_keys.public_key.as_slice())
    .await?;

  let mut client_monitor = subscriber.monitor_default().await?;
  subscriber.connect(endpoint).await?;
  common::wait_for_monitor_event(&client_monitor, LONG_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .map_err(|e| ZmqError::Internal(format!("Handshake event wait failed: {}", e)))?;
  println!("[SUB Client {}] Handshake Succeeded.", endpoint);

  // --- Subscribe and Exchange ---
  println!(
    "[SUB Client] Subscribing to topic '{}'...",
    String::from_utf8_lossy(topic)
  );
  subscriber.set_option(SUBSCRIBE, topic).await?;
  tokio::time::sleep(CONNECT_DELAY).await; // Allow subscribe message to be sent and processed

  println!(
    "[PUB Server] Sending message on topic '{}'...",
    String::from_utf8_lossy(topic)
  );
  let msg_payload = b"WEATHER sunny 25C";
  publisher.send(Msg::from_static(msg_payload)).await?;

  println!("[PUB Server] Sending message on different topic...");
  publisher.send(Msg::from_static(b"SPORTS results")).await?;

  println!("[SUB Client] Receiving message...");
  let received_msg = common::recv_timeout(&subscriber, LONG_TIMEOUT).await?;
  assert_eq!(
    received_msg.data().unwrap(),
    msg_payload,
    "SUB received wrong message"
  );
  println!("[SUB Client] Received message correctly.");

  // Check that the other topic is filtered
  let result = common::recv_timeout(&subscriber, SHORT_TIMEOUT).await;
  assert!(
    matches!(result, Err(ZmqError::Timeout)),
    "SUB should not receive non-subscribed topic"
  );
  println!("[SUB Client] Correctly timed out for other topic.");

  // --- Teardown ---
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}
