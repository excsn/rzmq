// tests/reconnect_tests.rs

use rzmq::{socket::{RCVTIMEO, RECONNECT_IVL, ROUTER_MANDATORY, ROUTING_ID, SNDTIMEO}, Blob, Context, Msg, MsgFlags, Socket, SocketType, ZmqError};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const TEST_ENDPOINT_BASE: &str = "tcp://127.0.0.1";
const SHORT_TIMEOUT: Duration = Duration::from_millis(250); // For send/recv attempts
const RECONNECT_INTERVAL_MS: i32 = 50; // Short reconnect interval for tests
const WAIT_FOR_RECONNECT: Duration = Duration::from_millis(RECONNECT_INTERVAL_MS as u64 * 5); // Give it a few intervals
const SERVER_START_DELAY: Duration = Duration::from_millis(RECONNECT_INTERVAL_MS as u64 * 2); // Delay before server starts

async fn setup_client_socket(ctx: &Context, sock_type: SocketType, endpoint: &str) -> Result<Socket, ZmqError> {
  let client = ctx.socket(sock_type)?;
  // Set a short SNDTIMEO/RCVTIMEO for initial send/recv attempts that might block before connection
  client
    .set_option_raw(SNDTIMEO, &(SHORT_TIMEOUT.as_millis() as i32).to_ne_bytes())
    .await?;
  client
    .set_option_raw(RCVTIMEO, &(SHORT_TIMEOUT.as_millis() as i32).to_ne_bytes())
    .await?;
  // Set a short reconnect interval for faster test execution
  client
    .set_option_raw(RECONNECT_IVL, &RECONNECT_INTERVAL_MS.to_ne_bytes())
    .await?;
  // ZMQ_RECONNECT_IVL_MAX can be left at default (0 = effectively same as RECONNECT_IVL for basic test)
  client.connect(endpoint).await?; // This call is non-blocking in rzmq regarding actual connection
  Ok(client)
}

async fn setup_server_socket(ctx: &Context, sock_type: SocketType, endpoint: &str) -> Result<Socket, ZmqError> {
  let server = ctx.socket(sock_type)?;
  server
    .set_option_raw(SNDTIMEO, &(SHORT_TIMEOUT.as_millis() as i32).to_ne_bytes())
    .await?;
  server
    .set_option_raw(RCVTIMEO, &(SHORT_TIMEOUT.as_millis() as i32).to_ne_bytes())
    .await?;
  server.bind(endpoint).await?;
  Ok(server)
}

fn generate_unique_endpoint(port_offset: u16) -> String {
  format!("{}:{}", TEST_ENDPOINT_BASE, 5700 + port_offset)
}

#[rzmq::main]
async fn test_push_pull_reconnect() -> Result<(), ZmqError> {
  let endpoint = generate_unique_endpoint(0);
  println!("Starting test_push_pull_reconnect on {}", endpoint);

  let ctx = Context::new()?;
  let push_socket = setup_client_socket(&ctx, SocketType::Push, &endpoint).await?;
  println!("PUSH socket setup and connect initiated to {}", endpoint);

  // Attempt to send when PULL is not yet bound - this should fail or block based on SNDTIMEO
  // Since PUSH blocks if no peer, and SNDTIMEO is short, this might return Timeout/ResourceLimitReached quickly.
  let initial_send_result = push_socket.send(Msg::from_static(b"Initial Ping")).await;
  println!("PUSH initial send attempt result: {:?}", initial_send_result);
  assert!(
    matches!(
      initial_send_result,
      Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout)
    ),
    "Initial PUSH send should fail with ResourceLimitReached or Timeout as no peer is available"
  );

  // Wait for a bit, then start the PULL server
  println!(
    "Waiting {}ms before starting PULL server...",
    SERVER_START_DELAY.as_millis()
  );
  sleep(SERVER_START_DELAY).await;

  println!("Starting PULL server on {}", endpoint);
  let pull_socket = setup_server_socket(&ctx, SocketType::Pull, &endpoint).await?;
  println!("PULL server bound to {}", endpoint);

  // Wait for the PUSH socket's connecter to establish the connection
  println!("Waiting {}ms for PUSH to reconnect...", WAIT_FOR_RECONNECT.as_millis());
  sleep(WAIT_FOR_RECONNECT).await;

  // Now, try sending from PUSH, it should succeed
  let test_msg_data = b"Hello from PUSH after reconnect";
  println!(
    "PUSH sending '{}' after PULL server start...",
    String::from_utf8_lossy(test_msg_data)
  );
  push_socket.send(Msg::from_static(test_msg_data)).await?;
  println!("PUSH sent message successfully.");

  // PULL socket should receive the message
  println!("PULL attempting to receive message...");
  let received_msg = pull_socket.recv().await?;
  println!(
    "PULL received: '{}'",
    String::from_utf8_lossy(received_msg.data().unwrap())
  );

  assert_eq!(received_msg.data().unwrap(), test_msg_data);

  println!("test_push_pull_reconnect PASSED");
  Ok(())
}

#[rzmq::main]
async fn test_req_rep_reconnect() -> Result<(), ZmqError> {
  let endpoint = generate_unique_endpoint(1);
  println!("Starting test_req_rep_reconnect on {}", endpoint);

  let ctx = Context::new()?;
  let req_socket = setup_client_socket(&ctx, SocketType::Req, &endpoint).await?;
  println!("REQ socket setup and connect initiated to {}", endpoint);

  // Attempt to send REQ when REP is not yet bound
  let initial_req_data = b"Initial Request";
  println!(
    "REQ initial send attempt for '{}'...",
    String::from_utf8_lossy(initial_req_data)
  );
  let initial_send_result = req_socket.send(Msg::from_static(initial_req_data)).await;
  println!("REQ initial send attempt result: {:?}", initial_send_result);
  assert!(
    matches!(
      initial_send_result,
      Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout)
    ),
    "Initial REQ send should fail with ResourceLimitReached or Timeout (no peer)"
  );
  // If send failed, REQ is still in ReadyToSend state.

  println!(
    "Waiting {}ms before starting REP server...",
    SERVER_START_DELAY.as_millis()
  );
  sleep(SERVER_START_DELAY).await;

  println!("Starting REP server on {}", endpoint);
  let rep_socket = setup_server_socket(&ctx, SocketType::Rep, &endpoint).await?;
  println!("REP server bound to {}", endpoint);

  println!("Waiting {}ms for REQ to reconnect...", WAIT_FOR_RECONNECT.as_millis());
  sleep(WAIT_FOR_RECONNECT).await;

  // Now, try sending from REQ, it should succeed
  let req_msg_data = b"Hello from REQ after reconnect";
  println!(
    "REQ sending '{}' after REP server start...",
    String::from_utf8_lossy(req_msg_data)
  );
  req_socket.send(Msg::from_static(req_msg_data)).await?;
  println!("REQ sent message successfully.");

  // REP receives and replies
  println!("REP attempting to receive request...");
  let received_req = rep_socket.recv().await?;
  println!(
    "REP received: '{}'",
    String::from_utf8_lossy(received_req.data().unwrap())
  );
  assert_eq!(received_req.data().unwrap(), req_msg_data);

  let rep_msg_data = b"Reply from REP";
  println!("REP sending reply: '{}'", String::from_utf8_lossy(rep_msg_data));
  rep_socket.send(Msg::from_static(rep_msg_data)).await?;
  println!("REP sent reply.");

  // REQ receives reply
  println!("REQ attempting to receive reply...");
  let received_reply = req_socket.recv().await?;
  println!(
    "REQ received reply: '{}'",
    String::from_utf8_lossy(received_reply.data().unwrap())
  );
  assert_eq!(received_reply.data().unwrap(), rep_msg_data);

  println!("test_req_rep_reconnect PASSED");
  Ok(())
}

#[rzmq::main]
async fn test_dealer_router_reconnect() -> Result<(), ZmqError> {
  let endpoint = generate_unique_endpoint(2);
  println!("Starting test_dealer_router_reconnect on {}", endpoint);

  let ctx = Context::new()?;

  // Setup ROUTER (server) - but don't bind it yet
  let router_socket = ctx.socket(SocketType::Router)?;
  router_socket
    .set_option_raw(SNDTIMEO, &(SHORT_TIMEOUT.as_millis() as i32).to_ne_bytes())
    .await?;
  router_socket
    .set_option_raw(RCVTIMEO, &(SHORT_TIMEOUT.as_millis() as i32).to_ne_bytes())
    .await?;
  // ROUTER_MANDATORY=true so we get an error if peer not found after reconnect
  router_socket
    .set_option_raw(ROUTER_MANDATORY, &1i32.to_ne_bytes())
    .await?;
  println!("ROUTER socket created.");

  // Setup DEALER (client)
  let dealer_id_val = "dealer1";
  let dealer_socket = ctx.socket(SocketType::Dealer)?;
  dealer_socket
    .set_option_raw(SNDTIMEO, &(SHORT_TIMEOUT.as_millis() as i32).to_ne_bytes())
    .await?;
  dealer_socket
    .set_option_raw(RCVTIMEO, &(SHORT_TIMEOUT.as_millis() as i32).to_ne_bytes())
    .await?;
  dealer_socket
    .set_option_raw(RECONNECT_IVL, &RECONNECT_INTERVAL_MS.to_ne_bytes())
    .await?;
  dealer_socket
    .set_option_raw(ROUTING_ID, dealer_id_val.as_bytes())
    .await?; // Set DEALER ID

  println!(
    "DEALER socket ({}) setup, initiating connect to {}",
    dealer_id_val, endpoint
  );
  dealer_socket.connect(&endpoint).await?;

  // Attempt to send from DEALER when ROUTER is not yet bound
  // DEALER sends [delimiter, payload]
  let initial_dealer_payload = b"Initial Dealer Msg";
  println!(
    "DEALER initial send attempt for '{}'...",
    String::from_utf8_lossy(initial_dealer_payload)
  );
  // Dealer send might succeed in queuing if SNDHWM allows, or block/timeout if SNDTIMEO=0
  // For this test, we expect it to block until timeout due to no peer.
  let initial_send_result = dealer_socket.send(Msg::from_static(initial_dealer_payload)).await;
  println!("DEALER initial send attempt result: {:?}", initial_send_result);
  assert!(
    matches!(
      initial_send_result,
      Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout)
    ),
    "Initial DEALER send should fail with ResourceLimitReached or Timeout (no peer)"
  );

  println!(
    "Waiting {}ms before starting ROUTER server...",
    SERVER_START_DELAY.as_millis()
  );
  sleep(SERVER_START_DELAY).await;

  println!("Starting ROUTER server on {}", endpoint);
  router_socket.bind(&endpoint).await?;
  println!("ROUTER server bound to {}", endpoint);

  println!(
    "Waiting {}ms for DEALER to reconnect...",
    WAIT_FOR_RECONNECT.as_millis()
  );
  sleep(WAIT_FOR_RECONNECT).await;

  // Now, try sending from DEALER, it should succeed
  let dealer_msg_data = b"Hello from DEALER after reconnect";
  println!(
    "DEALER ({}) sending '{}' after ROUTER server start...",
    dealer_id_val,
    String::from_utf8_lossy(dealer_msg_data)
  );
  dealer_socket.send(Msg::from_static(dealer_msg_data)).await?;
  println!("DEALER ({}) sent message successfully.", dealer_id_val);

  // ROUTER receives: [dealer_identity, delimiter, payload]
  println!("ROUTER attempting to receive message parts...");
  let router_recv_id = router_socket.recv().await?;
  let router_recv_delimiter = router_socket.recv().await?; // DEALER prepends empty delimiter
  let router_recv_payload = router_socket.recv().await?;

  println!(
    "ROUTER received ID: {:?}",
    String::from_utf8_lossy(router_recv_id.data().unwrap())
  );
  println!("ROUTER received Delimiter size: {}", router_recv_delimiter.size());
  println!(
    "ROUTER received Payload: '{}'",
    String::from_utf8_lossy(router_recv_payload.data().unwrap())
  );

  assert_eq!(
    router_recv_id.data().unwrap(),
    dealer_id_val.as_bytes(),
    "Router did not receive correct dealer ID"
  );
  assert!(router_recv_id.is_more(), "Identity frame should have MORE flag");
  assert_eq!(router_recv_delimiter.size(), 0, "Delimiter frame should be empty");
  assert!(router_recv_delimiter.is_more(), "Delimiter frame should have MORE flag");
  assert_eq!(
    router_recv_payload.data().unwrap(),
    dealer_msg_data,
    "Router received incorrect payload"
  );
  assert!(
    !router_recv_payload.is_more(),
    "Payload frame should not have MORE flag"
  );

  // ROUTER sends reply: [dealer_identity, delimiter, reply_payload]
  let router_reply_data = b"Reply from ROUTER";
  println!(
    "ROUTER sending reply '{}' to ID {:?}",
    String::from_utf8_lossy(router_reply_data),
    String::from_utf8_lossy(router_recv_id.data().unwrap())
  );

  // send_multipart for ROUTER expects [identity, payload_frames...]
  // The ISocket::send_multipart will handle adding the delimiter internally.
  // However, our test setup uses individual `send` calls.
  // For ROUTER, `send()` expects identity first, then payload parts.
  let mut id_frame_for_reply = Msg::from_bytes(router_recv_id.data_bytes().unwrap());
  id_frame_for_reply.set_flags(MsgFlags::MORE); // Identity has MORE
  router_socket.send(id_frame_for_reply).await?;

  // ZMQ ROUTER, when sending, expects the app to provide the destination identity, then payload.
  // It doesn't automatically add the delimiter after the identity *if you are using the ZMQ_ROUTING_ID*.
  // If we send to a ZMTP peer (like DEALER), the DEALER expects [delimiter, payload].
  // The ROUTER socket, when sending to a known peer (identified by first frame), will typically send the first frame (identity)
  // then an empty delimiter, then the subsequent payload frames.
  // Let's assume rzmq ROUTER's `send` when `current_send_target` is None (first frame)
  // correctly sends the identity, then for subsequent `send` calls, it sends payload *after an implicit delimiter*.
  // Or, if `send_multipart` is used, it should handle it.
  // For this test, let's directly construct what the DEALER expects if the rzmq ROUTER doesn't add the delimiter for us.
  //
  // UPDATE: The current `RouterSocket::send` logic sends the identity frame first (if `current_send_target` is None).
  // Then for subsequent `send` calls (payload parts), it does *not* automatically add a delimiter.
  // This means if a DEALER is on the other side, it will receive [identity, payload1, payload2...].
  // This is fine if the DEALER is also rzmq and handles this (current DEALER expects delimiter).
  //
  // To make it compatible with a standard ZMQ DEALER (or rzmq DEALER expecting delimiter),
  // the ROUTER needs to send: [identity_to_network, empty_delimiter, payload_part1, payload_part2...]
  // The rzmq DEALER receives: [empty_delimiter (stripped), payload_part1 ...]
  //
  // Let's adjust the ROUTER send part to include the delimiter explicitly for this test.
  let mut delimiter_frame_for_reply = Msg::new(); // Empty delimiter
  delimiter_frame_for_reply.set_flags(MsgFlags::MORE);
  router_socket.send(delimiter_frame_for_reply).await?;

  router_socket.send(Msg::from_static(router_reply_data)).await?; // Last part, no MORE
  println!("ROUTER sent reply.");

  // DEALER receives reply: [delimiter (stripped), payload]
  println!("DEALER ({}) attempting to receive reply...", dealer_id_val);
  let dealer_recv_reply = dealer_socket.recv().await?;
  println!(
    "DEALER ({}) received reply: '{}'",
    dealer_id_val,
    String::from_utf8_lossy(dealer_recv_reply.data().unwrap())
  );
  assert_eq!(dealer_recv_reply.data().unwrap(), router_reply_data);

  println!("test_dealer_router_reconnect PASSED");
  Ok(())
}

// Add more tests if needed, or a main function to run them all.
// Example:
// #[tokio::main]
// async fn main() {
//     if let Err(e) = test_push_pull_reconnect().await { eprintln!("PUSH/PULL test failed: {}", e); }
//     if let Err(e) = test_req_rep_reconnect().await { eprintln!("REQ/REP test failed: {}", e); }
//     if let Err(e) = test_dealer_router_reconnect().await { eprintln!("DEALER/ROUTER test failed: {}", e); }
// }
