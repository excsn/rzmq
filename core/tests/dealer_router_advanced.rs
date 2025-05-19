use common::wait_for_monitor_event;
use rzmq::socket::options::ROUTER_MANDATORY;
use rzmq::socket::SocketEvent;
use rzmq::{Msg, MsgFlags, SocketType, ZmqError};
use serial_test::serial;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::task;

mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(250); // Slightly longer short timeout
const LONG_TIMEOUT: Duration = Duration::from_secs(2);
const MONITOR_EVENT_TIMEOUT: Duration = Duration::from_secs(3);

// --- Test: Multiple Dealers ---
#[rzmq_macros::test]
#[serial]
async fn test_dealer_router_multiple_dealers() -> Result<(), ZmqError> {
  println!("Starting test_dealer_router_multiple_dealers...");
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let endpoint = "tcp://127.0.0.1:5610"; // Unique port

  println!("Binding ROUTER to {}...", endpoint);
  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind

  let num_dealers = 3;
  let mut dealer_sockets = Vec::new();
  let mut task_handles = Vec::new();

  // Use a barrier to synchronize dealer sends
  let barrier = Arc::new(Barrier::new(num_dealers));

  for i in 0..num_dealers {
    let dealer = ctx.socket(SocketType::Dealer)?;
    let endpoint_clone = endpoint.to_string();
    let barrier_clone = barrier.clone();

    let handle = task::spawn(async move {
      println!("Dealer {} connecting...", i);
      dealer.connect(&endpoint_clone).await?;
      tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect

      // Wait for all dealers to connect before sending
      println!("Dealer {} waiting at barrier...", i);
      barrier_clone.wait().await;

      let msg_payload = format!("Hello from Dealer {}", i);
      println!("Dealer {} sending: {}", i, msg_payload);
      dealer.send(Msg::from_vec(msg_payload.into_bytes())).await?;
      println!("Dealer {} sent.", i);

      // Return the dealer socket for receiving later
      Ok::<_, ZmqError>(dealer)
    });
    task_handles.push(handle);
  }

  // Wait for dealer tasks to finish connecting and sending
  for handle in task_handles {
    let dealer = handle.await.expect("Dealer task panicked")?; // Propagate ZmqError
    dealer_sockets.push(dealer);
  }
  println!("All dealers connected and sent messages.");

  // Router receives messages
  let mut received_messages = HashMap::new(); // Store Identity -> Payload
  let mut received_identities = HashSet::new();

  println!("ROUTER receiving messages...");
  for _ in 0..num_dealers {
    let id_frame = common::recv_timeout(&router, LONG_TIMEOUT).await?;
    let payload_frame = common::recv_timeout(&router, SHORT_TIMEOUT).await?;

    assert!(id_frame.is_more());
    let identity = id_frame.data().unwrap().to_vec();
    assert!(!identity.is_empty());
    assert!(!payload_frame.is_more());
    let payload = payload_frame.data().unwrap().to_vec();

    println!(
      "ROUTER received from ID {:?}: {}",
      identity,
      String::from_utf8_lossy(&payload)
    );

    assert!(
      received_identities.insert(identity.clone()),
      "Received duplicate identity!"
    );
    received_messages.insert(identity, payload);
  }
  println!("ROUTER received all messages.");
  assert_eq!(received_messages.len(), num_dealers);

  // Router replies specifically to Dealer 1 (assuming index 1 exists)
  let target_dealer_index: usize = 1;
  let target_identity = received_identities
    .iter()
    .find(|id| {
      received_messages
        .get(*id)
        .map(|p| String::from_utf8_lossy(p) == format!("Hello from Dealer {}", target_dealer_index))
        .unwrap_or(false)
    })
    .expect("Could not find identity for Dealer 1")
    .clone();

  println!(
    "ROUTER replying to Dealer {} (ID: {:?})...",
    target_dealer_index, target_identity
  );
  let mut id_reply = Msg::from_vec(target_identity);
  id_reply.set_flags(MsgFlags::MORE);
  router.send(id_reply).await?;
  router.send(Msg::from_static(b"Reply to Dealer 1")).await?;
  println!("ROUTER sent reply to Dealer 1.");

  // Check which dealer receives
  let mut received_reply_count = 0;
  for (i, dealer) in dealer_sockets.iter().enumerate() {
    match common::recv_timeout(dealer, SHORT_TIMEOUT).await {
      Ok(msg) => {
        println!(
          "Dealer {} received reply: {}",
          i,
          String::from_utf8_lossy(msg.data().unwrap())
        );
        assert_eq!(i, target_dealer_index, "Incorrect dealer received reply");
        assert_eq!(msg.data().unwrap(), b"Reply to Dealer 1");
        received_reply_count += 1;
      }
      Err(ZmqError::Timeout) => {
        println!("Dealer {} correctly timed out.", i);
        assert_ne!(i, target_dealer_index, "Target dealer should not have timed out");
      }
      Err(e) => return Err(e), // Propagate other errors
    }
  }
  assert_eq!(received_reply_count, 1, "Exactly one dealer should receive the reply");
  println!("Verified targeted reply.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_dealer_router_multiple_dealers finished.");
  Ok(())
}

// --- Test: ROUTER_MANDATORY = false (Default) ---
#[rzmq_macros::test]
#[serial]
async fn test_dealer_router_router_sends_to_unknown_mandatory_false() -> Result<(), ZmqError> {
  println!("Starting test_dealer_router_router_sends_to_unknown_mandatory_false...");
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let dealer = ctx.socket(SocketType::Dealer)?;
  let endpoint = "tcp://127.0.0.1:5611"; // Unique port

  println!("Binding ROUTER to {}...", endpoint);
  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Connecting DEALER to {}...", endpoint);
  dealer.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Send a message just to ensure connection is up and router knows dealer ID
  dealer.send(Msg::from_static(b"Ping")).await?;
  let ping_id = common::recv_timeout(&router, LONG_TIMEOUT).await?;
  let _ping_payload = common::recv_timeout(&router, SHORT_TIMEOUT).await?;
  println!("ROUTER received initial ping from ID: {:?}", ping_id.data().unwrap());

  // ROUTER attempts to send to an unknown identity
  let unknown_id_data = b"NoSuchDealer";
  let mut id_msg = Msg::from_static(unknown_id_data);
  id_msg.set_flags(MsgFlags::MORE);
  let payload_msg_data = b"PayloadToNowhere";

  println!(
    "ROUTER sending to unknown ID {:?} (mandatory=false, identity frame)...",
    unknown_id_data
  );
  
  let send_id_res = router.send(id_msg).await;
  assert!(
    send_id_res.is_ok(),
    "Expected Ok(()) (silent drop) sending identity to unknown ID (mandatory=false), got {:?}",
    send_id_res
  );

  println!("ROUTER correctly returned Ok (identity frame dropped).");

  // Attempt to send payload. Since current_target_guard is None, this will be treated
  // as an attempt to send an identity. If it's not MORE, it's an error.
  // If it IS MORE, it will be treated as another identity send and also dropped.
  // To be robust, let's assume the user *would* follow protocol and send payload after identity.
  // After the previous send_id_res was Ok(()), current_send_target in RouterSocket is still None.
  // So, trying to send a non-MORE payload frame now would be an InvalidMessage error.
  // If the user tries to send another MORE frame (as if it's a new identity), that will also be Ok(())/dropped.
  // Let's test the scenario where the payload *would* have been sent if the ID was known.
  // The first send attempt for the identity has already returned Ok(()).
  // If the user then tries to send the payload part:
  let payload_to_send = Msg::from_static(payload_msg_data);
  println!(
    "ROUTER sending payload to (still) unknown ID (mandatory=false, payload frame)..."
  );
  let send_payload_res = router.send(payload_to_send).await;
   
  assert!(
    matches!(send_payload_res, Err(ZmqError::InvalidMessage(_))),
    "Expected InvalidMessage sending payload without MORE after identity was dropped (mandatory=false), got {:?}",
    send_payload_res
  );
  println!("ROUTER correctly returned InvalidMessage for subsequent payload part.");

  // DEALER attempts recv - should timeout as message was dropped
  println!("DEALER attempting recv (should timeout)...");
  let recv_result = common::recv_timeout(&dealer, LONG_TIMEOUT).await;
  assert!(
    matches!(recv_result, Err(ZmqError::Timeout)),
    "Expected Timeout error for DEALER, got {:?}",
    recv_result
  );
  println!("DEALER correctly timed out.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_dealer_router_router_sends_to_unknown_mandatory_false finished.");
  Ok(())
}

// --- Test: ROUTER_MANDATORY = true ---
#[rzmq_macros::test]
#[serial]
async fn test_dealer_router_router_sends_to_unknown_mandatory_true() -> Result<(), ZmqError> {
  println!("Starting test_dealer_router_router_sends_to_unknown_mandatory_true...");
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let dealer = ctx.socket(SocketType::Dealer)?;
  let endpoint = "tcp://127.0.0.1:5612"; // Unique port

  println!("Binding ROUTER to {}...", endpoint);
  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Setting ROUTER_MANDATORY=true...");
  router.set_option(ROUTER_MANDATORY, &(1i32).to_ne_bytes()).await?; // Set to true

  println!("Connecting DEALER to {}...", endpoint);
  dealer.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Send a message just to ensure connection is up
  dealer.send(Msg::from_static(b"Ping")).await?;
  let _ = common::recv_timeout(&router, LONG_TIMEOUT).await?; // receive ID
  let _ = common::recv_timeout(&router, SHORT_TIMEOUT).await?; // receive Payload

  // ROUTER attempts to send to an unknown identity
  let unknown_id = Msg::from_static(b"NonExistent");
  let mut id_msg = unknown_id.clone();
  id_msg.set_flags(MsgFlags::MORE);

  println!(
    "ROUTER sending to unknown ID {:?} (mandatory=true)...",
    unknown_id.data().unwrap()
  );
  let send_id_res = router.send(id_msg).await;
  println!("ROUTER send result: {:?}", send_id_res);

  // Send should fail immediately when mandatory=true and ID is unknown
  assert!(
    matches!(send_id_res, Err(ZmqError::HostUnreachable(_))),
    "Expected HostUnreachable error, got {:?}",
    send_id_res
  );
  println!("ROUTER correctly failed with HostUnreachable.");

  // Attempting to send payload should also fail or be irrelevant
  // let send_payload_res = router.send(Msg::from_static(b"Payload")).await;
  // assert!(send_payload_res.is_err()); // Should likely fail

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_dealer_router_router_sends_to_unknown_mandatory_true finished.");
  Ok(())
}

// --- Test: Dealer disconnects, Router tries to send (Using Monitor) ---
#[rzmq_macros::test]
#[serial]
async fn test_dealer_router_dealer_disconnects_router_sends() -> Result<(), ZmqError> {
  println!("Starting test_dealer_router_dealer_disconnects_router_sends (Monitor)...");
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let endpoint = "tcp://127.0.0.1:5613"; // Unique port

  println!("Setting up monitor for ROUTER...");
  let monitor_rx = router.monitor_default().await?;
  println!("Monitor setup.");

  println!("Binding ROUTER to {}...", endpoint);
  router.bind(endpoint).await?;
  
  println!("ROUTER waiting for Listening event...");
  wait_for_monitor_event(
    &monitor_rx,
    MONITOR_EVENT_TIMEOUT,
    SHORT_TIMEOUT,
    |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == endpoint),
  )
  .await
  .map_err(|e| ZmqError::Internal(format!("Bind event wait failed: {}", e)))?;
  println!("ROUTER received Listening event.");
  // Short sleep after bind still sometimes helpful, though event is better
  tokio::time::sleep(Duration::from_millis(10)).await;

  let dealer_identity: Vec<u8>;
  let mut dealer_endpoint_uri = "".to_string(); // Store the specific endpoint URI seen by router

  {
    // Scope for the dealer
    let dealer = ctx.socket(SocketType::Dealer)?;
    println!("Connecting DEALER to {}...", endpoint);
    dealer.connect(endpoint).await?;

    println!("ROUTER waiting for Accepted/Handshake event...");
    let connected_event = wait_for_monitor_event(&monitor_rx, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
      matches!(e, SocketEvent::Accepted { .. } | SocketEvent::HandshakeSucceeded { .. })
    })
    .await
    .map_err(|e| ZmqError::Internal(format!("Connect event wait failed: {}", e)))?;
    // Store the endpoint URI reported by the router
    match connected_event {
      SocketEvent::Accepted { endpoint: _, peer_addr } => dealer_endpoint_uri = format!("tcp://{}", peer_addr), // Should be tcp://<peer_ip>:<peer_port>
      SocketEvent::HandshakeSucceeded { endpoint: ep } => dealer_endpoint_uri = ep,
      _ => unreachable!(),
    }
    println!("ROUTER received connection event for: {}", dealer_endpoint_uri);

    // Dealer sends a message so Router learns its identity
    println!("DEALER sending initial message...");
    dealer.send(Msg::from_static(b"Identify Me")).await?;

    // Router receives to get the identity
    println!("ROUTER receiving dealer identity...");
    let id_frame = common::recv_timeout(&router, LONG_TIMEOUT).await?;
    let _payload = common::recv_timeout(&router, SHORT_TIMEOUT).await?;
    dealer_identity = id_frame.data().unwrap().to_vec();
    println!("ROUTER learned dealer ID: {:?}", dealer_identity);

    println!("DEALER closing...");
    let _ = dealer.close().await;

    println!("DEALER disconnecting (going out of scope)...");
    // Dealer socket is dropped here
  } // dealer dropped

  println!("ROUTER waiting for Disconnected event for {}...", dealer_endpoint_uri);
  wait_for_monitor_event(
    &monitor_rx,
    MONITOR_EVENT_TIMEOUT,
    SHORT_TIMEOUT,
    |e| matches!(e, SocketEvent::Disconnected { endpoint: ep } if *ep == dealer_endpoint_uri),
  )
  .await
  .map_err(|e| ZmqError::Internal(format!("Disconnect event wait failed: {}", e)))?;
  println!("ROUTER received Disconnected event.");

  // Router attempts to send to the now-disconnected dealer
  let mut id_msg = Msg::from_vec(dealer_identity.clone());
  id_msg.set_flags(MsgFlags::MORE);
  let _payload_msg = Msg::from_static(b"Are you still there?"); // Not used if first send fails

  println!("ROUTER sending to disconnected ID {:?}...", dealer_identity);
  let send_id_res = router.send(id_msg).await;
  println!("ROUTER send ID result: {:?}", send_id_res);

  // Default router_mandatory is false. After disconnect, identity is unknown.
  assert!(
    send_id_res.is_ok(),
    "Expected Ok(()) (silent drop) sending ID after disconnect (mandatory=false), got {:?}",
    send_id_res
  );
  println!("ROUTER correctly returned Ok (message to disconnected DEALER dropped).");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_dealer_router_dealer_disconnects_router_sends (Monitor) finished.");
  Ok(())
}

// --- Test: Multi-part message from Dealer -> Router ---
#[rzmq_macros::test]
#[serial]
async fn test_dealer_router_multi_part_dealer_to_router() -> Result<(), ZmqError> {
  println!("Starting test_dealer_router_multi_part_dealer_to_router...");
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let dealer = ctx.socket(SocketType::Dealer)?;
  let endpoint = "tcp://127.0.0.1:5614"; // Unique port

  println!("Binding ROUTER to {}...", endpoint);
  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Connecting DEALER to {}...", endpoint);
  dealer.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Dealer sends multi-part message
  let mut msg1 = Msg::from_static(b"Part1");
  msg1.set_flags(MsgFlags::MORE);
  let mut msg2 = Msg::from_static(b"Part2");
  msg2.set_flags(MsgFlags::MORE);
  let msg3 = Msg::from_static(b"Part3"); // Last part, no MORE flag

  println!("DEALER sending multi-part message...");
  dealer.send(msg1).await?;
  dealer.send(msg2).await?;
  dealer.send(msg3).await?;
  println!("DEALER sent multi-part message.");

  // Router receives: [identity(MORE), payload1(MORE), payload2(MORE), payload3(false)]
  println!("ROUTER receiving multi-part message...");
  let id_frame = common::recv_timeout(&router, LONG_TIMEOUT).await?;
  let p1_frame = common::recv_timeout(&router, SHORT_TIMEOUT).await?;
  let p2_frame = common::recv_timeout(&router, SHORT_TIMEOUT).await?;
  let p3_frame = common::recv_timeout(&router, SHORT_TIMEOUT).await?;

  // Verify Identity frame
  assert!(id_frame.is_more(), "Identity frame missing MORE flag");
  assert!(!id_frame.data().unwrap().is_empty(), "Identity frame is empty");
  let _dealer_id = id_frame.data().unwrap(); // Can optionally store/print ID

  // Verify Payload frames
  assert!(p1_frame.is_more(), "Part 1 frame missing MORE flag");
  assert_eq!(p1_frame.data().unwrap(), b"Part1");

  assert!(p2_frame.is_more(), "Part 2 frame missing MORE flag");
  assert_eq!(p2_frame.data().unwrap(), b"Part2");

  assert!(!p3_frame.is_more(), "Part 3 frame should not have MORE flag");
  assert_eq!(p3_frame.data().unwrap(), b"Part3");

  println!("ROUTER correctly received all parts with correct flags.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_dealer_router_multi_part_dealer_to_router finished.");
  Ok(())
}

// --- Test: Multi-part message from Router -> Dealer ---
#[rzmq_macros::test]
#[serial]
async fn test_dealer_router_multi_part_router_to_dealer() -> Result<(), ZmqError> {
  println!("Starting test_dealer_router_multi_part_router_to_dealer...");
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let dealer = ctx.socket(SocketType::Dealer)?;
  let endpoint = "tcp://127.0.0.1:5615"; // Unique port

  println!("Binding ROUTER to {}...", endpoint);
  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Connecting DEALER to {}...", endpoint);
  dealer.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Dealer sends request so router knows its identity
  println!("DEALER sending request...");
  dealer.send(Msg::from_static(b"Request for multi-part")).await?;

  // Router receives request
  println!("ROUTER receiving request...");
  let id_frame = common::recv_timeout(&router, LONG_TIMEOUT).await?;
  let _req_payload = common::recv_timeout(&router, SHORT_TIMEOUT).await?;
  assert!(id_frame.is_more());
  let dealer_identity = id_frame.data().unwrap().to_vec(); // Store identity
  println!("ROUTER received request from ID: {:?}", dealer_identity);

  // Router sends multi-part reply: [identity(MORE), reply1(MORE), reply2(false)]
  println!("ROUTER sending multi-part reply...");
  let mut id_reply = Msg::from_vec(dealer_identity);
  id_reply.set_flags(MsgFlags::MORE);
  let mut rep1 = Msg::from_static(b"ReplyPart1");
  rep1.set_flags(MsgFlags::MORE);
  let rep2 = Msg::from_static(b"ReplyPart2"); // Last part

  router.send(id_reply).await?;
  router.send(rep1).await?;
  router.send(rep2).await?;
  println!("ROUTER sent multi-part reply.");

  // Dealer receives: [reply1(MORE), reply2(false)] (identity stripped)
  println!("DEALER receiving multi-part reply...");
  let rec_rep1 = common::recv_timeout(&dealer, LONG_TIMEOUT).await?;
  let rec_rep2 = common::recv_timeout(&dealer, SHORT_TIMEOUT).await?;

  // Verify received parts
  assert!(rec_rep1.is_more(), "Reply Part 1 missing MORE flag");
  assert_eq!(rec_rep1.data().unwrap(), b"ReplyPart1");

  assert!(!rec_rep2.is_more(), "Reply Part 2 should not have MORE flag");
  assert_eq!(rec_rep2.data().unwrap(), b"ReplyPart2");

  println!("DEALER correctly received all reply parts with correct flags.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_dealer_router_multi_part_router_to_dealer finished.");
  Ok(())
}
