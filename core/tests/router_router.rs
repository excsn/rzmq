mod common;

use rzmq::{Context, Msg, MsgFlags, SocketType, ZmqError, socket::options::ROUTING_ID};
use std::time::Duration;

const LONG_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn test_router_router_tcp_forwarding() -> Result<(), ZmqError> {
  println!("\n--- Starting test_router_router_tcp_forwarding ---");
  let ctx = common::test_context();
  {
    let router_a = ctx.socket(SocketType::Router)?;
    let router_b = ctx.socket(SocketType::Router)?;

    let endpoint = "tcp://127.0.0.1:5652"; // Unique port for this test
    let id_a = "RouterA";
    let id_b = "RouterB";

    println!("[ROUTER A] Setting ROUTING_ID to '{}'", id_a);
    router_a.set_option_raw(ROUTING_ID, id_a.as_bytes()).await?;
    println!("[ROUTER B] Setting ROUTING_ID to '{}'", id_b);
    router_b.set_option_raw(ROUTING_ID, id_b.as_bytes()).await?;

    println!("[ROUTER A] Binding to {}...", endpoint);
    router_a.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind

    println!("[ROUTER B] Connecting to {}...", endpoint);
    router_b.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect + handshake

    // --- Message from A to B ---
    let payload_a_to_b = b"Message from A to B";
    println!("[APP @ A] Sending message to peer '{}'", id_b);

    // Application provides the destination identity and the payload
    let mut dest_id_frame = Msg::from_static(id_b.as_bytes());
    dest_id_frame.set_flags(MsgFlags::MORE);
    let payload_frame = Msg::from_static(payload_a_to_b);

    router_a
      .send_multipart(vec![dest_id_frame, payload_frame])
      .await?;
    println!("[ROUTER A] Sent multipart message.");

    // --- ROUTER B Receives ---
    println!("[ROUTER B] Receiving message...");
    let received_frames_on_b = router_b.recv_multipart().await?;

    println!("[ROUTER B] received {} frames", received_frames_on_b.len());
    assert_eq!(
      received_frames_on_b.len(),
      2, // CORRECT: Expecting [sender_id, payload]
      "ROUTER B application should receive 2 frames (sender_id, payload)"
    );
    let sender_identity = &received_frames_on_b[0];
    let payload = &received_frames_on_b[1];

    assert_eq!(
      sender_identity.data().unwrap(),
      id_a.as_bytes(),
      "The first frame should be the identity of the sender (RouterA)"
    );
    assert!(sender_identity.is_more());

    assert_eq!(
      payload.data().unwrap(),
      payload_a_to_b,
      "The second frame should be the payload"
    );
    assert!(!payload.is_more());

    println!("[APP @ B] Received message correctly from peer '{}'.", id_a);
  }

  println!("[SYS] Terminating context...");
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

/// This test verifies a 3-hop ROUTER chain: A -> B -> C.
/// It confirms that an intermediate ROUTER (B) can act as a message forwarder.
///
/// Topology:
///   - ROUTER A (sender) connects to ROUTER B.
///   - ROUTER B (broker/forwarder) binds, accepting connections.
///   - ROUTER C (receiver) connects to ROUTER B.
///
/// Message Flow (Corrected based on libzmq):
/// 1. App @ A wants to send [payload] to C. It must tell its socket to route the message to peer B.
///    The message it wants B to receive is [identity_of_C, payload].
///    Therefore, App @ A calls send_multipart with: [identity_of_B(MORE), identity_of_C(MORE), payload].
/// 2. ROUTER A (socket) strips the first frame (B) for routing and sends the rest on the wire to B:
///    Wire format A->B: [identity_of_C(MORE), payload].
/// 3. ROUTER B receives this from the connection associated with A. Its socket logic prepends A's identity.
///    App @ B receives: [identity_of_A(MORE), identity_of_C(MORE), payload].
/// 4. App @ B (the forwarder) inspects the message. It sees the destination is C (frame 2).
///    It forwards the message by calling send_multipart: [identity_of_C(MORE), identity_of_A(MORE), payload].
/// 5. ROUTER B (socket) strips the first frame (C) for routing and sends the rest on the wire to C:
///    Wire format B->C: [identity_of_A(MORE), payload].
/// 6. ROUTER C receives this from the connection associated with B. Its socket logic prepends B's identity.
///    App @ C receives: [identity_of_B(MORE), identity_of_A(MORE), payload].
#[tokio::test]
async fn test_router_chain_forwarding() -> Result<(), ZmqError> {
  println!("\n--- Starting test_router_chain_forwarding ---");
  let ctx = common::test_context();
  {
    let router_a = ctx.socket(SocketType::Router)?;
    let router_b = ctx.socket(SocketType::Router)?;
    let router_c = ctx.socket(SocketType::Router)?;

    let endpoint_b = "tcp://127.0.0.1:5653"; // Broker's endpoint
    let id_a = "RouterA";
    let id_b = "RouterB";
    let id_c = "RouterC";

    // --- Configure Identities ---
    println!("[ROUTER A] Setting ROUTING_ID to '{}'", id_a);
    router_a.set_option_raw(ROUTING_ID, id_a.as_bytes()).await?;
    println!("[ROUTER B] Setting ROUTING_ID to '{}'", id_b);
    router_b.set_option_raw(ROUTING_ID, id_b.as_bytes()).await?;
    println!("[ROUTER C] Setting ROUTING_ID to '{}'", id_c);
    router_c.set_option_raw(ROUTING_ID, id_c.as_bytes()).await?;

    // --- Establish Topology ---
    println!("[ROUTER B] Binding to {}...", endpoint_b);
    router_b.bind(endpoint_b).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("[ROUTER A] Connecting to {}...", endpoint_b);
    router_a.connect(endpoint_b).await?;
    println!("[ROUTER C] Connecting to {}...", endpoint_b);
    router_c.connect(endpoint_b).await?;
    tokio::time::sleep(Duration::from_millis(150)).await; // Allow all connections

    // --- Forwarder Logic for ROUTER B ---
    let router_b_task = tokio::spawn(async move {
      println!("[APP @ B] Forwarder task started. Waiting for message...");
      let frames = router_b
        .recv_multipart()
        .await
        .expect("Broker failed to receive");
      println!("[APP @ B] Received {} frames. Forwarding...", frames.len());

      // Expected frames at broker: [sender_id(A), final_dest_id(C), original_payload]
      assert_eq!(frames.len(), 3, "Broker should receive 3 frames");
      let sender_id = &frames[0];
      let final_dest_id = &frames[1];
      let original_payload = &frames[2];

      assert_eq!(sender_id.data().unwrap(), id_a.as_bytes());
      assert_eq!(final_dest_id.data().unwrap(), id_c.as_bytes());

      // Forward to C. The message to send tells our socket to route to C,
      // and the payload will contain the original sender's ID (A) for C to see.
      let mut forward_envelope = Vec::new();
      forward_envelope.push(final_dest_id.clone()); // Frame 1: Destination for *this* send (Router C)
      forward_envelope.push(sender_id.clone()); // Frame 2: Part of the payload for C
      forward_envelope.push(original_payload.clone()); // Frame 3: Part of the payload for C

      router_b
        .send_multipart(forward_envelope)
        .await
        .expect("Broker failed to send");
      println!("[APP @ B] Message forwarded to C.");
    });

    // --- App @ A sends a message targeted at C, routed via B ---
    let payload_from_a = b"A->B->C";
    println!(
      "[APP @ A] Sending message to final destination '{}' via peer '{}'",
      id_c, id_b
    );

    // Frame 1: The direct peer to send to (the broker, B)
    let mut peer_dest_frame = Msg::from_static(id_b.as_bytes());
    peer_dest_frame.set_flags(MsgFlags::MORE);

    // Frame 2: The final destination for the broker to use (C)
    let mut final_dest_frame = Msg::from_static(id_c.as_bytes());
    final_dest_frame.set_flags(MsgFlags::MORE);

    // Frame 3: The actual payload
    let payload_frame = Msg::from_static(payload_from_a);

    // Give the forwarder task a moment to enter its `recv` await
    tokio::time::sleep(Duration::from_millis(50)).await;

    router_a
      .send_multipart(vec![peer_dest_frame, final_dest_frame, payload_frame])
      .await?;
    println!("[APP @ A] Message sent.");

    // --- App @ C receives the forwarded message ---
    println!("[APP @ C] Receiving forwarded message...");
    let received_frames_on_c = router_c.recv_multipart().await?;
    println!("[APP @ C] Received {} frames.", received_frames_on_c.len());

    // Expected frames at C: [broker_id(B), original_sender_id(A), original_payload]
    assert_eq!(
      received_frames_on_c.len(),
      3,
      "ROUTER C should receive 3 frames (broker_id, original_sender_id, payload)"
    );

    let broker_id_frame = &received_frames_on_c[0];
    let original_sender_id_frame = &received_frames_on_c[1];
    let final_payload_frame = &received_frames_on_c[2];

    assert_eq!(
      broker_id_frame.data().unwrap(),
      id_b.as_bytes(),
      "Frame 0 should be broker's ID"
    );
    assert_eq!(
      original_sender_id_frame.data().unwrap(),
      id_a.as_bytes(),
      "Frame 1 should be original sender's ID"
    );
    assert_eq!(
      final_payload_frame.data().unwrap(),
      payload_from_a,
      "Frame 2 should be the payload"
    );

    println!(
      "[APP @ C] Correctly received forwarded message from '{}' via '{}'",
      id_a, id_b
    );

    // Wait for the forwarder task to complete
    router_b_task.await.expect("Forwarder task panicked");
  }

  println!("[SYS] Terminating context...");
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}
