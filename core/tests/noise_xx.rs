#![cfg(feature = "noise_xx")]

use rzmq::{
  socket::options::{
    NOISE_XX_ENABLED,
    NOISE_XX_REMOTE_STATIC_PUBLIC_KEY,
    NOISE_XX_STATIC_SECRET_KEY,
    RCVHWM,
    RCVTIMEO,
    SNDHWM,
    SNDTIMEO, // Added SNDTIMEO for PUSH consistency
  },
  socket::SocketEvent, // For monitor events if we add them later
  Context,
  Msg,
  SocketType,
  ZmqError,
};
use serial_test::serial;
use std::time::Duration; // To run tests serially as they might use fixed ports

// For key generation
use rand::{rngs::StdRng, SeedableRng};
use x25519_dalek::{PublicKey, StaticSecret}; // Cryptographically secure random number generator

mod common; // Include your test helpers

const SHORT_TIMEOUT: Duration = Duration::from_millis(500); // Slightly increased for CI
const LONG_TIMEOUT: Duration = Duration::from_secs(3); // For potentially slow handshakes/ops
const CONNECT_DELAY: Duration = Duration::from_millis(200); // Time for connect + handshake

// Helper struct to hold a keypair
struct Keypair {
  sk: [u8; 32],
  pk: [u8; 32],
}

impl Keypair {
  fn new() -> Self {
    let static_secret = StaticSecret::random_from_rng(StdRng::from_rng(&mut rand::rng()));
    let public_key = PublicKey::from(&static_secret);
    Self {
      sk: static_secret.to_bytes(),
      pk: public_key.to_bytes(),
    }
  }
}

#[rzmq_macros::test]
#[serial]
async fn test_noise_xx_push_pull_basic_encrypted_exchange() -> Result<(), ZmqError> {
    println!("\n--- Starting test_noise_xx_push_pull_basic_encrypted_exchange ---");
    let ctx = common::test_context(); 

    let server_keys = Keypair::new();
    let client_keys = Keypair::new();
    let endpoint = "tcp://127.0.0.1:5754"; // << NEW PORT FOR CLEANLINESS
    let connect_timeout = Duration::from_secs(3); // Time for connect + handshake

    // --- PULL Server Setup ---
    let pull_server = ctx.socket(SocketType::Pull)?;
    println!("[PULL Server {}] Configuring Noise_XX...", endpoint);
    pull_server.set_option_raw(NOISE_XX_ENABLED, &(1i32).to_ne_bytes()).await?;
    pull_server.set_option_raw(NOISE_XX_STATIC_SECRET_KEY, &server_keys.sk).await?;
    pull_server.set_option_raw(RCVHWM, &(10i32).to_ne_bytes()).await?;
    println!("[PULL Server {}] Binding...", endpoint);
    pull_server.bind(endpoint).await?;
    println!("[PULL Server {}] Bound and listening with Noise_XX.", endpoint);
    // Optional: let mut server_monitor = pull_server.monitor_default().await?;

    // --- PUSH Client Setup ---
    let push_client = ctx.socket(SocketType::Push)?;
    println!("[PUSH Client {}] Configuring Noise_XX...", endpoint);
    push_client.set_option_raw(NOISE_XX_ENABLED, &(1i32).to_ne_bytes()).await?;
    push_client.set_option_raw(NOISE_XX_STATIC_SECRET_KEY, &client_keys.sk).await?;
    push_client.set_option_raw(NOISE_XX_REMOTE_STATIC_PUBLIC_KEY, &server_keys.pk).await?;
    push_client.set_option_raw(SNDHWM, &(10i32).to_ne_bytes()).await?;
    push_client.set_option_raw(SNDTIMEO, &(connect_timeout.as_millis() as i32).to_ne_bytes()).await?;
    
    let mut client_monitor = push_client.monitor_default().await?;
    println!("[PUSH Client {}] Monitor created.", endpoint);

    println!("[PUSH Client {}] Connecting...", endpoint);
    push_client.connect(endpoint).await?;
    println!("[PUSH Client {}] Connect call returned. Waiting for HANDSHAKE SUCCEEDED event...", endpoint);

    // Wait for HandshakeSucceeded event from the client's monitor
    let mut handshake_succeeded = false;
    loop {
        match tokio::time::timeout(connect_timeout + Duration::from_secs(1), client_monitor.recv()).await {
            Ok(Ok(event)) => {
                println!("[PUSH Client {}] Monitor Event: {:?}", endpoint, event);
                if let SocketEvent::HandshakeSucceeded { endpoint: ep } = event {
                    if ep == endpoint || ep.contains(endpoint) { // Check primary or resolved endpoint
                        println!("[PUSH Client {}] Handshake Succeeded event received!", endpoint);
                        handshake_succeeded = true;
                        break;
                    }
                } else if let SocketEvent::HandshakeFailed { endpoint: ep, error_msg } = event {
                     if ep == endpoint || ep.contains(endpoint) {
                        panic!("[PUSH Client {}] Received UNEXPECTED HandshakeFailed: {}", endpoint, error_msg);
                    }
                } else if let SocketEvent::ConnectFailed { endpoint: ep, error_msg } = event {
                    if ep == endpoint || ep.contains(endpoint) {
                        panic!("[PUSH Client {}] Received UNEXPECTED ConnectFailed: {}", endpoint, error_msg);
                    }
                }
            }
            Ok(Err(_recv_err)) => { // Monitor channel closed
                panic!("[PUSH Client {}] Monitor channel closed unexpectedly.", endpoint);
            }
            Err(_timeout_elapsed) => {
                panic!("[PUSH Client {}] Timed out waiting for HandshakeSucceeded event.", endpoint);
            }
        }
    }
    assert!(handshake_succeeded, "Client must receive HandshakeSucceeded event.");
    println!("[SYSTEM {}] Client handshake confirmed successful via monitor.", endpoint);

    // --- Message Exchange ---
    let message_data = b"Secure Hello via Noise_XX!";
    println!("[PUSH Client {}] Sending: \"{}\"", endpoint, String::from_utf8_lossy(message_data));

    match tokio::time::timeout(SHORT_TIMEOUT, push_client.send(Msg::from_static(message_data))).await {
        Ok(Ok(())) => println!("[PUSH Client {}] Send successful.", endpoint),
        Ok(Err(e)) => {
            println!("[PUSH Client {}] Send failed: {}", endpoint, e);
            return Err(e);
        }
        Err(_) => {
            println!("[PUSH Client {}] Send timed out.", endpoint);
            return Err(ZmqError::Timeout);
        }
    }

    println!("[PULL Server {}] Attempting to receive message...", endpoint);
    let received_msg = common::recv_timeout(&pull_server, LONG_TIMEOUT).await?; 

    println!("[PULL Server {}] Received: \"{}\"", endpoint, String::from_utf8_lossy(received_msg.data().unwrap_or_default()));
    assert_eq!(received_msg.data().unwrap_or_default(), message_data);
    assert!(!received_msg.is_more());
    println!("[SYSTEM {}] Secure message exchange successful!", endpoint);

    // --- Teardown ---
    println!("[SYSTEM {}] Closing client and server sockets...", endpoint);
    push_client.close().await?;
    pull_server.close().await?;
    println!("[SYSTEM {}] Sockets closed.", endpoint);

    println!("[SYSTEM {}] Terminating context...", endpoint);
    ctx.term().await?;
    println!("[SYSTEM {}] Context terminated.", endpoint);
    println!("--- Test test_noise_xx_push_pull_basic_encrypted_exchange finished ---");

    Ok(())
}

#[rzmq_macros::test]
#[serial]
async fn test_noise_xx_client_auth_server_pk_mismatch() -> Result<(), ZmqError> {
  println!("\n--- Starting test_noise_xx_client_auth_server_pk_mismatch ---");
  let ctx = common::test_context();

  let server_keys = Keypair::new();
  let client_keys = Keypair::new();
  let fake_server_keys = Keypair::new(); // Different keypair for mismatch

  let endpoint = "tcp://127.0.0.1:5753"; // Use a new, unique port

  // --- PULL Server Setup (Genuine Server) ---
  let pull_server = ctx.socket(SocketType::Pull)?;
  pull_server.set_option_raw(NOISE_XX_ENABLED, &(1i32).to_ne_bytes()).await?;
  pull_server.set_option_raw(NOISE_XX_STATIC_SECRET_KEY, &server_keys.sk).await?;
  // Server doesn't need to know client's PK beforehand for XX responder
  pull_server.bind(endpoint).await?;
  println!("[PULL Server {}] Bound with genuine PK: {:?}", endpoint, server_keys.pk);

  // --- PUSH Client Setup (Expects FAKE Server PK) ---
  let push_client = ctx.socket(SocketType::Push)?;
  push_client.set_option_raw(NOISE_XX_ENABLED, &(1i32).to_ne_bytes()).await?;
  push_client.set_option_raw(NOISE_XX_STATIC_SECRET_KEY, &client_keys.sk).await?;
  push_client.set_option_raw(NOISE_XX_REMOTE_STATIC_PUBLIC_KEY, &fake_server_keys.pk).await?; // Crucial: Expecting the wrong server PK

  // Monitor client events to observe handshake failure
  let mut client_monitor = push_client.monitor_default().await?;
  println!("[PUSH Client {}] Monitor channel created.", endpoint);

  println!("[PUSH Client {}] Connecting (expecting server PK mismatch)...", endpoint);
  // connect() is asynchronous; the actual connection and handshake happen in background actors.
  push_client.connect(endpoint).await?;
  println!("[PUSH Client {}] Connect call returned. Waiting for handshake outcome via monitor...", endpoint);

  // Wait for a monitor event indicating handshake failure or connection drop.
  // The timeout should be generous enough for TCP connect + Noise handshake attempt.
  let client_event_wait_timeout = Duration::from_secs(3); // Increased for reliability
  let mut expected_failure_event_received = false;
  let mut received_error_message = String::new();

  loop {
      match tokio::time::timeout(client_event_wait_timeout, client_monitor.recv()).await {
          Ok(Ok(event)) => { // Successfully received an event from the monitor
              println!("[PUSH Client {}] Monitor Event: {:?}", endpoint, event);
              match event {
                  SocketEvent::HandshakeFailed { endpoint: ep, error_msg } => {
                      if ep == endpoint || ep.contains(endpoint) { // Check primary endpoint or resolved one
                          println!("[PUSH Client {}] Received HandshakeFailed: {}", endpoint, error_msg);
                          assert!(error_msg.contains("Noise decrypt/authentication failed") || 
                                  error_msg.contains("Server public key mismatch") ||
                                  error_msg.contains("Security error"), 
                                  "Error message content mismatch: {}", error_msg);
                          expected_failure_event_received = true;
                          received_error_message = error_msg;
                          break;
                      }
                  }
                  SocketEvent::ConnectFailed { endpoint: ep, error_msg } => {
                      // This can also be a valid outcome if the security failure is reported as a general connect fail
                        if ep == endpoint || ep.contains(endpoint) {
                          println!("[PUSH Client {}] Received ConnectFailed: {}", endpoint, error_msg);
                          assert!(error_msg.contains("Security error") || 
                                  error_msg.contains("Noise"),
                                  "Error message content mismatch: {}", error_msg);
                          expected_failure_event_received = true;
                          received_error_message = error_msg;
                          break; 
                      }
                  }
                  SocketEvent::Disconnected { endpoint: ep } => {
                      // If the client actively disconnects due to the handshake failure.
                      if ep.contains(endpoint) {
                            println!("[PUSH Client {}] Received Disconnected event. This implies handshake failure led to teardown.", endpoint);
                          // This is also a valid failure outcome. We might not get a specific error message.
                          expected_failure_event_received = true;
                          received_error_message = "Disconnected after handshake attempt".to_string();
                          break;
                      }
                  }
                  // We might briefly see 'Connected' before 'HandshakeFailed' if TCP connects then ZMTP handshake starts
                  SocketEvent::Connected { endpoint: ep, .. } => {
                        println!("[PUSH Client {}] Monitor: Saw transient Connected event for {}. Still expecting handshake failure.", endpoint, ep);
                  }
                  _ => {
                      println!("[PUSH Client {}] Monitor: Ignoring event: {:?}", endpoint, event);
                  }
              }
          }
          Ok(Err(_recv_error)) => { // Monitor channel was closed
              panic!("[PUSH Client {}] Monitor channel closed unexpectedly while waiting for failure event.", endpoint);
          }
          Err(_timeout_elapsed) => { // Timeout waiting for a relevant event
              panic!("[PUSH Client {}] Timed out waiting for a HandshakeFailed, ConnectFailed, or Disconnected event. No failure observed.", endpoint);
          }
      }
  }

  assert!(expected_failure_event_received, 
          "Client should have emitted a HandshakeFailed, ConnectFailed (with security error), or Disconnected event due to PK mismatch. Last error: '{}'", received_error_message);
  println!("[PUSH Client {}] Correctly observed connection/handshake failure via monitor: {}", endpoint, received_error_message);

  // Server-side check (optional, but good for sanity):
  // Ensure the server did not consider a session fully established or receive data.
  // It might have seen a TCP connection attempt.
  let recv_result_server = common::recv_timeout(&pull_server, SHORT_TIMEOUT).await;
  assert!(
      matches!(recv_result_server, Err(ZmqError::Timeout)),
      "Server should not receive any message after client's failed handshake, got {:?}", recv_result_server
  );
  println!("[PULL Server {}] Correctly received no message from client.", endpoint);

  // Teardown
  println!("[SYSTEM {}] Closing client and server sockets...", endpoint);
  push_client.close().await?;
  pull_server.close().await?;
  println!("[SYSTEM {}] Sockets closed.", endpoint);

  println!("[SYSTEM {}] Terminating context...", endpoint);
  ctx.term().await?;
  println!("[SYSTEM {}] Context terminated.", endpoint);
  println!("--- Test test_noise_xx_client_auth_server_pk_mismatch finished ---");

  Ok(())
}

// Add more tests:
// - Server authenticates client (if NOISE_XX_ALLOWED_CLIENT_PKS option is added)
// - Handshake with corrupted messages (harder to test without specific test hooks)
// - REQ/REP over Noise_XX
// - DEALER/ROUTER over Noise_XX
// - Test with io_uring engine if it's also updated for Noise_XX support
//   (verifying fallback from ZC/multishot when Noise is active).
