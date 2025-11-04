mod common;

use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;

const LONG_TIMEOUT: Duration = Duration::from_secs(2);

/// This test verifies bidirectional communication between two DEALER sockets.
/// This pattern is for fully asynchronous, peer-to-peer messaging.
///
/// The expected message flow is:
/// 1. DEALER A sends [payload_A]. The socket implementation prepends an empty delimiter,
///    so the wire format is [empty_delimiter(MORE), payload_A(NOMORE)].
/// 2. DEALER B receives the message. Its implementation strips the delimiter and presents
///    only [payload_A] to the application.
/// 3. Communication can happen in the other direction at any time, following the same logic.
#[tokio::test]
async fn test_dealer_dealer_tcp_bidirectional() -> Result<(), ZmqError> {
  println!("\n--- Starting test_dealer_dealer_tcp_bidirectional ---");
  let ctx = common::test_context();
  {
    let dealer_a = ctx.socket(SocketType::Dealer)?;
    let dealer_b = ctx.socket(SocketType::Dealer)?;
    let endpoint = "tcp://127.0.0.1:5651"; // Unique port for this test

    println!("[DEALER A] Binding to {}...", endpoint);
    dealer_a.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind

    println!("[DEALER B] Connecting to {}...", endpoint);
    dealer_b.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect + handshake

    // --- Communication from A to B ---
    let msg_a_to_b = b"Message from A to B";
    println!("[DEALER A] Sending: '{}'", String::from_utf8_lossy(msg_a_to_b));
    dealer_a.send(Msg::from_static(msg_a_to_b)).await?;

    println!("[DEALER B] Receiving...");
    let received_on_b = common::recv_timeout(&dealer_b, LONG_TIMEOUT).await?;
    assert_eq!(
      received_on_b.data().unwrap(),
      msg_a_to_b,
      "DEALER B did not receive the correct message from A"
    );
    println!("[DEALER B] Received correctly.");

    // --- Communication from B to A ---
    let msg_b_to_a = b"Message from B to A";
    println!("[DEALER B] Sending: '{}'", String::from_utf8_lossy(msg_b_to_a));
    dealer_b.send(Msg::from_static(msg_b_to_a)).await?;

    println!("[DEALER A] Receiving...");
    let received_on_a = common::recv_timeout(&dealer_a, LONG_TIMEOUT).await?;
    assert_eq!(
      received_on_a.data().unwrap(),
      msg_b_to_a,
      "DEALER A did not receive the correct message from B"
    );
    println!("[DEALER A] Received correctly.");
  }
  
  println!("[SYS] Terminating context...");
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}