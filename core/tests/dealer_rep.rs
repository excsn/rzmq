mod common;

use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);

/// This test verifies the interaction between a DEALER (client) and a REP (server).
///
/// The expected message flow is:
/// 1. DEALER sends [payload]. The socket implementation prepends an empty delimiter frame,
///    so the wire format is [empty_delimiter(MORE), payload(NOMORE)].
/// 2. REP receives the message. Its socket implementation processes the envelope (the empty delimiter)
///    and presents only the payload to the application via `recv()`.
/// 3. REP sends [reply_payload]. The socket implementation prepends the saved routing envelope,
///    so the wire format is [empty_delimiter(MORE), reply_payload(NOMORE)].
/// 4. DEALER receives the message. Its socket implementation strips the empty delimiter
///    and presents only the reply_payload to the application via `recv()`.
#[tokio::test]
async fn test_dealer_rep_tcp_basic() -> Result<(), ZmqError> {
  println!("\n--- Starting test_dealer_rep_tcp_basic ---");
  let ctx = common::test_context();
  {
    let dealer = ctx.socket(SocketType::Dealer)?;
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = "tcp://127.0.0.1:5650"; // Unique port for this test

    println!("[REP] Binding to {}...", endpoint);
    rep.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind

    println!("[DEALER] Connecting to {}...", endpoint);
    dealer.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect + handshake

    // DEALER sends a request
    let request_payload = b"Request from DEALER";
    println!("[DEALER] Sending request: '{}'", String::from_utf8_lossy(request_payload));
    dealer.send(Msg::from_static(request_payload)).await?;

    // REP receives the request (should only see the payload)
    println!("[REP] Receiving request...");
    let received_req = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(
      received_req.data().unwrap(),
      request_payload,
      "REP socket did not receive the correct payload from DEALER"
    );
    assert!(!received_req.is_more(), "Payload received by REP should not have MORE flag");
    println!("[REP] Received request correctly.");

    // REP sends a reply
    let reply_payload = b"Reply from REP";
    println!("[REP] Sending reply: '{}'", String::from_utf8_lossy(reply_payload));
    rep.send(Msg::from_static(reply_payload)).await?;

    // DEALER receives the reply (should only see the payload)
    println!("[DEALER] Receiving reply...");
    let received_reply = common::recv_timeout(&dealer, LONG_TIMEOUT).await?;
    assert_eq!(
      received_reply.data().unwrap(),
      reply_payload,
      "DEALER socket did not receive the correct reply payload from REP"
    );
    assert!(!received_reply.is_more(), "Payload received by DEALER should not have MORE flag");
    println!("[DEALER] Received reply correctly.");
  }
  
  println!("[SYS] Terminating context...");
  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}