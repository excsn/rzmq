// tests/dealer_router.rs

use rzmq::{Context, Msg, MsgFlags, SocketType, ZmqError};
use std::collections::HashSet;
use std::time::Duration;
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);

// --- TCP Tests ---

#[rzmq_macros::test]
async fn test_dealer_router_tcp_basic() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let dealer = ctx.socket(SocketType::Dealer)?;
    let router = ctx.socket(SocketType::Router)?;
    let endpoint = "tcp://127.0.0.1:5565"; // Unique port

    router.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind

    dealer.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect + handshake

    // DEALER -> ROUTER
    println!("DEALER sending request");
    dealer.send(Msg::from_static(b"RequestPayload")).await?;

    // ROUTER receives [identity, payload]
    println!("ROUTER waiting for message");
    let id_frame = common::recv_timeout(&router, LONG_TIMEOUT).await?;
    let payload_frame = common::recv_timeout(&router, SHORT_TIMEOUT).await?;
    assert!(id_frame.is_more(), "Identity frame should have MORE flag");
    assert!(
      !id_frame.data().unwrap().is_empty(),
      "Identity frame should not be empty"
    );
    assert!(!payload_frame.is_more(), "Payload frame should not have MORE flag");
    assert_eq!(payload_frame.data().unwrap(), b"RequestPayload");
    println!("ROUTER received identity + payload");

    let received_identity = id_frame.data().unwrap().to_vec(); // Save identity

    // ROUTER -> DEALER
    println!("ROUTER sending reply");
    let mut id_reply = Msg::from_vec(received_identity);
    id_reply.set_flags(MsgFlags::MORE); // Set MORE on identity
    router.send(id_reply).await?; // Send identity first
    router.send(Msg::from_static(b"ReplyPayload")).await?; // Send payload

    // DEALER receives [payload] (identity/delimiter stripped by socket)
    println!("DEALER waiting for reply");
    let reply_payload = common::recv_timeout(&dealer, LONG_TIMEOUT).await?;

    // --- Verification Needed: Does DealerSocket::recv strip identity/delimiter? ---
    // Based on current impl analysis: Dealer recv pulls frames from FairQueue.
    // Router SEND does NOT add the empty delimiter.
    // Router RECV prepends identity.
    // Dealer SEND prepends delimiter.
    // Dealer RECV -> FairQueue gets all frames.
    // Let's assume for now the test needs to see both identity+payload from router.
    // **Update: Dealer SEND adds delimiter, Router RECV consumes it.
    //           Router SEND adds identity, Dealer RECV consumes it.
    //           So Dealer should receive only the payload!**
    // Let's test this assumption.

    assert_eq!(reply_payload.data().unwrap(), b"ReplyPayload");
    assert!(!reply_payload.is_more(), "Dealer reply should not have MORE flag");
    println!("DEALER received reply payload");
  }
  ctx.term().await?;
  Ok(())
}

// --- IPC Tests ---

#[rzmq_macros::test]
#[cfg(feature = "ipc")]
async fn test_dealer_router_ipc_basic() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let dealer = ctx.socket(SocketType::Dealer)?;
    let router = ctx.socket(SocketType::Router)?;
    let endpoint = common::unique_ipc_endpoint();
    println!("Using IPC endpoint: {}", endpoint);

    router.bind(&endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    dealer.connect(&endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // DEALER -> ROUTER
    dealer.send(Msg::from_static(b"IPC Request")).await?;

    // ROUTER receives [identity, payload]
    let id_frame = common::recv_timeout(&router, LONG_TIMEOUT).await?;
    let payload_frame = common::recv_timeout(&router, SHORT_TIMEOUT).await?;
    assert!(id_frame.is_more());
    assert!(!id_frame.data().unwrap().is_empty());
    assert!(!payload_frame.is_more());
    assert_eq!(payload_frame.data().unwrap(), b"IPC Request");

    let received_identity = id_frame.data().unwrap().to_vec();

    // ROUTER -> DEALER
    let mut id_reply = Msg::from_vec(received_identity);
    id_reply.set_flags(MsgFlags::MORE);
    router.send(id_reply).await?;
    router.send(Msg::from_static(b"IPC Reply")).await?;

    // DEALER receives [payload]
    let reply_payload = common::recv_timeout(&dealer, LONG_TIMEOUT).await?;
    assert_eq!(reply_payload.data().unwrap(), b"IPC Reply");
    assert!(!reply_payload.is_more());
  }
  ctx.term().await?;
  Ok(())
}

// --- Inproc Tests ---

#[rzmq_macros::test]
#[cfg(feature = "inproc")]
async fn test_dealer_router_inproc_basic() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let dealer = ctx.socket(SocketType::Dealer)?;
    let router = ctx.socket(SocketType::Router)?;
    let endpoint = common::unique_inproc_endpoint();

    router.bind(&endpoint).await?;
    dealer.connect(&endpoint).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // DEALER -> ROUTER
    dealer.send(Msg::from_static(b"Inproc Req")).await?;

    // ROUTER receives [identity, payload]
    let id_frame = common::recv_timeout(&router, LONG_TIMEOUT).await?;
    let payload_frame = common::recv_timeout(&router, SHORT_TIMEOUT).await?;
    assert!(id_frame.is_more());
    assert!(!id_frame.data().unwrap().is_empty());
    assert!(!payload_frame.is_more());
    assert_eq!(payload_frame.data().unwrap(), b"Inproc Req");

    let received_identity = id_frame.data().unwrap().to_vec();

    // ROUTER -> DEALER
    let mut id_reply = Msg::from_vec(received_identity);
    id_reply.set_flags(MsgFlags::MORE);
    router.send(id_reply).await?;
    router.send(Msg::from_static(b"Inproc Rep")).await?;

    // DEALER receives [payload]
    let reply_payload = common::recv_timeout(&dealer, LONG_TIMEOUT).await?;
    assert_eq!(reply_payload.data().unwrap(), b"Inproc Rep");
    assert!(!reply_payload.is_more());
  }
  ctx.term().await?;
  Ok(())
}

// TODO: Add tests for multiple DEALERs, ROUTER_MANDATORY option.
