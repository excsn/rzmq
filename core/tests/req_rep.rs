// tests/req_rep.rs

use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);

// --- TCP Tests ---

#[rzmq::main]
async fn test_req_rep_tcp_basic() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let req = ctx.socket(SocketType::Req)?;
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = "tcp://127.0.0.1:5560"; // Unique port

    rep.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind

    req.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect + handshake

    // Send request
    let req_msg = Msg::from_static(b"Request 1");
    req.send(req_msg).await?;
    println!("REQ sent request");

    // Receive request
    let received_req = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(received_req.data().unwrap(), b"Request 1");
    println!("REP received request");

    // Send reply
    let rep_msg = Msg::from_static(b"Reply 1");
    rep.send(rep_msg).await?;
    println!("REP sent reply");

    // Receive reply
    let received_rep = common::recv_timeout(&req, LONG_TIMEOUT).await?;
    assert_eq!(received_rep.data().unwrap(), b"Reply 1");
    println!("REQ received reply");

    // Try sending another request
    req.send(Msg::from_static(b"Request 2")).await?;
    println!("REQ sent request 2");
    let received_req2 = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(received_req2.data().unwrap(), b"Request 2");
    println!("REP received request 2");
    rep.send(Msg::from_static(b"Reply 2")).await?;
    println!("REP sent reply 2");
    let received_rep2 = common::recv_timeout(&req, LONG_TIMEOUT).await?;
    assert_eq!(received_rep2.data().unwrap(), b"Reply 2");
    println!("REQ received reply 2");
  }
  ctx.term().await?;
  Ok(())
}

#[rzmq::main]
async fn test_req_rep_tcp_connect_before_bind() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let req = ctx.socket(SocketType::Req)?;
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = "tcp://127.0.0.1:5561"; // Unique port

    // Connect first (will retry internally)
    req.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Give connect time to start attempts

    // Bind later
    rep.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(200)).await; // Allow bind & connect successful retry

    // Send/Recv
    let req_msg = Msg::from_static(b"Hello Late Bind");
    req.send(req_msg).await?;
    println!("REQ sent");
    let received_req = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(received_req.data().unwrap(), b"Hello Late Bind");
    println!("REP received");
    rep.send(Msg::from_static(b"Hi Back")).await?;
    println!("REP sent");
    let received_rep = common::recv_timeout(&req, LONG_TIMEOUT).await?;
    assert_eq!(received_rep.data().unwrap(), b"Hi Back");
    println!("REQ received");
  }
  ctx.term().await?;
  Ok(())
}

// --- IPC Tests ---

#[rzmq::main]
#[cfg(feature = "ipc")]
async fn test_req_rep_ipc_basic() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let req = ctx.socket(SocketType::Req)?;
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = common::unique_ipc_endpoint();
    println!("Using IPC endpoint: {}", endpoint);

    rep.bind(&endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    req.connect(&endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    req.send(Msg::from_static(b"IPC Req")).await?;
    let received_req = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(received_req.data().unwrap(), b"IPC Req");

    rep.send(Msg::from_static(b"IPC Rep")).await?;
    let received_rep = common::recv_timeout(&req, LONG_TIMEOUT).await?;
    assert_eq!(received_rep.data().unwrap(), b"IPC Rep");
  }
  ctx.term().await?;
  Ok(())
}

// --- Inproc Tests ---

#[rzmq::main]
#[cfg(feature = "inproc")]
async fn test_req_rep_inproc_basic() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let req = ctx.socket(SocketType::Req)?;
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = common::unique_inproc_endpoint();

    rep.bind(&endpoint).await?;
    req.connect(&endpoint).await?;
    tokio::time::sleep(Duration::from_millis(20)).await; // Short delay for pipe setup

    req.send(Msg::from_static(b"Inproc Req")).await?;
    let received_req = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(received_req.data().unwrap(), b"Inproc Req");

    rep.send(Msg::from_static(b"Inproc Rep")).await?;
    let received_rep = common::recv_timeout(&req, LONG_TIMEOUT).await?;
    assert_eq!(received_rep.data().unwrap(), b"Inproc Rep");
  }
  ctx.term().await?;
  Ok(())
}

// TODO: Add tests for REQ/REP error states (send twice, etc.)
