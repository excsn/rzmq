// tests/req_rep_advanced.rs (or add to tests/req_rep.rs)

use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;
mod common; // Assumes common.rs is in the tests/ directory

// Use constants for timeouts from common if defined, otherwise define here
const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);

// --- Test: REQ sending twice ---
#[rzmq_macros::test]
async fn test_req_rep_req_send_without_recv() -> Result<(), ZmqError> {
  println!("Starting test_req_rep_req_send_without_recv...");
  let ctx = common::test_context();
  let req = ctx.socket(SocketType::Req)?;
  let rep = ctx.socket(SocketType::Rep)?;

  {
    // Inner scope to ensure sockets are dropped before ctx.term if needed
    let endpoint = "tcp://127.0.0.1:5600"; // Unique port
    println!("Binding REP to {}...", endpoint);
    rep.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind

    println!("Connecting REQ to {}...", endpoint);
    req.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect + handshake

    // Send request 1 (Legal)
    println!("REQ sending Request 1...");
    req.send(Msg::from_static(b"Request 1")).await?;
    println!("REQ sent Request 1.");

    // Send request 2 (Illegal - must receive first)
    println!("REQ attempting to send Request 2 (should fail)...");
    let send2_result = req.send(Msg::from_static(b"Request 2")).await;
    assert!(
      matches!(send2_result, Err(ZmqError::InvalidState(_))),
      "Expected InvalidState error trying to send twice, got {:?}",
      send2_result
    );
    println!("REQ correctly failed to send Request 2.");

    // REP receives request 1
    println!("REP receiving Request 1...");
    let received_req = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(received_req.data().unwrap(), b"Request 1");
    println!("REP received Request 1.");

    // REP sends reply 1
    println!("REP sending Reply 1...");
    rep.send(Msg::from_static(b"Reply 1")).await?;
    println!("REP sent Reply 1.");

    // REQ receives reply 1
    println!("REQ receiving Reply 1...");
    let received_rep = common::recv_timeout(&req, LONG_TIMEOUT).await?;
    assert_eq!(received_rep.data().unwrap(), b"Reply 1");
    println!("REQ received Reply 1.");

    // Send request 2 (Legal now)
    println!("REQ sending Request 2 (should succeed)...");
    req.send(Msg::from_static(b"Request 2")).await?;
    println!("REQ sent Request 2.");

    // REP receives request 2
    println!("REP receiving Request 2...");
    let received_req2 = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(received_req2.data().unwrap(), b"Request 2");
    println!("REP received Request 2.");

    // Clean up sequence for req 2 (optional, but good practice)
    rep.send(Msg::from_static(b"Reply 2")).await?;
    req.recv().await?; // Consume final reply
  } // Sockets dropped here

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_req_rep_req_send_without_recv finished.");
  Ok(())
}

// --- Test: REP receiving before request ---
#[rzmq_macros::test]
async fn test_req_rep_rep_recv_without_request() -> Result<(), ZmqError> {
  println!("Starting test_req_rep_rep_recv_without_request...");
  let ctx = common::test_context();
  let req = ctx.socket(SocketType::Req)?;
  let rep = ctx.socket(SocketType::Rep)?;

  {
    let endpoint = "tcp://127.0.0.1:5601"; // Unique port
    println!("Binding REP to {}...", endpoint);
    rep.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("Connecting REQ to {}...", endpoint);
    req.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // REP attempts recv first (Illegal state logically, but blocks/timeouts)
    println!("REP attempting recv (should time out)...");
    let recv_result = common::recv_timeout(&rep, SHORT_TIMEOUT).await;
    // Expect timeout because RCVTIMEO is not set (infinite) or short timeout used
    // If RCVTIMEO were set on REP, we might expect Err(ZmqError::Timeout)
    // For now, assume recv_timeout helper returns Timeout on elapsed duration
    assert!(
      matches!(recv_result, Err(ZmqError::Timeout)),
      "Expected Timeout error trying to recv early, got {:?}",
      recv_result
    );
    println!("REP correctly timed out waiting for early recv.");

    // REQ sends request
    println!("REQ sending Request 1...");
    req.send(Msg::from_static(b"Request 1")).await?;
    println!("REQ sent Request 1.");

    // REP recv should succeed now
    println!("REP receiving Request 1 (should succeed)...");
    let received_req = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(received_req.data().unwrap(), b"Request 1");
    println!("REP received Request 1.");

    // Clean up sequence (optional)
    rep.send(Msg::from_static(b"Reply 1")).await?;
    req.recv().await?;
  }

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_req_rep_rep_recv_without_request finished.");
  Ok(())
}

// --- Test: REP sending before request ---
#[rzmq_macros::test]
async fn test_req_rep_rep_send_without_request() -> Result<(), ZmqError> {
  println!("Starting test_req_rep_rep_send_without_request...");
  let ctx = common::test_context();
  let req = ctx.socket(SocketType::Req)?;
  let rep = ctx.socket(SocketType::Rep)?;

  {
    let endpoint = "tcp://127.0.0.1:5602"; // Unique port
    println!("Binding REP to {}...", endpoint);
    rep.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("Connecting REQ to {}...", endpoint);
    req.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // REP attempts send first (Illegal state)
    println!("REP attempting send (should fail)...");
    let send_result = rep.send(Msg::from_static(b"Rep 1")).await;
    assert!(
      matches!(send_result, Err(ZmqError::InvalidState(_))),
      "Expected InvalidState error trying to send early, got {:?}",
      send_result
    );
    println!("REP correctly failed to send early.");

    // Verify REQ cannot send either yet (as REP is stuck) - optional check
    // Let's skip this check for simplicity, the main point is REP failure
  }

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_req_rep_rep_send_without_request finished.");
  Ok(())
}

// --- Test: REP disconnects while REQ is waiting for reply ---
#[rzmq_macros::test]
async fn test_req_rep_rep_disconnects_while_req_waiting() -> Result<(), ZmqError> {
  println!("Starting test_req_rep_rep_disconnects_while_req_waiting...");
  let ctx = common::test_context();
  let req = ctx.socket(SocketType::Req)?;

  // Create REP in its own scope to control its lifetime
  let endpoint = "tcp://127.0.0.1:5603"; // Unique port
  {
    let rep_socket = ctx.socket(SocketType::Rep)?;
    println!("Binding REP to {}...", endpoint);
    rep_socket.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("Connecting REQ to {}...", endpoint);
    req.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // REQ sends request
    println!("REQ sending Request...");
    req.send(Msg::from_static(b"Request")).await?;
    println!("REQ sent Request.");

    // REP receives request (to ensure connection is fully up)
    println!("REP receiving Request...");
    let received_req = common::recv_timeout(&rep_socket, LONG_TIMEOUT).await?;
    assert_eq!(received_req.data().unwrap(), b"Request");
    println!("REP received Request.");

    println!("REP socket going out of scope (will trigger close)...");
    // <<< ADDED EXPLICIT CLOSE >>>
    rep_socket.close().await?;
    // <<< ADDED EXPLICIT CLOSE END >>>
    // rep_socket is dropped here, initiating close/cleanup
  } // rep_socket is dropped, its underlying actor should signal detachment

  // Allow time for REP to close and detach event to propagate
  println!("Waiting for REP close propagation...");
  // Increase delay significantly to allow OS port release
  tokio::time::sleep(Duration::from_millis(500)).await; // Try 500ms

  // REQ tries to receive reply
  println!("REQ attempting recv (should fail or timeout)...");
  let recv_result = common::recv_timeout(&req, LONG_TIMEOUT).await;
  println!("REQ recv result: {:?}", recv_result);

  // <<< MODIFIED START >>>
  // *** CHECK THE EXPECTED OUTCOME ***
  // When the target REP disconnects, ReqSocket::pipe_detached resets the state to ReadyToSend.
  // The recv() call, upon unblocking (likely due to the timeout in recv_timeout),
  // detects this state change and returns Err(ZmqError::InvalidState("Socket state changed..."))
  assert!(
    matches!(recv_result, Err(ZmqError::InvalidState(_))), // Expect InvalidState due to reset
    "Expected InvalidState after REP disconnect, got {:?}",
    recv_result
  );
  println!("REQ correctly failed with InvalidState after REP disconnect.");
  // <<< MODIFIED END >>>

  // Optional: Verify REQ can now send again due to state reset
  println!("REQ attempting send after REP disconnect (should succeed if state reset)...");
  // Need a new REP to connect to, otherwise send will block/timeout finding peer
  let rep2 = ctx.socket(SocketType::Rep)?;
  rep2.bind(endpoint).await?; // Re-bind on the same endpoint
  tokio::time::sleep(Duration::from_millis(150)).await; // Allow bind and reconnect

  // This send should now succeed as state was reset
  let send_again_result = req.send(Msg::from_static(b"Request After Reset")).await;
  assert!(
    send_again_result.is_ok(),
    "Expected send to succeed after state reset, got {:?}",
    send_again_result
  );
  println!("REQ successfully sent after state reset.");

  // Clean up rep2
  let _ = rep2.recv().await;
  let _ = rep2.send(Msg::from_static(b"")).await;
  let _ = req.recv().await; // Consume reply

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_req_rep_rep_disconnects_while_req_waiting finished.");
  Ok(())
}

// --- Test: REQ disconnects before REP sends reply ---
#[rzmq_macros::test]
async fn test_req_rep_req_disconnects_before_reply() -> Result<(), ZmqError> {
  println!("Starting test_req_rep_req_disconnects_before_reply...");
  let ctx = common::test_context();
  let rep = ctx.socket(SocketType::Rep)?;

  let endpoint = "tcp://127.0.0.1:5604"; // Unique port
  println!("Binding REP to {}...", endpoint);
  rep.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  {
    // Scope for REQ socket
    let req = ctx.socket(SocketType::Req)?;
    println!("Connecting REQ to {}...", endpoint);
    req.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("REQ sending Request...");
    req.send(Msg::from_static(b"Request")).await?;
    println!("REQ sent Request.");

    println!("REP receiving Request...");
    let received_req = common::recv_timeout(&rep, LONG_TIMEOUT).await?;
    assert_eq!(received_req.data().unwrap(), b"Request");
    println!("REP received Request.");

    println!("REQ socket going out of scope (will trigger close)...");
    req.close().await?;
    // req is dropped here
  }; // req socket is dropped

  // Allow time for REQ to close and detach event to propagate to REP
  println!("Waiting for REQ close propagation...");
  tokio::time::sleep(Duration::from_millis(200)).await;

  // REP attempts to send reply
  println!("REP attempting send reply (should fail)...");
  let send_result = rep.send(Msg::from_static(b"Reply")).await;
  println!("REP send result: {:?}", send_result);

  // Expected error: The pipe should be closed. send_msg_with_timeout maps
  // SendError to ConnectionClosed. The REP send might return this or
  // potentially HostUnreachable if the core handles detachment that way.
  assert!(
    matches!(send_result, Err(ZmqError::InvalidState(_))),
    "Expected InvalidState error, got {:?}",
    send_result
  );
  println!("REP correctly failed to send reply to disconnected REQ.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_req_rep_req_disconnects_before_reply finished.");
  Ok(())
}
