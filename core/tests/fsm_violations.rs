mod common;

use rand::random;
use rzmq::socket::options::{LAST_ENDPOINT, LINGER, RCVTIMEO, SNDTIMEO};
use rzmq::{Msg, SocketType, ZmqError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn bound_endpoint(socket: &rzmq::Socket) -> Result<String, ZmqError> {
  let bytes = socket.get_option(LAST_ENDPOINT).await?;
  Ok(String::from_utf8(bytes).expect("LAST_ENDPOINT is valid UTF-8"))
}

// ---------------------------------------------------------------------------
// Test 4.1 — Concurrent Send Contention on REQ Socket
// FSM-4.1: 10 concurrent sends on a REQ socket.
// Exactly 1 should succeed; 9 should fail with InvalidState.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_req_concurrent_send_contention() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let rep = ctx.socket(SocketType::Rep)?;
  rep.set_option_raw(RCVTIMEO, &2_000i32.to_ne_bytes()).await?;
  rep.set_option_raw(SNDTIMEO, &2_000i32.to_ne_bytes()).await?;
  rep.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  let req = ctx.socket(SocketType::Req)?;
  // Zero send/recv timeout so concurrent waiters return immediately
  req.set_option_raw(SNDTIMEO, &0i32.to_ne_bytes()).await?;
  req.set_option_raw(RCVTIMEO, &2_000i32.to_ne_bytes()).await?;
  req.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  rep.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&rep).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  req.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // REQ is in ReadyToSend. Launch 10 concurrent sends.
  let mut handles = Vec::new();
  for _ in 0..10 {
    let req_clone = req.clone();
    handles.push(tokio::spawn(async move {
      req_clone.send(Msg::from_static(b"concurrent-req")).await
    }));
  }

  let results: Vec<Result<(), ZmqError>> = futures::future::join_all(handles)
    .await
    .into_iter()
    .map(|r| r.unwrap())
    .collect();

  let successes = results.iter().filter(|r| r.is_ok()).count();
  let invalid_state_failures = results
    .iter()
    .filter(|r| matches!(r, Err(ZmqError::InvalidState(_))))
    .count();
  let other_failures = results
    .iter()
    .filter(|r| r.is_err() && !matches!(r, Err(ZmqError::InvalidState(_))))
    .count();

  println!(
    "FSM-4.1: successes={}, invalid_state={}, other={}",
    successes, invalid_state_failures, other_failures
  );

  assert_eq!(successes, 1, "expected exactly 1 successful send, got {}", successes);
  assert_eq!(
    invalid_state_failures,
    9,
    "expected 9 InvalidState failures, got {}",
    invalid_state_failures
  );
  assert_eq!(other_failures, 0, "unexpected error types: {:?}", results);

  // Drain: REP receives and replies, REQ receives reply to reset state
  let _msg = rep.recv().await?;
  rep.send(Msg::from_static(b"ack")).await?;
  let _ = req.recv().await?;

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 4.2 — Concurrent Recv Contention on REQ Socket
// FSM-4.2: REQ is in ExpectingReply (send already done).
// 10 concurrent recvs — exactly 1 gets the message, 9 get InvalidState or Timeout.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_req_concurrent_recv_contention() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let rep = ctx.socket(SocketType::Rep)?;
  rep.set_option_raw(RCVTIMEO, &2_000i32.to_ne_bytes()).await?;
  rep.set_option_raw(SNDTIMEO, &2_000i32.to_ne_bytes()).await?;
  rep.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  let req = ctx.socket(SocketType::Req)?;
  req.set_option_raw(SNDTIMEO, &2_000i32.to_ne_bytes()).await?;
  req.set_option_raw(RCVTIMEO, &500i32.to_ne_bytes()).await?; // short so blocked tasks time out
  req.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  rep.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&rep).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  req.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // Put REQ into ExpectingReply by sending one request
  req.send(Msg::from_static(b"request")).await?;

  // Spawn 10 concurrent recv tasks before REP sends its reply
  let mut handles = Vec::new();
  for _ in 0..10 {
    let req_clone = req.clone();
    handles.push(tokio::spawn(async move { req_clone.recv().await }));
  }

  // REP sends exactly one reply
  let _req_msg = rep.recv().await?;
  rep.send(Msg::from_static(b"reply")).await?;

  let results: Vec<Result<rzmq::Msg, ZmqError>> = futures::future::join_all(handles)
    .await
    .into_iter()
    .map(|r| r.unwrap())
    .collect();

  let successes = results.iter().filter(|r| r.is_ok()).count();
  let failures = results.iter().filter(|r| r.is_err()).count();

  println!("FSM-4.2: successes={}, failures={}", successes, failures);

  // Exactly one task should have consumed the reply
  assert_eq!(
    successes, 1,
    "expected exactly 1 successful recv, got {}",
    successes
  );
  assert_eq!(failures, 9, "expected 9 failures, got {}", failures);

  // All failures must be InvalidState or Timeout — no unexpected errors
  for result in &results {
    if let Err(e) = result {
      assert!(
        matches!(e, ZmqError::InvalidState(_) | ZmqError::Timeout),
        "unexpected error variant: {:?}",
        e
      );
    }
  }

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 4.3 — Concurrent Send on REP Without Prior Request
// FSM-4.3: REP is in WaitingForRequest state (no request received).
// All 10 concurrent sends must fail with InvalidState.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rep_concurrent_send_without_request() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let rep = ctx.socket(SocketType::Rep)?;
  rep.set_option_raw(SNDTIMEO, &0i32.to_ne_bytes()).await?; // immediate failure if invalid
  rep.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  rep.bind("tcp://127.0.0.1:0").await?;

  // REP is in WaitingForRequest. No REQ connected. Spawn 10 concurrent sends.
  let mut handles = Vec::new();
  for _ in 0..10 {
    let rep_clone = rep.clone();
    handles.push(tokio::spawn(async move {
      rep_clone.send(Msg::from_static(b"unsolicited")).await
    }));
  }

  let results: Vec<Result<(), ZmqError>> = futures::future::join_all(handles)
    .await
    .into_iter()
    .map(|r| r.unwrap())
    .collect();

  let successes = results.iter().filter(|r| r.is_ok()).count();
  let invalid_state_failures = results
    .iter()
    .filter(|r| matches!(r, Err(ZmqError::InvalidState(_))))
    .count();

  println!(
    "FSM-4.3: successes={}, invalid_state={}",
    successes, invalid_state_failures
  );

  assert_eq!(successes, 0, "expected 0 successes, got {}", successes);
  assert_eq!(
    invalid_state_failures,
    10,
    "expected 10 InvalidState failures, got {}",
    invalid_state_failures
  );

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 4.4 — Interleaved FSM Chaos Abuse (REQ/REP)
// FSM-4.4: 20 tasks randomly send/recv on REQ, 20 on REP for 10 seconds.
// Assertion: no panic, sockets remain closeable.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_req_rep_fsm_chaos() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let rep = ctx.socket(SocketType::Rep)?;
  let req = ctx.socket(SocketType::Req)?;

  // Short timeouts so tasks cycle quickly and check stop flag
  for s in [&req, &rep] {
    s.set_option_raw(RCVTIMEO, &100i32.to_ne_bytes()).await?;
    s.set_option_raw(SNDTIMEO, &100i32.to_ne_bytes()).await?;
    s.set_option_raw(LINGER, &0i32.to_ne_bytes()).await?;
  }

  rep.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&rep).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  req.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  let stop = Arc::new(AtomicBool::new(false));
  let mut handles = Vec::new();

  // 20 REQ chaos tasks
  for _ in 0..20 {
    let req_clone = req.clone();
    let stop_clone = stop.clone();
    handles.push(tokio::spawn(async move {
      while !stop_clone.load(Ordering::Relaxed) {
        if random::<bool>() {
          let _ = req_clone.send(Msg::from_static(b"chaos-req")).await;
        } else {
          let _ = req_clone.recv().await;
        }
      }
    }));
  }

  // 20 REP chaos tasks
  for _ in 0..20 {
    let rep_clone = rep.clone();
    let stop_clone = stop.clone();
    handles.push(tokio::spawn(async move {
      while !stop_clone.load(Ordering::Relaxed) {
        if random::<bool>() {
          let _ = rep_clone.send(Msg::from_static(b"chaos-rep")).await;
        } else {
          let _ = rep_clone.recv().await;
        }
      }
    }));
  }

  // Run chaos for 10 seconds
  tokio::time::sleep(Duration::from_secs(10)).await;
  stop.store(true, Ordering::Relaxed);

  // Abort and join all tasks; panics are re-raised
  for handle in &handles {
    handle.abort();
  }
  for handle in handles {
    match handle.await {
      Ok(()) => {}
      Err(e) if e.is_cancelled() => {}
      Err(e) => {
        assert!(!e.is_panic(), "Chaos task panicked: {:?}", e);
      }
    }
  }

  // Sockets must remain closeable after chaos
  req.close().await?;
  rep.close().await?;
  ctx.term().await?;
  Ok(())
}
