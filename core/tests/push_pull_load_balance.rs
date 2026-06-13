// Integration tests for PUSH socket load-balancing and HWM-bypass (peer-skipping).
// All TCP binds use ephemeral port 0 + LAST_ENDPOINT to avoid port collisions under
// parallel test runners

mod common;

use rzmq::socket::options::{RCVHWM, RCVTIMEO, SNDHWM, SNDTIMEO};
use rzmq::socket::{RCVBUF, SNDBUF};
use rzmq::{Msg, MsgFlags, SocketType, ZmqError};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::timeout;

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(3);

// ---------------------------------------------------------------------------
// Test 1 — Fair Distribution (1-to-N Round-Robin)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_load_balance_fair_distribution() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull1 = ctx.socket(SocketType::Pull)?;
  let pull2 = ctx.socket(SocketType::Pull)?;
  let pull3 = ctx.socket(SocketType::Pull)?;

  let ep1 = common::bind_and_resolve_tcp(&pull1).await?;
  let ep2 = common::bind_and_resolve_tcp(&pull2).await?;
  let ep3 = common::bind_and_resolve_tcp(&pull3).await?;

  push.connect(&ep1).await?;
  push.connect(&ep2).await?;
  push.connect(&ep3).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // Send 9 messages — round-robin should give exactly 3 to each PULL.
  for i in 0u8..9 {
    push.send(Msg::from_vec(vec![i])).await?;
  }

  for pull in [&pull1, &pull2, &pull3] {
    let mut count = 0;
    for _ in 0..3 {
      let _ = common::recv_timeout(pull, LONG_TIMEOUT).await?;
      count += 1;
    }
    assert_eq!(count, 3, "Expected each PULL to receive exactly 3 messages");
    assert!(
      matches!(
        common::recv_timeout(pull, SHORT_TIMEOUT).await,
        Err(ZmqError::Timeout)
      ),
      "A PULL received more than its fair share"
    );
  }

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 2 — Multipart Cohesion Invariant
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_load_balance_multipart_cohesion() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull1 = ctx.socket(SocketType::Pull)?;
  let pull2 = ctx.socket(SocketType::Pull)?;

  let ep1 = common::bind_and_resolve_tcp(&pull1).await?;
  let ep2 = common::bind_and_resolve_tcp(&pull2).await?;

  push.connect(&ep1).await?;
  push.connect(&ep2).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // Send two 3-frame messages — each must land atomically on one PULL.
  let mut a1 = Msg::from_static(b"A1");
  a1.set_flags(MsgFlags::MORE);
  let mut a2 = Msg::from_static(b"A2");
  a2.set_flags(MsgFlags::MORE);
  let a3 = Msg::from_static(b"A3");
  push.send_multipart(vec![a1, a2, a3]).await?;

  let mut b1 = Msg::from_static(b"B1");
  b1.set_flags(MsgFlags::MORE);
  let mut b2 = Msg::from_static(b"B2");
  b2.set_flags(MsgFlags::MORE);
  let b3 = Msg::from_static(b"B3");
  push.send_multipart(vec![b1, b2, b3]).await?;

  let res1 = pull1.recv_multipart().await?;
  let res2 = pull2.recv_multipart().await?;

  assert_eq!(res1.len(), 3, "PULL1 must receive exactly 3 frames");
  assert_eq!(res2.len(), 3, "PULL2 must receive exactly 3 frames");

  let p1_tag = res1[0].data().unwrap_or_default()[0];
  let p2_tag = res2[0].data().unwrap_or_default()[0];
  assert_ne!(
    p1_tag, p2_tag,
    "Both PULLs received the same message — duplication bug"
  );
  assert!(
    (p1_tag == b'A' && p2_tag == b'B') || (p1_tag == b'B' && p2_tag == b'A'),
    "Interleaved or corrupted frames detected"
  );

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 3 — Late Joiner Integration (Dynamic Connection)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_load_balance_late_joiner() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull_a = ctx.socket(SocketType::Pull)?;

  let ep_a = common::bind_and_resolve_tcp(&pull_a).await?;
  push.connect(&ep_a).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Prime the pipe: send and receive 4 messages to confirm PULL A is active.
  for i in 0u8..4 {
    push.send(Msg::from_vec(vec![i])).await?;
    let _ = common::recv_timeout(&pull_a, LONG_TIMEOUT).await?;
  }

  // Connect PULL B mid-stream.
  let pull_b = ctx.socket(SocketType::Pull)?;
  let ep_b = common::bind_and_resolve_tcp(&pull_b).await?;
  push.connect(&ep_b).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // Send 20 messages — both PULLs must receive a meaningful share.
  for i in 0u8..20 {
    push.send(Msg::from_vec(vec![i])).await?;
  }

  let mut count_a = 0usize;
  while timeout(SHORT_TIMEOUT, pull_a.recv()).await.is_ok() {
    count_a += 1;
  }
  let mut count_b = 0usize;
  while timeout(SHORT_TIMEOUT, pull_b.recv()).await.is_ok() {
    count_b += 1;
  }

  assert_eq!(
    count_a + count_b,
    20,
    "Message count mismatch after late join"
  );
  assert!(
    count_a >= 5,
    "PULL A starved after late joiner connected (got {})",
    count_a
  );
  assert!(
    count_b >= 5,
    "Late joiner PULL B never received messages (got {})",
    count_b
  );

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 4 — Abrupt Disconnect Recovery
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_load_balance_disconnect_recovery() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull_a = ctx.socket(SocketType::Pull)?;
  let pull_b = ctx.socket(SocketType::Pull)?;

  let ep_a = common::bind_and_resolve_tcp(&pull_a).await?;
  let ep_b = common::bind_and_resolve_tcp(&pull_b).await?;

  push.connect(&ep_a).await?;
  push.connect(&ep_b).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // Verify both peers are reachable.
  push.send(Msg::from_static(b"probe")).await?;
  push.send(Msg::from_static(b"probe")).await?;
  let _ = common::recv_timeout(&pull_a, LONG_TIMEOUT).await?;
  let _ = common::recv_timeout(&pull_b, LONG_TIMEOUT).await?;

  // Abruptly drop PULL A.
  pull_a.close().await?;
  drop(pull_a);
  tokio::time::sleep(Duration::from_millis(200)).await;

  // All subsequent messages must reach PULL B cleanly.
  for _ in 0..10 {
    push.send(Msg::from_static(b"after")).await?;
  }
  for _ in 0..10 {
    let msg = common::recv_timeout(&pull_b, LONG_TIMEOUT).await?;
    assert_eq!(msg.data().unwrap(), b"after");
  }

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 5 — Fair-Share Statistical Balance (Stress)
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_load_balance_statistical_fairness() -> Result<(), ZmqError> {
  let ctx = Arc::new(common::test_context());
  let push = Arc::new(ctx.socket(SocketType::Push)?);

  let mut pulls = Vec::new();
  let mut endpoints = Vec::new();
  for _ in 0..4 {
    let pull = ctx.socket(SocketType::Pull)?;
    let ep = common::bind_and_resolve_tcp(&pull).await?;
    pulls.push(Arc::new(pull));
    endpoints.push(ep);
  }
  for ep in &endpoints {
    push.connect(ep).await?;
  }
  tokio::time::sleep(Duration::from_millis(200)).await;

  const TOTAL: usize = 10_000;
  let sender = tokio::spawn({
    let push = push.clone();
    async move {
      for _ in 0..TOTAL {
        let _ = push.send(Msg::from_vec(vec![0u8])).await;
      }
    }
  });

  let mut recv_handles = Vec::new();
  for pull in pulls {
    recv_handles.push(tokio::spawn(async move {
      let mut count = 0usize;
      while timeout(Duration::from_millis(500), pull.recv())
        .await
        .is_ok()
      {
        count += 1;
      }
      count
    }));
  }

  sender.await.unwrap();
  let mut total = 0usize;
  for h in recv_handles {
    let count = h.await.unwrap();
    total += count;
    assert!(
      count >= 2_250 && count <= 2_750,
      "Statistical unfairness: peer received {}/2500 (expected 2250-2750)",
      count
    );
  }
  assert_eq!(total, TOTAL, "Message count mismatch in stress test");

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 6 — Write-Ready Skip (HWM Bypass)
// Proves that a full peer is skipped and healthy peers absorb the traffic.
// ---------------------------------------------------------------------------
#[tokio::test]
#[cfg(feature = "inproc")]
async fn test_load_balance_write_ready_skip() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull_a = ctx.socket(SocketType::Pull)?;
  let pull_b = ctx.socket(SocketType::Pull)?;

  push.set_option_raw(SNDHWM, &1i32.to_ne_bytes()).await?;
  push.set_option_raw(SNDTIMEO, &0i32.to_ne_bytes()).await?;
  pull_a.set_option_raw(RCVHWM, &1i32.to_ne_bytes()).await?;
  pull_b.set_option_raw(RCVHWM, &100i32.to_ne_bytes()).await?;

  let ep_a = common::unique_inproc_endpoint();
  let ep_b = common::unique_inproc_endpoint();

  pull_a.bind(&ep_a).await?;
  pull_b.bind(&ep_b).await?;

  push.connect(&ep_a).await?;
  push.connect(&ep_b).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  // Peer B drains constantly in the background and counts received messages
  let pull_b_count = Arc::new(AtomicUsize::new(0));
  let pull_b_task = tokio::spawn({
    let pull_b = pull_b.clone();
    let count = pull_b_count.clone();
    async move {
      while let Ok(_) = pull_b.recv().await {
        count.fetch_add(1, Ordering::Relaxed);
      }
    }
  });

  // Block A at the inproc pipe level.
  // The duplex stream has a default capacity of 64KB (65,536 bytes).
  // Sending a 65,537-byte message fills the duplex stream completely,
  // forcing the writer to block and stopping A's actor from pulling from the core pipe.
  push.send(Msg::from_vec(vec![0u8; 65537])).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  const N: usize = 10_000;
  let mut send_failures = 0;

  for i in 0..N {
    let msg = Msg::from_vec(format!("payload-{}", i).into_bytes());
    match push.send(msg).await {
      Ok(()) => {}
      Err(ZmqError::ResourceLimitReached) => {
        send_failures += 1;
      }
      Err(e) => panic!("Unexpected send error: {:?}", e),
    }
    // Yield on every iteration so B's actor can constantly drain the channel
    tokio::task::yield_now().await;
  }

  // Allow outstanding messages to reach the background task
  tokio::time::sleep(Duration::from_millis(100)).await;

  let count_b = pull_b_count.load(Ordering::Relaxed);

  // Drain PULL A to find out how many messages it absorbed
  let mut count_a = 0;
  while timeout(Duration::from_millis(50), pull_a.recv())
    .await
    .is_ok()
  {
    count_a += 1;
  }

  println!(
    "Bypass results: total_sent={}, to_b={}, to_a={}, send_failures={}",
    N, count_b, count_a, send_failures
  );

  // Peer A must have blocked quickly and absorbed exactly 2 messages
  // (1 in the duplex stream, 1 in the core mpmc pipe)
  assert_eq!(count_a, 2, "Peer A did not block as expected");

  // Peer B must have absorbed nearly 100% of the remaining traffic
  assert!(
    count_b >= N - 5,
    "Peer B missed too many messages: {}",
    count_b
  );

  pull_b_task.abort();
  let _ = pull_b_task.await;
  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 7 — Slow-Consumer Re-Entry
// Proves that a previously-blocked peer re-enters rotation once it drains.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_load_balance_slow_consumer_reentry() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull_a = ctx.socket(SocketType::Pull)?;
  let pull_b = ctx.socket(SocketType::Pull)?;

  push.set_option_raw(SNDHWM, &1i32.to_ne_bytes()).await?;
  push.set_option_raw(SNDTIMEO, &0i32.to_ne_bytes()).await?;
  pull_a.set_option_raw(RCVHWM, &1i32.to_ne_bytes()).await?;
  pull_b.set_option_raw(RCVHWM, &100i32.to_ne_bytes()).await?;

  let ep_a = common::bind_and_resolve_tcp(&pull_a).await?;
  let ep_b = common::bind_and_resolve_tcp(&pull_b).await?;

  push.connect(&ep_a).await?;
  push.connect(&ep_b).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // --- PHASE 1: Peer A is blocked, B is active ---
  let pull_b_count = Arc::new(AtomicUsize::new(0));
  let pull_b_task = tokio::spawn({
    let pull_b = pull_b.clone();
    let count = pull_b_count.clone();
    async move {
      while let Ok(_) = pull_b.recv().await {
        count.fetch_add(1, Ordering::Relaxed);
      }
    }
  });

  // Block A with first message
  push.send(Msg::from_static(b"blocked-a")).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  const HALF_N: usize = 5000;
  for i in 0..HALF_N {
    let msg = Msg::from_vec(format!("phase1-{}", i).into_bytes());
    let _ = push.send(msg).await;
    if i % 10 == 0 {
      tokio::task::yield_now().await;
    }
  }
  tokio::time::sleep(Duration::from_millis(200)).await;

  let b_phase1 = pull_b_count.load(Ordering::Relaxed);
  assert!(
    b_phase1 >= HALF_N - 5,
    "Peer B failed to absorb phase 1 traffic: {}",
    b_phase1
  );

  // --- PHASE 2: Unblock Peer A and verify reentry ---
  let pull_a_count = Arc::new(AtomicUsize::new(0));
  let pull_a_task = tokio::spawn({
    let pull_a = pull_a.clone();
    let count = pull_a_count.clone();
    async move {
      while let Ok(_) = pull_a.recv().await {
        count.fetch_add(1, Ordering::Relaxed);
      }
    }
  });

  // Reset B's counter for Phase 2
  pull_b_count.store(0, Ordering::Relaxed);

  // Give A's task time to drain the stale backlog
  tokio::time::sleep(Duration::from_millis(250)).await;

  // Reset A's counter so we only count new Phase 2 arrivals
  pull_a_count.store(0, Ordering::Relaxed);

  // Blast another 5000 messages
  for i in 0..HALF_N {
    let msg = Msg::from_vec(format!("phase2-{}", i).into_bytes());
    let _ = push.send(msg).await;
    if i % 10 == 0 {
      tokio::task::yield_now().await;
    }
  }
  tokio::time::sleep(Duration::from_millis(200)).await;

  let a_phase2 = pull_a_count.load(Ordering::Relaxed);
  let b_phase2 = pull_b_count.load(Ordering::Relaxed);

  println!(
    "Reentry results (Phase 2): total_sent={}, to_a={}, to_b={}",
    HALF_N, a_phase2, b_phase2
  );

  // Both should have participated roughly equally (at least 40% of traffic each)
  let min_expected = (HALF_N as f64 * 0.4) as usize;
  assert!(
    a_phase2 >= min_expected,
    "Peer A failed to re-enter rotation. Received too few: {}",
    a_phase2
  );
  assert!(
    b_phase2 >= min_expected,
    "Peer B was starved during Phase 2. Received: {}",
    b_phase2
  );

  pull_a_task.abort();
  pull_b_task.abort();
  let _ = tokio::join!(pull_a_task, pull_b_task);

  ctx.term().await?;
  Ok(())
}
