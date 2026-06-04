mod common;

use rzmq::socket::options::{LAST_ENDPOINT, LINGER, RCVHWM, RCVTIMEO, ROUTING_ID, SNDHWM, SNDTIMEO};
use rzmq::{Msg, MsgFlags, SocketType, ZmqError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn xor_checksum(data: &[u8]) -> u32 {
  data.iter().fold(0u32, |acc, &b| acc ^ b as u32)
}

async fn bound_endpoint(socket: &rzmq::Socket) -> Result<String, ZmqError> {
  let bytes = socket.get_option(LAST_ENDPOINT).await?;
  Ok(String::from_utf8(bytes).expect("LAST_ENDPOINT is valid UTF-8"))
}

#[cfg(target_os = "linux")]
fn read_rss_kb() -> Option<u64> {
  let status = std::fs::read_to_string("/proc/self/status").ok()?;
  for line in status.lines() {
    if line.starts_with("VmRSS:") {
      return line.split_whitespace().nth(1)?.parse().ok();
    }
  }
  None
}

// ---------------------------------------------------------------------------
// Test 1.1 — Sustained High-Volume Unidirectional Stream (PUSH/PULL)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_push_pull_sustained_high_volume() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let pull = ctx.socket(SocketType::Pull)?;
  pull.set_option_raw(SNDHWM, &10_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(RCVHWM, &10_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(SNDTIMEO, &5_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(RCVTIMEO, &5_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;

  pull.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&pull).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  const NUM_SENDERS: u64 = 4;
  const MSGS_PER_SENDER: u64 = 2_500_000;
  const TOTAL_MSGS: u64 = NUM_SENDERS * MSGS_PER_SENDER;

  let mut push_sockets = Vec::new();
  for _ in 0..NUM_SENDERS {
    let push = ctx.socket(SocketType::Push)?;
    push.set_option_raw(SNDHWM, &10_000i32.to_ne_bytes()).await?;
    push.set_option_raw(RCVHWM, &10_000i32.to_ne_bytes()).await?;
    push.set_option_raw(SNDTIMEO, &5_000i32.to_ne_bytes()).await?;
    push.set_option_raw(RCVTIMEO, &5_000i32.to_ne_bytes()).await?;
    push.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;
    push.connect(&endpoint).await?;
    push_sockets.push(push);
  }
  tokio::time::sleep(Duration::from_millis(200)).await;

  #[cfg(target_os = "linux")]
  let rss_before = read_rss_kb();

  let seq_tracker: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));

  let mut sender_tasks = Vec::new();
  for (client_id, push) in push_sockets.iter().enumerate() {
    let push = push.clone();
    let client_id = client_id as u64;
    sender_tasks.push(tokio::spawn(async move {
      for seq in 0..MSGS_PER_SENDER {
        let mut payload = vec![0u8; 128];
        payload[0..8].copy_from_slice(&seq.to_be_bytes());
        payload[8..16].copy_from_slice(&client_id.to_be_bytes());
        payload[16..124].fill(0xAB);
        let checksum = xor_checksum(&payload[0..124]);
        payload[124..128].copy_from_slice(&checksum.to_be_bytes());
        push.send(Msg::from_vec(payload)).await?;
      }
      Ok::<(), ZmqError>(())
    }));
  }

  let recv_task = tokio::spawn({
    let pull = pull.clone();
    let seq_tracker = seq_tracker.clone();
    async move {
      for _ in 0..TOTAL_MSGS {
        let msg = pull.recv().await?;
        let data = msg.data().unwrap_or_default();
        assert_eq!(data.len(), 128, "unexpected message size");

        let seq = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let client_id = u64::from_be_bytes(data[8..16].try_into().unwrap());

        let expected_csum = xor_checksum(&data[0..124]);
        let received_csum = u32::from_be_bytes(data[124..128].try_into().unwrap());
        assert_eq!(
          received_csum, expected_csum,
          "checksum mismatch from client {}",
          client_id
        );

        let mut tracker = seq_tracker.lock().unwrap();
        let expected_seq = tracker.entry(client_id).or_insert(0);
        assert_eq!(
          seq, *expected_seq,
          "out-of-order message from client {}: expected {}, got {}",
          client_id, expected_seq, seq
        );
        *expected_seq += 1;
      }
      Ok::<(), ZmqError>(())
    }
  });

  for task in sender_tasks {
    task.await.unwrap()?;
  }
  recv_task.await.unwrap()?;

  #[cfg(target_os = "linux")]
  if let (Some(before), Some(after)) = (rss_before, read_rss_kb()) {
    let growth_pct = (after as f64 - before as f64) / before.max(1) as f64 * 100.0;
    if growth_pct > 10.0 {
      eprintln!(
        "WARNING: RSS grew {:.1}% ({}KB → {}KB) — possible slow leak",
        growth_pct, before, after
      );
    } else {
      println!("RSS stable: {}KB → {}KB ({:+.1}%)", before, after, growth_pct);
    }
  }

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 1.2 — Sustained High-Frequency Bidirectional Ping-Pong (REQ/REP)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_req_rep_sustained_ping_pong() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let rep = ctx.socket(SocketType::Rep)?;
  let req = ctx.socket(SocketType::Req)?;

  rep.set_option_raw(RCVTIMEO, &2_000i32.to_ne_bytes()).await?;
  rep.set_option_raw(SNDTIMEO, &2_000i32.to_ne_bytes()).await?;
  rep.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;
  req.set_option_raw(RCVTIMEO, &2_000i32.to_ne_bytes()).await?;
  req.set_option_raw(SNDTIMEO, &2_000i32.to_ne_bytes()).await?;
  req.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;

  let endpoint = common::unique_ipc_endpoint();
  rep.bind(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  req.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  const ROUNDS: u64 = 2_000_000;

  let rep_task = tokio::spawn({
    let rep = rep.clone();
    async move {
      for _ in 0..ROUNDS {
        let msg = rep.recv().await?;
        rep.send(msg).await?;
      }
      Ok::<(), ZmqError>(())
    }
  });

  let test_start = Instant::now();
  let mut latencies = Vec::with_capacity(ROUNDS as usize);

  for seq in 0..ROUNDS {
    let mut payload = [0u8; 32];
    payload[0..8].copy_from_slice(&seq.to_be_bytes());
    let ts_nanos = test_start.elapsed().as_nanos() as u64;
    payload[8..16].copy_from_slice(&ts_nanos.to_be_bytes());

    let t0 = Instant::now();
    req.send(Msg::from_vec(payload.to_vec())).await?;
    let reply = req.recv().await?;
    latencies.push(t0.elapsed());

    let reply_data = reply.data().unwrap_or_default();
    assert!(reply_data.len() >= 8, "reply too short at round {}", seq);
    let reply_seq = u64::from_be_bytes(reply_data[0..8].try_into().unwrap());
    assert_eq!(reply_seq, seq, "sequence mismatch at round {}", seq);
  }

  rep_task.await.unwrap()?;

  latencies.sort_unstable();
  let n = latencies.len();
  println!(
    "Ping-pong latency ({} rounds): p50={:?}, p99={:?}, p99.9={:?}",
    ROUNDS,
    latencies[n / 2],
    latencies[n * 99 / 100],
    latencies[n * 999 / 1000],
  );

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 1.3 — High-Pipelined Sustained Messaging (DEALER/ROUTER)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_dealer_router_sustained_pipelined() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let router = ctx.socket(SocketType::Router)?;
  router.set_option_raw(SNDHWM, &5_000i32.to_ne_bytes()).await?;
  router.set_option_raw(RCVHWM, &5_000i32.to_ne_bytes()).await?;
  router.set_option_raw(SNDTIMEO, &100i32.to_ne_bytes()).await?;
  router.set_option_raw(RCVTIMEO, &100i32.to_ne_bytes()).await?;
  router.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;

  router.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&router).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  const NUM_DEALERS: usize = 8;
  const PIPELINE_DEPTH: usize = 256;
  const TEST_DURATION: Duration = Duration::from_secs(300);

  let stop = Arc::new(AtomicBool::new(false));
  let total_sent = Arc::new(AtomicU64::new(0));
  let total_received = Arc::new(AtomicU64::new(0));

  // ROUTER echo task
  let router_task = tokio::spawn({
    let router = router.clone();
    let stop = stop.clone();
    async move {
      while !stop.load(Ordering::Relaxed) {
        let id_msg = match router.recv().await {
          Ok(m) if m.is_more() => m,
          Ok(_) => continue,
          Err(ZmqError::Timeout) => continue,
          Err(_) => break,
        };
        let payload_msg = match router.recv().await {
          Ok(m) => m,
          Err(_) => break,
        };
        let id_bytes = match id_msg.data_bytes() {
          Some(b) => b,
          None => continue,
        };
        let mut id_echo = Msg::from_bytes(id_bytes);
        id_echo.set_flags(MsgFlags::MORE);
        let _ = router.send(id_echo).await;
        let _ = router.send(payload_msg).await;
      }
      Ok::<(), ZmqError>(())
    }
  });

  // DEALER client tasks
  let mut dealer_tasks = Vec::new();
  for i in 0..NUM_DEALERS {
    let dealer = ctx.socket(SocketType::Dealer)?;
    dealer.set_option_raw(SNDHWM, &5_000i32.to_ne_bytes()).await?;
    dealer.set_option_raw(RCVHWM, &5_000i32.to_ne_bytes()).await?;
    dealer.set_option_raw(SNDTIMEO, &100i32.to_ne_bytes()).await?;
    dealer.set_option_raw(RCVTIMEO, &100i32.to_ne_bytes()).await?;
    dealer.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;
    let routing_id = format!("dealer-{:02}", i);
    dealer
      .set_option_raw(ROUTING_ID, routing_id.as_bytes())
      .await?;
    dealer.connect(&endpoint).await?;

    let sem = Arc::new(Semaphore::new(PIPELINE_DEPTH));
    let stop = stop.clone();
    let total_sent = total_sent.clone();
    let total_received = total_received.clone();

    dealer_tasks.push(tokio::spawn(async move {
      // Recv task: runs concurrently with send loop
      let recv_handle = tokio::spawn({
        let dealer = dealer.clone();
        let sem = sem.clone();
        let total_received = total_received.clone();
        let stop = stop.clone();
        async move {
          while !stop.load(Ordering::Relaxed) {
            match dealer.recv().await {
              Ok(_) => {
                total_received.fetch_add(1, Ordering::Relaxed);
                sem.add_permits(1);
              }
              Err(ZmqError::Timeout) => continue,
              Err(_) => break,
            }
          }
        }
      });

      // Send loop
      let start = Instant::now();
      while start.elapsed() < TEST_DURATION && !stop.load(Ordering::Relaxed) {
        match sem.acquire().await {
          Ok(permit) => {
            let seq = total_sent.fetch_add(1, Ordering::Relaxed);
            let payload = seq.to_be_bytes().to_vec();
            match dealer.send(Msg::from_vec(payload)).await {
              Ok(()) => permit.forget(),
              Err(_) => break,
            }
          }
          Err(_) => break, // Semaphore closed
        }
      }

      recv_handle.abort();
      let _ = recv_handle.await;
      Ok::<(), ZmqError>(())
    }));
  }

  tokio::time::sleep(Duration::from_millis(200)).await;
  tokio::time::sleep(TEST_DURATION).await;
  stop.store(true, Ordering::SeqCst);

  // Allow in-flight messages to drain
  tokio::time::sleep(Duration::from_secs(3)).await;

  for task in dealer_tasks {
    let _ = task.await;
  }
  router_task.abort();
  let _ = router_task.await;

  let sent = total_sent.load(Ordering::Relaxed);
  let received = total_received.load(Ordering::Relaxed);
  println!(
    "DEALER/ROUTER pipeline: sent={}, received={}, in-flight={}",
    sent,
    received,
    sent.saturating_sub(received)
  );

  let max_inflight = (PIPELINE_DEPTH * NUM_DEALERS) as u64;
  assert!(
    received <= sent,
    "received ({}) > sent ({})",
    received,
    sent
  );
  assert!(
    sent.saturating_sub(received) <= max_inflight,
    "excessive in-flight: sent={}, received={}, pipeline_depth={}",
    sent,
    received,
    max_inflight
  );

  ctx.term().await?;
  Ok(())
}
