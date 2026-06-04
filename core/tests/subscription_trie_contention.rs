mod common;

use rzmq::socket::options::{LAST_ENDPOINT, LINGER, RCVTIMEO, SNDTIMEO, SUBSCRIBE, UNSUBSCRIBE};
use rzmq::{Msg, SocketType, ZmqError};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn bound_endpoint(socket: &rzmq::Socket) -> Result<String, ZmqError> {
  let bytes = socket.get_option(LAST_ENDPOINT).await?;
  Ok(String::from_utf8(bytes).expect("LAST_ENDPOINT is valid UTF-8"))
}

// ---------------------------------------------------------------------------
// Test 5.1 — Massive Nested Topic Subscription Storm
// 20 concurrent tasks each subscribe to 5,000 unique nested topics via the
// socket API. A heartbeat via the wildcard "" subscription must keep flowing.
// The exact 100K-topic count assertion lives in trie.rs unit tests.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_massive_nested_topic_subscription_storm() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let pub_sock = ctx.socket(SocketType::Pub)?;
  pub_sock.set_option_raw(SNDTIMEO, &200i32.to_ne_bytes()).await?;
  pub_sock.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  let sub_sock = ctx.socket(SocketType::Sub)?;
  sub_sock.set_option_raw(RCVTIMEO, &1_000i32.to_ne_bytes()).await?;
  sub_sock.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  // Wildcard subscription to catch all messages regardless of specific topic churn
  sub_sock.set_option_raw(SUBSCRIBE, b"").await?;

  pub_sock.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&pub_sock).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  sub_sock.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(200)).await;

  const NUM_TASKS: usize = 20;
  const TOPICS_PER_TASK: usize = 5_000;
  let stop = Arc::new(AtomicBool::new(false));

  // Heartbeat publisher: sends continuously so we can verify wildcard works
  let heartbeat_count = Arc::new(AtomicU64::new(0));
  let heartbeat_task = tokio::spawn({
    let pub_sock = pub_sock.clone();
    let stop = stop.clone();
    let count = heartbeat_count.clone();
    async move {
      let mut seq = 0u64;
      while !stop.load(Ordering::Relaxed) {
        let payload = format!("heartbeat/{}", seq);
        if pub_sock
          .send(Msg::from_vec(payload.into_bytes()))
          .await
          .is_ok()
        {
          count.fetch_add(1, Ordering::Relaxed);
          seq += 1;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
      }
    }
  });

  // Heartbeat receiver: drain SUB socket while subscriptions are being added
  let received_count = Arc::new(AtomicU64::new(0));
  let recv_task = tokio::spawn({
    let sub_sock = sub_sock.clone();
    let stop = stop.clone();
    let received = received_count.clone();
    async move {
      while !stop.load(Ordering::Relaxed) {
        match sub_sock.recv().await {
          Ok(_) => {
            received.fetch_add(1, Ordering::Relaxed);
          }
          Err(ZmqError::Timeout) => {}
          Err(e) => return Err(e),
        }
      }
      Ok::<(), ZmqError>(())
    }
  });

  // 20 concurrent subscription tasks
  let mut sub_tasks = Vec::new();
  for task_id in 0..NUM_TASKS {
    let sub_sock = sub_sock.clone();
    sub_tasks.push(tokio::spawn(async move {
      for i in 0..TOPICS_PER_TASK {
        let topic = format!(
          "sports/football/stats/player/{}/match/{}/stats",
          task_id, i
        );
        sub_sock
          .set_option_raw(SUBSCRIBE, topic.as_bytes())
          .await?;
      }
      Ok::<(), ZmqError>(())
    }));
  }

  for task in sub_tasks {
    task.await.unwrap()?;
  }

  // Give heartbeats time to arrive
  tokio::time::sleep(Duration::from_secs(2)).await;
  stop.store(true, Ordering::Relaxed);
  heartbeat_task.abort();
  let _ = heartbeat_task.await;
  recv_task.await.unwrap()?;

  let sent = heartbeat_count.load(Ordering::Relaxed);
  let received = received_count.load(Ordering::Relaxed);
  println!(
    "Subscription storm: subscriptions={}, heartbeats sent={}, received={}",
    NUM_TASKS * TOPICS_PER_TASK,
    sent,
    received
  );

  // Wildcard must have delivered heartbeats throughout; allow for slow-joiner tolerance
  assert!(
    received > 0,
    "no heartbeat messages received during subscription storm"
  );

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 5.2 — Concurrent Match and Modification (Read/Write Contention)
// 10 writer tasks (subscribe/unsubscribe) + 10 reader tasks (publish/recv)
// run concurrently for 30 seconds without deadlock or panic.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_match_and_modification() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let pub_sock = ctx.socket(SocketType::Pub)?;
  pub_sock.set_option_raw(SNDTIMEO, &100i32.to_ne_bytes()).await?;
  pub_sock.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  let sub_sock = ctx.socket(SocketType::Sub)?;
  sub_sock.set_option_raw(RCVTIMEO, &50i32.to_ne_bytes()).await?;
  sub_sock.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  // Subscribe to 10 stable base topics that are never modified
  for i in 0..10usize {
    let topic = format!("stable/{}", i);
    sub_sock.set_option_raw(SUBSCRIBE, topic.as_bytes()).await?;
  }

  pub_sock.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&pub_sock).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  sub_sock.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(200)).await;

  let stop = Arc::new(AtomicBool::new(false));
  let mut handles = Vec::new();

  // 10 writer tasks: subscribe/unsubscribe churning unique topic sets
  for task_id in 0..10usize {
    let sub_sock = sub_sock.clone();
    let stop = stop.clone();
    handles.push(tokio::spawn(async move {
      let mut i = 0u64;
      while !stop.load(Ordering::Relaxed) {
        let topic = format!("news/world/politics/{}/{}", task_id, i % 100);
        sub_sock.set_option_raw(SUBSCRIBE, topic.as_bytes()).await?;
        sub_sock
          .set_option_raw(UNSUBSCRIBE, topic.as_bytes())
          .await?;
        i += 1;
      }
      Ok::<(), ZmqError>(())
    }));
  }

  // 10 reader/publisher tasks: PUB sends to both stable and contested topics;
  // SUB drains with short timeout (may or may not receive depending on churn state).
  for task_id in 0..10usize {
    let pub_sock = pub_sock.clone();
    let sub_sock = sub_sock.clone();
    let stop = stop.clone();
    handles.push(tokio::spawn(async move {
      let mut i = 0u64;
      while !stop.load(Ordering::Relaxed) {
        // Publish to a stable topic (always subscribed)
        let stable_topic = format!("stable/{}", task_id % 10);
        let _ = pub_sock
          .send(Msg::from_vec(stable_topic.into_bytes()))
          .await;

        // Publish to a contested topic (may or may not be subscribed)
        let contested = format!("news/world/politics/{}/{}", task_id, i % 100);
        let _ = pub_sock
          .send(Msg::from_vec(contested.into_bytes()))
          .await;

        // Drain SUB (ignore Timeout)
        let _ = sub_sock.recv().await;
        i += 1;
      }
      Ok::<(), ZmqError>(())
    }));
  }

  tokio::time::sleep(Duration::from_secs(30)).await;
  stop.store(true, Ordering::Relaxed);

  for handle in &handles {
    handle.abort();
  }
  for handle in handles {
    match handle.await {
      Ok(Ok(())) => {}
      Ok(Err(e)) => {
        // Tolerate cleanup errors on close
        if !matches!(e, ZmqError::ConnectionClosed | ZmqError::InvalidState(_)) {
          return Err(e);
        }
      }
      Err(e) if e.is_cancelled() => {}
      Err(e) => {
        assert!(!e.is_panic(), "contention task panicked: {:?}", e);
      }
    }
  }

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 5.3 — Overlapping Wildcard and Specific Topic Contention
// SUB subscribes to "" (matches all). 10 tasks churn specific topics via
// subscribe/unsubscribe. PUB sends 1,000 messages; all must be received.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_overlapping_wildcard_and_specific_topic() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let pub_sock = ctx.socket(SocketType::Pub)?;
  pub_sock.set_option_raw(SNDTIMEO, &500i32.to_ne_bytes()).await?;
  pub_sock.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  let sub_sock = ctx.socket(SocketType::Sub)?;
  sub_sock.set_option_raw(RCVTIMEO, &2_000i32.to_ne_bytes()).await?;
  sub_sock.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  // Wildcard: matches ALL published messages regardless of topic churn
  sub_sock.set_option_raw(SUBSCRIBE, b"").await?;

  pub_sock.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&pub_sock).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  sub_sock.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(200)).await;

  const MSG_COUNT: u64 = 1_000;
  const SYMBOLS: &[&str] = &["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"];
  let stop = Arc::new(AtomicBool::new(false));

  // 10 churn tasks: constantly subscribe/unsubscribe specific finance topics.
  // This must NOT affect the wildcard "" subscription.
  let mut churn_handles = Vec::new();
  for task_id in 0..10usize {
    let sub_sock = sub_sock.clone();
    let stop = stop.clone();
    churn_handles.push(tokio::spawn(async move {
      let mut i = 0usize;
      while !stop.load(Ordering::Relaxed) {
        let symbol = SYMBOLS[i % SYMBOLS.len()];
        let topic = format!("finance/stocks/us/tech/{}", symbol);
        sub_sock.set_option_raw(SUBSCRIBE, topic.as_bytes()).await?;
        tokio::task::yield_now().await;
        sub_sock
          .set_option_raw(UNSUBSCRIBE, topic.as_bytes())
          .await?;
        i += task_id + 1; // different stride per task for varied access patterns
      }
      Ok::<(), ZmqError>(())
    }));
  }

  // Publisher: sends 1,000 messages with the AAPL topic
  let pub_task = tokio::spawn({
    let pub_sock = pub_sock.clone();
    async move {
      for seq in 0..MSG_COUNT {
        let msg = format!("finance/stocks/us/tech/AAPL:{}", seq);
        pub_sock.send(Msg::from_vec(msg.into_bytes())).await?;
        // Small delay to let churn tasks interleave
        tokio::time::sleep(Duration::from_millis(5)).await;
      }
      Ok::<(), ZmqError>(())
    }
  });

  // Receiver: must receive all 1,000 messages via the wildcard
  let recv_task = tokio::spawn({
    let sub_sock = sub_sock.clone();
    async move {
      let mut received = 0u64;
      while received < MSG_COUNT {
        match sub_sock.recv().await {
          Ok(_) => received += 1,
          Err(ZmqError::Timeout) => {}
          Err(e) => return Err(e),
        }
      }
      Ok::<u64, ZmqError>(received)
    }
  });

  pub_task.await.unwrap()?;
  let received = recv_task.await.unwrap()?;

  stop.store(true, Ordering::Relaxed);
  for h in &churn_handles {
    h.abort();
  }
  for h in churn_handles {
    let _ = h.await;
  }

  assert_eq!(
    received, MSG_COUNT,
    "wildcard must receive all {} messages; got {}",
    MSG_COUNT, received
  );

  ctx.term().await?;
  Ok(())
}
