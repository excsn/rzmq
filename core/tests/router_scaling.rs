mod common;

use common::wait_for_monitor_event;
use rzmq::socket::options::{LAST_ENDPOINT, LINGER, RCVTIMEO, ROUTER_MANDATORY, ROUTING_ID, SNDTIMEO};
use rzmq::socket::SocketEvent;
use rzmq::{Msg, MsgFlags, SocketType, ZmqError};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

async fn bound_endpoint(socket: &rzmq::Socket) -> Result<String, ZmqError> {
  let bytes = socket.get_option(LAST_ENDPOINT).await?;
  Ok(String::from_utf8(bytes).expect("LAST_ENDPOINT is valid UTF-8"))
}

// ---------------------------------------------------------------------------
// Test 3.1 — Massive Scaled Connection Storm (ROUTER)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_router_massive_connection_storm() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let router = ctx.socket(SocketType::Router)?;
  router.set_option_raw(RCVTIMEO, &5_000i32.to_ne_bytes()).await?;
  router.set_option_raw(SNDTIMEO, &1_000i32.to_ne_bytes()).await?;
  router.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;

  router.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&router).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  const NUM_DEALERS: usize = 500;

  // Spawn 500 DEALER tasks; each connects, waits for handshake, sends one message
  let mut dealer_tasks = Vec::new();
  for i in 0..NUM_DEALERS {
    let ctx_clone = ctx.clone();
    let ep = endpoint.clone();
    dealer_tasks.push(tokio::spawn(async move {
      let dealer = ctx_clone.socket(SocketType::Dealer)?;
      dealer.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;
      let routing_id = format!("client-{:04}", i);
      dealer
        .set_option_raw(ROUTING_ID, routing_id.as_bytes())
        .await?;

      let monitor = dealer.monitor_default().await?;
      dealer.connect(&ep).await?;

      wait_for_monitor_event(
        &monitor,
        Duration::from_secs(10),
        Duration::from_millis(50),
        |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }),
      )
      .await
      .map_err(|e| ZmqError::Internal(format!("client-{:04} handshake timeout: {}", i, e)))?;

      let payload = format!("hello-{}", i);
      dealer.send(Msg::from_vec(payload.into_bytes())).await?;
      dealer.close().await?;
      Ok::<(), ZmqError>(())
    }));
  }

  // ROUTER receives exactly NUM_DEALERS messages and collects unique identities
  let router_task = tokio::spawn({
    let router = router.clone();
    async move {
      let mut identities = HashSet::new();
      for _ in 0..NUM_DEALERS {
        // Identity frame (has MORE flag)
        let id_msg = router.recv().await?;
        // Payload frame
        let _payload = router.recv().await?;
        if let Some(id_data) = id_msg.data() {
          identities.insert(id_data.to_vec());
        }
      }
      Ok::<HashSet<Vec<u8>>, ZmqError>(identities)
    }
  });

  for task in dealer_tasks {
    task.await.unwrap()?;
  }

  let identities = router_task.await.unwrap()?;
  assert_eq!(
    identities.len(),
    NUM_DEALERS,
    "expected {} unique identities, got {}",
    NUM_DEALERS,
    identities.len()
  );

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 3.2 — High-Frequency Connection Churn (Connect / Disconnect Cycle)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_router_high_frequency_connection_churn() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let router = ctx.socket(SocketType::Router)?;
  router
    .set_option_raw(ROUTER_MANDATORY, &0i32.to_ne_bytes())
    .await?; // mandatory=false so stale sends don't error fatally
  router.set_option_raw(RCVTIMEO, &50i32.to_ne_bytes()).await?;
  router.set_option_raw(SNDTIMEO, &50i32.to_ne_bytes()).await?;
  router.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

  router.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&router).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Shared list of currently-active identities for background send task
  let active_ids: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
  let stop = Arc::new(AtomicBool::new(false));
  let churn_counter = Arc::new(AtomicU64::new(0));

  // Background send task: constantly sends to random identities from the active list.
  // Keeps ROUTER's RouterMap read lock heavily contended while writes happen.
  let bg_send_task = tokio::spawn({
    let router = router.clone();
    let active_ids = active_ids.clone();
    let stop = stop.clone();
    async move {
      let mut idx: u64 = 0;
      while !stop.load(Ordering::Relaxed) {
        let identity = {
          let ids = active_ids.read().await;
          if ids.is_empty() {
            drop(ids);
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
          }
          ids[(idx as usize) % ids.len()].clone()
        };
        idx += 1;

        let mut id_frame = Msg::from_vec(identity.into_bytes());
        id_frame.set_flags(MsgFlags::MORE);
        let _ = router.send(id_frame).await;
        let _ = router.send(Msg::from_static(b"probe")).await;
      }
    }
  });

  // ROUTER echo + drain task
  let router_echo_task = tokio::spawn({
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
        let payload = match router.recv().await {
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
        let _ = router.send(payload).await;
      }
    }
  });

  // 10 churn waves of 50 DEALER clients each
  const WAVES: usize = 10;
  const DEALERS_PER_WAVE: usize = 50;

  for wave in 0..WAVES {
    let mut wave_ids = Vec::new();
    let mut wave_tasks = Vec::new();

    for j in 0..DEALERS_PER_WAVE {
      let id_str = format!("churn-w{:02}-c{:02}", wave, j);
      wave_ids.push(id_str.clone());

      let ctx_clone = ctx.clone();
      let ep = endpoint.clone();
      wave_tasks.push(tokio::spawn(async move {
        let dealer = ctx_clone.socket(SocketType::Dealer)?;
        dealer.set_option_raw(LINGER, &200i32.to_ne_bytes()).await?;
        dealer.set_option_raw(RCVTIMEO, &2_000i32.to_ne_bytes()).await?;
        dealer.set_option_raw(SNDTIMEO, &2_000i32.to_ne_bytes()).await?;
        dealer
          .set_option_raw(ROUTING_ID, id_str.as_bytes())
          .await?;

        let monitor = dealer.monitor_default().await?;
        dealer.connect(&ep).await?;

        wait_for_monitor_event(
          &monitor,
          Duration::from_secs(5),
          Duration::from_millis(50),
          |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }),
        )
        .await
        .map_err(|e| ZmqError::Internal(format!("wave dealer handshake failed: {}", e)))?;

        dealer.send(Msg::from_static(b"ping")).await?;
        // Reply may or may not arrive if ROUTER is busy — tolerate timeout
        let _ = dealer.recv().await;
        dealer.close().await?;
        Ok::<(), ZmqError>(())
      }));
    }

    // Add this wave's IDs to the shared list
    {
      let mut ids = active_ids.write().await;
      ids.extend(wave_ids.clone());
    }

    for task in wave_tasks {
      task.await.unwrap()?;
    }

    churn_counter.fetch_add(DEALERS_PER_WAVE as u64, Ordering::Relaxed);

    // Remove this wave's IDs after clients close
    {
      let mut ids = active_ids.write().await;
      ids.retain(|id| !wave_ids.contains(id));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
  }

  // Run for 2 minutes total
  tokio::time::sleep(Duration::from_secs(120)).await;

  stop.store(true, Ordering::Relaxed);
  bg_send_task.abort();
  router_echo_task.abort();
  let _ = bg_send_task.await;
  let _ = router_echo_task.await;

  let total_churned = churn_counter.load(Ordering::Relaxed);
  println!("Churn test complete: {} total connections processed", total_churned);

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 3.3 — ROUTER Send to Aborted Peer (Mandatory vs Non-Mandatory)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_router_send_to_aborted_peer() -> Result<(), ZmqError> {
  // --- Scenario A: ROUTER_MANDATORY = false ---
  // Sending to a closed peer silently drops the message (Ok result).
  {
    let ctx = common::test_context();

    let router = ctx.socket(SocketType::Router)?;
    router
      .set_option_raw(ROUTER_MANDATORY, &0i32.to_ne_bytes())
      .await?;
    router.set_option_raw(SNDTIMEO, &500i32.to_ne_bytes()).await?;
    router.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

    router.bind("tcp://127.0.0.1:0").await?;
    let endpoint = bound_endpoint(&router).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let dealer = ctx.socket(SocketType::Dealer)?;
    dealer.set_option_raw(LINGER, &0i32.to_ne_bytes()).await?;
    let routing_id = b"peer-a";
    dealer.set_option_raw(ROUTING_ID, routing_id).await?;

    let monitor = dealer.monitor_default().await?;
    dealer.connect(&endpoint).await?;
    wait_for_monitor_event(
      &monitor,
      Duration::from_secs(5),
      Duration::from_millis(50),
      |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }),
    )
    .await
    .map_err(|e| ZmqError::Internal(format!("Scenario A handshake failed: {}", e)))?;

    // Register the identity with ROUTER by sending one message
    dealer.send(Msg::from_static(b"hello")).await?;
    let _id = router.recv().await?;
    let _payload = router.recv().await?;

    // Abruptly close the dealer
    dealer.close().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ROUTER sends to the now-closed peer — with mandatory=false, should succeed silently
    let mut id_frame = Msg::from_vec(b"peer-a".to_vec());
    id_frame.set_flags(MsgFlags::MORE);
    let send_result = router.send(id_frame).await;
    let _ = router.send(Msg::from_static(b"dropped")).await;

    assert!(
      send_result.is_ok(),
      "Scenario A (mandatory=false): expected Ok, got {:?}",
      send_result
    );

    ctx.term().await?;
  }

  // --- Scenario B: ROUTER_MANDATORY = true ---
  // Sending to a closed peer returns HostUnreachable.
  {
    let ctx = common::test_context();

    let router = ctx.socket(SocketType::Router)?;
    router
      .set_option_raw(ROUTER_MANDATORY, &1i32.to_ne_bytes())
      .await?;
    router.set_option_raw(SNDTIMEO, &500i32.to_ne_bytes()).await?;
    router.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;

    router.bind("tcp://127.0.0.1:0").await?;
    let endpoint = bound_endpoint(&router).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let dealer = ctx.socket(SocketType::Dealer)?;
    dealer.set_option_raw(LINGER, &0i32.to_ne_bytes()).await?;
    let routing_id = b"peer-b";
    dealer.set_option_raw(ROUTING_ID, routing_id).await?;

    let monitor = dealer.monitor_default().await?;
    dealer.connect(&endpoint).await?;
    wait_for_monitor_event(
      &monitor,
      Duration::from_secs(5),
      Duration::from_millis(50),
      |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }),
    )
    .await
    .map_err(|e| ZmqError::Internal(format!("Scenario B handshake failed: {}", e)))?;

    // Register the identity
    dealer.send(Msg::from_static(b"hello")).await?;
    let _id = router.recv().await?;
    let _payload = router.recv().await?;

    dealer.close().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ROUTER sends to the closed peer — with mandatory=true, should fail with HostUnreachable
    let mut id_frame = Msg::from_vec(b"peer-b".to_vec());
    id_frame.set_flags(MsgFlags::MORE);
    let send_result = router.send(id_frame).await;

    assert!(
      matches!(send_result, Err(ZmqError::HostUnreachable(_))),
      "Scenario B (mandatory=true): expected HostUnreachable, got {:?}",
      send_result
    );

    ctx.term().await?;
  }

  Ok(())
}
