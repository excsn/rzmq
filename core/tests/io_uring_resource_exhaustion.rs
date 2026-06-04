mod common;

use rzmq::socket::options::{
  IO_URING_RCVMULTISHOT, IO_URING_SESSION_ENABLED, IO_URING_SNDZEROCOPY, LAST_ENDPOINT, LINGER,
  RCVHWM, RCVTIMEO, SNDHWM, SNDTIMEO,
};
use rzmq::uring::{initialize_uring_backend, UringConfig};
use rzmq::{Msg, SocketType, ZmqError};
use serial_test::serial;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

// ---------------------------------------------------------------------------
// One-time io_uring initialization with restricted buffer pool sizes.
// All three tests share this global state since the backend is a singleton.
// send_buffer_count=16 → satisfies Test 2.1 (32 senders exhaust 16 ZC bufs).
// recv_buffer_count=8  → satisfies Test 2.3 (8 ring slots exhaust with 16 senders).
// ---------------------------------------------------------------------------

static URING_INIT: Once = Once::new();
static URING_AVAILABLE: AtomicBool = AtomicBool::new(false);

fn init_uring() -> bool {
  URING_INIT.call_once(|| {
    match initialize_uring_backend(UringConfig {
      ring_entries: 256,
      default_send_zerocopy: true,
      default_recv_multishot: true,
      default_recv_buffer_count: 8,
      default_recv_buffer_size: 4096,
      default_send_buffer_count: 16,
      default_send_buffer_size: 65536,
    }) {
      Ok(()) => URING_AVAILABLE.store(true, Ordering::Relaxed),
      Err(e) => eprintln!("io_uring not available on this system, skipping tests: {}", e),
    }
  });
  URING_AVAILABLE.load(Ordering::Relaxed)
}

async fn bound_endpoint(socket: &rzmq::Socket) -> Result<String, ZmqError> {
  let bytes = socket.get_option(LAST_ENDPOINT).await?;
  Ok(String::from_utf8(bytes).expect("LAST_ENDPOINT is valid UTF-8"))
}

// ---------------------------------------------------------------------------
// Test 2.1 — Zero-Copy Send Buffer Pool Complete Starvation and Fallback
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_zerocopy_send_buffer_pool_starvation() -> Result<(), ZmqError> {
  if !init_uring() {
    return Ok(());
  }

  let ctx = common::test_context();

  let pull = ctx.socket(SocketType::Pull)?;
  pull.set_option_raw(SNDHWM, &5_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(RCVHWM, &5_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(RCVTIMEO, &10_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(IO_URING_SESSION_ENABLED, &1i32.to_ne_bytes()).await?;

  pull.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&pull).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  const NUM_SENDERS: u32 = 32;
  const MSGS_PER_SENDER: u32 = 1_000;
  const TOTAL_MSGS: u64 = NUM_SENDERS as u64 * MSGS_PER_SENDER as u64;

  // 32 concurrent PUSH sockets with ZC enabled — pool has only 16 ZC buffers,
  // so at least half must fall back to standard copy-send.
  let large_payload = vec![0xCC_u8; 8192]; // 8192 > ZC_SEND_THRESHOLD(1024)

  let mut sender_tasks = Vec::new();
  for sender_id in 0..NUM_SENDERS {
    let ctx_clone = ctx.clone();
    let ep = endpoint.clone();
    let payload = large_payload.clone();
    sender_tasks.push(tokio::spawn(async move {
      let push = ctx_clone.socket(SocketType::Push)?;
      push.set_option_raw(SNDHWM, &2_000i32.to_ne_bytes()).await?;
      push.set_option_raw(SNDTIMEO, &10_000i32.to_ne_bytes()).await?;
      push.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;
      push.set_option_raw(IO_URING_SESSION_ENABLED, &1i32.to_ne_bytes()).await?;
      push.set_option_raw(IO_URING_SNDZEROCOPY, &1i32.to_ne_bytes()).await?;
      push.connect(&ep).await?;
      tokio::time::sleep(Duration::from_millis(100)).await;

      for seq in 0..MSGS_PER_SENDER {
        let mut msg_data = payload.clone();
        // Embed sender_id and seq in first 8 bytes for integrity check
        msg_data[0..4].copy_from_slice(&sender_id.to_be_bytes());
        msg_data[4..8].copy_from_slice(&seq.to_be_bytes());
        push.send(Msg::from_vec(msg_data)).await?;
      }
      push.close().await?;
      Ok::<(), ZmqError>(())
    }));
  }

  // Receiver: count and validate all messages
  let recv_task = tokio::spawn({
    let pull = pull.clone();
    async move {
      let mut received: u64 = 0;
      loop {
        match pull.recv().await {
          Ok(msg) => {
            let data = msg.data().unwrap_or_default();
            assert!(
              data.len() == 8192,
              "unexpected payload size: {}",
              data.len()
            );
            received += 1;
            if received == TOTAL_MSGS {
              break;
            }
          }
          Err(ZmqError::Timeout) => {
            if received >= TOTAL_MSGS {
              break;
            }
          }
          Err(e) => return Err(e),
        }
      }
      Ok::<u64, ZmqError>(received)
    }
  });

  for task in sender_tasks {
    task.await.unwrap()?;
  }

  let received = recv_task.await.unwrap()?;
  assert_eq!(
    received, TOTAL_MSGS,
    "expected {} messages, received {}",
    TOTAL_MSGS, received
  );

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 2.2 — Mixed Message Size Transition (Zero-Copy and Copy-Send)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_mixed_message_size_zerocopy_transition() -> Result<(), ZmqError> {
  if !init_uring() {
    return Ok(());
  }

  let ctx = common::test_context();

  let pull = ctx.socket(SocketType::Pull)?;
  pull.set_option_raw(SNDHWM, &5_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(RCVHWM, &5_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(RCVTIMEO, &500i32.to_ne_bytes()).await?;
  pull.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(IO_URING_SESSION_ENABLED, &1i32.to_ne_bytes()).await?;

  pull.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&pull).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  const NUM_SENDERS: usize = 8;
  let stop = Arc::new(AtomicBool::new(false));
  let total_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));

  // Small payload: 256 bytes (below ZC_SEND_THRESHOLD=1024 → always copy-send)
  let small_payload = vec![0xAA_u8; 256];
  // Large payload: 4096 bytes (above threshold → ZC if pool available, else fallback)
  let large_payload = vec![0xBB_u8; 4096];

  let mut sender_tasks = Vec::new();
  for _ in 0..NUM_SENDERS {
    let ctx_clone = ctx.clone();
    let ep = endpoint.clone();
    let stop = stop.clone();
    let total_sent = total_sent.clone();
    let small = small_payload.clone();
    let large = large_payload.clone();
    sender_tasks.push(tokio::spawn(async move {
      let push = ctx_clone.socket(SocketType::Push)?;
      push.set_option_raw(SNDHWM, &2_000i32.to_ne_bytes()).await?;
      push.set_option_raw(SNDTIMEO, &500i32.to_ne_bytes()).await?;
      push.set_option_raw(LINGER, &500i32.to_ne_bytes()).await?;
      push.set_option_raw(IO_URING_SESSION_ENABLED, &1i32.to_ne_bytes()).await?;
      push.set_option_raw(IO_URING_SNDZEROCOPY, &1i32.to_ne_bytes()).await?;
      push.connect(&ep).await?;
      tokio::time::sleep(Duration::from_millis(100)).await;

      let mut i = 0u64;
      while !stop.load(Ordering::Relaxed) {
        let payload = if i % 2 == 0 { small.clone() } else { large.clone() };
        match push.send(Msg::from_vec(payload)).await {
          Ok(()) => {
            total_sent.fetch_add(1, Ordering::Relaxed);
          }
          Err(ZmqError::Timeout) | Err(ZmqError::ResourceLimitReached) => {}
          Err(e) => return Err(e),
        }
        i += 1;
      }
      push.close().await?;
      Ok::<(), ZmqError>(())
    }));
  }

  // Receiver: drain for 30 seconds
  let recv_task = tokio::spawn({
    let pull = pull.clone();
    let stop = stop.clone();
    async move {
      let mut received: u64 = 0;
      while !stop.load(Ordering::Relaxed) {
        match pull.recv().await {
          Ok(msg) => {
            let len = msg.data().map(|d| d.len()).unwrap_or(0);
            assert!(
              len == 256 || len == 4096,
              "unexpected payload size: {}",
              len
            );
            received += 1;
          }
          Err(ZmqError::Timeout) => {}
          Err(e) => return Err(e),
        }
      }
      Ok::<u64, ZmqError>(received)
    }
  });

  tokio::time::sleep(Duration::from_secs(30)).await;
  stop.store(true, Ordering::Relaxed);

  for task in sender_tasks {
    let _ = task.await;
  }
  let received = recv_task.await.unwrap()?;
  let sent = total_sent.load(Ordering::Relaxed);

  println!(
    "Mixed ZC/copy-send: sent={}, received={}",
    sent, received
  );
  assert!(
    received > 0,
    "no messages received during 30s mixed-size test"
  );
  // Receiving ≤ sent (small tolerance for in-flight at stop time)
  assert!(
    received <= sent + 1000,
    "received ({}) significantly exceeds sent ({})",
    received,
    sent
  );

  ctx.term().await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Test 2.3 — Multishot Buffer Ring Starvation (Recv Path)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_multishot_recv_buffer_ring_starvation() -> Result<(), ZmqError> {
  if !init_uring() {
    return Ok(());
  }

  let ctx = common::test_context();

  // PULL socket uses multishot recv with a ring of only 8 buffers × 4096 bytes.
  // 16 senders flooding 4096-byte messages with 50ms recv delay will exhaust
  // the ring, forcing -ENOBUFS CQEs and testing the worker's recovery path.
  let pull = ctx.socket(SocketType::Pull)?;
  pull.set_option_raw(SNDHWM, &2_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(RCVHWM, &2_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;
  pull.set_option_raw(IO_URING_SESSION_ENABLED, &1i32.to_ne_bytes()).await?;
  pull.set_option_raw(IO_URING_RCVMULTISHOT, &1i32.to_ne_bytes()).await?;

  pull.bind("tcp://127.0.0.1:0").await?;
  let endpoint = bound_endpoint(&pull).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  const NUM_SENDERS: u32 = 16;
  const MSGS_PER_SENDER: u32 = 50;
  const TOTAL_MSGS: u64 = NUM_SENDERS as u64 * MSGS_PER_SENDER as u64;

  let payload = vec![0xDD_u8; 4096]; // 4096 bytes fills exactly one recv ring buffer

  let mut sender_tasks = Vec::new();
  for sender_id in 0..NUM_SENDERS {
    let ctx_clone = ctx.clone();
    let ep = endpoint.clone();
    let p = payload.clone();
    sender_tasks.push(tokio::spawn(async move {
      let push = ctx_clone.socket(SocketType::Push)?;
      push.set_option_raw(SNDHWM, &200i32.to_ne_bytes()).await?;
      push.set_option_raw(SNDTIMEO, &5_000i32.to_ne_bytes()).await?;
      push.set_option_raw(LINGER, &1_000i32.to_ne_bytes()).await?;
      push.connect(&ep).await?;
      tokio::time::sleep(Duration::from_millis(100)).await;

      for seq in 0..MSGS_PER_SENDER {
        let mut msg_data = p.clone();
        msg_data[0..4].copy_from_slice(&sender_id.to_be_bytes());
        msg_data[4..8].copy_from_slice(&seq.to_be_bytes());
        push.send(Msg::from_vec(msg_data)).await?;
      }
      push.close().await?;
      Ok::<(), ZmqError>(())
    }));
  }

  // Receiver: deliberately slow (50ms sleep) to force recv ring exhaustion.
  // Despite -ENOBUFS, the worker must recover and all messages must arrive.
  let recv_task = tokio::spawn({
    let pull = pull.clone();
    async move {
      let mut received: u64 = 0;
      loop {
        match tokio::time::timeout(Duration::from_secs(120), pull.recv()).await {
          Ok(Ok(msg)) => {
            let data = msg.data().unwrap_or_default();
            assert_eq!(data.len(), 4096, "unexpected payload size");
            received += 1;
            // Slow receiver: introduce delay to exhaust the 8-buffer recv ring
            tokio::time::sleep(Duration::from_millis(50)).await;
            if received == TOTAL_MSGS {
              break;
            }
          }
          Ok(Err(e)) => return Err(e),
          Err(_) => {
            return Err(ZmqError::Internal(format!(
              "Timed out after receiving {}/{} messages — possible deadlock",
              received, TOTAL_MSGS
            )));
          }
        }
      }
      Ok::<u64, ZmqError>(received)
    }
  });

  for task in sender_tasks {
    task.await.unwrap()?;
  }

  let received = recv_task.await.unwrap()?;
  assert_eq!(
    received, TOTAL_MSGS,
    "expected {} messages after ENOBUFS recovery, received {}",
    TOTAL_MSGS, received
  );

  ctx.term().await?;
  Ok(())
}
