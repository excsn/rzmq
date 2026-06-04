mod common;

use rzmq::socket::options::{LINGER, RCVHWM, RCVTIMEO, ROUTING_ID, SNDHWM, SNDTIMEO, SUBSCRIBE, TCP_CORK};
use rzmq::socket::SocketEvent;
use rzmq::{Msg, MsgFlags, Socket, SocketType, ZmqError};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

#[cfg(feature = "io-uring")]
use rzmq::socket::options::{IO_URING_RCVMULTISHOT, IO_URING_SESSION_ENABLED, IO_URING_SNDZEROCOPY};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct StressConfig {
  pub use_io_uring: bool,
  pub send_zerocopy: bool,
  pub recv_multishot: bool,
  pub tcp_cork: bool,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn calculate_checksum(data: &[u8]) -> u32 {
  data.iter().fold(0u32, |acc, &b| acc ^ (b as u32))
}

/// Returns the number of open file descriptors for the current process, if the
/// platform supports it.  On Linux this reads /proc/self/fd; other platforms
/// return None and the associated assertions are skipped.
fn count_open_fds() -> Option<usize> {
  #[cfg(target_os = "linux")]
  {
    return std::fs::read_dir("/proc/self/fd")
      .ok()
      .map(|entries| entries.filter_map(|e| e.ok()).count());
  }
  #[allow(unreachable_code)]
  None
}

/// Asserts that no file descriptors were leaked.  A tolerance of +2 is
/// permitted to absorb any transient async-cleanup racing.
fn check_fd_leak(before: Option<usize>, after: Option<usize>) {
  if let (Some(b), Some(a)) = (before, after) {
    assert!(
      a <= b + 2,
      "file descriptor leak: {} fds before ctx.term(), {} after",
      b,
      a
    );
  }
}

async fn apply_stress_config(socket: &Socket, config: StressConfig) -> Result<(), ZmqError> {
  socket.set_option_raw(SNDHWM, &(1000i32).to_ne_bytes()).await?;
  socket.set_option_raw(RCVHWM, &(1000i32).to_ne_bytes()).await?;
  socket.set_option_raw(SNDTIMEO, &(5000i32).to_ne_bytes()).await?;
  socket.set_option_raw(RCVTIMEO, &(5000i32).to_ne_bytes()).await?;
  socket.set_option_raw(LINGER, &(1000i32).to_ne_bytes()).await?;

  #[cfg(target_os = "linux")]
  if config.tcp_cork {
    socket.set_option_raw(TCP_CORK, &(1i32).to_ne_bytes()).await?;
  }

  #[cfg(feature = "io-uring")]
  if config.use_io_uring {
    socket
      .set_option_raw(IO_URING_SESSION_ENABLED, &(1i32).to_ne_bytes())
      .await?;
    socket
      .set_option_raw(IO_URING_SNDZEROCOPY, &(config.send_zerocopy as i32).to_ne_bytes())
      .await?;
    socket
      .set_option_raw(IO_URING_RCVMULTISHOT, &(config.recv_multishot as i32).to_ne_bytes())
      .await?;
  }

  Ok(())
}

// ---------------------------------------------------------------------------
// Workload A — checksum stream
// ---------------------------------------------------------------------------

pub async fn run_checksum_stream_test(config: StressConfig, endpoint: &str) -> Result<(), ZmqError> {
  let fds_before = count_open_fds();
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  let push = ctx.socket(SocketType::Push)?;

  apply_stress_config(&pull, config).await?;
  apply_stress_config(&push, config).await?;

  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  const MSG_COUNT: u64 = 1000;

  let sender = tokio::spawn({
    let push = push.clone();
    async move {
      for i in 0..MSG_COUNT {
        let mut payload = Vec::with_capacity(64);
        payload.extend_from_slice(&i.to_be_bytes()); // 8-byte sequence ID
        payload.extend_from_slice(b"-stress-test-payload-for-integrity-");
        let checksum = calculate_checksum(&payload);
        payload.extend_from_slice(&checksum.to_be_bytes()); // 4-byte trailing checksum
        push.send(Msg::from_vec(payload)).await?;
      }
      Ok::<(), ZmqError>(())
    }
  });

  let receiver = tokio::spawn({
    let pull = pull.clone();
    async move {
      for i in 0..MSG_COUNT {
        let msg = pull.recv().await?;
        let data = msg.data().unwrap_or_default();

        assert!(
          data.len() >= 12,
          "msg {} too short: {} bytes",
          i,
          data.len()
        );

        let seq = u64::from_be_bytes(data[..8].try_into().unwrap());
        assert_eq!(seq, i, "sequence ID mismatch at message {}", i);

        let body_len = data.len() - 4;
        let received = u32::from_be_bytes(data[body_len..].try_into().unwrap());
        let computed = calculate_checksum(&data[..body_len]);
        assert_eq!(received, computed, "data corruption at message {}", i);
      }
      Ok::<(), ZmqError>(())
    }
  });

  let (s, r) = tokio::join!(sender, receiver);
  s.unwrap()?;
  r.unwrap()?;

  ctx.term().await?;
  check_fd_leak(fds_before, count_open_fds());
  Ok(())
}

// ---------------------------------------------------------------------------
// Workload B — multipart fragmentation
// ---------------------------------------------------------------------------

pub async fn run_multipart_fragmentation_test(
  config: StressConfig,
  endpoint: &str,
) -> Result<(), ZmqError> {
  let fds_before = count_open_fds();
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  let push = ctx.socket(SocketType::Push)?;

  apply_stress_config(&pull, config).await?;
  apply_stress_config(&push, config).await?;

  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  const MSG_COUNT: usize = 100;

  let sender = tokio::spawn({
    let push = push.clone();
    async move {
      for i in 0..MSG_COUNT {
        let num_parts = 5 + (i % 6); // 5 to 10 parts
        let mut parts = Vec::with_capacity(num_parts);
        for j in 0..num_parts {
          let mut frame =
            Msg::from_vec(format!("frame-msg-{}-part-{}", i, j).into_bytes());
          if j < num_parts - 1 {
            frame.set_flags(MsgFlags::MORE);
          }
          parts.push(frame);
        }
        push.send_multipart(parts).await?;
      }
      Ok::<(), ZmqError>(())
    }
  });

  let receiver = tokio::spawn({
    let pull = pull.clone();
    async move {
      for i in 0..MSG_COUNT {
        let frames = pull.recv_multipart().await?;
        let expected_parts = 5 + (i % 6);

        assert_eq!(
          frames.len(),
          expected_parts,
          "frame count mismatch at message {}",
          i
        );

        for j in 0..expected_parts {
          let expected = format!("frame-msg-{}-part-{}", i, j);
          assert_eq!(
            frames[j].data().unwrap_or_default(),
            expected.as_bytes(),
            "frame content mismatch at msg {} part {}",
            i,
            j
          );
        }
      }
      Ok::<(), ZmqError>(())
    }
  });

  let (s, r) = tokio::join!(sender, receiver);
  s.unwrap()?;
  r.unwrap()?;

  ctx.term().await?;
  check_fd_leak(fds_before, count_open_fds());
  Ok(())
}

// ---------------------------------------------------------------------------
// Workload C — connection churn
// ---------------------------------------------------------------------------

pub async fn run_connection_churn_test(config: StressConfig, endpoint: &str) -> Result<(), ZmqError> {
  let fds_before = count_open_fds();
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;

  apply_stress_config(&pull, config).await?;
  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  const CHURN_COUNT: usize = 20;

  // Receiver — counts exactly CHURN_COUNT messages regardless of arrival order
  let receiver = tokio::spawn({
    let pull = pull.clone();
    async move {
      for _ in 0..CHURN_COUNT {
        pull.recv().await?;
      }
      Ok::<(), ZmqError>(())
    }
  });

  // Short-lived senders — each connects, waits for handshake confirmation, sends, closes
  let mut sender_tasks = Vec::with_capacity(CHURN_COUNT);
  for i in 0..CHURN_COUNT {
    let ctx_clone = ctx.clone();
    let ep = endpoint.to_string();
    sender_tasks.push(tokio::spawn(async move {
      let push = ctx_clone.socket(SocketType::Push)?;
      apply_stress_config(&push, config).await?;
      // Create monitor before connect so no events are missed
      let monitor = push.monitor_default().await?;
      push.connect(&ep).await?;
      // Block until the ZMTP handshake completes — guarantees an active pipe
      common::wait_for_monitor_event(
        &monitor,
        Duration::from_secs(3),
        Duration::from_millis(50),
        |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }),
      )
      .await
      .map_err(|e| ZmqError::Internal(format!("handshake sync failed for sender {}: {}", i, e)))?;
      push.send(Msg::from_vec(format!("churn-{}", i).into_bytes())).await?;
      push.close().await?;
      Ok::<(), ZmqError>(())
    }));
  }

  for task in sender_tasks {
    task.await.unwrap()?;
  }
  receiver.await.unwrap()?;

  ctx.term().await?;
  check_fd_leak(fds_before, count_open_fds());
  Ok(())
}

// ---------------------------------------------------------------------------
// Port counters for matrix tests (each spawned test gets a unique port)
// ---------------------------------------------------------------------------

static URING_STREAM_PORT: AtomicU16 = AtomicU16::new(27000);
static URING_PINGPONG_PORT: AtomicU16 = AtomicU16::new(28000);

// ---------------------------------------------------------------------------
// Workload D — unidirectional stream (PUSH/PULL and PUB/SUB)
// ---------------------------------------------------------------------------

pub async fn run_unidirectional_stream(
  sender_type: SocketType,
  receiver_type: SocketType,
  config: StressConfig,
  endpoint: &str,
) -> Result<(), ZmqError> {
  let fds_before = count_open_fds();
  let ctx = common::test_context();
  let receiver = ctx.socket(receiver_type)?;
  let sender = ctx.socket(sender_type)?;

  apply_stress_config(&receiver, config).await?;
  apply_stress_config(&sender, config).await?;

  // SUB sockets must subscribe before bind to catch all messages
  if receiver_type == SocketType::Sub {
    receiver.set_option_raw(SUBSCRIBE, b"").await?;
  }

  receiver.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  // For PUB/SUB, monitor the sender to wait for subscription propagation before
  // streaming begins — prevents "slow-joiner" packet loss.
  let monitor_rx = if receiver_type == SocketType::Sub {
    Some(sender.monitor_default().await?)
  } else {
    None
  };

  sender.connect(endpoint).await?;

  if let Some(ref monitor) = monitor_rx {
    common::wait_for_monitor_event(
      monitor,
      Duration::from_secs(3),
      Duration::from_millis(50),
      |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }),
    )
    .await
    .map_err(|e| ZmqError::Internal(format!("PUB/SUB handshake sync failed: {}", e)))?;
    tokio::time::sleep(Duration::from_millis(100)).await; // subscription propagation
  } else {
    tokio::time::sleep(Duration::from_millis(150)).await;
  }

  const MSG_COUNT: u64 = 1000;

  let sender_task = tokio::spawn({
    let sender = sender.clone();
    async move {
      for i in 0..MSG_COUNT {
        let mut payload = Vec::with_capacity(64);
        payload.extend_from_slice(&i.to_be_bytes());
        payload.extend_from_slice(b"-stress-stream-payload-");
        let checksum = calculate_checksum(&payload);
        payload.extend_from_slice(&checksum.to_be_bytes());
        sender.send(Msg::from_vec(payload)).await?;
      }
      Ok::<(), ZmqError>(())
    }
  });

  let receiver_task = tokio::spawn({
    let receiver = receiver.clone();
    async move {
      for i in 0..MSG_COUNT {
        let msg = receiver.recv().await?;
        let data = msg.data().unwrap_or_default();
        assert!(data.len() >= 12, "msg {} too short: {} bytes", i, data.len());

        let seq = u64::from_be_bytes(data[..8].try_into().unwrap());
        assert_eq!(seq, i, "sequence ID mismatch at message {}", i);

        let body_len = data.len() - 4;
        let received = u32::from_be_bytes(data[body_len..].try_into().unwrap());
        let computed = calculate_checksum(&data[..body_len]);
        assert_eq!(received, computed, "data corruption at message {}", i);
      }
      Ok::<(), ZmqError>(())
    }
  });

  let (s, r) = tokio::join!(sender_task, receiver_task);
  s.unwrap()?;
  r.unwrap()?;

  ctx.term().await?;
  check_fd_leak(fds_before, count_open_fds());
  Ok(())
}

// ---------------------------------------------------------------------------
// Workload E — bidirectional ping-pong (REQ/REP and DEALER/ROUTER)
// ---------------------------------------------------------------------------

pub async fn run_bidirectional_ping_pong(
  client_type: SocketType,
  server_type: SocketType,
  config: StressConfig,
  endpoint: &str,
) -> Result<(), ZmqError> {
  let fds_before = count_open_fds();
  let ctx = common::test_context();
  let server = ctx.socket(server_type)?;
  let client = ctx.socket(client_type)?;

  apply_stress_config(&server, config).await?;
  apply_stress_config(&client, config).await?;

  // DEALER clients need a stable routing identity so ROUTER can address them back.
  if client_type == SocketType::Dealer {
    client.set_option_raw(ROUTING_ID, b"dealer-client-identity").await?;
  }

  server.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  client.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  // For DEALER/ROUTER, prime the ROUTER's identity table with one round-trip
  // before entering the concurrent test loop.
  if server_type == SocketType::Router {
    client.send(Msg::from_static(b"PING-HANDSHAKE")).await?;
    let _id_frame = server.recv().await?; // routing identity (MORE flag)
    let ping = server.recv().await?; // payload
    assert_eq!(ping.data().unwrap_or_default(), b"PING-HANDSHAKE");
  }

  const ROUNDS: u64 = 500;

  let client_task = tokio::spawn({
    let client = client.clone();
    async move {
      for i in 0..ROUNDS {
        let mut req = Vec::with_capacity(64);
        req.extend_from_slice(&i.to_be_bytes());
        req.extend_from_slice(b"-request-payload-");
        let req_sum = calculate_checksum(&req);
        req.extend_from_slice(&req_sum.to_be_bytes());

        client.send(Msg::from_vec(req)).await?;

        let reply_msg = client.recv().await?;
        let reply = reply_msg.data().unwrap_or_default();

        let seq = u64::from_be_bytes(reply[..8].try_into().unwrap());
        assert_eq!(seq, i, "reply sequence mismatch at round {}", i);

        let body_len = reply.len() - 4;
        let rec_sum = u32::from_be_bytes(reply[body_len..].try_into().unwrap());
        let comp_sum = calculate_checksum(&reply[..body_len]);
        assert_eq!(rec_sum, comp_sum, "reply data corruption at round {}", i);
      }
      Ok::<(), ZmqError>(())
    }
  });

  let server_task = tokio::spawn({
    let server = server.clone();
    async move {
      for i in 0..ROUNDS {
        // ROUTER receives [identity_frame(MORE), payload_frame]
        let target_id = if server_type == SocketType::Router {
          let id_msg = server.recv().await?;
          Some(id_msg.data_bytes().unwrap())
        } else {
          None
        };

        let req_msg = server.recv().await?;
        let req = req_msg.data().unwrap_or_default();

        let seq = u64::from_be_bytes(req[..8].try_into().unwrap());
        assert_eq!(seq, i, "request sequence mismatch at round {}", i);

        let mut rep = Vec::with_capacity(64);
        rep.extend_from_slice(&i.to_be_bytes());
        rep.extend_from_slice(b"-reply-payload-");
        let rep_sum = calculate_checksum(&rep);
        rep.extend_from_slice(&rep_sum.to_be_bytes());

        // ROUTER must send identity frame first so DEALER receives only the payload
        if let Some(id_bytes) = target_id {
          let mut id_frame = Msg::from_bytes(id_bytes);
          id_frame.set_flags(MsgFlags::MORE);
          server.send(id_frame).await?;
        }
        server.send(Msg::from_vec(rep)).await?;
      }
      Ok::<(), ZmqError>(())
    }
  });

  let (c, s) = tokio::join!(client_task, server_task);
  c.unwrap()?;
  s.unwrap()?;

  ctx.term().await?;
  check_fd_leak(fds_before, count_open_fds());
  Ok(())
}

// ---------------------------------------------------------------------------
// Standard test suite (all platforms)
// ---------------------------------------------------------------------------

mod standard_tests {
  use super::*;
  use rstest::rstest;

  #[rstest]
  #[case::standard_tcp(
    StressConfig { use_io_uring: false, send_zerocopy: false, recv_multishot: false, tcp_cork: false },
    "tcp://127.0.0.1:25501"
  )]
  #[cfg_attr(target_os = "linux", case::tcp_with_cork(
    StressConfig { use_io_uring: false, send_zerocopy: false, recv_multishot: false, tcp_cork: true },
    "tcp://127.0.0.1:25502"
  ))]
  #[tokio::test]
  async fn test_standard_checksum_stream(
    #[case] config: StressConfig,
    #[case] endpoint: &'static str,
  ) {
    run_checksum_stream_test(config, endpoint)
      .await
      .expect("standard stream test failed");
  }

  #[rstest]
  #[case::standard_tcp(
    StressConfig { use_io_uring: false, send_zerocopy: false, recv_multishot: false, tcp_cork: false },
    "tcp://127.0.0.1:25503"
  )]
  #[cfg_attr(target_os = "linux", case::tcp_with_cork(
    StressConfig { use_io_uring: false, send_zerocopy: false, recv_multishot: false, tcp_cork: true },
    "tcp://127.0.0.1:25504"
  ))]
  #[tokio::test]
  async fn test_standard_multipart_fragmentation(
    #[case] config: StressConfig,
    #[case] endpoint: &'static str,
  ) {
    run_multipart_fragmentation_test(config, endpoint)
      .await
      .expect("standard multipart test failed");
  }

  #[rstest]
  #[case::standard_tcp(
    StressConfig { use_io_uring: false, send_zerocopy: false, recv_multishot: false, tcp_cork: false },
    "tcp://127.0.0.1:25505"
  )]
  #[cfg_attr(target_os = "linux", case::tcp_with_cork(
    StressConfig { use_io_uring: false, send_zerocopy: false, recv_multishot: false, tcp_cork: true },
    "tcp://127.0.0.1:25506"
  ))]
  #[tokio::test]
  async fn test_standard_connection_churn(
    #[case] config: StressConfig,
    #[case] endpoint: &'static str,
  ) {
    run_connection_churn_test(config, endpoint)
      .await
      .expect("standard connection churn test failed");
  }

  #[rstest]
  #[case::push_pull(SocketType::Push, SocketType::Pull, "tcp://127.0.0.1:26001")]
  #[case::pub_sub(SocketType::Pub, SocketType::Sub, "tcp://127.0.0.1:26002")]
  #[tokio::test]
  async fn test_standard_unidirectional_stream(
    #[case] sender_type: SocketType,
    #[case] receiver_type: SocketType,
    #[case] endpoint: &'static str,
  ) {
    let config = StressConfig {
      use_io_uring: false,
      send_zerocopy: false,
      recv_multishot: false,
      tcp_cork: false,
    };
    run_unidirectional_stream(sender_type, receiver_type, config, endpoint)
      .await
      .expect("standard unidirectional stream test failed");
  }

  #[rstest]
  #[case::req_rep(SocketType::Req, SocketType::Rep, "tcp://127.0.0.1:26003")]
  #[case::dealer_router(SocketType::Dealer, SocketType::Router, "tcp://127.0.0.1:26004")]
  #[tokio::test]
  async fn test_standard_bidirectional_ping_pong(
    #[case] client_type: SocketType,
    #[case] server_type: SocketType,
    #[case] endpoint: &'static str,
  ) {
    let config = StressConfig {
      use_io_uring: false,
      send_zerocopy: false,
      recv_multishot: false,
      tcp_cork: false,
    };
    run_bidirectional_ping_pong(client_type, server_type, config, endpoint)
      .await
      .expect("standard bidirectional ping-pong test failed");
  }
}

// ---------------------------------------------------------------------------
// io_uring test suite (Linux + io-uring feature only)
// ---------------------------------------------------------------------------

#[cfg(feature = "io-uring")]
mod uring_tests {
  use super::*;
  use rstest::rstest;

  #[rstest]
  #[case::uring_basic(
    StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: false, tcp_cork: false },
    "tcp://127.0.0.1:25601"
  )]
  #[case::uring_zerocopy(
    StressConfig { use_io_uring: true, send_zerocopy: true, recv_multishot: false, tcp_cork: false },
    "tcp://127.0.0.1:25602"
  )]
  #[case::uring_multishot(
    StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: true, tcp_cork: false },
    "tcp://127.0.0.1:25603"
  )]
  #[case::uring_fully_optimized(
    StressConfig { use_io_uring: true, send_zerocopy: true, recv_multishot: true, tcp_cork: false },
    "tcp://127.0.0.1:25604"
  )]
  #[cfg_attr(target_os = "linux", case::uring_with_cork(
    StressConfig { use_io_uring: true, send_zerocopy: true, recv_multishot: true, tcp_cork: true },
    "tcp://127.0.0.1:25605"
  ))]
  #[tokio::test]
  async fn test_uring_checksum_stream(
    #[case] config: StressConfig,
    #[case] endpoint: &'static str,
  ) {
    run_checksum_stream_test(config, endpoint)
      .await
      .expect("io_uring stream test failed");
  }

  #[rstest]
  #[case::uring_basic(
    StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: false, tcp_cork: false },
    "tcp://127.0.0.1:25701"
  )]
  #[case::uring_zerocopy(
    StressConfig { use_io_uring: true, send_zerocopy: true, recv_multishot: false, tcp_cork: false },
    "tcp://127.0.0.1:25702"
  )]
  #[case::uring_multishot(
    StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: true, tcp_cork: false },
    "tcp://127.0.0.1:25703"
  )]
  #[case::uring_fully_optimized(
    StressConfig { use_io_uring: true, send_zerocopy: true, recv_multishot: true, tcp_cork: false },
    "tcp://127.0.0.1:25704"
  )]
  #[cfg_attr(target_os = "linux", case::uring_with_cork(
    StressConfig { use_io_uring: true, send_zerocopy: true, recv_multishot: true, tcp_cork: true },
    "tcp://127.0.0.1:25705"
  ))]
  #[tokio::test]
  async fn test_uring_multipart_fragmentation(
    #[case] config: StressConfig,
    #[case] endpoint: &'static str,
  ) {
    run_multipart_fragmentation_test(config, endpoint)
      .await
      .expect("io_uring multipart test failed");
  }

  #[rstest]
  #[case::uring_basic(
    StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: false, tcp_cork: false },
    "tcp://127.0.0.1:25801"
  )]
  #[case::uring_zerocopy(
    StressConfig { use_io_uring: true, send_zerocopy: true, recv_multishot: false, tcp_cork: false },
    "tcp://127.0.0.1:25802"
  )]
  #[case::uring_multishot(
    StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: true, tcp_cork: false },
    "tcp://127.0.0.1:25803"
  )]
  #[case::uring_fully_optimized(
    StressConfig { use_io_uring: true, send_zerocopy: true, recv_multishot: true, tcp_cork: false },
    "tcp://127.0.0.1:25804"
  )]
  #[cfg_attr(target_os = "linux", case::uring_with_cork(
    StressConfig { use_io_uring: true, send_zerocopy: true, recv_multishot: true, tcp_cork: true },
    "tcp://127.0.0.1:25805"
  ))]
  #[tokio::test]
  async fn test_uring_connection_churn(
    #[case] config: StressConfig,
    #[case] endpoint: &'static str,
  ) {
    run_connection_churn_test(config, endpoint)
      .await
      .expect("io_uring connection churn test failed");
  }

  // Matrix: 2 socket-type pairs × 4 io_uring configs = 8 tests per function.
  // Each test allocates a unique port from the global atomic counter.

  #[rstest]
  #[case::push_pull(SocketType::Push, SocketType::Pull)]
  #[case::pub_sub(SocketType::Pub, SocketType::Sub)]
  #[tokio::test]
  async fn test_uring_unidirectional_matrix(
    #[case] sender_type: SocketType,
    #[case] receiver_type: SocketType,
    #[values(
      StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: false, tcp_cork: false },
      StressConfig { use_io_uring: true, send_zerocopy: true,  recv_multishot: false, tcp_cork: false },
      StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: true,  tcp_cork: false },
      StressConfig { use_io_uring: true, send_zerocopy: true,  recv_multishot: true,  tcp_cork: false }
    )]
    config: StressConfig,
  ) {
    let port = URING_STREAM_PORT.fetch_add(1, Ordering::Relaxed);
    let endpoint = format!("tcp://127.0.0.1:{}", port);
    run_unidirectional_stream(sender_type, receiver_type, config, &endpoint)
      .await
      .expect("io_uring unidirectional matrix test failed");
  }

  #[rstest]
  #[case::req_rep(SocketType::Req, SocketType::Rep)]
  #[case::dealer_router(SocketType::Dealer, SocketType::Router)]
  #[tokio::test]
  async fn test_uring_bidirectional_matrix(
    #[case] client_type: SocketType,
    #[case] server_type: SocketType,
    #[values(
      StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: false, tcp_cork: false },
      StressConfig { use_io_uring: true, send_zerocopy: true,  recv_multishot: false, tcp_cork: false },
      StressConfig { use_io_uring: true, send_zerocopy: false, recv_multishot: true,  tcp_cork: false },
      StressConfig { use_io_uring: true, send_zerocopy: true,  recv_multishot: true,  tcp_cork: false }
    )]
    config: StressConfig,
  ) {
    let port = URING_PINGPONG_PORT.fetch_add(1, Ordering::Relaxed);
    let endpoint = format!("tcp://127.0.0.1:{}", port);
    run_bidirectional_ping_pong(client_type, server_type, config, &endpoint)
      .await
      .expect("io_uring bidirectional matrix test failed");
  }
}
