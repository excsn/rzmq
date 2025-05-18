// benches/dealer_router_throughput.rs

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rzmq::{
  socket::{
    options::{RCVHWM, SNDHWM},
    MonitorReceiver, SocketEvent,
  },
  Blob, Context, Msg, MsgFlags, SocketType, ZmqError,
};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::time::{sleep, timeout};

// --- Benchmarking Constants ---
const NUM_DEALER_ROUTER_MSGS: usize = 1000;
const BIND_ADDR_DEALER_ROUTER: &str = "tcp://127.0.0.1:5683"; // Unique port
const SETUP_TIMEOUT_DEALER_ROUTER: Duration = Duration::from_secs(10); // Increased setup timeout
const EVENT_RECV_TIMEOUT_DEALER_ROUTER: Duration = Duration::from_secs(4);
const BENCH_HWM_DEALER_ROUTER: i32 = 100_000;
const SETUP_PING_RETRY_TIMEOUT: Duration = Duration::from_millis(200); // Timeout for individual recv in setup ping
const SETUP_PING_MAX_ATTEMPTS: usize = 25; // (25 * (100ms sleep + 2*200ms recv)) = ~12.5s max for ping

// --- Helper: wait_for_event ---
async fn wait_for_event_dealer_router(
  monitor_rx: &MonitorReceiver,
  check_event: impl Fn(&SocketEvent) -> bool,
) -> Result<SocketEvent, String> {
  let start_time = Instant::now();
  loop {
    if start_time.elapsed() > EVENT_RECV_TIMEOUT_DEALER_ROUTER {
      return Err(format!("Timeout after {:?}", EVENT_RECV_TIMEOUT_DEALER_ROUTER));
    }
    match timeout(Duration::from_millis(50), monitor_rx.recv()).await {
      Ok(Ok(event)) => {
        if check_event(&event) {
          return Ok(event);
        }
      }
      Ok(Err(e)) => return Err(format!("Monitor channel error: {}", e)),
      Err(_) => {} // Timeout poll, continue
    }
  }
}

// --- Setup Function for DEALER-ROUTER ---
async fn setup_dealer_router(ctx: &Context) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let dealer = ctx.socket(SocketType::Dealer)?;
  let router = ctx.socket(SocketType::Router)?;

  dealer
    .set_option(SNDHWM, &BENCH_HWM_DEALER_ROUTER.to_ne_bytes())
    .await?;
  dealer
    .set_option(RCVHWM, &BENCH_HWM_DEALER_ROUTER.to_ne_bytes())
    .await?;
  router
    .set_option(SNDHWM, &BENCH_HWM_DEALER_ROUTER.to_ne_bytes())
    .await?;
  router
    .set_option(RCVHWM, &BENCH_HWM_DEALER_ROUTER.to_ne_bytes())
    .await?;

  let router_monitor = router.monitor_default().await?;

  router.bind(BIND_ADDR_DEALER_ROUTER).await?;
  wait_for_event_dealer_router(
    &router_monitor,
    |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == BIND_ADDR_DEALER_ROUTER),
  )
  .await
  .map_err(|e| ZmqError::Internal(format!("ROUTER Listening event error: {}", e)))?;

  dealer.connect(BIND_ADDR_DEALER_ROUTER).await?;
  wait_for_event_dealer_router(&router_monitor, |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }))
    .await
    .map_err(|e| ZmqError::Internal(format!("ROUTER HandshakeSucceeded event error: {}", e)))?;

  sleep(Duration::from_millis(50)).await; // Allow PeerIdentityEstablished to propagate

  // Robust PING_SETUP sequence to ensure Router is ready and knows Dealer's identity path
  dealer.send(Msg::from_static(b"PING_SETUP")).await?;

  let mut id_frame_opt: Option<Msg> = None;
  let mut payload_frame_opt: Option<Msg> = None;

  for attempt in 0..SETUP_PING_MAX_ATTEMPTS {
    // Try to receive identity
    match timeout(SETUP_PING_RETRY_TIMEOUT, router.recv()).await {
      Ok(Ok(id_frame_candidate)) => {
        if !id_frame_candidate.is_more() {
          eprintln!(
            "[Setup] PING_SETUP attempt {}: Received ID frame without MORE flag. Retrying.",
            attempt
          );
          sleep(Duration::from_millis(100)).await; // Brief pause before full retry
          dealer.send(Msg::from_static(b"PING_SETUP")).await?; // Resend ping
          continue;
        }
        id_frame_opt = Some(id_frame_candidate);

        // Now try for payload
        match timeout(SETUP_PING_RETRY_TIMEOUT, router.recv()).await {
          Ok(Ok(payload_frame_candidate)) => {
            if payload_frame_candidate.data().map_or(false, |d| d == b"PING_SETUP")
              && !payload_frame_candidate.is_more()
            {
              payload_frame_opt = Some(payload_frame_candidate);
              break; // Success!
            } else {
              eprintln!(
                "[Setup] PING_SETUP attempt {}: Received unexpected payload ({:?}) or MORE flag. Retrying.",
                attempt,
                payload_frame_candidate.data()
              );
              id_frame_opt = None; // Reset, need to get ID again
              sleep(Duration::from_millis(100)).await;
              dealer.send(Msg::from_static(b"PING_SETUP")).await?; // Resend ping
              continue;
            }
          }
          Ok(Err(e)) => {
            return Err(ZmqError::Internal(format!(
              "Router setup PING_SETUP recv payload failed (attempt {}): {}",
              attempt, e
            )));
          }
          Err(_) => {
            // Payload recv timeout
            eprintln!(
              "[Setup] PING_SETUP attempt {}: Timeout receiving payload. Retrying.",
              attempt
            );
            id_frame_opt = None; // Reset, need to get ID again
            sleep(Duration::from_millis(100)).await;
            dealer.send(Msg::from_static(b"PING_SETUP")).await?; // Resend ping
          }
        }
      }
      Ok(Err(e)) => {
        return Err(ZmqError::Internal(format!(
          "Router setup PING_SETUP recv identity failed (attempt {}): {}",
          attempt, e
        )));
      }
      Err(_) => {
        // ID recv timeout
        eprintln!(
          "[Setup] PING_SETUP attempt {}: Timeout receiving identity. Retrying.",
          attempt
        );
        sleep(Duration::from_millis(100)).await;
        dealer.send(Msg::from_static(b"PING_SETUP")).await?; // Resend ping
      }
    }
    if id_frame_opt.is_some() && payload_frame_opt.is_some() {
      break; // Successfully received the full PING_SETUP sequence
    }
  }

  if id_frame_opt.is_none() || payload_frame_opt.is_none() {
    return Err(ZmqError::Internal(
      "Router setup: PING_SETUP sequence failed after multiple attempts".to_string(),
    ));
  }

  Ok((dealer, router))
}

// --- Benchmark Function ---
fn dealer_router_tcp_throughput(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime");
  let mut group = c.benchmark_group("DEALER_ROUTER_TCP_Throughput");

  // === Criterion Settings Example ===
  group
    .warm_up_time(Duration::from_secs(5)) // Increase warm-up time
    .measurement_time(Duration::from_secs(10)) // Increase measurement time
    .sample_size(20); // Increase sample size (number of loops for measurement)
                      // Note: sample_size for iter_custom is a bit different; iter_custom runs its
                      //       async block *once* per sample. Criterion decides how many samples.
                      //       The main effect here is on how many times the setup/teardown + loop runs.

  for size in [16, 256, 1024, 4096].iter() {
    group.throughput(Throughput::Bytes((NUM_DEALER_ROUTER_MSGS * size * 2) as u64));
    let bench_id = BenchmarkId::from_parameter(format!("{}B", size));

    group.bench_with_input(bench_id, size, |b, &msg_size| {
      b.to_async(&rt).iter_custom(|_iters| async move {
        let ctx = Context::new().expect("Bench context creation failed");
        let (dealer, router) = match timeout(SETUP_TIMEOUT_DEALER_ROUTER, setup_dealer_router(&ctx)).await {
          Ok(Ok(sockets)) => sockets,
          Ok(Err(e)) => panic!("Bench DEALER-ROUTER setup failed: {}", e),
          Err(_) => panic!("Bench DEALER-ROUTER setup timed out"),
        };

        let message_payload = vec![0u8; msg_size];
        let start = Instant::now();

        let router_task = {
          let router = router.clone();
          let payload_to_send = message_payload.clone();
          tokio::spawn(async move {
            for _i in 0..NUM_DEALER_ROUTER_MSGS {
              let id_frame = router.recv().await?;
              black_box(id_frame.data());
              if !id_frame.is_more() {
                return Err(ZmqError::ProtocolViolation("Router: ID missing MORE".into()));
              }

              let mut actual_payload_frame = router.recv().await?;
              black_box(actual_payload_frame.data());
              if actual_payload_frame.is_more() {
                while actual_payload_frame.is_more() {
                  actual_payload_frame = router.recv().await?;
                }
              }

              let mut id_reply = Msg::from_vec(id_frame.data().unwrap().to_vec());
              id_reply.set_flags(MsgFlags::MORE);
              router.send(id_reply).await?;

              let payload_reply_msg = Msg::from_vec(black_box(payload_to_send.clone()));
              router.send(payload_reply_msg).await?;
            }
            Ok::<(), ZmqError>(())
          })
        };

        let dealer_task = {
          let dealer = dealer.clone();
          let payload_to_send = message_payload.clone();
          tokio::spawn(async move {
            for _i in 0..NUM_DEALER_ROUTER_MSGS {
              let msg = Msg::from_vec(black_box(payload_to_send.clone()));
              dealer.send(msg).await?;

              let mut reply_payload = dealer.recv().await?;
              black_box(reply_payload.data());
              if reply_payload.is_more() {
                while reply_payload.is_more() {
                  reply_payload = dealer.recv().await?;
                }
              }
            }
            Ok::<(), ZmqError>(())
          })
        };

        let (dealer_join_result, router_join_result) = tokio::join!(dealer_task, router_task);
        let dealer_result = dealer_join_result.expect("DEALER task panicked");
        let router_result = router_join_result.expect("ROUTER task panicked");

        let elapsed = start.elapsed();
        sleep(Duration::from_millis(100)).await;

        if let Err(e) = dealer.close().await {
          eprintln!("[Iter Warning] Error closing DEALER: {}", e);
        }
        if let Err(e) = router.close().await {
          eprintln!("[Iter Warning] Error closing ROUTER: {}", e);
        }
        ctx.term().await.expect("Context termination failed");

        if let Err(e) = dealer_result {
          panic!("Benchmark DEALER task failed: {}", e);
        }
        if let Err(e) = router_result {
          panic!("Benchmark ROUTER task failed: {}", e);
        }

        elapsed
      });
    });
  }
  group.finish();
}

criterion_group!(benches_dealer_router, dealer_router_tcp_throughput);
criterion_main!(benches_dealer_router);
