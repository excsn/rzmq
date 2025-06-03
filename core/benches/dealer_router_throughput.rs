use bench_matrix::{
  criterion_runner::{
    async_suite::{AsyncBenchmarkLogicFn, AsyncBenchmarkSuite, AsyncSetupFn, AsyncTeardownFn},
    ExtractorFn, GlobalSetupFn, GlobalTeardownFn,
  },
  AbstractCombination, MatrixCellValue,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rzmq::{
  socket::{
    options::{RCVHWM, SNDHWM},
    MonitorReceiver, SocketEvent,
  },
  Blob, Context, Msg, MsgFlags, SocketType, ZmqError,
};
use std::{
  future::Future,
  pin::Pin,
  sync::Arc, // For sharing Context
  time::{Duration, Instant},
};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

// --- Benchmarking Constants (can be part of Config or remain global if fixed) ---
const NUM_DEALER_ROUTER_MSGS: usize = 1000;
const BIND_ADDR_DEALER_ROUTER_BASE: &str = "tcp://127.0.0.1";
const SETUP_TIMEOUT_DEALER_ROUTER: Duration = Duration::from_secs(10);
const EVENT_RECV_TIMEOUT_DEALER_ROUTER: Duration = Duration::from_secs(4);
const BENCH_HWM_DEALER_ROUTER: i32 = 100_000;
const SETUP_PING_RETRY_TIMEOUT: Duration = Duration::from_millis(200);
const SETUP_PING_MAX_ATTEMPTS: usize = 25;

// --- Configuration for Benchmarks ---
#[derive(Debug, Clone)]
pub struct ConfigDealerRouter {
  pub msg_size: usize,
  pub port: u16, // To allow multiple concurrent benchmarks if needed, or for parameterization
                 // Add other parameters here if they vary, e.g., HWM, num_msgs
}

// --- State and Context for Benchmarks ---
#[derive(Default, Debug)] // Context can be simple for this one
struct BenchContext {}

struct BenchState {
  ctx_arc: Arc<Context>, // Share context for cleanup
  dealer_socket: rzmq::Socket,
  router_socket: rzmq::Socket,
  message_payload: Vec<u8>,
  // router_monitor: MonitorReceiver, // Keep monitor if needed for teardown checks
}

// --- Helper: wait_for_event (from original, unchanged) ---
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

// --- Extractor Function ---
fn extract_config(combo: &AbstractCombination) -> Result<ConfigDealerRouter, String> {
  let msg_size = combo.get_u64(0)? as usize;
  let port_offset = combo.get_u64(1)? as u16; // Assuming second param is a port offset or ID

  Ok(ConfigDealerRouter {
    msg_size,
    port: 5683 + port_offset, // Base port + offset
  })
}

// --- Global Setup (Optional, e.g. if something needs to be done once per config type) ---
// For this specific benchmark, a global setup might not be strictly necessary per ConfigDealerRouter variant
// if the main setup is socket creation. But we can include it for demonstration.
fn bench_global_setup(_cfg: &ConfigDealerRouter) -> Result<(), String> {
  // println!("[GLOBAL SETUP] Config: {:?}", _cfg);
  // E.g., initialize some global resource pool if needed
  Ok(())
}

// --- Async Setup Function ---
fn setup_dealer_router_bench(
  _runtime: &Runtime, // Tokio runtime provided by bench_matrix
  cfg: &ConfigDealerRouter,
) -> Pin<Box<dyn Future<Output = Result<(BenchContext, BenchState), String>> + Send>> {
  let cfg_clone = cfg.clone();
  Box::pin(async move {
    let zmq_ctx = Context::new().map_err(|e| format!("Zmq Context creation failed: {}", e))?;
    let ctx_arc = Arc::new(zmq_ctx); // Wrap in Arc for sharing and later cleanup

    let dealer = ctx_arc
      .socket(SocketType::Dealer)
      .map_err(|e| format!("Dealer socket creation failed: {}", e))?;
    let router = ctx_arc
      .socket(SocketType::Router)
      .map_err(|e| format!("Router socket creation failed: {}", e))?;

    dealer
      .set_option(SNDHWM, &BENCH_HWM_DEALER_ROUTER.to_ne_bytes())
      .await
      .map_err(|e| format!("Dealer SNDHWM failed: {}", e))?;
    dealer
      .set_option(RCVHWM, &BENCH_HWM_DEALER_ROUTER.to_ne_bytes())
      .await
      .map_err(|e| format!("Dealer RCVHWM failed: {}", e))?;
    router
      .set_option(SNDHWM, &BENCH_HWM_DEALER_ROUTER.to_ne_bytes())
      .await
      .map_err(|e| format!("Router SNDHWM failed: {}", e))?;
    router
      .set_option(RCVHWM, &BENCH_HWM_DEALER_ROUTER.to_ne_bytes())
      .await
      .map_err(|e| format!("Router RCVHWM failed: {}", e))?;

    let router_monitor = router
      .monitor_default()
      .await
      .map_err(|e| format!("Router monitor creation failed: {}", e))?;

    let bind_addr = format!("{}:{}", BIND_ADDR_DEALER_ROUTER_BASE, cfg_clone.port);

    router
      .bind(&bind_addr)
      .await
      .map_err(|e| format!("Router bind to {} failed: {}", bind_addr, e))?;
    wait_for_event_dealer_router(
      &router_monitor,
      |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == &bind_addr),
    )
    .await
    .map_err(|e| format!("ROUTER Listening event error: {}", e))?;

    dealer
      .connect(&bind_addr)
      .await
      .map_err(|e| format!("Dealer connect to {} failed: {}", bind_addr, e))?;
    wait_for_event_dealer_router(&router_monitor, |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }))
      .await
      .map_err(|e| format!("ROUTER HandshakeSucceeded event error: {}", e))?;

    sleep(Duration::from_millis(50)).await;

    // Robust PING_SETUP sequence (from original)
    dealer
      .send(Msg::from_static(b"PING_SETUP"))
      .await
      .map_err(|e| format!("Dealer PING_SETUP send failed: {}", e))?;
    let mut id_frame_opt: Option<Msg> = None;
    let mut payload_frame_opt: Option<Msg> = None;

    for attempt in 0..SETUP_PING_MAX_ATTEMPTS {
      match timeout(SETUP_PING_RETRY_TIMEOUT, router.recv()).await {
        Ok(Ok(id_frame_candidate)) => {
          if !id_frame_candidate.is_more() {
            eprintln!(
              "[Setup] PING_SETUP attempt {}: Received ID frame without MORE flag for {}. Retrying.",
              attempt, bind_addr
            );
            sleep(Duration::from_millis(100)).await;
            dealer
              .send(Msg::from_static(b"PING_SETUP"))
              .await
              .map_err(|e| format!("Dealer PING_SETUP resend failed: {}", e))?;
            continue;
          }
          id_frame_opt = Some(id_frame_candidate);
          match timeout(SETUP_PING_RETRY_TIMEOUT, router.recv()).await {
            Ok(Ok(payload_frame_candidate)) => {
              if payload_frame_candidate.data().map_or(false, |d| d == b"PING_SETUP")
                && !payload_frame_candidate.is_more()
              {
                payload_frame_opt = Some(payload_frame_candidate);
                break;
              } else {
                eprintln!(
                  "[Setup] PING_SETUP attempt {}: Received unexpected payload ({:?}) or MORE flag for {}. Retrying.",
                  attempt,
                  payload_frame_candidate.data(),
                  bind_addr
                );
                id_frame_opt = None;
                sleep(Duration::from_millis(100)).await;
                dealer
                  .send(Msg::from_static(b"PING_SETUP"))
                  .await
                  .map_err(|e| format!("Dealer PING_SETUP resend failed: {}", e))?;
              }
            }
            Ok(Err(e)) => {
              return Err(format!(
                "Router PING_SETUP recv payload failed (attempt {}): {}",
                attempt, e
              ))
            }
            Err(_) => {
              eprintln!(
                "[Setup] PING_SETUP attempt {}: Timeout receiving payload for {}. Retrying.",
                attempt, bind_addr
              );
              id_frame_opt = None;
              sleep(Duration::from_millis(100)).await;
              dealer
                .send(Msg::from_static(b"PING_SETUP"))
                .await
                .map_err(|e| format!("Dealer PING_SETUP resend failed: {}", e))?;
            }
          }
        }
        Ok(Err(e)) => {
          return Err(format!(
            "Router PING_SETUP recv identity failed (attempt {}): {}",
            attempt, e
          ))
        }
        Err(_) => {
          eprintln!(
            "[Setup] PING_SETUP attempt {}: Timeout receiving identity for {}. Retrying.",
            attempt, bind_addr
          );
          sleep(Duration::from_millis(100)).await;
          dealer
            .send(Msg::from_static(b"PING_SETUP"))
            .await
            .map_err(|e| format!("Dealer PING_SETUP resend failed: {}", e))?;
        }
      }
      if id_frame_opt.is_some() && payload_frame_opt.is_some() {
        break;
      }
    }
    if id_frame_opt.is_none() || payload_frame_opt.is_none() {
      return Err(format!(
        "Router setup: PING_SETUP sequence failed for {} after multiple attempts",
        bind_addr
      ));
    }
    // End PING_SETUP

    let message_payload = vec![0u8; cfg_clone.msg_size];
    Ok((
      BenchContext {},
      BenchState {
        ctx_arc,
        dealer_socket: dealer,
        router_socket: router,
        message_payload,
      },
    ))
  })
}

// --- Async Benchmark Logic ---
fn benchmark_logic(
  ctx: BenchContext,
  state: BenchState,
  _cfg: &ConfigDealerRouter, // cfg might be used if NUM_DEALER_ROUTER_MSGS varied
) -> Pin<Box<dyn Future<Output = (BenchContext, BenchState, Duration)> + Send>> {
  Box::pin(async move {
    let start_time = Instant::now();

    let router_task: JoinHandle<Result<(), ZmqError>> = {
      let router = state.router_socket.clone(); // Clone for the task
      let payload_to_send = state.message_payload.clone();
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
        Ok(())
      })
    };

    let dealer_task: JoinHandle<Result<(), ZmqError>> = {
      let dealer = state.dealer_socket.clone(); // Clone for the task
      let payload_to_send = state.message_payload.clone();
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
        Ok(())
      })
    };

    let (dealer_join_result, router_join_result) = tokio::join!(dealer_task, router_task);

    let elapsed = start_time.elapsed();

    // Handle task results
    match dealer_join_result {
      Ok(Ok(_)) => {} // Dealer success
      Ok(Err(e)) => panic!("Benchmark DEALER task failed: {}", e),
      Err(e) => panic!("Benchmark DEALER task panicked: {}", e),
    }
    match router_join_result {
      Ok(Ok(_)) => {} // Router success
      Ok(Err(e)) => panic!("Benchmark ROUTER task failed: {}", e),
      Err(e) => panic!("Benchmark ROUTER task panicked: {}", e),
    }

    (ctx, state, elapsed)
  })
}

// --- Async Teardown Function ---
fn teardown_dealer_router_bench(
  _ctx: BenchContext,
  state: BenchState,
  _runtime: &Runtime,
  _cfg: &ConfigDealerRouter,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
  Box::pin(async move {
    // Close sockets first
    if let Err(e) = state.dealer_socket.close().await {
      eprintln!("[Teardown Warning] Error closing DEALER: {}", e);
    }
    if let Err(e) = state.router_socket.close().await {
      eprintln!("[Teardown Warning] Error closing ROUTER: {}", e);
    }
    // Then terminate context
    // The Arc<Context> will be dropped here if this is the last reference.
    // rzmq's Context::drop handles term implicitly if not already termed.
    // Explicit term can be done if needed, but often drop is sufficient.
    // For explicit term:
    // if let Ok(ctx) = Arc::try_unwrap(state.ctx_arc) { // Only if we are the sole owner
    //    ctx.term().await.expect("Context termination failed in teardown");
    // } else {
    //    eprintln!("[Teardown Warning] Context Arc still shared, not terminating explicitly.");
    // }
    // Simpler: rely on Drop if no other complex cleanup with context is needed.
    // The ctx_arc being part of BenchState ensures it's dropped when BenchState is.
    sleep(Duration::from_millis(50)).await; // Brief pause for cleanup propagation
  })
}

// --- Global Teardown (Optional) ---
fn bench_global_teardown(_cfg: &ConfigDealerRouter) -> Result<(), String> {
  // println!("[GLOBAL TEARDOWN] Config: {:?}", _cfg);
  Ok(())
}

// --- Main Benchmark Function ---
fn dealer_router_matrix_benchmark(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime for benchmarks");

  let parameter_axes = vec![
    // Axis 0: Message Size
    vec![
      MatrixCellValue::Unsigned(16),
      MatrixCellValue::Unsigned(256),
      MatrixCellValue::Unsigned(1024),
      MatrixCellValue::Unsigned(4096),
    ],
    // Axis 1: Port Offset
    vec![
      MatrixCellValue::Unsigned(0),
      MatrixCellValue::Unsigned(1),
      MatrixCellValue::Unsigned(2),
      MatrixCellValue::Unsigned(3),
    ],
  ];

  // Define names for these axes
  let parameter_names = vec!["MsgSize".to_string(), "PortOffset".to_string()];

  let suite = AsyncBenchmarkSuite::new(
    c,
    &rt,
    "DEALER_ROUTER_Matrix_TCP_Throughput".to_string(), // Base name for the suite
    Some(parameter_names),
    parameter_axes,
    Box::new(extract_config),
    setup_dealer_router_bench,
    benchmark_logic,
    teardown_dealer_router_bench,
  )
  .global_setup(bench_global_setup) // Optional global setup
  .global_teardown(bench_global_teardown) // Optional global teardown
  .configure_criterion_group(|group| {
    // Configure the Criterion group for each combination
    group
      .warm_up_time(Duration::from_secs(3)) // Adjusted warm-up
      .measurement_time(Duration::from_secs(5)) // Adjusted measurement
      .sample_size(15); // Adjusted sample size
  })
  .throughput(|cfg: &ConfigDealerRouter| {
    // Calculate throughput based on the config
    Throughput::Bytes((NUM_DEALER_ROUTER_MSGS * cfg.msg_size * 2) as u64) // x2 for send and receive
  });

  suite.run();
}

criterion_group!(benches, dealer_router_matrix_benchmark);
criterion_main!(benches);
