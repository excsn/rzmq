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
  sync::{
    atomic::{AtomicU16, Ordering as AtomicOrdering},
    Arc,
  },
  time::{Duration, Instant},
};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

// --- Benchmarking Constants ---
const BIND_ADDR_BASE: &str = "tcp://127.0.0.1";
const SETUP_TIMEOUT: Duration = Duration::from_secs(20);
const EVENT_RECV_TIMEOUT: Duration = Duration::from_secs(5);
const BENCH_HWM: i32 = 100_000;
const PING_RETRY_TIMEOUT: Duration = Duration::from_millis(200);
const PRINT_BENCH_INFO: bool = false;

// Use a global atomic counter for unique ports.
static NEXT_BENCH_PORT: AtomicU16 = AtomicU16::new(5883);

// --- Configuration for Benchmarks ---
#[derive(Debug, Clone)]
pub struct ConfigDealerRouter {
  pub num_dealers: usize,
  pub msg_size: usize,
  pub total_requests: usize,
}

// --- State and Context for Benchmarks ---
#[derive(Default, Debug)]
struct BenchContext {}

struct BenchState {
  ctx_arc: Arc<Context>,
  dealer_sockets: Vec<rzmq::Socket>,
  router_socket: rzmq::Socket,
  message_payload: Vec<u8>,
}

// --- Helper: wait_for_event ---
async fn wait_for_event(
  monitor_rx: &MonitorReceiver,
  check_event: impl Fn(&SocketEvent) -> bool,
) -> Result<SocketEvent, String> {
  let start_time = Instant::now();
  loop {
    if start_time.elapsed() > EVENT_RECV_TIMEOUT {
      return Err(format!("Timeout after {:?}", EVENT_RECV_TIMEOUT));
    }
    match timeout(Duration::from_millis(50), monitor_rx.recv()).await {
      Ok(Ok(event)) => {
        if check_event(&event) {
          return Ok(event);
        }
      }
      Ok(Err(e)) => return Err(format!("Monitor channel error: {}", e)),
      Err(_) => {}
    }
  }
}

// --- Extractor Function ---
fn extract_config(combo: &AbstractCombination) -> Result<ConfigDealerRouter, String> {
  let num_dealers = combo.get_u64(0)? as usize;
  let msg_size = combo.get_u64(1)? as usize;
  let total_requests = combo.get_u64(2)? as usize;

  Ok(ConfigDealerRouter {
    num_dealers,
    msg_size,
    total_requests,
  })
}

// --- Global Setup/Teardown (Optional) ---
fn bench_global_setup(_cfg: &ConfigDealerRouter) -> Result<(), String> {
  Ok(())
}
fn bench_global_teardown(_cfg: &ConfigDealerRouter) -> Result<(), String> {
  Ok(())
}

// --- Async Setup Function ---
fn setup_dealer_router_bench(
  _runtime: &Runtime,
  cfg: &ConfigDealerRouter,
) -> Pin<Box<dyn Future<Output = Result<(BenchContext, BenchState), String>> + Send>> {
  let cfg_clone = cfg.clone();
  Box::pin(async move {
    if PRINT_BENCH_INFO {
      println!("[Setup] Starting for config: {:?}", cfg_clone);
    }
    let ctx = Context::new().map_err(|e| format!("Zmq Context creation failed: {}", e))?;
    let ctx_arc = Arc::new(ctx);

    // --- Setup ROUTER ---
    let router = ctx_arc
      .socket(SocketType::Router)
      .map_err(|e| format!("Router socket creation failed: {}", e))?;
    router
      .set_option(SNDHWM, BENCH_HWM)
      .await
      .map_err(|e| e.to_string())?;
    router
      .set_option(RCVHWM, BENCH_HWM)
      .await
      .map_err(|e| e.to_string())?;
    let router_monitor = router.monitor_default().await.map_err(|e| e.to_string())?;
    let port = NEXT_BENCH_PORT.fetch_add(1, AtomicOrdering::Relaxed);
    let bind_addr = format!("{}:{}", BIND_ADDR_BASE, port);
    router.bind(&bind_addr).await.map_err(|e| e.to_string())?;
    wait_for_event(
      &router_monitor,
      |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == &bind_addr),
    )
    .await?;

    // --- Setup DEALERs ---
    let mut dealer_sockets = Vec::with_capacity(cfg_clone.num_dealers);
    for i in 0..cfg_clone.num_dealers {
      let dealer = ctx_arc
        .socket(SocketType::Dealer)
        .map_err(|e| format!("Dealer {} socket creation failed: {}", i, e))?;
      dealer
        .set_option(SNDHWM, BENCH_HWM)
        .await
        .map_err(|e| e.to_string())?;
      dealer
        .set_option(RCVHWM, BENCH_HWM)
        .await
        .map_err(|e| e.to_string())?;
      dealer
        .connect(&bind_addr)
        .await
        .map_err(|e| e.to_string())?;
      dealer_sockets.push(dealer);
    }

    // --- Verify All Connections ---
    for i in 0..cfg_clone.num_dealers {
      wait_for_event(&router_monitor, |e| {
        matches!(e, SocketEvent::HandshakeSucceeded { .. })
      })
      .await
      .map_err(|e| format!("ROUTER HandshakeSucceeded event {} error: {}", i, e))?;
    }
    if PRINT_BENCH_INFO {
      println!(
        "[Setup] {} dealers on {} connected. All handshakes seen by ROUTER.",
        cfg_clone.num_dealers, bind_addr
      );
    }

    // --- PING/PONG Warmup (Untimed) ---
    for dealer in &dealer_sockets {
      dealer
        .send(Msg::from_static(b"PING_SETUP"))
        .await
        .map_err(|e| e.to_string())?;
    }
    for _ in 0..cfg_clone.num_dealers {
      let _id_frame = timeout(PING_RETRY_TIMEOUT, router.recv())
        .await
        .map_err(|e| format!("Router PING recv ID timeout: {}", e))?
        .map_err(|e| format!("Router PING recv ID error: {}", e))?;
      let _payload_frame = timeout(PING_RETRY_TIMEOUT, router.recv())
        .await
        .map_err(|e| format!("Router PING recv payload timeout: {}", e))?
        .map_err(|e| format!("Router PING recv payload error: {}", e))?;
    }
    if PRINT_BENCH_INFO {
      println!("[Setup] Warmup PING/PONG complete.");
    }

    let message_payload = vec![0u8; cfg_clone.msg_size];
    Ok((
      BenchContext {},
      BenchState {
        ctx_arc,
        dealer_sockets,
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
  cfg: &ConfigDealerRouter,
) -> Pin<Box<dyn Future<Output = (BenchContext, BenchState, Duration)> + Send>> {
  let total_requests = cfg.total_requests;
  let num_dealers = cfg.num_dealers;

  Box::pin(async move {
    let start_time = Instant::now();

    let router_task: JoinHandle<Result<(), ZmqError>> = {
      let router = state.router_socket.clone();
      tokio::spawn(async move {
        for _ in 0..total_requests {
          let id_frame = router.recv().await?;
          let _payload_frame = router.recv().await?;
          let mut id_reply = Msg::from_bytes(id_frame.data_bytes().unwrap());
          id_reply.set_flags(MsgFlags::MORE);
          router.send(id_reply).await?;
          router.send(Msg::new()).await?;
        }
        Ok(())
      })
    };

    let requests_per_dealer = (total_requests as f64 / num_dealers as f64).ceil() as usize;
    let mut dealer_tasks = Vec::new();

    for (i, dealer_socket) in state.dealer_sockets.iter().cloned().enumerate() {
      let payload_to_send = state.message_payload.clone();
      let num_reqs_for_this_dealer = if i == num_dealers - 1 {
        total_requests - (requests_per_dealer * (num_dealers - 1))
      } else {
        requests_per_dealer
      };

      let dealer_task: JoinHandle<Result<(), ZmqError>> = tokio::spawn(async move {
        for _ in 0..num_reqs_for_this_dealer {
          let msg = Msg::from_vec(black_box(payload_to_send.clone()));
          dealer_socket.send(msg).await?;
          let _ = dealer_socket.recv().await?;
        }
        Ok(())
      });
      dealer_tasks.push(dealer_task);
    }

    let router_join_result = router_task.await;
    let dealer_join_results = futures::future::join_all(dealer_tasks).await;
    let elapsed = start_time.elapsed();

    match router_join_result {
      Ok(Ok(_)) => {}
      Ok(Err(e)) => panic!("Benchmark ROUTER task failed: {}", e),
      Err(e) => panic!("Benchmark ROUTER task panicked: {}", e),
    }
    for (i, result) in dealer_join_results.into_iter().enumerate() {
      match result {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => panic!("Benchmark DEALER task {} failed: {}", i, e),
        Err(e) => panic!("Benchmark DEALER task {} panicked: {}", i, e),
      }
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
    if PRINT_BENCH_INFO {
      println!("[Teardown] Closing sockets...");
    }
    for (i, dealer_socket) in state.dealer_sockets.into_iter().enumerate() {
      if let Err(e) = dealer_socket.close().await {
        eprintln!("[Teardown Warning] Error closing DEALER {}: {}", i, e);
      }
    }
    if let Err(e) = state.router_socket.close().await {
      eprintln!("[Teardown Warning] Error closing ROUTER: {}", e);
    }
    sleep(Duration::from_millis(100)).await;
    if PRINT_BENCH_INFO {
      println!("[Teardown] Finished.");
    }
  })
}

// --- Main Benchmark Definition ---
fn dealer_router_matrix_benchmark(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime for benchmarks");

  let parameter_axes = vec![
    vec![
      MatrixCellValue::Unsigned(1),
      MatrixCellValue::Unsigned(4),
      MatrixCellValue::Unsigned(8),
      MatrixCellValue::Unsigned(16),
    ],
    vec![
      MatrixCellValue::Unsigned(64),
      MatrixCellValue::Unsigned(1024),
    ],
    vec![MatrixCellValue::Unsigned(10000)],
  ];
  let parameter_names = vec![
    "NumDealers".to_string(),
    "MsgSize".to_string(),
    "TotalRequests".to_string(),
  ];

  let suite = AsyncBenchmarkSuite::new(
    c,
    &rt,
    "DEALER_ROUTER_Scalability_Throughput".to_string(),
    Some(parameter_names),
    parameter_axes,
    Box::new(extract_config),
    setup_dealer_router_bench,
    benchmark_logic,
    teardown_dealer_router_bench,
  )
  .global_setup(bench_global_setup)
  .global_teardown(bench_global_teardown)
  .configure_criterion_group(|group| {
    group
      .warm_up_time(Duration::from_secs(3))
      .measurement_time(Duration::from_secs(5))
      .sample_size(10);
  })
  .throughput(|cfg: &ConfigDealerRouter| {
    Throughput::Bytes((cfg.total_requests * cfg.msg_size) as u64)
  });

  suite.run();
}

criterion_group!(benches, dealer_router_matrix_benchmark);
criterion_main!(benches);
