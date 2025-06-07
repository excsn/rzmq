use bench_matrix::{
  criterion_runner::{
    async_suite::{AsyncBenchmarkSuite, AsyncSetupFn},
    ExtractorFn,
  },
  AbstractCombination, MatrixCellValue,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rzmq::{
  socket::{
    options::{RCVHWM, SNDHWM},
    MonitorReceiver, SocketEvent,
  },
  Context, Msg, SocketType, ZmqError,
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
const EVENT_RECV_TIMEOUT: Duration = Duration::from_secs(5);
const BENCH_HWM: i32 = 100_000;
const PRINT_BENCH_INFO: bool = false;

// Global atomic port counter to prevent collisions.
static NEXT_BENCH_PORT: AtomicU16 = AtomicU16::new(5600);

// --- Configuration for Benchmarks ---
#[derive(Debug, Clone)]
pub struct ConfigReqRep {
  pub num_requesters: usize,
  pub msg_size: usize,
  pub requests_per_requester: usize,
}

// --- State and Context for Benchmarks ---
#[derive(Default, Debug)]
struct BenchContext {}

struct BenchState {
  ctx_arc: Arc<Context>,
  req_sockets: Vec<rzmq::Socket>,
  rep_socket: rzmq::Socket,
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
fn extract_config(combo: &AbstractCombination) -> Result<ConfigReqRep, String> {
  let num_requesters = combo.get_u64(0)? as usize;
  let msg_size = combo.get_u64(1)? as usize;
  let requests_per_requester = combo.get_u64(2)? as usize;

  Ok(ConfigReqRep {
    num_requesters,
    msg_size,
    requests_per_requester,
  })
}

// --- Async Setup Function ---
fn setup_req_rep_bench(
  _runtime: &Runtime,
  cfg: &ConfigReqRep,
) -> Pin<Box<dyn Future<Output = Result<(BenchContext, BenchState), String>> + Send>> {
  let cfg_clone = cfg.clone();
  Box::pin(async move {
    if PRINT_BENCH_INFO {
      println!("[Setup] Starting for config: {:?}", cfg_clone);
    }
    let ctx = Context::new().map_err(|e| format!("Zmq Context creation failed: {}", e))?;
    let ctx_arc = Arc::new(ctx);

    // --- Setup REP Socket ---
    let rep_socket = ctx_arc.socket(SocketType::Rep).map_err(|e| e.to_string())?;
    rep_socket
      .set_option(SNDHWM, &BENCH_HWM.to_ne_bytes())
      .await
      .map_err(|e| e.to_string())?;
    rep_socket
      .set_option(RCVHWM, &BENCH_HWM.to_ne_bytes())
      .await
      .map_err(|e| e.to_string())?;
    let rep_monitor = rep_socket
      .monitor_default()
      .await
      .map_err(|e| e.to_string())?;
    let port = NEXT_BENCH_PORT.fetch_add(1, AtomicOrdering::Relaxed);
    let bind_addr = format!("{}:{}", BIND_ADDR_BASE, port);
    rep_socket
      .bind(&bind_addr)
      .await
      .map_err(|e| e.to_string())?;
    wait_for_event(
      &rep_monitor,
      |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == &bind_addr),
    )
    .await?;

    // --- Setup REQ Sockets ---
    let mut req_sockets = Vec::with_capacity(cfg_clone.num_requesters);
    for _ in 0..cfg_clone.num_requesters {
      let req_socket = ctx_arc.socket(SocketType::Req).map_err(|e| e.to_string())?;
      req_socket
        .set_option(SNDHWM, &BENCH_HWM.to_ne_bytes())
        .await
        .map_err(|e| e.to_string())?;
      req_socket
        .set_option(RCVHWM, &BENCH_HWM.to_ne_bytes())
        .await
        .map_err(|e| e.to_string())?;
      req_socket
        .connect(&bind_addr)
        .await
        .map_err(|e| e.to_string())?;
      req_sockets.push(req_socket);
    }

    // --- Verify All Connections ---
    for i in 0..cfg_clone.num_requesters {
      wait_for_event(&rep_monitor, |e| {
        matches!(e, SocketEvent::HandshakeSucceeded { .. })
      })
      .await
      .map_err(|e| format!("REP HandshakeSucceeded event {} error: {}", i, e))?;
    }
    if PRINT_BENCH_INFO {
      println!(
        "[Setup] {} requesters on {} connected.",
        cfg_clone.num_requesters, bind_addr
      );
    }
    sleep(Duration::from_millis(50)).await;

    let message_payload = vec![0u8; cfg_clone.msg_size];
    Ok((
      BenchContext {},
      BenchState {
        ctx_arc,
        req_sockets,
        rep_socket,
        message_payload,
      },
    ))
  })
}

// --- Async Benchmark Logic ---
fn benchmark_logic(
  ctx: BenchContext,
  state: BenchState,
  cfg: &ConfigReqRep,
) -> Pin<Box<dyn Future<Output = (BenchContext, BenchState, Duration)> + Send>> {
  let total_requests = cfg.num_requesters * cfg.requests_per_requester;
  let requests_per_requester = cfg.requests_per_requester;

  Box::pin(async move {
    let start_time = Instant::now();

    // --- REP Task ---
    let rep_task: JoinHandle<Result<(), ZmqError>> = {
      let rep_socket = state.rep_socket.clone();
      let reply_payload = state.message_payload.clone();
      tokio::spawn(async move {
        for _ in 0..total_requests {
          let req_msg = rep_socket.recv().await?;
          black_box(req_msg.data());
          let rep_msg = Msg::from_vec(black_box(reply_payload.clone()));
          rep_socket.send(rep_msg).await?;
        }
        Ok(())
      })
    };

    // --- REQ Tasks ---
    let mut req_tasks = Vec::new();
    for req_socket in state.req_sockets.iter().cloned() {
      let request_payload = state.message_payload.clone();
      let req_task: JoinHandle<Result<(), ZmqError>> = tokio::spawn(async move {
        for _ in 0..requests_per_requester {
          let req_msg = Msg::from_vec(black_box(request_payload.clone()));
          req_socket.send(req_msg).await?;
          let rep_msg = req_socket.recv().await?;
          black_box(rep_msg.data());
        }
        Ok(())
      });
      req_tasks.push(req_task);
    }

    let rep_result = rep_task.await.expect("REP task panicked");
    let req_results = futures::future::join_all(req_tasks).await;
    let elapsed = start_time.elapsed();

    rep_result.expect("REP task failed with ZmqError");
    for (i, result) in req_results.into_iter().enumerate() {
      result
        .unwrap_or_else(|e| panic!("REQ task {} panicked: {}", i, e))
        .expect("REQ task failed with ZmqError");
    }

    (ctx, state, elapsed)
  })
}

// --- Async Teardown Function ---
fn teardown_req_rep_bench(
  _ctx: BenchContext,
  state: BenchState,
  _runtime: &Runtime,
  _cfg: &ConfigReqRep,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
  Box::pin(async move {
    if PRINT_BENCH_INFO {
      println!("[Teardown] Closing sockets...");
    }
    if let Err(e) = state.rep_socket.close().await {
      eprintln!("[Teardown Warning] Error closing REP socket: {}", e);
    }
    for req_socket in state.req_sockets {
      if let Err(e) = req_socket.close().await {
        eprintln!("[Teardown Warning] Error closing REQ socket: {}", e);
      }
    }
    sleep(Duration::from_millis(100)).await;
  })
}

// --- Main Benchmark Definition ---
fn req_rep_matrix_benchmark(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime for benchmarks");

  let parameter_axes = vec![
    vec![
      MatrixCellValue::Unsigned(1),
      MatrixCellValue::Unsigned(4),
      MatrixCellValue::Unsigned(8),
    ],
    vec![
      MatrixCellValue::Unsigned(64),
      MatrixCellValue::Unsigned(1024),
    ],
    vec![MatrixCellValue::Unsigned(1000)],
  ];
  let parameter_names = vec![
    "NumRequesters".to_string(),
    "MsgSize".to_string(),
    "ReqsPerRequester".to_string(),
  ];

  let suite = AsyncBenchmarkSuite::new(
    c,
    &rt,
    "REQ_REP_Scalability_RoundTrip".to_string(),
    Some(parameter_names),
    parameter_axes,
    Box::new(extract_config),
    setup_req_rep_bench,
    benchmark_logic,
    teardown_req_rep_bench,
  )
  .configure_criterion_group(|group| {
    group
      .warm_up_time(Duration::from_secs(3))
      .measurement_time(Duration::from_secs(10))
      .sample_size(10);
  })
  .throughput(|cfg: &ConfigReqRep| {
    let total_bytes_per_round_trip = (cfg.msg_size * 2) as u64;
    let total_round_trips = (cfg.num_requesters * cfg.requests_per_requester) as u64;
    Throughput::Bytes(total_round_trips * total_bytes_per_round_trip)
  });

  suite.run();
}

criterion_group!(benches, req_rep_matrix_benchmark);
criterion_main!(benches);
