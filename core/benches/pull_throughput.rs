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
static NEXT_BENCH_PORT: AtomicU16 = AtomicU16::new(5680);

// --- Configuration for Benchmarks ---
#[derive(Debug, Clone)]
pub struct ConfigPushPull {
  pub num_pushers: usize,
  pub msg_size: usize,
  pub num_messages_per_pusher: usize,
}

// --- State and Context for Benchmarks ---
#[derive(Default, Debug)]
struct BenchContext {}

struct BenchState {
  ctx_arc: Arc<Context>,
  push_sockets: Vec<rzmq::Socket>,
  pull_socket: rzmq::Socket,
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
fn extract_config(combo: &AbstractCombination) -> Result<ConfigPushPull, String> {
  let num_pushers = combo.get_u64(0)? as usize;
  let msg_size = combo.get_u64(1)? as usize;
  let num_messages_per_pusher = combo.get_u64(2)? as usize;

  Ok(ConfigPushPull {
    num_pushers,
    msg_size,
    num_messages_per_pusher,
  })
}

// --- Global Setup/Teardown (Optional) ---
fn bench_global_setup(_cfg: &ConfigPushPull) -> Result<(), String> {
  Ok(())
}
fn bench_global_teardown(_cfg: &ConfigPushPull) -> Result<(), String> {
  Ok(())
}

// --- Async Setup Function ---
fn setup_push_pull_bench(
  _runtime: &Runtime,
  cfg: &ConfigPushPull,
) -> Pin<Box<dyn Future<Output = Result<(BenchContext, BenchState), String>> + Send>> {
  let cfg_clone = cfg.clone();
  Box::pin(async move {
    if PRINT_BENCH_INFO {
      println!("[Setup] Starting for config: {:?}", cfg_clone);
    }
    let ctx = Context::new().map_err(|e| format!("Zmq Context creation failed: {}", e))?;
    let ctx_arc = Arc::new(ctx);

    // --- Setup PULL Socket ---
    let pull_socket = ctx_arc
      .socket(SocketType::Pull)
      .map_err(|e| e.to_string())?;
    pull_socket
      .set_option(RCVHWM, &BENCH_HWM.to_ne_bytes())
      .await
      .map_err(|e| e.to_string())?;
    let pull_monitor = pull_socket
      .monitor_default()
      .await
      .map_err(|e| e.to_string())?;
    let port = NEXT_BENCH_PORT.fetch_add(1, AtomicOrdering::Relaxed);
    let bind_addr = format!("{}:{}", BIND_ADDR_BASE, port);
    pull_socket
      .bind(&bind_addr)
      .await
      .map_err(|e| e.to_string())?;
    wait_for_event(
      &pull_monitor,
      |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == &bind_addr),
    )
    .await?;

    // --- Setup PUSH Sockets ---
    let mut push_sockets = Vec::with_capacity(cfg_clone.num_pushers);
    for _ in 0..cfg_clone.num_pushers {
      let push_socket = ctx_arc
        .socket(SocketType::Push)
        .map_err(|e| e.to_string())?;
      push_socket
        .set_option(SNDHWM, &BENCH_HWM.to_ne_bytes())
        .await
        .map_err(|e| e.to_string())?;
      push_socket
        .connect(&bind_addr)
        .await
        .map_err(|e| e.to_string())?;
      push_sockets.push(push_socket);
    }

    // --- Verify All Connections ---
    for i in 0..cfg_clone.num_pushers {
      wait_for_event(&pull_monitor, |e| {
        matches!(e, SocketEvent::HandshakeSucceeded { .. })
      })
      .await
      .map_err(|e| format!("PULL HandshakeSucceeded event {} error: {}", i, e))?;
    }
    if PRINT_BENCH_INFO {
      println!(
        "[Setup] {} pushers on {} connected. All handshakes seen by PULL.",
        cfg_clone.num_pushers, bind_addr
      );
    }
    sleep(Duration::from_millis(50)).await;

    let message_payload = vec![0u8; cfg_clone.msg_size];
    Ok((
      BenchContext {},
      BenchState {
        ctx_arc,
        push_sockets,
        pull_socket,
        message_payload,
      },
    ))
  })
}

// --- Async Benchmark Logic ---
// Corrected signature: Future is now 'static because we copy config values.
fn benchmark_logic(
  ctx: BenchContext,
  state: BenchState,
  cfg: &ConfigPushPull,
) -> Pin<Box<dyn Future<Output = (BenchContext, BenchState, Duration)> + Send>> {
  // *** THE FIX IS HERE ***
  // Copy the needed values from the `cfg` reference *before* the `async move` block.
  // This means the block no longer captures a reference with a limited lifetime.
  let total_messages = cfg.num_pushers * cfg.num_messages_per_pusher;
  let num_messages_per_pusher = cfg.num_messages_per_pusher;

  Box::pin(async move {
    let start_time = Instant::now();

    // --- Sender Tasks ---
    let mut sender_tasks = Vec::new();
    for push_socket in state.push_sockets.iter().cloned() {
      let payload = state.message_payload.clone();
      // Use the copied value, not a reference to cfg
      let sender_task: JoinHandle<Result<(), ZmqError>> = tokio::spawn(async move {
        for _ in 0..num_messages_per_pusher {
          let msg = Msg::from_vec(black_box(payload.clone()));
          push_socket.send(msg).await?;
        }
        Ok(())
      });
      sender_tasks.push(sender_task);
    }

    // --- Receiver Task ---
    let receiver_task: JoinHandle<Result<usize, ZmqError>> = {
      let pull_socket = state.pull_socket.clone();
      tokio::spawn(async move {
        for i in 0..total_messages {
          match pull_socket.recv().await {
            Ok(msg) => {
              black_box(msg.data());
            }
            Err(e) => {
              eprintln!(
                "[Receiver Task] Error receiving msg {}/{}: {}",
                i + 1,
                total_messages,
                e
              );
              return Err(e);
            }
          }
        }
        Ok(total_messages)
      })
    };

    // --- Wait for all tasks to complete ---
    let send_results = futures::future::join_all(sender_tasks).await;
    let recv_result = receiver_task.await.expect("Receiver task panicked");
    let elapsed = start_time.elapsed();

    // --- Handle Task Results ---
    for (i, result) in send_results.into_iter().enumerate() {
      result
        .unwrap_or_else(|e| panic!("Sender task {} panicked: {}", i, e))
        .expect("Sender task failed with ZmqError");
    }
    recv_result.expect("Receiver task failed with ZmqError");

    (ctx, state, elapsed)
  })
}

// --- Async Teardown Function ---
fn teardown_push_pull_bench(
  _ctx: BenchContext,
  state: BenchState,
  _runtime: &Runtime,
  _cfg: &ConfigPushPull,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
  Box::pin(async move {
    if PRINT_BENCH_INFO {
      println!("[Teardown] Closing sockets...");
    }
    for push_socket in state.push_sockets {
      if let Err(e) = push_socket.close().await {
        eprintln!("[Teardown Warning] Error closing PUSH socket: {}", e);
      }
    }
    if let Err(e) = state.pull_socket.close().await {
      eprintln!("[Teardown Warning] Error closing PULL socket: {}", e);
    }
    sleep(Duration::from_millis(100)).await;
  })
}

// --- Main Benchmark Definition ---
fn push_pull_matrix_benchmark(c: &mut Criterion) {
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
    vec![MatrixCellValue::Unsigned(5000)],
  ];
  let parameter_names = vec![
    "NumPushers".to_string(),
    "MsgSize".to_string(),
    "MsgsPerPusher".to_string(),
  ];

  let suite = AsyncBenchmarkSuite::new(
    c,
    &rt,
    "PUSH_PULL_Scalability_Throughput".to_string(),
    Some(parameter_names),
    parameter_axes,
    Box::new(extract_config),
    setup_push_pull_bench,
    benchmark_logic, // Now correctly matches the expected fn pointer type
    teardown_push_pull_bench,
  )
  .global_setup(bench_global_setup)
  .global_teardown(bench_global_teardown)
  .configure_criterion_group(|group| {
    group
      .warm_up_time(Duration::from_secs(3))
      .measurement_time(Duration::from_secs(10))
      .sample_size(10);
  })
  .throughput(|cfg: &ConfigPushPull| {
    let total_bytes = cfg.num_pushers * cfg.num_messages_per_pusher * cfg.msg_size;
    Throughput::Bytes(total_bytes as u64)
  });

  suite.run();
}

criterion_group!(benches, push_pull_matrix_benchmark);
criterion_main!(benches);
