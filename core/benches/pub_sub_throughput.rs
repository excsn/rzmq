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
    options::{RCVHWM, SNDHWM, SUBSCRIBE},
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
const BENCH_HWM: i32 = 1_000_000;
const PRINT_BENCH_INFO: bool = false;

// Global atomic port counter to prevent collisions.
static NEXT_BENCH_PORT: AtomicU16 = AtomicU16::new(5900);

// --- Configuration for Benchmarks ---
#[derive(Debug, Clone)]
pub struct ConfigPubSub {
  pub num_subscribers: usize,
  pub msg_size: usize,
  pub num_messages: usize,
}

// --- State and Context for Benchmarks ---
#[derive(Default, Debug)]
struct BenchContext {}

struct BenchState {
  ctx_arc: Arc<Context>,
  pub_socket: rzmq::Socket,
  sub_sockets: Vec<rzmq::Socket>,
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
fn extract_config(combo: &AbstractCombination) -> Result<ConfigPubSub, String> {
  let num_subscribers = combo.get_u64(0)? as usize;
  let msg_size = combo.get_u64(1)? as usize;
  let num_messages = combo.get_u64(2)? as usize;

  Ok(ConfigPubSub {
    num_subscribers,
    msg_size,
    num_messages,
  })
}

// --- Global Setup/Teardown (Optional) ---
fn bench_global_setup(_cfg: &ConfigPubSub) -> Result<(), String> { Ok(()) }
fn bench_global_teardown(_cfg: &ConfigPubSub) -> Result<(), String> { Ok(()) }

// --- Async Setup Function ---
fn setup_pub_sub_bench(
  _runtime: &Runtime,
  cfg: &ConfigPubSub,
) -> Pin<Box<dyn Future<Output = Result<(BenchContext, BenchState), String>> + Send>> {
  let cfg_clone = cfg.clone();
  Box::pin(async move {
    if PRINT_BENCH_INFO {
        println!("[Setup] Starting for config: {:?}", cfg_clone);
    }
    let ctx = Context::new().map_err(|e| format!("Zmq Context creation failed: {}", e))?;
    let ctx_arc = Arc::new(ctx);

    // --- Setup PUB Socket ---
    let pub_socket = ctx_arc
      .socket(SocketType::Pub)
      .map_err(|e| e.to_string())?;
    pub_socket
      .set_option(SNDHWM, BENCH_HWM)
      .await
      .map_err(|e| e.to_string())?; // Corrected here
    let pub_monitor = pub_socket.monitor_default().await.map_err(|e| e.to_string())?; // Corrected here
    let port = NEXT_BENCH_PORT.fetch_add(1, AtomicOrdering::Relaxed);
    let bind_addr = format!("{}:{}", BIND_ADDR_BASE, port);
    pub_socket.bind(&bind_addr).await.map_err(|e| e.to_string())?; // Corrected here
    wait_for_event(&pub_monitor, |e| {
      matches!(e, SocketEvent::Listening { endpoint: ep } if ep == &bind_addr)
    }).await?;

    // --- Setup SUB Sockets ---
    let mut sub_sockets = Vec::with_capacity(cfg_clone.num_subscribers);
    for _ in 0..cfg_clone.num_subscribers {
      let sub_socket = ctx_arc
        .socket(SocketType::Sub)
        .map_err(|e| e.to_string())?;
      sub_socket
        .set_option(RCVHWM, BENCH_HWM)
        .await
        .map_err(|e| e.to_string())?; // Corrected here
      sub_socket.set_option(SUBSCRIBE, b"").await.map_err(|e| e.to_string())?; // Corrected here
      sub_socket.connect(&bind_addr).await.map_err(|e| e.to_string())?; // Corrected here
      sub_sockets.push(sub_socket);
    }
    
    // --- Verify All Connections ---
    for i in 0..cfg_clone.num_subscribers {
      wait_for_event(&pub_monitor, |e| {
        matches!(e, SocketEvent::HandshakeSucceeded { .. })
      })
      .await
      .map_err(|e| format!("PUB HandshakeSucceeded event {} error: {}", i, e))?;
    }
    if PRINT_BENCH_INFO {
        println!(
          "[Setup] {} subscribers on {} connected. All handshakes seen by PUB.",
          cfg_clone.num_subscribers, bind_addr
        );
    }

    // Crucial sleep to allow subscriptions to be processed by the PUB socket's distributor
    sleep(Duration::from_millis(100)).await;

    let message_payload = vec![0u8; cfg_clone.msg_size];
    Ok((
      BenchContext {},
      BenchState {
        ctx_arc,
        pub_socket,
        sub_sockets,
        message_payload,
      },
    ))
  })
}

// --- Async Benchmark Logic ---
fn benchmark_logic(
  ctx: BenchContext,
  state: BenchState,
  cfg: &ConfigPubSub,
) -> Pin<Box<dyn Future<Output = (BenchContext, BenchState, Duration)> + Send>> {
  let num_messages = cfg.num_messages;

  Box::pin(async move {
    let start_time = Instant::now();

    // --- Publisher Task ---
    let publisher_task: JoinHandle<Result<(), ZmqError>> = {
      let pub_socket = state.pub_socket.clone();
      let payload = state.message_payload.clone();
      tokio::spawn(async move {
        for _ in 0..num_messages {
          let msg = Msg::from_vec(black_box(payload.clone()));
          pub_socket.send(msg).await?;
        }
        Ok(())
      })
    };

    // --- Subscriber Tasks ---
    let mut subscriber_tasks = Vec::new();
    for sub_socket in state.sub_sockets.iter().cloned() {
        let sub_task: JoinHandle<Result<(), ZmqError>> = tokio::spawn(async move {
            for _ in 0..num_messages {
                let msg = sub_socket.recv().await?;
                black_box(msg.data());
            }
            Ok(())
        });
        subscriber_tasks.push(sub_task);
    }

    // --- Wait for all tasks to complete ---
    let pub_result = publisher_task.await.expect("Publisher task panicked");
    let sub_results = futures::future::join_all(subscriber_tasks).await;
    let elapsed = start_time.elapsed();

    // --- Handle Task Results ---
    pub_result.expect("Publisher task failed with ZmqError");
    for (i, result) in sub_results.into_iter().enumerate() {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("Benchmark SUB task {} failed: {}", i, e),
            Err(e) => panic!("Benchmark SUB task {} panicked: {}", i, e),
        }
    }
    
    (ctx, state, elapsed)
  })
}

// --- Async Teardown Function ---
fn teardown_pub_sub_bench(
  _ctx: BenchContext,
  state: BenchState,
  _runtime: &Runtime,
  _cfg: &ConfigPubSub,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
  Box::pin(async move {
    if PRINT_BENCH_INFO {
        println!("[Teardown] Closing sockets...");
    }
    if let Err(e) = state.pub_socket.close().await {
      eprintln!("[Teardown Warning] Error closing PUB socket: {}", e);
    }
    for sub_socket in state.sub_sockets {
      if let Err(e) = sub_socket.close().await {
        eprintln!("[Teardown Warning] Error closing SUB socket: {}", e);
      }
    }
    sleep(Duration::from_millis(100)).await;
    if PRINT_BENCH_INFO {
        println!("[Teardown] Finished.");
    }
  })
}

// --- Main Benchmark Definition ---
fn pub_sub_matrix_benchmark(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime for benchmarks");

  let parameter_axes = vec![
    vec![
      MatrixCellValue::Unsigned(1),
      MatrixCellValue::Unsigned(4),
      MatrixCellValue::Unsigned(8),
    ],
    vec![MatrixCellValue::Unsigned(64), MatrixCellValue::Unsigned(1024)],
    vec![MatrixCellValue::Unsigned(5000)],
  ];
  let parameter_names = vec!["NumSubscribers".to_string(), "MsgSize".to_string(), "NumMessages".to_string()];

  let suite = AsyncBenchmarkSuite::new(
    c,
    &rt,
    "PUB_SUB_Scalability_Throughput".to_string(),
    Some(parameter_names),
    parameter_axes,
    Box::new(extract_config),
    setup_pub_sub_bench,
    benchmark_logic,
    teardown_pub_sub_bench,
  )
  .global_setup(bench_global_setup)
  .global_teardown(bench_global_teardown)
  .configure_criterion_group(|group| {
    group
      .warm_up_time(Duration::from_secs(3))
      .measurement_time(Duration::from_secs(10))
      .sample_size(10);
  })
  .throughput(|cfg: &ConfigPubSub| {
    // Throughput = bytes sent * number of subscribers (fan-out)
    Throughput::Bytes((cfg.num_messages * cfg.msg_size * cfg.num_subscribers) as u64)
  });

  suite.run();
}

criterion_group!(benches, pub_sub_matrix_benchmark);
criterion_main!(benches);