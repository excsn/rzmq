// benches/tcp_cork_bench.rs

use criterion::{
  criterion_group, criterion_main, measurement::WallTime, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use rzmq::{
  socket::{options::{RCVHWM, SNDHWM, SUBSCRIBE, TCP_CORK}, LINGER},
  Context, Msg, SocketType, ZmqError,
};
use std::{sync::atomic::{AtomicU16, Ordering as AtomicOrdering}, time::Duration};
use tokio::runtime::Builder as TokioBuilder;

// Helper to create a context for benchmarks
fn bench_context() -> Context {
  rzmq::Context::new().expect("Failed to create benchmark context")
}

const PAYLOAD_SMALL: usize = 64;
const PAYLOAD_LARGE: usize = 64 * 1024;
const NUM_MESSAGES_BENCH: usize = 1000;
const BENCH_ENDPOINT_BASE_PORT_CORK: u16 = 43582;

static NEXT_BENCH_PORT: AtomicU16 = AtomicU16::new(BENCH_ENDPOINT_BASE_PORT_CORK);

fn get_next_bench_port() -> u16 {
    NEXT_BENCH_PORT.fetch_add(1, AtomicOrdering::Relaxed)
}

// --- DEALER-ROUTER Setup for Cork Bench ---
// (setup_dealer_router_cork remains the same as previously corrected)
async fn setup_dealer_router_cork(
  ctx: &Context, // Takes a reference to the shared context
  endpoint: &str,
  enable_cork: bool,
) -> Result<(rzmq::Socket, rzmq::Socket, Vec<u8>), ZmqError> {
  let dealer = ctx.socket(SocketType::Dealer)?;
  let router = ctx.socket(SocketType::Router)?;

  dealer.set_option(LINGER, &(0i32).to_ne_bytes()).await?;
  router.set_option(LINGER, &(0i32).to_ne_bytes()).await?;

  if enable_cork {
    #[cfg(target_os = "linux")]
    dealer.set_option(TCP_CORK, &(1i32).to_ne_bytes()).await?;
    #[cfg(not(target_os = "linux"))]
    {
      let _ = dealer.set_option(TCP_CORK, &(1i32).to_ne_bytes()).await;
    }
  }
  dealer.set_option(SNDHWM, &(100i32).to_ne_bytes()).await?;
  router.set_option(RCVHWM, &(100i32).to_ne_bytes()).await?;

  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  dealer.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  dealer.send(Msg::from_static(b"INIT_DEALER_CORK")).await?;
  let identity_msg = router.recv().await?;
  let identity = identity_msg
    .data_bytes()
    .ok_or_else(|| ZmqError::Internal("Identity message frame had no data".to_string()))?
    .to_vec();
  let _init_payload = router.recv().await?;

  Ok((dealer, router, identity))
}

// --- PUB-SUB Setup for Cork Bench ---
// (setup_pub_sub_cork remains the same as previously corrected)
async fn setup_pub_sub_cork(
  ctx: &Context, // Takes a reference to the shared context
  endpoint: &str,
  enable_cork_on_pub: bool,
) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let publisher = ctx.socket(SocketType::Pub)?;
  let subscriber = ctx.socket(SocketType::Sub)?;

  if enable_cork_on_pub {
    publisher.set_option(LINGER, &(0i32).to_ne_bytes()).await?;
    subscriber.set_option(LINGER, &(0i32).to_ne_bytes()).await?;
    #[cfg(target_os = "linux")]
    publisher.set_option(TCP_CORK, &(1i32).to_ne_bytes()).await?;
    #[cfg(not(target_os = "linux"))]
    {
      let _ = publisher.set_option(TCP_CORK, &(1i32).to_ne_bytes()).await;
    }
  }
  publisher.set_option(SNDHWM, &(1000i32).to_ne_bytes()).await?;
  subscriber.set_option(RCVHWM, &(1000i32).to_ne_bytes()).await?;
  subscriber.set_option(SUBSCRIBE, b"").await?;

  publisher.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  subscriber.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  Ok((publisher, subscriber))
}

fn tcp_cork_benchmarks(c: &mut Criterion<WallTime>) {
  let rt = TokioBuilder::new_current_thread().enable_all().build().unwrap();

  // --- DEALER-ROUTER with TCP_CORK ---
  let mut dr_group = c.benchmark_group("DealerRouter_Corking");
  for &payload_size in [PAYLOAD_SMALL, PAYLOAD_LARGE].iter() {
    dr_group.throughput(Throughput::Bytes(payload_size as u64 * NUM_MESSAGES_BENCH as u64));
    dr_group.sampling_mode(SamplingMode::Flat);

    for &cork_enabled in [false, true].iter() {
      if cork_enabled && !cfg!(target_os = "linux") {
        continue;
      }
      let desc = if cork_enabled { "CorkEnabled" } else { "CorkDisabled" };

      let current_port = get_next_bench_port();
      let endpoint = format!("tcp://127.0.0.1:{}", current_port);

      dr_group.bench_function(
        BenchmarkId::new(format!("DealerToRouter_payload_{}B", payload_size), desc),
        // The closure passed to bench_function is run once for each BenchmarkId.
        // Sockets are created AND destroyed within this closure's scope.
        |b| {

          // Create ONE ZMQ context for ALL benchmarks in this file.
          let local_ctx = bench_context();
          // Sockets and message template are set up here.
          // This entire block is executed by Criterion to prepare for `iter_batched`.
          // `rt.block_on` is used for this async setup.
          // CRITICAL: `dealer`, `router` are scoped to this block_on, then to this closure.
          // They will be dropped (and thus closed) when this bench_function closure ends for *this* specific BenchmarkId.
          let (dealer, router, _identity, msg_template) = rt.block_on(async {
            // Pass the shared `ctx` by reference
            let (d, r, id) = setup_dealer_router_cork(&local_ctx, &endpoint, cork_enabled)
              .await
              .unwrap_or_else(|e| panic!("Failed to setup DEALER-ROUTER for endpoint {}: {}", endpoint, e));
            let payload_vec = vec![0u8; payload_size];
            let mt = Msg::from_vec(payload_vec);
            (d, r, id, mt)
          });

          b.to_async(&rt).iter_batched(
            || msg_template.clone(),
            |msg| async {
              // `dealer` and `router` are captured from the scope above
              dealer.send(msg).await.unwrap();
              let _identity_frame = router.recv().await.unwrap();
              let _payload_frame = router.recv().await.unwrap();
            },
            criterion::BatchSize::NumBatches(NUM_MESSAGES_BENCH as u64),
          );

          // Teardown for this BenchmarkId:
          // 1. Ensure sockets are dropped (happens when they go out of scope here).
          //    Explicit drop can be used for clarity if desired before term.
          //    `drop(dealer); drop(router);`
          // 2. Terminate the local ZMQ context.
          rt.block_on(async {
            // Explicitly drop sockets before terminating context to ensure proper order
            // if there's any subtlety in rzmq's Drop or async close.
            // If Socket has an async close(), use that:
            // let _ = dealer.close().await;
            // let _ = router.close().await;
            drop(dealer); // Ensure these are dropped before local_ctx.term()
            drop(router);
            local_ctx.term().await.unwrap_or_else(|e| {
                eprintln!("Error terminating local ZMQ context for {}: {}", endpoint, e);
            });
          });
        },
      );
    }
  }
  dr_group.finish();

  // --- PUB-SUB with TCP_CORK ---
  let mut ps_group = c.benchmark_group("PubSub_Corking");
  for &payload_size in [PAYLOAD_SMALL, PAYLOAD_LARGE].iter() {
    ps_group.throughput(Throughput::Bytes(payload_size as u64 * NUM_MESSAGES_BENCH as u64));
    ps_group.sampling_mode(SamplingMode::Flat);

    for &cork_enabled in [false, true].iter() {
      if cork_enabled && !cfg!(target_os = "linux") {
        continue;
      }
      let desc = if cork_enabled { "CorkEnabled" } else { "CorkDisabled" };

      let current_port = get_next_bench_port();
      let endpoint = format!("tcp://127.0.0.1:{}", current_port);

      ps_group.bench_function(
        BenchmarkId::new(format!("PubToSub_payload_{}B", payload_size), desc),
        |b| {

          // Create ONE ZMQ context for ALL benchmarks in this file.
          let local_ctx = bench_context();
          let (publisher, subscriber, msg_template) = rt.block_on(async {
            // Pass the shared `ctx` by reference
            let (p, s) = setup_pub_sub_cork(&local_ctx, &endpoint, cork_enabled)
              .await
              .unwrap_or_else(|e| panic!("Failed to setup PUB-SUB for endpoint {}: {}", endpoint, e));
            let payload_vec = vec![0u8; payload_size];
            let mt = Msg::from_vec(payload_vec);
            (p, s, mt)
          });

          b.to_async(&rt).iter_batched(
            || msg_template.clone(),
            |msg| async {
              // `publisher` and `subscriber` are captured
              publisher.send(msg).await.unwrap();
              let _ = subscriber.recv().await.unwrap();
            },
            criterion::BatchSize::NumBatches(NUM_MESSAGES_BENCH as u64),
          );
          

          // Teardown for this BenchmarkId
          rt.block_on(async {
            drop(publisher);
            drop(subscriber);
            local_ctx.term().await.unwrap_or_else(|e| {
                eprintln!("Error terminating local ZMQ context for {}: {}", endpoint, e);
            });
          });
        },
      );
    }
  }
  ps_group.finish();
}

criterion_group!(
    name = cork_benches;
    config = Criterion::default().sample_size(10).warm_up_time(Duration::from_millis(500));
    targets = tcp_cork_benchmarks
);
criterion_main!(cork_benches);
