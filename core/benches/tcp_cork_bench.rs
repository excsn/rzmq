// benches/tcp_cork_bench.rs

use criterion::{
  criterion_group, criterion_main, measurement::WallTime, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use rzmq::{
  socket::options::{RCVHWM, SNDHWM, TCP_CORK_OPT},
  Context, Msg, SocketType, ZmqError,
};
use std::time::Duration;
use tokio::runtime::Builder as TokioBuilder;

// Helper to create a context for benchmarks
fn bench_context() -> Context {
  rzmq::Context::new().expect("Failed to create benchmark context")
}

const PAYLOAD_SMALL: usize = 64;
const PAYLOAD_LARGE: usize = 64 * 1024;
const NUM_MESSAGES_BENCH: usize = 1000; // Adjust as needed for reasonable bench times
const BENCH_ENDPOINT_BASE_PORT_CORK: u16 = 6100;

// --- DEALER-ROUTER Setup for Cork Bench ---
async fn setup_dealer_router_cork(
  ctx: &Context,
  endpoint: &str,
  enable_cork: bool,
) -> Result<(rzmq::Socket, rzmq::Socket, Vec<u8>), ZmqError> {
  let dealer = ctx.socket(SocketType::Dealer)?;
  let router = ctx.socket(SocketType::Router)?;

  if enable_cork {
    #[cfg(target_os = "linux")]
    dealer.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await?;
    // On non-Linux, this option might be an error or no-op, depending on SocketCore
    // Assuming it's gracefully handled (e.g. UnsupportedOption or ignored)
    #[cfg(not(target_os = "linux"))]
    let _ = dealer.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await;
  }
  dealer.set_option(SNDHWM, &(100i32).to_ne_bytes()).await?;
  router.set_option(RCVHWM, &(100i32).to_ne_bytes()).await?;

  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(20)).await;
  dealer.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  dealer.send(Msg::from_static(b"INIT_DEALER_CORK")).await?;
  let identity = router.recv().await?.data_bytes().unwrap().to_vec();
  let _ = router.recv().await?; // Consume payload

  Ok((dealer, router, identity))
}

// --- PUB-SUB Setup for Cork Bench ---
async fn setup_pub_sub_cork(
  ctx: &Context,
  endpoint: &str,
  enable_cork_on_pub: bool,
) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let publisher = ctx.socket(SocketType::Pub)?;
  let subscriber = ctx.socket(SocketType::Sub)?;

  if enable_cork_on_pub {
    #[cfg(target_os = "linux")]
    publisher.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await?;
    #[cfg(not(target_os = "linux"))]
    let _ = publisher.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await;
  }
  publisher.set_option(SNDHWM, &(1000i32).to_ne_bytes()).await?;
  subscriber.set_option(RCVHWM, &(1000i32).to_ne_bytes()).await?;

  publisher.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(20)).await;
  subscriber.connect(endpoint).await?;
  subscriber.set_option(rzmq::socket::options::SUBSCRIBE, b"").await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  Ok((publisher, subscriber))
}

fn tcp_cork_benchmarks(c: &mut Criterion<WallTime>) {
  let rt = TokioBuilder::new_current_thread().enable_all().build().unwrap();
  let ctx = bench_context(); // Create one context for all benchmarks in this file

  // --- DEALER-ROUTER with TCP_CORK ---
  let mut dr_group = c.benchmark_group("DealerRouter_Corking");
  for &payload_size in [PAYLOAD_SMALL, PAYLOAD_LARGE].iter() {
    dr_group.throughput(Throughput::Bytes(payload_size as u64 * NUM_MESSAGES_BENCH as u64));
    dr_group.sampling_mode(SamplingMode::Flat);

    for &cork_enabled in [false, true].iter() {
      // Only run "cork_enabled=true" meaningfully on Linux
      if cork_enabled && !cfg!(target_os = "linux") {
        continue;
      }
      let desc = if cork_enabled { "CorkEnabled" } else { "CorkDisabled" };
      let endpoint = format!(
        "tcp://127.0.0.1:{}_{}",
        BENCH_ENDPOINT_BASE_PORT_CORK + payload_size as u16 % 10,
        desc.to_lowercase()
      );

      dr_group.bench_function(
        BenchmarkId::new(format!("DealerToRouter_payload_{}B", payload_size), desc),
        |b| {
          rt.block_on(async {
            let (dealer, router, _identity) = setup_dealer_router_cork(&ctx, &endpoint, cork_enabled).await.unwrap();
            let payload_vec = vec![0u8; payload_size];
            let msg_template = Msg::from_vec(payload_vec);

            b.iter_batched(
              || msg_template.clone(),
              |msg| async {
                dealer.send(msg).await.unwrap();
                let _id = router.recv().await.unwrap();
                let _p = router.recv().await.unwrap();
              },
              criterion::BatchSize::NumBatches(NUM_MESSAGES_BENCH as u64),
            );
            // Sockets will be closed when ctx is termed
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
      let endpoint = format!(
        "tcp://127.0.0.1:{}_{}",
        BENCH_ENDPOINT_BASE_PORT_CORK + 10 + payload_size as u16 % 10,
        desc.to_lowercase()
      );

      ps_group.bench_function(
        BenchmarkId::new(format!("PubToSub_payload_{}B", payload_size), desc),
        |b| {
          rt.block_on(async {
            let (publisher, subscriber) = setup_pub_sub_cork(&ctx, &endpoint, cork_enabled).await.unwrap();
            let payload_vec = vec![0u8; payload_size];
            let msg_template = Msg::from_vec(payload_vec);

            b.iter_batched(
              || msg_template.clone(),
              |msg| async {
                publisher.send(msg).await.unwrap();
                let _ = subscriber.recv().await.unwrap();
              },
              criterion::BatchSize::NumBatches(NUM_MESSAGES_BENCH as u64),
            );
          });
        },
      );
    }
  }
  ps_group.finish();

  // Terminate context after all benchmarks in this group are done
  rt.block_on(async {
    ctx.term().await.unwrap();
  });
}

criterion_group!(
    name = cork_benches;
    config = Criterion::default().sample_size(10).warm_up_time(Duration::from_millis(500)); // Adjust sample size
    targets = tcp_cork_benchmarks
);
criterion_main!(cork_benches);
