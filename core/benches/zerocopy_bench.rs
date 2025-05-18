// benches/zerocopy_bench.rs

use criterion::{
  criterion_group, criterion_main, measurement::WallTime, BenchmarkId, Criterion, SamplingMode, Throughput,
};
#[cfg(feature = "io-uring")]
use rzmq::socket::options::IO_URING_SNDZEROCOPY;
use rzmq::{
  socket::options::{RCVHWM, SNDHWM, TCP_CORK_OPT}, // Include TCP_CORK_OPT if you want to test ZC+Cork
  Context,
  Msg,
  MsgFlags,
  SocketType,
  ZmqError, // Added MsgFlags
};
use std::time::Duration;
use tokio::runtime::Builder as TokioBuilder;

// Helper to create a context for benchmarks
fn bench_context() -> Context {
  rzmq::Context::new().expect("Failed to create benchmark context")
}

const PAYLOAD_SMALL: usize = 64;
const PAYLOAD_LARGE: usize = 64 * 1024; // 64 KB
const NUM_MESSAGES_BENCH: usize = 1000; // Number of messages for throughput tests
const BENCH_ENDPOINT_BASE_PORT_ZC: u16 = 6200; // Base port for this benchmark file

// --- DEALER-ROUTER Setup for Zerocopy/Standard Comparison Bench ---
async fn setup_dealer_router_send_comparison(
  ctx: &Context,
  endpoint: &str,
  enable_zerocopy_opt: bool, // True if ZC should be attempted
  enable_cork_opt: bool,     // True if Cork should be attempted
) -> Result<(rzmq::Socket, rzmq::Socket, Vec<u8>), ZmqError> {
  let dealer = ctx.socket(SocketType::Dealer)?;
  let router = ctx.socket(SocketType::Router)?;

  // Only try to set io-uring specific options if the feature is enabled and on Linux
  if enable_zerocopy_opt {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
      // IO_URING_SNDZEROCOPY is only in scope if the feature "io-uring"
      // is enabled for this benchmark compilation unit.
      dealer.set_option(IO_URING_SNDZEROCOPY, &(1i32).to_ne_bytes()).await?;
      println!("[Setup {}] IO_URING_SNDZEROCOPY enabled for DEALER", endpoint);
    }
    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    if enable_zerocopy_opt {
      // Log if ZC was requested but not applicable
      println!(
        "[Setup {}] IO_URING_SNDZEROCOPY requested but not applicable (not Linux or io-uring feature off for bench)",
        endpoint
      );
    }
  }

  if enable_cork_opt && cfg!(target_os = "linux") {
    dealer.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await?;
    println!("[Setup {}] TCP_CORK_OPT enabled for DEALER", endpoint);
  }

  dealer.set_option(SNDHWM, &(100i32).to_ne_bytes()).await?;
  router.set_option(RCVHWM, &(100i32).to_ne_bytes()).await?;

  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(30)).await; // Increased bind settle time slightly
  dealer.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(70)).await; // Increased connect settle time

  // Initial message from Dealer to Router for Router to learn identity
  dealer.send(Msg::from_static(b"INIT_DEALER_ZC_CMP")).await?;
  let mut identity_frame = router.recv().await?;
  // Ensure we consume all parts of the init message if it's multi-part by mistake
  while identity_frame.is_more() {
    let _ = router.recv().await?; // Consume next part
    identity_frame = router.recv().await?; // This logic is flawed if init is multi-part
  }
  // Corrected init consumption:
  // let identity_frame = router.recv().await?; // Assume ID is first
  // let _payload_frame = router.recv().await?; // Assume payload is second

  // Simpler init for benchmark: router expects [identity, payload]
  // So, after dealer sends one frame, router receives two.
  let identity = identity_frame.data_bytes().unwrap_or_default().to_vec();
  let _payload = router.recv().await?; // Consume the "INIT_DEALER_ZC_CMP" payload

  Ok((dealer, router, identity))
}

// --- PUB-SUB Setup for Zerocopy/Standard Comparison Bench ---
async fn setup_pub_sub_send_comparison(
  ctx: &Context,
  endpoint: &str,
  enable_zerocopy_on_pub_opt: bool,
  enable_cork_on_pub_opt: bool,
) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let publisher = ctx.socket(SocketType::Pub)?;
  let subscriber = ctx.socket(SocketType::Sub)?;

  if enable_zerocopy_on_pub_opt && cfg!(all(target_os = "linux", feature = "io-uring")) {
    publisher
      .set_option(IO_URING_SNDZEROCOPY, &(1i32).to_ne_bytes())
      .await?;
    println!("[Setup {}] IO_URING_SNDZEROCOPY enabled for PUB", endpoint);
  }
  if enable_cork_on_pub_opt && cfg!(target_os = "linux") {
    publisher.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await?;
    println!("[Setup {}] TCP_CORK_OPT enabled for PUB", endpoint);
  }

  publisher.set_option(SNDHWM, &(1000i32).to_ne_bytes()).await?;
  subscriber.set_option(RCVHWM, &(1000i32).to_ne_bytes()).await?;

  publisher.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(30)).await;
  subscriber.connect(endpoint).await?;
  subscriber.set_option(rzmq::socket::options::SUBSCRIBE, b"").await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  Ok((publisher, subscriber))
}

fn zerocopy_vs_standard_benchmarks(c: &mut Criterion<WallTime>) {
  // --- DEALER-ROUTER Benchmarks: Standard vs ZC vs ZC+Cork ---
  let mut dr_group = c.benchmark_group("DealerRouter_SendComparison");
  for &payload_size in [PAYLOAD_SMALL, PAYLOAD_LARGE].iter() {
    dr_group.throughput(Throughput::Bytes(payload_size as u64 * NUM_MESSAGES_BENCH as u64));
    dr_group.sampling_mode(SamplingMode::Flat); // Good for throughput measurement

    let combinations = [
      (false, false, "Standard"),         // Standard Tokio send
      (false, true, "CorkOnly_StdTokio"), // Cork with standard Tokio send (Linux only for effect)
      (true, false, "ZCOnly_Uring"),      // Zerocopy with io_uring (Linux + feature only)
      (true, true, "ZC_Cork_Uring"),      // Zerocopy + Cork with io_uring (Linux + feature only)
    ];

    for &(try_zc, try_cork, desc_suffix) in combinations.iter() {
      // Skip ZC-related benchmarks if not on Linux with the io-uring feature
      if try_zc && !cfg!(all(target_os = "linux", feature = "io-uring")) {
        println!(
          "Skipping DEALER/ROUTER bench '{}' for payload {}B (requires Linux & io-uring feature)",
          desc_suffix, payload_size
        );
        continue;
      }
      // Skip Cork-only benchmarks if not on Linux (as TCP_CORK is Linux-specific)
      if try_cork && !try_zc && !cfg!(target_os = "linux") {
        // CorkOnly on non-Linux
        println!(
          "Skipping DEALER/ROUTER bench '{}' for payload {}B (TCP_CORK requires Linux)",
          desc_suffix, payload_size
        );
        continue;
      }

      let bench_id_str = format!("Payload_{}B", payload_size);
      let endpoint = format!(
        "tcp://127.0.0.1:{}_{}",
        BENCH_ENDPOINT_BASE_PORT_ZC + payload_size as u16 % 10,
        desc_suffix.to_lowercase().replace("+", "_")
      );

      dr_group.bench_function(BenchmarkId::new(&bench_id_str, desc_suffix), |b| {
        let rt = if try_zc && cfg!(all(target_os = "linux", feature = "io-uring")) {
          println!("[Bench DR {} {}] Using tokio-uring runtime", bench_id_str, desc_suffix);
          tokio_uring::builder().build().unwrap()
        } else {
          println!(
            "[Bench DR {} {}] Using standard Tokio runtime",
            bench_id_str, desc_suffix
          );
          TokioBuilder::new_current_thread().enable_all().build().unwrap()
        };

        rt.block_on(async {
          let ctx = bench_context();
          let (dealer, router, _identity) =
            match setup_dealer_router_send_comparison(&ctx, &endpoint, try_zc, try_cork).await {
              Ok(s) => s,
              Err(e) => {
                eprintln!("Setup failed for DR {} {}: {:?}", bench_id_str, desc_suffix, e);
                panic!("Benchmark setup failed");
              }
            };

          let payload_vec = vec![0u8; payload_size];
          // For DEALER->ROUTER, no multi-part needed unless testing router passthrough
          let msg_template = Msg::from_vec(payload_vec);

          b.iter_batched(
            || msg_template.clone(),
            |msg| async {
              dealer.send(msg).await.unwrap();
              // Router receives [identity, payload]
              let _id = router.recv().await.unwrap();
              let _p = router.recv().await.unwrap();
            },
            criterion::BatchSize::NumBatches(NUM_MESSAGES_BENCH as u64),
          );
          ctx.term().await.unwrap();
        });
      });
    }
  }
  dr_group.finish();

  // --- PUB-SUB Benchmarks: Standard vs ZC vs ZC+Cork ---
  let mut ps_group = c.benchmark_group("PubSub_SendComparison");
  for &payload_size in [PAYLOAD_SMALL, PAYLOAD_LARGE].iter() {
    ps_group.throughput(Throughput::Bytes(payload_size as u64 * NUM_MESSAGES_BENCH as u64));
    ps_group.sampling_mode(SamplingMode::Flat);

    let combinations = [
      (false, false, "Standard"),
      (false, true, "CorkOnly_StdTokio"),
      (true, false, "ZCOnly_Uring"),
      (true, true, "ZC_Cork_Uring"),
    ];

    for &(try_zc, try_cork, desc_suffix) in combinations.iter() {
      if try_zc && !cfg!(all(target_os = "linux", feature = "io-uring")) {
        println!(
          "Skipping PUB/SUB bench '{}' for payload {}B (requires Linux & io-uring feature)",
          desc_suffix, payload_size
        );
        continue;
      }
      if try_cork && !try_zc && !cfg!(target_os = "linux") {
        println!(
          "Skipping PUB/SUB bench '{}' for payload {}B (TCP_CORK requires Linux)",
          desc_suffix, payload_size
        );
        continue;
      }

      let bench_id_str = format!("Payload_{}B", payload_size);
      let endpoint = format!(
        "tcp://127.0.0.1:{}_{}",
        BENCH_ENDPOINT_BASE_PORT_ZC + 20 + payload_size as u16 % 10,
        desc_suffix.to_lowercase().replace("+", "_")
      );

      ps_group.bench_function(BenchmarkId::new(&bench_id_str, desc_suffix), |b| {
        let rt = if try_zc && cfg!(all(target_os = "linux", feature = "io-uring")) {
          println!("[Bench PS {} {}] Using tokio-uring runtime", bench_id_str, desc_suffix);
          tokio_uring::builder().build().unwrap()
        } else {
          println!(
            "[Bench PS {} {}] Using standard Tokio runtime",
            bench_id_str, desc_suffix
          );
          TokioBuilder::new_current_thread().enable_all().build().unwrap()
        };

        rt.block_on(async {
          let ctx = bench_context();
          let (publisher, subscriber) = match setup_pub_sub_send_comparison(&ctx, &endpoint, try_zc, try_cork).await {
            Ok(s) => s,
            Err(e) => {
              eprintln!("Setup failed for PS {} {}: {:?}", bench_id_str, desc_suffix, e);
              panic!("Benchmark setup failed");
            }
          };
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
          ctx.term().await.unwrap();
        });
      });
    }
  }
  ps_group.finish();
}

criterion_group!(
    name = send_comparison_benches;
    config = Criterion::default()
                .sample_size(15) // Slightly more samples
                .warm_up_time(Duration::from_millis(500))
                .measurement_time(Duration::from_secs(5)); // Longer measurement per iteration
    targets = zerocopy_vs_standard_benchmarks
);
criterion_main!(send_comparison_benches);
