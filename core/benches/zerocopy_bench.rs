// benches/zerocopy_bench.rs

use criterion::{
  criterion_group, criterion_main, measurement::WallTime, BenchmarkId, Criterion, SamplingMode, Throughput,
};
#[cfg(feature = "io-uring")]
use rzmq::socket::options::IO_URING_SNDZEROCOPY;
use rzmq::{
  socket::options::{RCVHWM, SNDHWM, SUBSCRIBE, TCP_CORK_OPT},
  Context, Msg, SocketType, ZmqError,
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
  enable_zerocopy_opt: bool,
  enable_cork_opt: bool,
) -> Result<(rzmq::Socket, rzmq::Socket, Vec<u8>), ZmqError> {
  let dealer = ctx.socket(SocketType::Dealer)?;
  let router = ctx.socket(SocketType::Router)?;

  if enable_zerocopy_opt {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
      // IO_URING_SNDZEROCOPY is only in scope if the feature "io-uring"
      // is enabled for this benchmark compilation unit (and thus for rzmq).
      dealer.set_option(IO_URING_SNDZEROCOPY, &(1i32).to_ne_bytes()).await?;
      println!("[Setup {}] IO_URING_SNDZEROCOPY enabled for DEALER", endpoint);
    }
    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    if enable_zerocopy_opt { // Ensure this check is also inside the cfg_not block
      println!(
        "[Setup {}] IO_URING_SNDZEROCOPY requested but not applicable (not Linux or io-uring feature off)",
        endpoint
      );
    }
  }

  if enable_cork_opt {
    #[cfg(target_os = "linux")]
    {
        dealer.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await?;
        println!("[Setup {}] TCP_CORK_OPT enabled for DEALER", endpoint);
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Silently ignore or log if TCP_CORK is attempted on non-Linux
        let _ = dealer.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await;
        println!("[Setup {}] TCP_CORK_OPT requested but not applicable (not Linux)", endpoint);
    }
  }

  dealer.set_option(SNDHWM, &(100i32).to_ne_bytes()).await?;
  router.set_option(RCVHWM, &(100i32).to_ne_bytes()).await?;

  router.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await; // Increased bind settle time
  dealer.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await; // Increased connect settle time

  // Initial message from Dealer to Router for Router to learn identity
  dealer.send(Msg::from_static(b"INIT_DEALER_ZC_CMP")).await?;
  // Router receives two frames: [identity, actual_message_from_dealer]
  let identity_msg = router.recv().await?;
  let identity = identity_msg.data_bytes()
    .ok_or_else(|| ZmqError::Other("Identity message frame had no data during setup"))?
    .to_vec();
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

  if enable_zerocopy_on_pub_opt {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
      publisher.set_option(IO_URING_SNDZEROCOPY, &(1i32).to_ne_bytes()).await?;
      println!("[Setup {}] IO_URING_SNDZEROCOPY enabled for PUB", endpoint);
    }
    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    if enable_zerocopy_on_pub_opt {
         println!(
        "[Setup {}] IO_URING_SNDZEROCOPY (PUB) requested but not applicable (not Linux or io-uring feature off)",
        endpoint
      );
    }
  }

  if enable_cork_on_pub_opt {
    #[cfg(target_os = "linux")]
    {
        publisher.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await?;
        println!("[Setup {}] TCP_CORK_OPT enabled for PUB", endpoint);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = publisher.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await;
         println!("[Setup {}] TCP_CORK_OPT (PUB) requested but not applicable (not Linux)", endpoint);
    }
  }

  publisher.set_option(SNDHWM, &(1000i32).to_ne_bytes()).await?;
  subscriber.set_option(RCVHWM, &(1000i32).to_ne_bytes()).await?;
  subscriber.set_option(SUBSCRIBE, b"").await?;


  publisher.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  subscriber.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await; // Ensure subscription is processed

  Ok((publisher, subscriber))
}

fn zerocopy_vs_standard_benchmarks(c: &mut Criterion<WallTime>) {
  let mut port_allocator = BENCH_ENDPOINT_BASE_PORT_ZC;

  // --- DEALER-ROUTER Benchmarks: Standard vs ZC vs ZC+Cork ---
  let mut dr_group = c.benchmark_group("DealerRouter_SendComparison");
  for &payload_size in [PAYLOAD_SMALL, PAYLOAD_LARGE].iter() {
    dr_group.throughput(Throughput::Bytes(payload_size as u64 * NUM_MESSAGES_BENCH as u64));
    dr_group.sampling_mode(SamplingMode::Flat);

    let combinations = [
      // (try_zerocopy, try_cork, description_suffix)
      (false, false, "Standard"),
      (false, true, "CorkOnly"),       // Cork implies Linux for effect
      (true, false, "ZCOnly_Uring"),   // ZC implies Linux + io-uring feature
      (true, true, "ZC_Cork_Uring"), // ZC implies Linux + io-uring feature, Cork implies Linux
    ];

    for &(try_zc, try_cork, desc_suffix) in combinations.iter() {
      // Conditional skipping logic
      if try_zc && !cfg!(all(target_os = "linux", feature = "io-uring")) {
        println!(
          "Skipping DEALER/ROUTER bench '{}' for payload {}B (requires Linux & io-uring feature)",
          desc_suffix, payload_size
        );
        continue;
      }
      // If only cork is tried, it's only meaningful on Linux.
      // If ZC is also tried, the ZC condition above handles the Linux check.
      if try_cork && !try_zc && !cfg!(target_os = "linux") {
        println!(
          "Skipping DEALER/ROUTER bench '{}' (CorkOnly) for payload {}B (TCP_CORK requires Linux)",
          desc_suffix, payload_size
        );
        continue;
      }

      let current_port = port_allocator;
      port_allocator += 1;
      let endpoint = format!("tcp://127.0.0.1:{}", current_port);
      let bench_id_str = format!("Payload_{}B", payload_size);

      dr_group.bench_function(BenchmarkId::new(&bench_id_str, desc_suffix), |b| {
        // Runtime selection is per-benchmark function, as it can change.
        // This `rt` instance is what `b.to_async` will use.
        let rt = if try_zc && cfg!(all(target_os = "linux", feature = "io-uring")) {
          println!("[Bench DR {}:{}] Using tokio-uring runtime for endpoint {}", bench_id_str, desc_suffix, endpoint);
          #[cfg(all(target_os = "linux", feature = "io-uring"))] // Ensure tokio_uring is only referenced when available
          { tokio_uring::builder().build().unwrap() }
          #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
          unreachable!("Runtime selection logic error: Should have skipped ZC bench if not Linux/io-uring");
        } else {
          println!("[Bench DR {}:{}] Using standard Tokio runtime for endpoint {}", bench_id_str, desc_suffix, endpoint);
          TokioBuilder::new_current_thread().enable_all().build().unwrap()
        };

        // Setup sockets, ZMQ context, and message template ONCE for this BenchmarkId.
        // This block uses the `rt` chosen above.
        let (ctx, dealer, router, _identity, msg_template) = rt.block_on(async {
            let zmq_ctx = bench_context();
            let (d, r, id) =
                setup_dealer_router_send_comparison(&zmq_ctx, &endpoint, try_zc, try_cork)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "Setup failed for DR {}:{} on {}: {:?}",
                        bench_id_str, desc_suffix, endpoint, e
                    )
                });
            let payload_vec = vec![0u8; payload_size];
            let mt = Msg::from_vec(payload_vec);
            (zmq_ctx, d, r, id, mt)
        });

        b.to_async(&rt).iter_batched(
            || msg_template.clone(), // Setup for each sample (clone the message)
            |msg| async {            // Routine for each iteration within a sample
                dealer.send(msg).await.unwrap();
                // Router receives [identity, payload]
                let _id_frame = router.recv().await.unwrap();
                let _payload_frame = router.recv().await.unwrap();
            },
            criterion::BatchSize::NumBatches(NUM_MESSAGES_BENCH as u64),
        );

        // Teardown: Terminate the ZMQ context used for this specific BenchmarkId.
        // Sockets (`dealer`, `router`) are dropped automatically when they go out of scope here.
        rt.block_on(async {
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
      (false, true, "CorkOnly"),
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
          "Skipping PUB/SUB bench '{}' (CorkOnly) for payload {}B (TCP_CORK requires Linux)",
          desc_suffix, payload_size
        );
        continue;
      }

      let current_port = port_allocator;
      port_allocator += 1;
      let endpoint = format!("tcp://127.0.0.1:{}", current_port);
      let bench_id_str = format!("Payload_{}B", payload_size);

      ps_group.bench_function(BenchmarkId::new(&bench_id_str, desc_suffix), |b| {
        let rt = if try_zc && cfg!(all(target_os = "linux", feature = "io-uring")) {
          println!("[Bench PS {}:{}] Using tokio-uring runtime for endpoint {}", bench_id_str, desc_suffix, endpoint);
          #[cfg(all(target_os = "linux", feature = "io-uring"))]
          { tokio_uring::builder().build().unwrap() }
          #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
          unreachable!("Runtime selection logic error: Should have skipped ZC bench if not Linux/io-uring");
        } else {
          println!("[Bench PS {}:{}] Using standard Tokio runtime for endpoint {}", bench_id_str, desc_suffix, endpoint);
          TokioBuilder::new_current_thread().enable_all().build().unwrap()
        };

        let (ctx, publisher, subscriber, msg_template) = rt.block_on(async {
            let zmq_ctx = bench_context();
            let (p, s) =
                setup_pub_sub_send_comparison(&zmq_ctx, &endpoint, try_zc, try_cork)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "Setup failed for PS {}:{} on {}: {:?}",
                        bench_id_str, desc_suffix, endpoint, e
                    )
                });
            let payload_vec = vec![0u8; payload_size];
            let mt = Msg::from_vec(payload_vec);
            (zmq_ctx, p, s, mt)
        });

        b.to_async(&rt).iter_batched(
            || msg_template.clone(),
            |msg| async {
                publisher.send(msg).await.unwrap();
                let _received_msg = subscriber.recv().await.unwrap();
            },
            criterion::BatchSize::NumBatches(NUM_MESSAGES_BENCH as u64),
        );

        rt.block_on(async {
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
                .sample_size(15)
                .warm_up_time(Duration::from_millis(500))
                .measurement_time(Duration::from_secs(5));
    targets = zerocopy_vs_standard_benchmarks
);
criterion_main!(send_comparison_benches);