// benches/multishot_recv_bench.rs

#![cfg(all(target_os = "linux", feature = "io-uring"))]

use criterion::{
  criterion_group, criterion_main, measurement::WallTime, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use rzmq::{
  socket::options::{
    IO_URING_RCVMULTISHOT, IO_URING_RECV_BUFFER_COUNT, IO_URING_RECV_BUFFER_SIZE, RCVHWM, SNDHWM, SNDTIMEO,
  },
  Context, Msg, SocketType, ZmqError,
};
use std::{sync::atomic::{AtomicU16, Ordering as AtomicOrdering}, time::Instant}; // For unique ports
use std::time::Duration;

// --- Port Management for Benchmarks ---
const BENCH_ENDPOINT_BASE_PORT_MS_RECV: u16 = 6300;
static NEXT_BENCH_PORT_MS_RECV: AtomicU16 = AtomicU16::new(BENCH_ENDPOINT_BASE_PORT_MS_RECV);

fn get_next_ms_recv_bench_port() -> u16 {
  NEXT_BENCH_PORT_MS_RECV.fetch_add(1, AtomicOrdering::Relaxed)
}
// --- End Port Management ---

fn bench_context() -> Context {
  rzmq::Context::new().expect("Failed to create benchmark context")
}

const PAYLOAD_SMALL: usize = 128;
const PAYLOAD_LARGE: usize = 32 * 1024;
const NUM_MESSAGES_BENCH: usize = 5000;

const MS_BUF_COUNT: usize = 16;
const MS_BUF_SIZE: usize = 65536;

async fn setup_push_pull_for_recv_bench(
  ctx: &Context,
  endpoint: &str, // Now receives the fully formed endpoint string
  enable_multishot_on_pull: bool,
) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  if enable_multishot_on_pull {
    println!("[Setup {}] Configuring PULL for Multishot Receive", endpoint);
    pull.set_option(IO_URING_RCVMULTISHOT, &(1i32).to_ne_bytes()).await?;
    pull
      .set_option(IO_URING_RECV_BUFFER_COUNT, &(MS_BUF_COUNT as i32).to_ne_bytes())
      .await?;
    pull
      .set_option(IO_URING_RECV_BUFFER_SIZE, &(MS_BUF_SIZE as i32).to_ne_bytes())
      .await?;
  } else {
    println!("[Setup {}] Configuring PULL for Standard Receive", endpoint);
  }
  pull
    .set_option(RCVHWM, (NUM_MESSAGES_BENCH as i32 / 2).max(100))
    .await?;
  push
    .set_option(SNDHWM, ((NUM_MESSAGES_BENCH as i32 / 2).max(1000)))
    .await?;
  push.set_option(SNDTIMEO, &(-1i32).to_ne_bytes()).await?;

  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  Ok((push, pull))
}

fn multishot_receive_throughput(c: &mut Criterion<WallTime>) {
  let mut group = c.benchmark_group("PullSocket_ReceiveThroughput_Uring"); // Clarify group name

  for &payload_size in [PAYLOAD_SMALL, PAYLOAD_LARGE].iter() {
    group.throughput(Throughput::Bytes(payload_size as u64 * NUM_MESSAGES_BENCH as u64));
    group.sampling_mode(SamplingMode::Flat);

    // Cases to test, all on io_uring runtime because the file is gated
    let bench_cases = [
      ("StandardFramedRecv", false), // Standard Framed::next() on UringTcpStream
      ("MultishotRecv", true),       // Custom multishot logic on UringTcpStream
    ];

    for &(desc_suffix, enable_multishot) in bench_cases.iter() {
      let port = get_next_ms_recv_bench_port();
      let endpoint = format!("tcp://127.0.0.1:{}", port);

      let bench_id_str = format!("Payload_{}B", payload_size);
      let bench_name_for_id = format!("{}_{}", bench_id_str, desc_suffix); // For Criterion ID

      group.bench_function(
        BenchmarkId::new(bench_name_for_id.clone(), &format!("Endpoint: {}", endpoint)),
        |b| {
          let rt = UringBuilder::new().build().unwrap();
          println!(
            "[Bench {} {} on {}] Using tokio-uring runtime",
            bench_id_str, desc_suffix, endpoint
          );

          rt.block_on(async {
            let ctx = bench_context();
            let (push, pull) = match setup_push_pull_for_recv_bench(&ctx, &endpoint, enable_multishot).await {
              Ok(s) => s,
              Err(e) => {
                eprintln!("Setup failed for {} {}: {:?}", bench_id_str, desc_suffix, e);
                panic!("Benchmark setup failed");
              }
            };

            let payload_vec = vec![7u8; payload_size];
            let msg_template = Msg::from_vec(payload_vec.clone()); // Clone for template

            let push_handle = tokio::spawn({
              let push_clone = push.clone();
              let msg_template_clone = msg_template.clone();
              async move {
                for _i in 0..NUM_MESSAGES_BENCH {
                  if push_clone.send(msg_template_clone.clone()).await.is_err() {
                    break;
                  }
                }
              }
            });

            let expected_payload_bytes = payload_vec.as_slice();
            b.iter_custom(|iters| {
              let pull_clone = pull.clone();
              async move {
                let start = Instant::now();
                for _i in 0..NUM_MESSAGES_BENCH {
                  match pull_clone.recv().await {
                    Ok(msg) => {
                      if msg.size() != payload_size || msg.data().unwrap_or_default() != expected_payload_bytes {
                        panic!(
                          "Received message mismatch (payload_size: {}, expected: {:?}, got: {:?})",
                          payload_size,
                          expected_payload_bytes,
                          msg.data()
                        );
                      }
                    }
                    Err(e) => {
                      if !matches!(e, ZmqError::Timeout) {
                        panic!("Pull recv error: {:?}", e);
                      }
                      break;
                    }
                  }
                }
                start.elapsed()
              }
            });

            if let Err(e) = push_handle.await {
              eprintln!("Push task panicked or errored: {:?}", e);
            }
            ctx.term().await.unwrap();
          });
        },
      );
    }
  }
  group.finish();
}

criterion_group!(
    name = multishot_recv_benches;
    config = Criterion::default()
                .sample_size(10)
                .warm_up_time(Duration::from_millis(1000))
                .measurement_time(Duration::from_secs(10));
    targets = multishot_receive_throughput
);
criterion_main!(multishot_recv_benches);
