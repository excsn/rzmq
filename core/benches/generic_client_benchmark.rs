use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rzmq::socket::{
  options::{RCVHWM, SNDHWM, SUBSCRIBE},
  SocketType,
};
use rzmq::{Context, Msg, ZmqError};
use std::env;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::time::{sleep, timeout};

// Setup details
// export RZRUST_BENCH_SOCKET_TYPE="PUSH"
// export RZRUST_BENCH_TARGET_ADDR="tcp://your-pull-server:port"
// # export RZRUST_BENCH_NUM_OPS="10000" # Optional
// cargo bench --bench generic_client_benchmark

// --- Benchmarking Constants ---
const DEFAULT_NUM_OPS: usize = 1000;
const DEFAULT_TARGET_SERVER_ADDR: &str = "tcp://127.0.0.1:5555";
const CLIENT_BENCH_HWM: i32 = 100_000;
const CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const CLIENT_OP_TIMEOUT: Duration = Duration::from_secs(10);
const CLIENT_SINGLE_OP_TIMEOUT: Duration = Duration::from_secs(2);

// Helper to parse SocketType from string
fn parse_socket_type(s: &str) -> Result<SocketType, String> {
  match s.to_uppercase().as_str() {
    "REQ" => Ok(SocketType::Req),
    "PUSH" => Ok(SocketType::Push),
    "DEALER" => Ok(SocketType::Dealer),
    "PUB" => Ok(SocketType::Pub),
    "SUB" => Ok(SocketType::Sub),
    _ => Err(format!("Unsupported or invalid socket type: {}", s)),
  }
}

// --- Core Benchmark Logic (parameterized) ---
async fn run_client_operations(
  client_socket_type: SocketType,
  client_socket: rzmq::Socket, // Pass the already created and connected socket
  msg_size: usize,
  num_ops: usize,
  _subscribe_topic: &str, // Kept for SUB, though not used in this simplified op loop
) {
  let operation_payload = vec![0u8; msg_size];

  match client_socket_type {
    SocketType::Req | SocketType::Dealer => {
      for _i in 0..num_ops {
        let req_msg = Msg::from_vec(black_box(operation_payload.clone()));
        timeout(CLIENT_SINGLE_OP_TIMEOUT, client_socket.send(req_msg))
          .await
          .expect("Send op timed out")
          .expect("Send op failed");
        let rep_msg = timeout(CLIENT_SINGLE_OP_TIMEOUT, client_socket.recv())
          .await
          .expect("Recv op timed out")
          .expect("Recv op failed");
        black_box(rep_msg.data());
      }
    }
    SocketType::Push | SocketType::Pub => {
      for _i in 0..num_ops {
        let msg_to_send = Msg::from_vec(black_box(operation_payload.clone()));
        timeout(CLIENT_SINGLE_OP_TIMEOUT, client_socket.send(msg_to_send))
          .await
          .expect("Send op timed out")
          .expect("Send op failed");
      }
    }
    SocketType::Sub => {
      for _i in 0..num_ops {
        let received_msg = timeout(CLIENT_SINGLE_OP_TIMEOUT, client_socket.recv())
          .await
          .expect("Recv op timed out")
          .expect("Recv op failed");
        black_box(received_msg.data());
      }
    }
    _ => {
      panic!("Unsupported socket type in operation loop");
    }
  }
}

// --- Main Benchmark Definition Function ---
fn external_client_benchmarks(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime");

  // --- Read Configuration from Environment Variables ---
  let selected_socket_type_str = env::var("RZRUST_BENCH_SOCKET_TYPE")
    .unwrap_or_else(|_| "ALL".to_string())
    .to_uppercase();
  let target_addr = env::var("RZRUST_BENCH_TARGET_ADDR").unwrap_or_else(|_| DEFAULT_TARGET_SERVER_ADDR.to_string());
  let num_ops = env::var("RZRUST_BENCH_NUM_OPS")
    .ok()
    .and_then(|s| s.parse::<usize>().ok())
    .unwrap_or(DEFAULT_NUM_OPS);
  let subscribe_topic = env::var("RZRUST_BENCH_SUB_TOPIC").unwrap_or_else(|_| "".to_string());

  // Define all possible client types you want to benchmark
  let all_benchmarkable_types: Vec<(&str, SocketType)> = vec![
    ("REQ", SocketType::Req),
    ("PUSH", SocketType::Push),
    ("DEALER", SocketType::Dealer),
    ("PUB", SocketType::Pub),
    ("SUB", SocketType::Sub),
  ];

  for (type_name_str, client_socket_type_enum) in all_benchmarkable_types {
    // If a specific type is selected, only run that group
    if selected_socket_type_str != "ALL" && selected_socket_type_str != type_name_str {
      continue; // Skip this socket type's group
    }

    let mut group = c.benchmark_group(format!("{}_Client_vs_External_at_{}", type_name_str, target_addr));

    group
      .warm_up_time(Duration::from_secs(3)) // Adjust as needed
      .measurement_time(Duration::from_secs(5)) // Adjust as needed
      .sample_size(10); // Adjust as needed

    for size in [16, 256, 1024, 4096].iter() {
      let throughput_bytes = match client_socket_type_enum {
        SocketType::Req | SocketType::Dealer => (num_ops * size * 2) as u64,
        SocketType::Push | SocketType::Pub | SocketType::Sub => (num_ops * size) as u64,
        _ => 0,
      };
      if throughput_bytes == 0 {
        continue;
      }

      group.throughput(Throughput::Bytes(throughput_bytes));
      let bench_id_name = format!("{}B_{}", size, type_name_str);
      let bench_id = BenchmarkId::from_parameter(&bench_id_name);

      let iter_target_addr = target_addr.clone();
      let iter_subscribe_topic = subscribe_topic.clone();

      group.bench_with_input(bench_id, size, |b, &msg_size| {
        b.to_async(&rt).iter_custom(|_iters| {
          // Clone for this specific iteration's async block
          let current_iter_target_addr = iter_target_addr.clone();
          let current_iter_subscribe_topic = iter_subscribe_topic.clone();

          async move {
            let client_ctx = Context::new().expect("Client context creation failed");
            let client_socket = client_ctx
              .socket(client_socket_type_enum)
              .expect("Failed to create client socket");

            client_socket
              .set_option(SNDHWM, CLIENT_BENCH_HWM)
              .await
              .unwrap();
            client_socket
              .set_option(RCVHWM, CLIENT_BENCH_HWM)
              .await
              .unwrap();

            match timeout(CLIENT_CONNECT_TIMEOUT, client_socket.connect(&current_iter_target_addr)).await {
              Ok(Ok(())) => {}
              Ok(Err(e)) => panic!("Client connect to {} failed: {}", current_iter_target_addr, e),
              Err(_) => panic!("Client connect to {} timed out", current_iter_target_addr),
            }

            if client_socket_type_enum == SocketType::Sub {
              client_socket
                .set_option(SUBSCRIBE, current_iter_subscribe_topic)
                .await
                .unwrap();
              sleep(Duration::from_millis(150)).await; // Allow subscribe to propagate (important!)
            } else {
              // Generic handshake/warmup for other types
              match client_socket_type_enum {
                SocketType::Req | SocketType::Dealer => {
                  client_socket
                    .send(Msg::from_static(b"BENCH_HELLO"))
                    .await
                    .expect("Handshake send failed");
                  client_socket.recv().await.expect("Handshake recv failed");
                }
                SocketType::Push | SocketType::Pub => {
                  client_socket
                    .send(Msg::from_static(b"BENCH_WARMUP"))
                    .await
                    .expect("Warmup send failed");
                }
                _ => {}
              }
              sleep(Duration::from_millis(50)).await;
            }

            let start_time = Instant::now();
            run_client_operations(
              client_socket_type_enum,
              client_socket.clone(), // run_client_operations needs to own/use the socket
              msg_size,
              num_ops,
              &current_iter_subscribe_topic,
            )
            .await;
            let elapsed = start_time.elapsed();

            client_socket
              .close()
              .await
              .unwrap_or_else(|e| eprintln!("[Client Bench Warning] Error closing client socket: {}", e));
            client_ctx.term().await.expect("Client context termination failed");

            elapsed
          }
        });
      });
    }
    group.finish();
  } // End loop over all_benchmarkable_types
}

criterion_group!(benches, external_client_benchmarks);
criterion_main!(benches);
