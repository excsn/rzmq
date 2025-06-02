// benches/pub_sub_throughput.rs

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rzmq::{
  socket::{
    options::{RCVHWM, SNDHWM, SUBSCRIBE}, // Added SUBSCRIBE
    MonitorReceiver,
    SocketEvent,
  },
  Context, Msg, SocketType, ZmqError,
};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::time::{sleep, timeout};

// --- Benchmarking Constants ---
const NUM_MESSAGES_PUB_SUB: usize = 1000;
const BIND_ADDR_PUB_SUB: &str = "tcp://127.0.0.1:5682"; // Unique port
const SETUP_TIMEOUT_PUB_SUB: Duration = Duration::from_secs(5);
const EVENT_RECV_TIMEOUT_PUB_SUB: Duration = Duration::from_secs(4);
const BENCH_HWM_PUB_SUB: i32 = 1_000_000;

// --- Helper: wait_for_event (can be in common.rs) ---
async fn wait_for_event_pub_sub(
  monitor_rx: &MonitorReceiver,
  check_event: impl Fn(&SocketEvent) -> bool,
) -> Result<SocketEvent, String> {
  let start_time = Instant::now();
  loop {
    if start_time.elapsed() > EVENT_RECV_TIMEOUT_PUB_SUB {
      return Err(format!("Timeout after {:?}", EVENT_RECV_TIMEOUT_PUB_SUB));
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

// --- Setup Function for PUB-SUB ---
async fn setup_pub_sub(ctx: &Context) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let pub_socket = ctx.socket(SocketType::Pub)?;
  let sub_socket = ctx.socket(SocketType::Sub)?;

  pub_socket.set_option(SNDHWM, &BENCH_HWM_PUB_SUB.to_ne_bytes()).await?;
  sub_socket.set_option(RCVHWM, &BENCH_HWM_PUB_SUB.to_ne_bytes()).await?;

  let pub_monitor = pub_socket.monitor_default().await?;
  let sub_monitor = sub_socket.monitor_default().await?;

  pub_socket.bind(BIND_ADDR_PUB_SUB).await?;
  sub_socket.connect(BIND_ADDR_PUB_SUB).await?;
  
  wait_for_event_pub_sub(&pub_monitor, |e| {
    // Wait for PUB to see SUB
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .map_err(|e| ZmqError::Internal(format!("PUB Connection event error: {}", e)))?;

  wait_for_event_pub_sub(&sub_monitor, |e| {
    // Wait for PUB to see SUB
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .map_err(|e| ZmqError::Internal(format!("PUB Connection event error: {}", e)))?;

  // SUB subscribes to all messages
  sub_socket.set_option(SUBSCRIBE, b"").await?;

  // Allow time for SUBSCRIBE message to be processed by PUB's outgoing pipe
  // and for pipe attachment to fully register in PUB's distributor
  sleep(Duration::from_millis(50)).await; // Was 20, slightly more for subscribe
  Ok((pub_socket, sub_socket))
}

// --- Benchmark Function ---
fn pub_sub_tcp_throughput(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime");
  let mut group = c.benchmark_group("PUB_SUB_TCP_Throughput");

  group
    .warm_up_time(Duration::from_secs(5)) // Increase warm-up time
    .measurement_time(Duration::from_secs(10)) // Increase measurement time
    .sample_size(20); // Increase sample size (number of loops for measurement)
                      // Note: sample_size for iter_custom is a bit different; iter_custom runs its
                      //       async block *once* per sample. Criterion decides how many samples.
                      //       The main effect here is on how many times the setup/teardown + loop runs.

  for size in [16, 256, 1024, 4096, 16384].iter() {
    group.throughput(Throughput::Bytes((NUM_MESSAGES_PUB_SUB * size) as u64));
    let bench_id = BenchmarkId::from_parameter(format!("{}B", size));

    group.bench_with_input(bench_id, size, |b, &msg_size| {
      b.to_async(&rt).iter_custom(|_iters| async move {
        let ctx = Context::new().expect("Bench context creation failed");
        let (pub_socket, sub_socket) = match timeout(SETUP_TIMEOUT_PUB_SUB, setup_pub_sub(&ctx)).await {
          Ok(Ok(sockets)) => sockets,
          Ok(Err(e)) => panic!("Bench PUB-SUB setup failed: {}", e),
          Err(_) => panic!("Bench PUB-SUB setup timed out"),
        };

        let message_payload = vec![0u8; msg_size];
        let start = Instant::now();

        let publisher_task = {
          let pub_socket = pub_socket.clone();
          let payload = message_payload.clone();
          tokio::spawn(async move {
            for _i in 0..NUM_MESSAGES_PUB_SUB {
              let msg = Msg::from_vec(black_box(payload.clone()));
              if let Err(e) = pub_socket.send(msg).await {
                eprintln!("[PUB Task] Error sending: {}", e);
                return Err(e);
              }
            }
            Ok(())
          })
        };

        let subscriber_task = {
          let sub_socket = sub_socket.clone();
          tokio::spawn(async move {
            for i in 0..NUM_MESSAGES_PUB_SUB {
              match sub_socket.recv().await {
                Ok(msg) => {
                  black_box(msg.data());
                }
                Err(ZmqError::InvalidState(ref s)) if s == &"Socket terminated" => {
                  eprintln!(
                    "[SUB Task] Tolerating 'Socket terminated' on iter {}/{}",
                    i + 1,
                    NUM_MESSAGES_PUB_SUB
                  );
                  return Ok(i);
                }
                Err(e) => {
                  eprintln!("[SUB Task] Error receiving: {}", e);
                  return Err(e);
                }
              }
            }
            Ok(NUM_MESSAGES_PUB_SUB)
          })
        };

        let pub_result = publisher_task.await.expect("Publisher task panicked");
        let sub_result = subscriber_task.await.expect("Subscriber task panicked");
        let elapsed = start.elapsed();

        sleep(Duration::from_millis(100)).await;

        if let Err(e) = pub_socket.close().await {
          eprintln!("[Iter Warning] Error closing PUB: {}", e);
        }
        if let Err(e) = sub_socket.close().await {
          eprintln!("[Iter Warning] Error closing SUB: {}", e);
        }
        ctx.term().await.expect("Context termination failed");

        if let Err(e) = pub_result {
          panic!("Benchmark PUB task failed: {}", e);
        }
        match sub_result {
          Ok(count) if count < NUM_MESSAGES_PUB_SUB => {
            eprintln!(
              "[Iter Warning] SUB task received only {}/{} msgs",
              count, NUM_MESSAGES_PUB_SUB
            );
          }
          Err(e) => panic!("Benchmark SUB task failed: {}", e),
          _ => {}
        }
        elapsed
      });
    });
  }
  group.finish();
}

criterion_group!(benches_pub_sub, pub_sub_tcp_throughput);
criterion_main!(benches_pub_sub);
