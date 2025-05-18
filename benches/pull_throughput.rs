// benches/throughput.rs

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rzmq::{
  socket::{
    options::{RCVHWM, SNDHWM},
    MonitorReceiver, SocketEvent,
  },
  Context, Msg, SocketType, ZmqError,
};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::time::{sleep, timeout};

// --- Benchmarking Constants ---
const NUM_MESSAGES: usize = 1000;
const BIND_ADDR: &str = "tcp://127.0.0.1:5680";
const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_RECV_TIMEOUT: Duration = Duration::from_secs(4);
const BENCH_HWM: i32 = 100_000;

// --- Helper function to wait for a specific monitor event ---
async fn wait_for_event(
  monitor_rx: &MonitorReceiver,
  check_event: impl Fn(&SocketEvent) -> bool,
) -> Result<SocketEvent, String> {
  let start_time = Instant::now();
  loop {
    if start_time.elapsed() > EVENT_RECV_TIMEOUT {
      return Err(format!(
        "Timeout waiting for specific event after {:?}",
        EVENT_RECV_TIMEOUT
      ));
    }
    match timeout(Duration::from_millis(50), monitor_rx.recv()).await {
      Ok(Ok(event)) => {
        if check_event(&event) {
          return Ok(event);
        }
      }
      Ok(Err(e)) => {
        return Err(format!("Monitor channel error: {}", e));
      }
      Err(_) => { /* Timeout poll, continue */ }
    }
  }
}

// --- Setup Function ---
async fn setup_push_pull(ctx: &Context) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  push.set_option(SNDHWM, &BENCH_HWM.to_ne_bytes()).await?;
  pull.set_option(RCVHWM, &BENCH_HWM.to_ne_bytes()).await?;

  let pull_monitor = pull.monitor_default().await?;

  pull.bind(BIND_ADDR).await?;
  wait_for_event(
    &pull_monitor,
    |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == BIND_ADDR),
  )
  .await
  .map_err(|e| ZmqError::Internal(format!("PULL Listening event error: {}", e)))?;

  push.connect(BIND_ADDR).await?;

  wait_for_event(&pull_monitor, |e| {
    matches!(e, SocketEvent::Accepted { .. } | SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .map_err(|e| ZmqError::Internal(format!("PULL Connection event error: {}", e)))?;

  sleep(Duration::from_millis(20)).await;

  Ok((push, pull))
}

// --- Benchmark Function ---
fn push_pull_tcp_throughput(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime");
  let mut group = c.benchmark_group("PUSH_PULL_TCP_Throughput");
  group
    .warm_up_time(Duration::from_secs(5)) // Increase warm-up time
    .measurement_time(Duration::from_secs(10)) // Increase measurement time
    .sample_size(20); // Increase sample size (number of loops for measurement)
                      // Note: sample_size for iter_custom is a bit different; iter_custom runs its
                      //       async block *once* per sample. Criterion decides how many samples.
                      //       The main effect here is on how many times the setup/teardown + loop runs.

  for size in [16, 256, 1024, 4096, 16384].iter() {
    group.throughput(Throughput::Bytes((NUM_MESSAGES * size) as u64));
    let bench_id = BenchmarkId::from_parameter(format!("{}B", size));

    group.bench_with_input(bench_id, size, |b, &msg_size| {
      b.to_async(&rt).iter_custom(|_iters| async move {
        let ctx = Context::new().expect("Bench context creation failed");

        let (push, pull) = match timeout(SETUP_TIMEOUT, setup_push_pull(&ctx)).await {
          Ok(Ok(sockets)) => sockets,
          Ok(Err(e)) => panic!("Bench socket setup failed: {}", e),
          Err(_) => panic!("Bench socket setup timed out overall"),
        };

        let message_payload = vec![0u8; msg_size];
        let start = Instant::now();

        let sender_task = {
          let push = push.clone();
          let payload = message_payload.clone();
          tokio::spawn(async move {
            for _i in 0..NUM_MESSAGES {
              let msg = Msg::from_vec(black_box(payload.clone()));
              if let Err(e) = push.send(msg).await {
                eprintln!("[Sender Task] Error sending msg: {}", e);
                return Err(e);
              }
            }
            Ok(())
          })
        };

        let receiver_task = {
          let pull = pull.clone();
          tokio::spawn(async move {
            for i in 0..NUM_MESSAGES {
              match pull.recv().await {
                Ok(msg) => {
                  black_box(msg.data());
                }
                Err(ZmqError::InvalidState(ref s)) if s == &"Socket terminated" => {
                  eprintln!(
                    "[Receiver Task] Tolerating 'Socket terminated' error on iter {}/{}",
                    i + 1,
                    NUM_MESSAGES
                  );
                  return Ok(i);
                }
                Err(e) => {
                  eprintln!("[Receiver Task] Error receiving msg: {}", e);
                  return Err(e);
                }
              }
            }
            Ok(NUM_MESSAGES)
          })
        };

        let send_result = sender_task.await.expect("Sender task panicked");
        let recv_result = receiver_task.await.expect("Receiver task panicked");
        let elapsed = start.elapsed();

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Err(e) = push.close().await {
          eprintln!("[Iter Warning] Error closing PUSH: {}", e);
        }
        if let Err(e) = pull.close().await {
          eprintln!("[Iter Warning] Error closing PULL: {}", e);
        }
        ctx.term().await.expect("Context termination failed");

        if let Err(e) = send_result {
          panic!("Benchmark sender task failed: {}", e);
        }
        match recv_result {
          Ok(count) if count < NUM_MESSAGES => {
            eprintln!(
              "[Iter Warning] Receiver task finished but received only {}/{} messages",
              count, NUM_MESSAGES
            );
          }
          Err(e) => panic!("Benchmark receiver task failed: {}", e),
          _ => {}
        }
        elapsed
      });
    });
  }
  group.finish();
}

criterion_group!(benches, push_pull_tcp_throughput);
criterion_main!(benches);
