// benches/req_rep_throughput.rs

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
const NUM_ROUND_TRIPS: usize = 1000; // Fewer messages due to round-trip nature
const BIND_ADDR_REQ_REP: &str = "tcp://127.0.0.1:5681"; // Unique port
const SETUP_TIMEOUT_REQ_REP: Duration = Duration::from_secs(5);
const EVENT_RECV_TIMEOUT_REQ_REP: Duration = Duration::from_secs(4);
const BENCH_HWM_REQ_REP: i32 = 100_000;

// --- Helper: wait_for_event (can be in common.rs) ---
async fn wait_for_event_req_rep(
  monitor_rx: &MonitorReceiver,
  check_event: impl Fn(&SocketEvent) -> bool,
) -> Result<SocketEvent, String> {
  let start_time = Instant::now();
  loop {
    if start_time.elapsed() > EVENT_RECV_TIMEOUT_REQ_REP {
      return Err(format!("Timeout after {:?}", EVENT_RECV_TIMEOUT_REQ_REP));
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

// --- Setup Function for REQ-REP ---
async fn setup_req_rep(ctx: &Context) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let req = ctx.socket(SocketType::Req)?;
  let rep = ctx.socket(SocketType::Rep)?;

  req.set_option(SNDHWM, &BENCH_HWM_REQ_REP.to_ne_bytes()).await?;
  req.set_option(RCVHWM, &BENCH_HWM_REQ_REP.to_ne_bytes()).await?;
  rep.set_option(SNDHWM, &BENCH_HWM_REQ_REP.to_ne_bytes()).await?;
  rep.set_option(RCVHWM, &BENCH_HWM_REQ_REP.to_ne_bytes()).await?;

  let rep_monitor = rep.monitor_default().await?;

  rep.bind(BIND_ADDR_REQ_REP).await?;
  wait_for_event_req_rep(
    &rep_monitor,
    |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == BIND_ADDR_REQ_REP),
  )
  .await
  .map_err(|e| ZmqError::Internal(format!("REP Listening event error: {}", e)))?;

  req.connect(BIND_ADDR_REQ_REP).await?;
  wait_for_event_req_rep(&rep_monitor, |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }))
    .await
    .map_err(|e| ZmqError::Internal(format!("REP Connection event error: {}", e)))?;

  sleep(Duration::from_millis(20)).await;
  Ok((req, rep))
}

// --- Benchmark Function ---
fn req_rep_tcp_throughput(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime");
  let mut group = c.benchmark_group("REQ_REP_TCP_RoundTrip");

  group
    .warm_up_time(Duration::from_secs(5)) // Increase warm-up time
    .measurement_time(Duration::from_secs(10)) // Increase measurement time
    .sample_size(20); // Increase sample size (number of loops for measurement)
                      // Note: sample_size for iter_custom is a bit different; iter_custom runs its
                      //       async block *once* per sample. Criterion decides how many samples.
                      //       The main effect here is on how many times the setup/teardown + loop runs.
  for size in [16, 256, 1024, 4096].iter() {
    // Smaller messages often for REQ-REP
    // Throughput is number of round trips * (request size + reply size)
    // Assuming reply size is same as request size for this benchmark
    group.throughput(Throughput::Bytes((NUM_ROUND_TRIPS * size * 2) as u64));
    let bench_id = BenchmarkId::from_parameter(format!("{}B", size));

    group.bench_with_input(bench_id, size, |b, &msg_size| {
      b.to_async(&rt).iter_custom(|_iters| async move {
        let ctx = Context::new().expect("Bench context creation failed");
        let (req, rep) = match timeout(SETUP_TIMEOUT_REQ_REP, setup_req_rep(&ctx)).await {
          Ok(Ok(sockets)) => sockets,
          Ok(Err(e)) => panic!("Bench REQ-REP setup failed: {}", e),
          Err(_) => panic!("Bench REQ-REP setup timed out"),
        };

        let request_payload = vec![0u8; msg_size];
        let reply_payload = vec![0u8; msg_size]; // Assuming same size reply

        let start = Instant::now();

        let rep_task = {
          let rep = rep.clone();
          let reply_payload = reply_payload.clone();
          tokio::spawn(async move {
            for _i in 0..NUM_ROUND_TRIPS {
              match rep.recv().await {
                Ok(req_msg) => {
                  black_box(req_msg.data());
                  let rep_msg = Msg::from_vec(black_box(reply_payload.clone()));
                  if let Err(e) = rep.send(rep_msg).await {
                    eprintln!("[REP Task] Error sending reply: {}", e);
                    return Err(e);
                  }
                }
                Err(ZmqError::InvalidState(ref s)) if s == &"Socket terminated" => break,
                Err(e) => {
                  eprintln!("[REP Task] Error receiving request: {}", e);
                  return Err(e);
                }
              }
            }
            Ok(())
          })
        };

        let req_task = {
          let req = req.clone();
          let request_payload = request_payload.clone();
          tokio::spawn(async move {
            for _i in 0..NUM_ROUND_TRIPS {
              let req_msg = Msg::from_vec(black_box(request_payload.clone()));
              if let Err(e) = req.send(req_msg).await {
                eprintln!("[REQ Task] Error sending request: {}", e);
                return Err(e);
              }
              match req.recv().await {
                Ok(rep_msg) => {
                  black_box(rep_msg.data());
                }
                Err(ZmqError::InvalidState(ref s)) if s == &"Socket terminated" => break,
                Err(e) => {
                  eprintln!("[REQ Task] Error receiving reply: {}", e);
                  return Err(e);
                }
              }
            }
            Ok(())
          })
        };

        let req_result = req_task.await.expect("REQ task panicked");
        let rep_result = rep_task.await.expect("REP task panicked");
        let elapsed = start.elapsed();

        sleep(Duration::from_millis(100)).await; // Quiesce time

        if let Err(e) = req.close().await {
          eprintln!("[Iter Warning] Error closing REQ: {}", e);
        }
        if let Err(e) = rep.close().await {
          eprintln!("[Iter Warning] Error closing REP: {}", e);
        }
        ctx.term().await.expect("Context termination failed");

        if let Err(e) = req_result {
          panic!("Benchmark REQ task failed: {}", e);
        }
        if let Err(e) = rep_result {
          panic!("Benchmark REP task failed: {}", e);
        }

        elapsed
      });
    });
  }
  group.finish();
}

criterion_group!(benches_req_rep, req_rep_tcp_throughput);
criterion_main!(benches_req_rep);
