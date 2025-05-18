// benches/throughput.rs

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rzmq::{
  socket::{
    options::{RCVHWM, SNDHWM},
    MonitorReceiver, SocketEvent,
  }, // Import monitor types
  Context,
  Msg,
  SocketType,
  ZmqError,
};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::time::{sleep, timeout};

// --- Benchmarking Constants ---
const NUM_MESSAGES: usize = 10000;
const BIND_ADDR: &str = "tcp://127.0.0.1:5680";
const SETUP_TIMEOUT: Duration = Duration::from_secs(5); // Timeout for the whole setup
const EVENT_RECV_TIMEOUT: Duration = Duration::from_secs(4); // Timeout waiting for a specific event
const BENCH_HWM: i32 = 100_000;

// --- Helper function to wait for a specific monitor event ---
// (Could move this to a common test/bench helper module)
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
      // Poll briefly
      Ok(Ok(event)) => {
        // Received an event
        println!("Monitor received during setup: {:?}", event);
        if check_event(&event) {
          return Ok(event); // Found the event we want
        }
        // Event received, but not the one we're looking for, continue loop
      }
      Ok(Err(_recv_err)) => {
        // Channel closed
        return Err("Monitor channel closed unexpectedly".to_string());
      }
      Err(_elapsed) => { // Timeout for this recv attempt
         // Continue loop to check overall timeout
      }
    }
  }
}

// --- Revised Setup Function ---
async fn setup_push_pull(ctx: &Context) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  // *** SET HIGH HWM OPTIONS FOR BENCHMARK ***
  println!(
    "Benchmark Setup: Setting SNDHWM={} on PUSH and RCVHWM={} on PULL",
    BENCH_HWM, BENCH_HWM
  );
  push.set_option(SNDHWM, &BENCH_HWM.to_ne_bytes()).await?;
  pull.set_option(RCVHWM, &BENCH_HWM.to_ne_bytes()).await?;

  // --- Setup Monitoring ---
  // Monitor the PULL socket to know when PUSH connects successfully
  let pull_monitor = pull.monitor_default().await?;
  println!("Benchmark Setup: PULL monitor created.");

  // --- Bind PULL ---
  pull.bind(BIND_ADDR).await?;
  println!("Benchmark Setup: PULL bound.");
  // Wait specifically for the Listening event on PULL
  wait_for_event(
    &pull_monitor,
    |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == BIND_ADDR),
  )
  .await
  .map_err(|e| ZmqError::Internal(format!("PULL Listening event error: {}", e)))?;
  println!("Benchmark Setup: PULL Listening event confirmed.");

  // --- Connect PUSH ---
  push.connect(BIND_ADDR).await?;
  println!("Benchmark Setup: PUSH connect initiated.");

  // --- Wait for Connection Confirmation on PULL's Monitor ---
  // We wait for the PULL socket (the listener) to report either Accepted
  // or HandshakeSucceeded, which indicates the connection from PUSH
  // is established and the ZMTP handshake is done.
  println!("Benchmark Setup: Waiting for connection confirmation on PULL monitor...");
  wait_for_event(&pull_monitor, |e| {
    matches!(e, SocketEvent::Accepted { .. } | SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .map_err(|e| ZmqError::Internal(format!("PULL Connection event error: {}", e)))?;
  println!("Benchmark Setup: PULL Connection confirmed via monitor.");

  // Short delay *after* confirmation might still be useful for pipe attachment, though less critical now.
  sleep(Duration::from_millis(20)).await;

  println!("Benchmark Setup: Sockets ready.");
  Ok((push, pull))
}

// --- Benchmark Function (using the revised setup) ---
fn push_pull_tcp_throughput(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime");
  let mut group = c.benchmark_group("PUSH_PULL_TCP_Throughput");

  for size in [16, 256, 1024, 4096, 16384].iter() {
    group.throughput(Throughput::Bytes((NUM_MESSAGES * size) as u64));
    let bench_id = BenchmarkId::from_parameter(format!("{}B", size));

    group.bench_with_input(bench_id, size, |b, &msg_size| {
      b.to_async(&rt).iter_custom(|iters| async move {
        // Add iters argument
        println!("\n--- Starting Benchmark Iteration ({} iters planned) ---", iters); // Log start of iter_custom

        let ctx = Context::new().expect("Bench context creation failed");
        println!("[Iter] Context created.");

        let (push, pull) = match timeout(SETUP_TIMEOUT, setup_push_pull(&ctx)).await {
          Ok(Ok(sockets)) => sockets,
          Ok(Err(e)) => panic!("Bench socket setup failed: {}", e),
          Err(_) => panic!("Bench socket setup timed out overall"),
        };
        println!("[Iter] Sockets setup complete.");

        let message_payload = vec![0u8; msg_size];
        println!("[Iter] Payload allocated ({} bytes).", msg_size);

        let start = Instant::now();
        println!("[Iter] Timer started. Spawning tasks...");

        let sender_task = {
          let push = push.clone();
          let payload = message_payload.clone();
          tokio::spawn(async move {
            println!("[Sender Task] Started.");
            for i in 0..NUM_MESSAGES {
              let msg = Msg::from_vec(black_box(payload.clone()));
              if let Err(e) = push.send(msg).await {
                println!("[Sender Task] Error sending msg {}: {}", i, e);
                return Err(e);
              }
              if (i + 1) % (NUM_MESSAGES / 10).max(1) == 0 {
                // Log progress
                println!("[Sender Task] Sent {}/{}", i + 1, NUM_MESSAGES);
              }
            }
            println!("[Sender Task] Finished sending {} messages.", NUM_MESSAGES);
            Ok(())
          })
        };

        let receiver_task = {
          let pull = pull.clone();
          tokio::spawn(async move {
            println!("[Receiver Task] Started.");
            for i in 0..NUM_MESSAGES {
              match pull.recv().await {
                Ok(msg) => {
                  black_box(msg.data());
                  if (i + 1) % (NUM_MESSAGES / 10).max(1) == 0 {
                    // Log progress
                    println!("[Receiver Task] Received {}/{}", i + 1, NUM_MESSAGES);
                  }
                }
                Err(ZmqError::InvalidState(ref s)) if s == &"Socket terminated" => {
                  println!(
                    "[Receiver Task] Tolerating 'Socket terminated' error on iter {}/{}",
                    i + 1,
                    NUM_MESSAGES
                  );
                  return Ok(i);
                }
                Err(e) => {
                  println!("[Receiver Task] Error receiving msg {}: {}", i, e);
                  return Err(e);
                }
              }
            }
            println!("[Receiver Task] Finished receiving {} messages.", NUM_MESSAGES);
            Ok(NUM_MESSAGES)
          })
        };

        println!("[Iter] Tasks spawned. Awaiting sender...");
        let send_result = sender_task.await.expect("Sender task panicked");
        println!("[Iter] Sender task awaited. Result: {:?}", send_result);

        println!("[Iter] Awaiting receiver...");
        let recv_result = receiver_task.await.expect("Receiver task panicked");
        println!("[Iter] Receiver task awaited. Result: {:?}", recv_result);

        let elapsed = start.elapsed();
        println!("[Iter] Timer stopped. Elapsed: {:?}", elapsed);

        // --- ADD DELAY BEFORE TEARDOWN ---
        println!("[Iter] Send/Receive complete. Waiting before teardown...");
        tokio::time::sleep(Duration::from_millis(100)).await; // Wait 100ms
        println!("[Iter] Proceeding with teardown...");
        // --- END ADDED DELAY ---

        // Teardown
        println!("[Iter] Closing PUSH explicitly...");
        if let Err(e) = push.close().await {
          eprintln!("[Iter] Warning: Error closing PUSH: {}", e);
        }
        println!("[Iter] PUSH close returned.");

        println!("[Iter] Closing PULL explicitly...");
        if let Err(e) = pull.close().await {
          eprintln!("[Iter] Warning: Error closing PULL: {}", e);
        }
        println!("[Iter] PULL close returned.");

        println!("[Iter] Terminating context...");
        ctx.term().await.expect("Context termination failed");
        println!("[Iter] Context terminated.");

        if let Err(e) = send_result {
          panic!("Benchmark sender task failed: {}", e);
        }
        match recv_result {
          Ok(count) if count < NUM_MESSAGES => {
            eprintln!("[Iter] Warning: Receiver task finished early ({} msgs)", count)
          }
          Err(e) => panic!("Benchmark receiver task failed: {}", e),
          _ => {}
        }

        println!("[Iter] Iteration finished. Returning elapsed time.");
        elapsed
      });
    });
  }
  group.finish();
}

criterion_group!(benches, push_pull_tcp_throughput);
criterion_main!(benches);
