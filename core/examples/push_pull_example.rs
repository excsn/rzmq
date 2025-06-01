use futures::future::join_all;
use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;
use tokio::time::sleep;

const PUSH_PULL_ADDR: &str = "tcp://127.0.0.1:5557";
const NUM_WORKERS: usize = 3;
const NUM_TASKS_TOTAL: usize = 10;

async fn run_pusher_ventilator(ctx: Context) -> Result<(), ZmqError> {
  let push_socket = ctx.socket(SocketType::Push)?;
  println!("[PUSH] Binding to {}...", PUSH_PULL_ADDR);
  push_socket.bind(PUSH_PULL_ADDR).await?;
  println!("[PUSH] Bound. Waiting for workers to connect...");

  // Give workers time to connect
  sleep(Duration::from_secs(1)).await;

  println!("[PUSH] Sending tasks to workers...");
  for i in 0..NUM_TASKS_TOTAL {
    let task_load = i % 100; // Simulate varying task loads
    let message_content = format!("Task_{}_Load_{}", i, task_load);
    push_socket
      .send(Msg::from_vec(message_content.clone().into_bytes()))
      .await?;
    println!("[PUSH] Sent: '{}'", message_content);
    if task_load == 0 {
      // Send a "batch end" signal occasionally (example pattern)
      sleep(Duration::from_millis(100)).await;
    }
  }

  // Send a final "END" signal to all workers
  for _ in 0..NUM_WORKERS {
    // Send one END for each worker for this simple pattern
    push_socket.send(Msg::from_static(b"CONTROL:END")).await?;
  }
  println!("[PUSH] Sent all tasks and END signals. Closing socket.");
  push_socket.close().await?;
  Ok(())
}

async fn run_pull_worker(ctx: Context, worker_id: usize) -> Result<(), ZmqError> {
  let pull_socket = ctx.socket(SocketType::Pull)?;
  println!("[PULL Worker {}] Connecting to {}...", worker_id, PUSH_PULL_ADDR);
  pull_socket.connect(PUSH_PULL_ADDR).await?;
  println!("[PULL Worker {}] Connected.", worker_id);

  loop {
    let received_msg = pull_socket.recv().await?;
    let message_str = String::from_utf8_lossy(received_msg.data().unwrap_or_default());

    if message_str == "CONTROL:END" {
      println!("[PULL Worker {}] Received END signal. Exiting.", worker_id);
      break;
    }
    println!("[PULL Worker {}] Received: '{}'", worker_id, message_str);

    // Simulate work based on "load"
    if let Some(load_str) = message_str.split_once("_Load_") {
      if let Ok(load_val) = load_str.1.parse::<u64>() {
        sleep(Duration::from_millis(load_val * 5)).await; // Work
      }
    }
    println!("[PULL Worker {}] Finished processing: '{}'", worker_id, message_str);
  }

  println!("[PULL Worker {}] Closing socket.", worker_id);
  pull_socket.close().await?;
  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

  println!("--- Push-Pull Example ---");
  let ctx = Context::new()?;

  let pusher_ctx = ctx.clone();
  let pusher_handle = tokio::spawn(async move {
    if let Err(e) = run_pusher_ventilator(pusher_ctx).await {
      eprintln!("[PUSH] Error: {}", e);
    }
  });

  // Give pusher time to bind
  sleep(Duration::from_millis(200)).await;

  let mut worker_handles = Vec::new();
  for i in 0..NUM_WORKERS {
    let worker_ctx = ctx.clone();
    let handle = tokio::spawn(async move {
      if let Err(e) = run_pull_worker(worker_ctx, i).await {
        eprintln!("[PULL Worker {}] Error: {}", i, e);
      }
    });
    worker_handles.push(handle);
  }

  // Wait for tasks to complete
  let _ = pusher_handle.await; // Wait for pusher first
  println!("[Main] Pusher task finished. Waiting for workers...");
  join_all(worker_handles).await; // Wait for all workers

  println!("[Main] Terminating context...");
  ctx.term().await?;
  println!("--- Push-Pull Example Finished ---");
  Ok(())
}
