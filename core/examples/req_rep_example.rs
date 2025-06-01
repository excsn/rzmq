// examples/req_rep_example.rs

use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;
use tokio::time::sleep;

const REP_ADDR: &str = "tcp://127.0.0.1:5555";
const NUM_REQUESTS: usize = 5;

async fn run_rep_server(ctx: Context) -> Result<(), ZmqError> {
  let rep_socket = ctx.socket(SocketType::Rep)?;
  println!("[REP] Binding to {}...", REP_ADDR);
  rep_socket.bind(REP_ADDR).await?;
  println!("[REP] Bound and waiting for requests...");

  for i in 0..NUM_REQUESTS {
    let received_msg = rep_socket.recv().await?;
    let request_str = String::from_utf8_lossy(received_msg.data().unwrap_or_default());
    println!("[REP] Received request ({}): '{}'", i, request_str);

    sleep(Duration::from_millis(100)).await; // Simulate work

    let reply_str = format!("World_{}", i);
    rep_socket.send(Msg::from_vec(reply_str.as_bytes().to_vec())).await?;
    println!("[REP] Sent reply: '{}'", reply_str);
  }

  println!("[REP] Processed all requests. Closing socket.");
  rep_socket.close().await?;
  Ok(())
}

async fn run_req_client(ctx: Context) -> Result<(), ZmqError> {
  let req_socket = ctx.socket(SocketType::Req)?;
  println!("[REQ] Connecting to {}...", REP_ADDR);
  req_socket.connect(REP_ADDR).await?;
  println!("[REQ] Connected.");

  for i in 0..NUM_REQUESTS {
    let request_str = format!("Hello_{}", i);
    println!("[REQ] Sending request ({}): '{}'", i, request_str);
    req_socket.send(Msg::from_vec(request_str.into_bytes())).await?;

    let reply_msg = req_socket.recv().await?;
    let reply_str = String::from_utf8_lossy(reply_msg.data().unwrap_or_default());
    println!("[REQ] Received reply: '{}'", reply_str);

    sleep(Duration::from_millis(500)).await; // Wait before next request
  }

  println!("[REQ] Sent all requests. Closing socket.");
  req_socket.close().await?;
  Ok(())
}

#[tokio::main] // Or #[tokio::main]
async fn main() -> Result<(), ZmqError> {
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO) // Use INFO for examples
    .init();

  println!("--- Request-Reply Example ---");
  let ctx = Context::new()?;

  let server_ctx = ctx.clone();
  let server_handle = tokio::spawn(async move {
    if let Err(e) = run_rep_server(server_ctx).await {
      eprintln!("[REP] Error: {}", e);
    }
  });

  // Give server a moment to bind
  sleep(Duration::from_millis(200)).await;

  let client_ctx = ctx.clone();
  let client_handle = tokio::spawn(async move {
    if let Err(e) = run_req_client(client_ctx).await {
      eprintln!("[REQ] Error: {}", e);
    }
  });

  // Wait for tasks to complete
  let _ = tokio::try_join!(server_handle, client_handle);

  println!("[Main] Terminating context...");
  ctx.term().await?;
  println!("--- Request-Reply Example Finished ---");
  Ok(())
}
