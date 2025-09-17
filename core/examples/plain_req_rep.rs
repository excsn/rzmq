use rzmq::{
  socket::{LINGER, PLAIN_PASSWORD, PLAIN_SERVER, PLAIN_USERNAME},
  Context, Msg, SocketType, ZmqError,
};
use std::time::Duration;

// Helper to print message parts for diagnostics
fn print_msg_parts(msg_type: &str, msg: &Msg) {
  println!(
    "  {}: '{}' (Size: {}, More: {})",
    msg_type,
    String::from_utf8_lossy(msg.data().unwrap_or_default()),
    msg.size(),
    msg.is_more()
  );
}

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  // --- Configuration ---
  let bind_addr = "tcp://127.0.0.1:5555";
  let username = "testuser";
  let password = "testpassword";

  println!("Starting PLAIN REQ-REP example...");
  println!("Server will bind to: {}", bind_addr);
  println!("Client will connect with user: '{}', pass: '{}'", username, password);

  // 1. Create Context
  let ctx = match Context::new() {
    Ok(c) => c,
    Err(e) => {
      eprintln!("Failed to create rzmq context: {}", e);
      return Err(e);
    }
  };

  // --- Server (REP) Setup ---
  let server_socket = ctx.socket(SocketType::Rep)?;
  println!("[Server] Setting LINGER option...");
  server_socket.set_option(LINGER, 0i32).await?; // Set LINGER option to 0 for quick close
  println!("[Server] Setting PLAIN_SERVER option to true...");
  server_socket.set_option(PLAIN_SERVER, true).await?; // Enable PLAIN server role
  println!("[Server] Binding to {}...", bind_addr);
  server_socket.bind(bind_addr).await?;
  println!("[Server] Bound successfully. Waiting for requests...");

  // --- Client (REQ) Setup ---
  let client_socket = ctx.socket(SocketType::Req)?;
  println!("[Client] Setting LINGER option...");
  client_socket.set_option(LINGER, 0i32).await?; // Set LINGER option to 0 for quick close
  println!("[Client] Setting PLAIN_USERNAME to '{}'...", username);
  client_socket
    .set_option(PLAIN_USERNAME, username)
    .await?;
  println!("[Client] Setting PLAIN_PASSWORD to '{}'...", password);
  client_socket
    .set_option(PLAIN_PASSWORD, password)
    .await?;
  println!("[Client] Connecting to {}...", bind_addr);
  client_socket.connect(bind_addr).await?;
  println!("[Client] Connected successfully.");

  // --- Communication Tasks ---

  // Server Task
  let server_task = tokio::spawn(async move {
    println!("[Server] Task started. Receiving request...");
    match server_socket.recv().await {
      Ok(request_msg) => {
        print_msg_parts("[Server] Received Request", &request_msg);
        assert_eq!(request_msg.data().unwrap_or_default(), b"Hello from REQ");

        let reply_data = "Reply from REP";
        println!("[Server] Sending reply: '{}'", reply_data);
        let reply_msg = Msg::from_static(reply_data.as_bytes());
        if let Err(e) = server_socket.send(reply_msg).await {
          eprintln!("[Server] Error sending reply: {}", e);
        } else {
          println!("[Server] Reply sent.");
        }
      }
      Err(e) => {
        eprintln!("[Server] Error receiving request: {}", e);
      }
    }
    println!("[Server] Closing server socket.");
    if let Err(e) = server_socket.close().await {
      eprintln!("[Server] Error closing server socket: {}", e);
    }
    println!("[Server] Task finished.");
  });

  // Client Task
  let client_task = tokio::spawn(async move {
    // Give server a moment to fully bind and be ready (especially in CI)
    tokio::time::sleep(Duration::from_millis(100)).await;

    let request_data = "Hello from REQ";
    println!("[Client] Task started. Sending request: '{}'", request_data);
    let request_msg = Msg::from_static(request_data.as_bytes());

    if let Err(e) = client_socket.send(request_msg).await {
      eprintln!("[Client] Error sending request: {}", e);
      if let Err(e_close) = client_socket.close().await {
        eprintln!("[Client] Error closing client socket after send error: {}", e_close);
      }
      return;
    }
    println!("[Client] Request sent. Receiving reply...");

    match client_socket.recv().await {
      Ok(reply_msg) => {
        print_msg_parts("[Client] Received Reply", &reply_msg);
        assert_eq!(reply_msg.data().unwrap_or_default(), b"Reply from REP");
        println!("[Client] Reply matches expected.");
      }
      Err(e) => {
        eprintln!("[Client] Error receiving reply: {}", e);
      }
    }
    println!("[Client] Closing client socket.");
    if let Err(e) = client_socket.close().await {
      eprintln!("[Client] Error closing client socket: {}", e);
    }
    println!("[Client] Task finished.");
  });

  // Wait for tasks to complete
  println!("Waiting for client and server tasks to complete...");
  if let Err(e) = client_task.await {
    eprintln!("Client task panicked or error joining: {:?}", e);
  }
  if let Err(e) = server_task.await {
    eprintln!("Server task panicked or error joining: {:?}", e);
  }
  println!("Client and server tasks completed.");

  // Terminate context
  println!("Terminating context...");
  ctx.term().await?;
  println!("Context terminated. Example finished successfully.");

  Ok(())
}
