use rzmq::{
  socket::options::{LAST_ENDPOINT, LINGER, RCVHWM, SNDHWM},
  Context,
  Msg,
  SocketType,
  ZmqError,
};
use std::time::Duration;
mod common;

#[rzmq_macros::test]
async fn test_option_last_endpoint_tcp_ephemeral_port() -> Result<(), ZmqError> {
  println!("\n--- Starting test_option_last_endpoint_tcp_ephemeral_port ---");
  let ctx = common::test_context();
  let pull_socket = ctx.socket(SocketType::Pull)?;

  let bind_uri_ephemeral = "tcp://127.0.0.1:0";
  println!("Binding PULL socket to ephemeral endpoint: {}", bind_uri_ephemeral);

  // 1. Bind to ephemeral port
  pull_socket.bind(bind_uri_ephemeral).await?;
  println!("PULL socket bound.");
  // Allow a moment for the bind to fully complete and the listener to be ready
  tokio::time::sleep(Duration::from_millis(50)).await;

  // 2. Get ZMQ_LAST_ENDPOINT
  println!("Getting ZMQ_LAST_ENDPOINT option...");
  let last_endpoint_bytes = pull_socket.get_option(LAST_ENDPOINT).await?;
  let last_endpoint_str = String::from_utf8(last_endpoint_bytes).expect("LAST_ENDPOINT should be valid UTF-8");
  println!("Received ZMQ_LAST_ENDPOINT: {}", last_endpoint_str);

  // 3. Verify the retrieved endpoint
  assert!(
    !last_endpoint_str.is_empty(),
    "LAST_ENDPOINT should not be empty after bind."
  );
  assert!(
    last_endpoint_str.starts_with("tcp://127.0.0.1:"),
    "LAST_ENDPOINT format is incorrect for TCP IPv4."
  );

  let parts: Vec<&str> = last_endpoint_str.split(':').collect();
  assert_eq!(parts.len(), 3, "LAST_ENDPOINT should have 3 parts (tcp:ip:port)");
  let port_str = parts[2];
  let port_num: u16 = port_str.parse().expect("Port should be a number");
  assert_ne!(
    port_num, 0,
    "Port number should not be 0 after binding to ephemeral port."
  );
  println!("LAST_ENDPOINT successfully resolved to port: {}", port_num);

  // 4. Optional: Test connectivity to the resolved endpoint
  let push_socket = ctx.socket(SocketType::Push)?;
  println!("Connecting PUSH socket to resolved endpoint: {}", last_endpoint_str);
  push_socket.connect(&last_endpoint_str).await?;
  // Allow some time for the connection to establish
  tokio::time::sleep(Duration::from_millis(100)).await;
  println!("PUSH socket connected.");

  let test_msg_data = b"Hello to resolved endpoint!";
  println!("PUSH sending message...");
  push_socket.send(Msg::from_static(test_msg_data)).await?;
  println!("PUSH message sent.");

  println!("PULL receiving message...");
  let received_msg = common::recv_timeout(&pull_socket, Duration::from_secs(1)).await?;
  assert_eq!(received_msg.data().unwrap(), test_msg_data);
  println!("PULL received message successfully from resolved endpoint.");

  // Test getting LAST_ENDPOINT before any bind
  let new_socket = ctx.socket(SocketType::Pull)?;
  let last_endpoint_before_bind_bytes = new_socket.get_option(LAST_ENDPOINT).await?;
  let last_endpoint_before_bind_str =
    String::from_utf8(last_endpoint_before_bind_bytes).expect("LAST_ENDPOINT should be valid UTF-8");
  assert_eq!(
    last_endpoint_before_bind_str, "",
    "LAST_ENDPOINT should be an empty string before any bind"
  );
  println!("LAST_ENDPOINT is correctly empty for a new socket.");

  // Clean up
  println!("Terminating context...");
  ctx.term().await?;
  println!("--- Test test_option_last_endpoint_tcp_ephemeral_port Finished ---");
  Ok(())
}

#[rzmq_macros::test]
async fn test_option_last_endpoint_ipc() -> Result<(), ZmqError> {
  #[cfg(not(feature = "ipc"))]
  {
    println!("Skipping IPC test: 'ipc' feature not enabled.");
    return Ok(());
  }

  println!("\n--- Starting test_option_last_endpoint_ipc ---");
  let ctx = common::test_context();
  let pull_socket = ctx.socket(SocketType::Pull)?;

  let ipc_path = common::unique_ipc_endpoint(); // This returns "ipc:///tmp/rzmq_test_..."
  println!("Binding PULL socket to IPC endpoint: {}", ipc_path);

  // 1. Bind to IPC endpoint
  pull_socket.bind(&ipc_path).await?;
  println!("PULL socket bound to IPC.");
  tokio::time::sleep(Duration::from_millis(50)).await;

  // 2. Get ZMQ_LAST_ENDPOINT
  println!("Getting ZMQ_LAST_ENDPOINT option for IPC...");
  let last_endpoint_bytes = pull_socket.get_option(LAST_ENDPOINT).await?;
  let last_endpoint_str = String::from_utf8(last_endpoint_bytes).expect("LAST_ENDPOINT should be valid UTF-8");
  println!("Received ZMQ_LAST_ENDPOINT (IPC): {}", last_endpoint_str);

  // 3. Verify the retrieved endpoint
  assert_eq!(
    last_endpoint_str, ipc_path,
    "LAST_ENDPOINT for IPC should match the bind path."
  );

  // 4. Optional: Test connectivity
  let push_socket = ctx.socket(SocketType::Push)?;
  println!("Connecting PUSH socket to IPC endpoint: {}", last_endpoint_str);
  push_socket.connect(&last_endpoint_str).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;
  println!("PUSH socket connected to IPC.");

  let test_msg_data = b"Hello IPC!";
  println!("PUSH sending IPC message...");
  push_socket.send(Msg::from_static(test_msg_data)).await?;
  println!("PUSH IPC message sent.");

  println!("PULL receiving IPC message...");
  let received_msg = common::recv_timeout(&pull_socket, Duration::from_secs(1)).await?;
  assert_eq!(received_msg.data().unwrap(), test_msg_data);
  println!("PULL received IPC message successfully.");

  // Clean up
  println!("Terminating context for IPC test...");
  ctx.term().await?;
  println!("--- Test test_option_last_endpoint_ipc Finished ---");
  Ok(())
}

#[rzmq_macros::test]
async fn test_option_last_endpoint_inproc() -> Result<(), ZmqError> {
  #[cfg(not(feature = "inproc"))]
  {
    println!("Skipping inproc test: 'inproc' feature not enabled.");
    return Ok(());
  }
  println!("\n--- Starting test_option_last_endpoint_inproc ---");
  let ctx = common::test_context();
  let pull_socket = ctx.socket(SocketType::Pull)?;

  let inproc_name = common::unique_inproc_endpoint(); // This returns "inproc://rzmq_test_..."
  println!("Binding PULL socket to inproc endpoint: {}", inproc_name);

  // 1. Bind to inproc endpoint
  pull_socket.bind(&inproc_name).await?;
  println!("PULL socket bound to inproc.");
  // Inproc binding is usually very fast, but a small yield might be good practice if issues arise.
  tokio::task::yield_now().await;

  // 2. Get ZMQ_LAST_ENDPOINT
  println!("Getting ZMQ_LAST_ENDPOINT option for inproc...");
  let last_endpoint_bytes = pull_socket.get_option(LAST_ENDPOINT).await?;
  let last_endpoint_str = String::from_utf8(last_endpoint_bytes).expect("LAST_ENDPOINT should be valid UTF-8");
  println!("Received ZMQ_LAST_ENDPOINT (inproc): {}", last_endpoint_str);

  // 3. Verify the retrieved endpoint
  assert_eq!(
    last_endpoint_str, inproc_name,
    "LAST_ENDPOINT for inproc should match the bind name."
  );

  // 4. Optional: Test connectivity
  let push_socket = ctx.socket(SocketType::Push)?;
  println!("Connecting PUSH socket to inproc endpoint: {}", last_endpoint_str);
  push_socket.connect(&last_endpoint_str).await?;
  tokio::time::sleep(Duration::from_millis(20)).await; // inproc connect is also quick
  println!("PUSH socket connected to inproc.");

  let test_msg_data = b"Hello Inproc!";
  println!("PUSH sending inproc message...");
  push_socket.send(Msg::from_static(test_msg_data)).await?;
  println!("PUSH inproc message sent.");

  println!("PULL receiving inproc message...");
  let received_msg = common::recv_timeout(&pull_socket, Duration::from_secs(1)).await?;
  assert_eq!(received_msg.data().unwrap(), test_msg_data);
  println!("PULL received inproc message successfully.");

  // Clean up
  println!("Terminating context for inproc test...");
  ctx.term().await?;
  println!("--- Test test_option_last_endpoint_inproc Finished ---");
  Ok(())
}
