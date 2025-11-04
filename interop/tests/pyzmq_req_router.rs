// interop/tests/pyzmq_req_router.rs

mod common;

use anyhow::Result;
use rzmq::{Msg, SocketType, ZmqError, context::context};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct ChildProcessGuard {
  child: Child,
}
impl Drop for ChildProcessGuard {
  fn drop(&mut self) {
    let _ = self.child.kill();
    let _ = self.child.wait();
  }
}

#[tokio::test]
async fn test_req_to_pyzmq_router() -> Result<()> {
  common::setup_logging();
  let endpoint = "tcp://127.0.0.1:65244";

  // Start the Python pyzmq ROUTER server. It will echo back what it receives.
  let mut cmd = Command::new("python3")
    .arg("python_scripts/router_server_for_req.py")
    .arg(endpoint)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;

  let stdout = cmd.stdout.take().expect("Failed to open stdout");
  let _guard = ChildProcessGuard { child: cmd };

  let mut reader = BufReader::new(stdout);
  let mut line = String::new();
  let mut is_ready = false;

  let start = std::time::Instant::now();
  while start.elapsed() < Duration::from_secs(5) {
    if reader.read_line(&mut line)? == 0 {
      break;
    }
    println!("[py-stdout] {}", line.trim());
    if line.contains("READY") {
      is_ready = true;
      break;
    }
    line.clear();
  }
  anyhow::ensure!(is_ready, "Python ROUTER server did not signal READY.");

  let ctx = context()?;
  let req_socket = ctx.socket(SocketType::Req)?;
  req_socket.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  let request_payload = b"hello world";
  tracing::info!(
    "rzmq REQ: Sending '{}'...",
    String::from_utf8_lossy(request_payload)
  );

  req_socket.send(Msg::from_static(request_payload)).await?;

  tracing::info!("rzmq REQ: Receiving reply...");

  let reply = req_socket.recv().await?;

  // The test asserts that the application receives only the payload.
  assert_eq!(
    reply.data().unwrap(),
    request_payload,
    "The received payload should match the sent payload."
  );
  assert!(
    !reply.is_more(),
    "The single reply frame should not have the MORE flag."
  );

  tracing::info!("SUCCESS: rzmq REQ socket correctly received only the payload from pyzmq ROUTER.");

  tracing::info!("rzmq REQ: Verifying that calling recv() again fails...");
  let second_recv_result = req_socket.recv().await;

  assert!(
    matches!(second_recv_result, Err(ZmqError::InvalidState(_))),
    "Expected InvalidState error on second recv(), but got {:?}",
    second_recv_result
  );

  tracing::info!("SUCCESS: recv() correctly returned InvalidState as expected.");

  Ok(())
}

// This test creates an rzmq ROUTER server and a pyzmq REQ client.
// It validates that the rzmq ROUTER can correctly receive from and reply to a libzmq REQ socket.
#[tokio::test]
async fn test_pyzmq_req_to_rzmq_router() -> Result<()> {
  common::setup_logging();
  let endpoint = "tcp://127.0.0.1:65245"; // New unique port for this test

  // 1. Setup Rust rzmq ROUTER server
  let ctx = context()?;
  let router_socket = ctx.socket(SocketType::Router)?;
  router_socket.bind(endpoint).await?;
  tracing::info!("rzmq ROUTER: Bound to {}", endpoint);
  tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind to settle

  // 2. Start Python pyzmq REQ client
  let mut cmd = Command::new("python3")
    .arg("python_scripts/req_client_for_router.py")
    .arg(endpoint)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;

  let stdout = cmd.stdout.take().expect("Failed to open stdout");
  let _guard = ChildProcessGuard { child: cmd };

  let mut reader = BufReader::new(stdout);
  let mut line = String::new();

  // Read the Python client's output to monitor its progress
  let mut client_sent = false;
  let start = std::time::Instant::now();
  while start.elapsed() < Duration::from_secs(5) {
    if reader.read_line(&mut line)? == 0 {
      break;
    }
    let trimmed = line.trim();
    println!("[py-stdout] {}", trimmed);
    if trimmed.contains("Request sent.") {
      client_sent = true;
      break; // Stop reading once we know the request is on its way
    }
    line.clear();
  }

  anyhow::ensure!(
    client_sent,
    "Python client did not confirm sending the request."
  );

  // 3. rzmq ROUTER receives the request
  tracing::info!("rzmq ROUTER: Waiting for request from pyzmq REQ...");
  let frames = router_socket.recv_multipart().await?;

  assert_eq!(
    frames.len(),
    2,
    "ROUTER should receive 2 frames (identity, payload)"
  );
  let identity = &frames[0];
  let payload = &frames[1];

  assert!(
    !identity.data().unwrap_or(&[1]).is_empty(),
    "Identity frame from ROUTER should not be empty"
  );
  assert_eq!(payload.data().unwrap(), b"hello from pyzmq req");
  tracing::info!("rzmq ROUTER: Correctly received identity and payload.");

  // 4. rzmq ROUTER sends a reply
  let reply_payload = b"reply from rzmq router";
  tracing::info!("rzmq ROUTER: Sending reply to pyzmq REQ...");

  // The application must now provide the full envelope for the ROUTER to send.
  let reply_frames = vec![
    identity.clone(),         // The routing frame (identity)
    Msg::from_static(reply_payload), // The payload
  ];
  router_socket.send_multipart(reply_frames).await?;

  tracing::info!("rzmq ROUTER: Reply sent. Waiting for Python client to confirm receipt...");

  let client_output_handle = tokio::task::spawn_blocking(move || -> std::io::Result<bool> {
    let mut client_received_success = false;
    let start_recv = std::time::Instant::now();
    let mut line = String::new(); // `line` must be moved into the closure

    while start_recv.elapsed() < Duration::from_secs(5) {
      // The `?` operator works here because the closure returns a Result
      if reader.read_line(&mut line)? == 0 {
        break; // EOF
      }
      let trimmed = line.trim();
      println!("[py-stdout] {}", trimmed);
      if trimmed.contains("SUCCESS - Received the expected reply.") {
        client_received_success = true;
        break;
      }
      line.clear();
    }
    Ok(client_received_success)
  });

  // Now, await the result from the blocking task.
  // The main async test task is free to yield while waiting.
  match client_output_handle.await {
    Ok(Ok(true)) => {
      // The blocking task completed successfully and found the success message.
      tracing::info!("Python client confirmed reply.");
      // The test can now pass.
    }
    Ok(Ok(false)) => {
      // The blocking task completed but timed out without finding the success message.
      panic!("Python client did not confirm receiving the correct reply.");
    }
    Ok(Err(io_error)) => {
      // The blocking task itself returned an I/O error.
      panic!("Failed to read Python client stdout: {}", io_error);
    }
    Err(join_error) => {
      // The blocking task panicked.
      panic!("Blocking task for reading client stdout panicked: {}", join_error);
    }
  }

  tracing::info!("SUCCESS: rzmq ROUTER successfully communicated with pyzmq REQ.");

  Ok(())
}
