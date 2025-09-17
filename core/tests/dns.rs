use rzmq::{
  socket::{
    options::{RCVTIMEO, RECONNECT_IVL}, SocketEvent, SNDTIMEO
  }, SocketType, ZmqError
};
use serial_test::serial;
use std::time::Duration;

// Import common test helpers
mod common;

const LONG_TIMEOUT: Duration = Duration::from_secs(5); // Generous timeout for tests involving network/DNS
const SHORT_TIMEOUT: Duration = Duration::from_millis(250);
const MONITOR_EVENT_TIMEOUT: Duration = Duration::from_secs(4);

/// Test connecting to a known valid hostname (`localhost`) that should always resolve.
#[tokio::test]
#[serial]
async fn test_dns_connect_to_localhost_succeeds() -> Result<(), ZmqError> {
  println!("\n--- Starting test_dns_connect_to_localhost_succeeds ---");
  let ctx = common::test_context();
  let server = ctx.socket(SocketType::Pull)?;
  let client = ctx.socket(SocketType::Push)?;

  let bind_endpoint = "tcp://127.0.0.1:5850"; // Bind to a specific IP
  let connect_endpoint = "tcp://localhost:5850"; // Connect to a hostname

  println!("SERVER binding to {}...", bind_endpoint);
  server.bind(bind_endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("CLIENT connecting to {}...", connect_endpoint);
  client.connect(connect_endpoint).await?;
  tokio::time::sleep(Duration::from_millis(150)).await; // Allow for connect + handshake

  println!("CLIENT sending message...");
  let msg_data = b"Hello via DNS";
  client.send(rzmq::Msg::from_static(msg_data)).await?;

  println!("SERVER receiving message...");
  let received = common::recv_timeout(&server, LONG_TIMEOUT).await?;
  assert_eq!(received.data().unwrap(), msg_data);
  println!("Message exchange successful.");

  ctx.term().await?;
  println!("--- Test Finished ---");
  Ok(())
}

/// Test attempting to connect to a hostname that is guaranteed not to exist.
/// This verifies that our `DnsResolutionFailed` error is correctly returned.
#[tokio::test]
#[serial]
async fn test_dns_connect_to_non_existent_host_fails() -> Result<(), ZmqError> {
  println!("\n--- Starting test_dns_connect_to_non_existent_host_fails ---");
  let ctx = common::test_context();
  let client = ctx.socket(SocketType::Push)?;

  // This is a reserved TLD that is guaranteed not to exist in public DNS.
  let non_existent_endpoint = "tcp://hostname.invalid:5851";

  // Set a short reconnect interval to speed up the failure detection process.
  client
    .set_option_raw(RECONNECT_IVL, &(100i32).to_ne_bytes())
    .await?;

  println!("CLIENT setting up monitor...");
  let monitor = client.monitor_default().await?;

  println!(
    "CLIENT connecting to non-existent host: {}",
    non_existent_endpoint
  );
  client.connect(non_existent_endpoint).await?; // connect() itself is async and just starts the process

  println!("CLIENT waiting for ConnectFailed monitor event...");
  let event = common::wait_for_monitor_event(&monitor, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::ConnectFailed { .. })
  })
  .await;

  assert!(
    event.is_ok(),
    "Expected to receive a ConnectFailed event from the monitor, but timed out or failed."
  );

  if let Ok(SocketEvent::ConnectFailed {
    endpoint,
    error_msg,
  }) = event
  {
    println!(
      "Received expected ConnectFailed event for endpoint '{}' with error: {}",
      endpoint, error_msg
    );
    assert_eq!(endpoint, non_existent_endpoint);
    // Check that the error message indicates a DNS or resolution failure.
    assert!(
      error_msg.contains("DNS resolution failed") || error_msg.contains("failed to resolve"),
      "Error message did not indicate a DNS failure. Got: {}",
      error_msg
    );
  } else {
    panic!("Received an unexpected event type: {:?}", event);
  }

  // Set SNDTIMEO=0 to make the subsequent send call non-blocking.
  // This prevents the test from hanging when the PUSH socket has no peers.
  println!("DEBUG: Setting SNDTIMEO=0 before final send check...");
  client
    .set_option_raw(SNDTIMEO, &(0i32).to_ne_bytes())
    .await?;

  // An attempt to send should also fail, though the monitor event is the primary check.
  let send_res = client.send(rzmq::Msg::from_static(b"wont_be_sent")).await;
  assert!(matches!(
    send_res,
    Err(ZmqError::ResourceLimitReached) // With SNDTIMEO=0, we expect ResourceLimitReached, not Timeout
  ));

  ctx.term().await?;
  println!("--- Test Finished ---");
  Ok(())
}

/// Test binding to a hostname (`localhost`) that resolves to a local IP.
#[tokio::test]
#[serial]
async fn test_dns_bind_to_localhost_succeeds() -> Result<(), ZmqError> {
  println!("\n--- Starting test_dns_bind_to_localhost_succeeds ---");
  let ctx = common::test_context();
  let server = ctx.socket(SocketType::Pull)?;
  let client = ctx.socket(SocketType::Push)?;

  // Bind to the hostname `localhost` on a specific port.
  let bind_endpoint_host = "tcp://localhost:5852";
  // The client will connect to the specific IP to confirm the bind worked as expected.
  let connect_endpoint_ip = "tcp://localhost:5852";

  println!("SERVER binding to hostname: {}", bind_endpoint_host);
  server.bind(bind_endpoint_host).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("CLIENT connecting to IP: {}", connect_endpoint_ip);
  client.connect(connect_endpoint_ip).await?;
  tokio::time::sleep(Duration::from_millis(150)).await;

  println!("CLIENT sending message...");
  let msg_data = b"Hello to bound hostname";
  client.send(rzmq::Msg::from_static(msg_data)).await?;

  println!("SERVER receiving message...");
  let received = common::recv_timeout(&server, LONG_TIMEOUT).await?;
  assert_eq!(received.data().unwrap(), msg_data);
  println!("Message exchange successful.");

  ctx.term().await?;
  println!("--- Test Finished ---");
  Ok(())
}

/// Test attempting to bind to a hostname that cannot be resolved.
#[tokio::test]
#[serial]
async fn test_dns_bind_to_non_existent_host_fails() -> Result<(), ZmqError> {
  println!("\n--- Starting test_dns_bind_to_non_existent_host_fails ---");
  let ctx = common::test_context();
  let server = ctx.socket(SocketType::Pull)?;

  let non_existent_endpoint = "tcp://hostname.invalid:5853";

  println!(
    "SERVER attempting to bind to non-existent host: {}",
    non_existent_endpoint
  );
  let bind_result = server.bind(non_existent_endpoint).await;

  println!("Bind result: {:?}", bind_result);
  assert!(
    matches!(bind_result, Err(ZmqError::DnsResolutionFailed(_))),
    "Expected DnsResolutionFailed error, but got: {:?}",
    bind_result
  );

  ctx.term().await?;
  println!("--- Test Finished ---");
  Ok(())
}
