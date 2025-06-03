// tests/multishot_receive.rs

// These tests are specifically for the io_uring multishot receive feature.
// They should only be compiled and run when 'io-uring' feature is enabled and on Linux.
#![cfg(all(target_os = "linux", feature = "io-uring"))]

use rzmq::{
  socket::options::{
    IO_URING_RCVMULTISHOT, // Boolean toggle for the feature
    RCVHWM,
    SNDHWM, // For general socket behavior
  },
  Context, Msg, MsgFlags, SocketType, ZmqError,
};
use std::time::Duration;

// Assuming a common module for test helpers if available
mod common; // For common::test_context() and common::recv_timeout_custom_socket()

const TEST_ENDPOINT_BASE: &str = "tcp://127.0.0.1";
const DEFAULT_CONNECT_DELAY: Duration = Duration::from_millis(150);
const DEFAULT_RECV_TIMEOUT: Duration = Duration::from_secs(3);
const POST_SEND_DELAY: Duration = Duration::from_millis(100); // Allow receiver to process

// Helper to create a PUSH (sender) and PULL (receiver) pair for testing receive.
// The PULL socket will be configured for multishot receive.
async fn setup_push_pull_for_multishot_recv(
  ctx: &Context,
  port: u16,
  buffer_count: usize,
  buffer_size: usize,
) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let push_socket = ctx.socket(SocketType::Push)?;
  let pull_socket = ctx.socket(SocketType::Pull)?;

  let endpoint = format!("{}:{}", TEST_ENDPOINT_BASE, port);

  // Configure PULL socket for multishot receive
  println!(
    "Configuring PULL socket for multishot: count={}, size={}, endpoint={}",
    buffer_count, buffer_size, endpoint
  );
  pull_socket
    .set_option(IO_URING_RCVMULTISHOT, &(1i32).to_ne_bytes()) // Enable multishot
    .await?;
  // pull_socket
  //   .set_option(IO_URING_RECV_BUFFER_COUNT, &(buffer_count as i32).to_ne_bytes())
  //   .await?;
  // pull_socket
  //   .set_option(IO_URING_RECV_BUFFER_SIZE, &(buffer_size as i32).to_ne_bytes())
  //   .await?;
  // Set a reasonable RCVHWM for the PULL socket's API-level queue,
  // though multishot buffer pool size is the primary receive buffer.
  pull_socket
    .set_option(RCVHWM, &((buffer_count * 2) as i32).to_ne_bytes())
    .await?;

  // Configure PUSH socket
  push_socket
    .set_option(SNDHWM, &((buffer_count * 2) as i32).to_ne_bytes())
    .await?;

  pull_socket.bind(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind to complete

  push_socket.connect(&endpoint).await?;
  tokio::time::sleep(DEFAULT_CONNECT_DELAY).await; // Allow connection & handshake

  Ok((push_socket, pull_socket))
}

#[tokio::test]
async fn test_multishot_recv_single_small_message() -> Result<(), ZmqError> {
  println!("\n--- test_multishot_recv_single_small_message ---");
  let ctx = common::test_context();
  // Use small buffers to see interaction if message is larger, but for this test, msg is smaller.
  let (push, pull) = setup_push_pull_for_multishot_recv(&ctx, 6000, 4, 1024).await?;

  let message_data = b"HelloMultishot!";
  println!("PUSH: Sending '{}'", String::from_utf8_lossy(message_data));
  push.send(Msg::from_static(message_data)).await?;
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("PULL (Multishot): Receiving...");
  let received_msg = common::recv_timeout_custom_socket(&pull, DEFAULT_RECV_TIMEOUT).await?;
  println!(
    "PULL (Multishot): Received '{}'",
    String::from_utf8_lossy(received_msg.data().unwrap_or_default())
  );

  assert_eq!(received_msg.data().unwrap_or_default(), message_data);
  assert!(!received_msg.is_more());

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_multishot_recv_multiple_small_messages() -> Result<(), ZmqError> {
  println!("\n--- test_multishot_recv_multiple_small_messages ---");
  let ctx = common::test_context();
  // Small buffers, multiple messages might fit in one uring buffer or span them.
  let (push, pull) = setup_push_pull_for_multishot_recv(&ctx, 6001, 4, 128).await?;

  let messages_to_send = vec![
    Msg::from_static(b"Msg1_Multi"),
    Msg::from_static(b"Msg2_Multi"),
    Msg::from_static(b"Msg3_Multi_LongerOne"),
  ];
  let num_messages = messages_to_send.len();

  for (i, msg) in messages_to_send.iter().enumerate() {
    println!("PUSH: Sending message #{}", i + 1);
    push.send(msg.clone()).await?; // Clone since we assert against original
                                   // No delay between sends to see if multishot handles rapid succession
  }
  tokio::time::sleep(POST_SEND_DELAY * 2).await; // Allow all to be processed

  for i in 0..num_messages {
    println!("PULL (Multishot): Receiving message #{}...", i + 1);
    let received_msg = common::recv_timeout_custom_socket(&pull, DEFAULT_RECV_TIMEOUT)
      .await
      .map_err(|e| {
        eprintln!("PULL Error receiving message #{}: {:?}", i + 1, e);
        e
      })?;
    assert_eq!(
      received_msg.data().unwrap_or_default(),
      messages_to_send[i].data().unwrap_or_default()
    );
    assert!(!received_msg.is_more());
    println!(
      "PULL (Multishot): Received '{}'",
      String::from_utf8_lossy(received_msg.data().unwrap_or_default())
    );
  }

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_multishot_recv_message_spanning_uring_buffers() -> Result<(), ZmqError> {
  println!("\n--- test_multishot_recv_message_spanning_uring_buffers ---");
  let ctx = common::test_context();
  // Very small buffer size to force a single ZMTP message (header + payload)
  // to span multiple physical io_uring buffer completions.
  // ZMTP header can be up to 9 bytes. Payload will be larger.
  let buffer_count = 8;
  let buffer_size = 16; // Intentionally small
  let (push, pull) = setup_push_pull_for_multishot_recv(&ctx, 6002, buffer_count, buffer_size).await?;

  // Message larger than buffer_size, and likely larger than buffer_count * buffer_size
  // to test multiple multishot submission cycles for one ZMTP message.
  let large_payload = [b'A'; 100]; // 100 bytes. Header (2-9) + 100 = ~109 bytes.
                                   // 8 buffers * 16 bytes/buf = 128 bytes total pool. Should fit.
  let zmtp_message = Msg::from_slice(&large_payload);

  println!(
    "PUSH: Sending large message ({} bytes) with small uring buffers ({}x{}B)...",
    zmtp_message.size(),
    buffer_count,
    buffer_size
  );
  push.send(zmtp_message.clone()).await?;
  tokio::time::sleep(POST_SEND_DELAY * 2).await; // Generous time for processing

  println!("PULL (Multishot): Receiving large message...");
  let received_msg = common::recv_timeout_custom_socket(&pull, DEFAULT_RECV_TIMEOUT).await?;
  println!("PULL (Multishot): Received message of size {}", received_msg.size());

  assert_eq!(received_msg.data().unwrap_or_default(), &large_payload[..]);
  assert!(!received_msg.is_more());

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_multishot_recv_multi_part_zmtp_message() -> Result<(), ZmqError> {
  println!("\n--- test_multishot_recv_multi_part_zmtp_message ---");
  let ctx = common::test_context();
  let (push, pull) = setup_push_pull_for_multishot_recv(&ctx, 6003, 4, 1024).await?;

  let frame1_data = b"MultiPartFrameOne";
  let frame2_data = b"SecondFrameData";

  let mut msg1 = Msg::from_static(frame1_data);
  msg1.set_flags(MsgFlags::MORE);
  let msg2 = Msg::from_static(frame2_data);

  println!("PUSH: Sending multi-part ZMTP message...");
  push.send(msg1.clone()).await?;
  push.send(msg2.clone()).await?;
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("PULL (Multishot): Receiving part 1...");
  let rec1 = common::recv_timeout_custom_socket(&pull, DEFAULT_RECV_TIMEOUT).await?;
  assert_eq!(rec1.data().unwrap_or_default(), frame1_data);
  assert!(rec1.is_more(), "Expected MORE flag on first part");

  println!("PULL (Multishot): Receiving part 2...");
  let rec2 = common::recv_timeout_custom_socket(&pull, DEFAULT_RECV_TIMEOUT).await?;
  assert_eq!(rec2.data().unwrap_or_default(), frame2_data);
  assert!(!rec2.is_more(), "Expected NO MORE flag on second part");

  ctx.term().await?;
  Ok(())
}

// Test to ensure PING/PONG still works when multishot receive is active
// This implicitly tests that the engine can still send (PONGs) while also
// managing multishot receives.
#[tokio::test]
async fn test_multishot_recv_with_ping_pong_activity() -> Result<(), ZmqError> {
  println!("\n--- test_multishot_recv_with_ping_pong_activity ---");
  let ctx = common::test_context();
  let (push, pull) = setup_push_pull_for_multishot_recv(&ctx, 6004, 4, 1024).await?;

  // Configure PULL socket for fast heartbeats to trigger PONGs from its engine
  pull
    .set_option(rzmq::socket::options::HEARTBEAT_IVL, &(100i32).to_ne_bytes())
    .await?; // 100ms
  pull
    .set_option(rzmq::socket::options::HEARTBEAT_TIMEOUT, &(500i32).to_ne_bytes())
    .await?; // 500ms

  // Configure PUSH socket for fast heartbeats to send PINGs
  push
    .set_option(rzmq::socket::options::HEARTBEAT_IVL, &(100i32).to_ne_bytes())
    .await?;
  push
    .set_option(rzmq::socket::options::HEARTBEAT_TIMEOUT, &(500i32).to_ne_bytes())
    .await?;

  println!("PUSH: Sending initial message to establish full connection state...");
  push.send(Msg::from_static(b"Initial Sync")).await?;
  let _ = common::recv_timeout_custom_socket(&pull, DEFAULT_RECV_TIMEOUT).await?;
  println!("Initial message exchanged.");

  println!("Waiting for heartbeat activity (PINGs/PONGs)...");
  // PUSH engine should send PING to PULL. PULL engine (using multishot recv) should
  // see the PING, and its logic should send a PONG back.
  tokio::time::sleep(Duration::from_secs(1)).await; // Allow time for multiple heartbeats

  println!("PUSH: Sending final message to check if connection is still alive...");
  let final_msg_data = b"FinalCheck";
  push.send(Msg::from_static(final_msg_data)).await?;
  tokio::time::sleep(POST_SEND_DELAY).await;

  let received_final = common::recv_timeout_custom_socket(&pull, DEFAULT_RECV_TIMEOUT).await?;
  assert_eq!(received_final.data().unwrap_or_default(), final_msg_data);
  println!("Final message exchanged successfully after heartbeat period.");

  ctx.term().await?;
  Ok(())
}
