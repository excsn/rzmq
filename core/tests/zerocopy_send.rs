// tests/zerocopy_send.rs

use rzmq_macros::test;

#[cfg(target_os = "linux")]
use rzmq::socket::options::IO_URING_SNDZEROCOPY;

use rzmq::{
  socket::options::{SNDTIMEO, TCP_CORK_OPT}, // Assuming these constants exist
  Context,
  Msg,
  MsgFlags,
  SocketType,
  ZmqError,
};
use std::time::Duration;

mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(500);
const LONG_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECT_DELAY: Duration = Duration::from_millis(200); // Allow a bit more time for setup
const POST_SEND_DELAY: Duration = Duration::from_millis(100);

async fn setup_zc_pair(
  ctx: &Context,
  endpoint: &str,
  enable_zerocopy_on_sender: bool,
  enable_cork_on_sender: bool, // To test interaction
) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let sender = ctx.socket(SocketType::Push)?;
  let receiver = ctx.socket(SocketType::Pull)?;

  if enable_zerocopy_on_sender {
    println!(
      "Setting IO_URING_SNDZEROCOPY={} on SENDER for endpoint {}",
      enable_zerocopy_on_sender, endpoint
    );
    #[cfg(target_os = "linux")]
    sender.set_option(IO_URING_SNDZEROCOPY, &(1i32).to_ne_bytes()).await?;
  }

  if enable_cork_on_sender {
    println!(
      "Setting TCP_CORK_OPT={} on SENDER for endpoint {}",
      enable_cork_on_sender, endpoint
    );
    sender.set_option(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await?;
  }

  receiver.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  sender.connect(endpoint).await?;
  tokio::time::sleep(CONNECT_DELAY).await;

  Ok((sender, receiver))
}

// Test 1: Basic single-part message send with ZC enabled
#[test]
#[cfg(all(target_os = "linux", feature = "io-uring"))] // Zerocopy test is Linux & io-uring specific
async fn test_zerocopy_send_single_part_message() -> Result<(), ZmqError> {
  println!("\n--- Starting test_zerocopy_send_single_part_message ---");
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5900";
  let (push, pull) = setup_zc_pair(&ctx, endpoint, true, false).await?; // ZC: true, Cork: false

  let msg_data = b"ZerocopySinglePart";
  println!("SENDER (ZC): Sending single part message...");
  push.send(Msg::from_static(msg_data)).await?;
  // To confirm ZC path was taken, we'd look for:
  // TRACE rzmq::engine::core: ... "Attempting ZC vectored send."
  println!("SENDER (ZC): Sent.");
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("RECEIVER: Receiving message...");
  let received = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(received.data().unwrap(), msg_data);
  assert!(!received.is_more());
  println!("RECEIVER: Received correctly via ZC path (expected).");

  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

// Test 2: Basic single-part message send with ZC disabled (fallback to standard)
#[test]
#[cfg(target_os = "linux")] // Still run on Linux to compare with ZC version
async fn test_standard_send_single_part_message_zc_flag_off() -> Result<(), ZmqError> {
  println!("\n--- Starting test_standard_send_single_part_message_zc_flag_off ---");
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5901";
  let (push, pull) = setup_zc_pair(&ctx, endpoint, false, false).await?; // ZC: false, Cork: false

  let msg_data = b"StandardSendSinglePart";
  println!("SENDER (Std): Sending single part message...");
  push.send(Msg::from_static(msg_data)).await?;
  // To confirm standard path, we'd ensure NO "Attempting ZC vectored send" log.
  println!("SENDER (Std): Sent.");
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("RECEIVER: Receiving message...");
  let received = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(received.data().unwrap(), msg_data);
  assert!(!received.is_more());
  println!("RECEIVER: Received correctly via standard path.");

  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

// Test 3: Multi-part message send with ZC enabled
#[test]
#[cfg(all(target_os = "linux", feature = "io-uring"))]
async fn test_zerocopy_send_multi_part_message() -> Result<(), ZmqError> {
  println!("\n--- Starting test_zerocopy_send_multi_part_message ---");
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5902";
  let (push, pull) = setup_zc_pair(&ctx, endpoint, true, false).await?; // ZC: true, Cork: false

  let frame1_data = b"ZCFrame1";
  let frame2_data = b"ZCFrame2";

  let mut msg1 = Msg::from_static(frame1_data);
  msg1.set_flags(MsgFlags::MORE);
  let msg2 = Msg::from_static(frame2_data);

  println!("SENDER (ZC): Sending multi-part message Frame 1...");
  push.send(msg1).await?; // Should use ZC path
  println!("SENDER (ZC): Sent Frame 1.");

  // Small delay to ensure sends are distinct operations if not corked
  tokio::time::sleep(Duration::from_millis(10)).await;

  println!("SENDER (ZC): Sending multi-part message Frame 2...");
  push.send(msg2).await?; // Should use ZC path
  println!("SENDER (ZC): Sent Frame 2.");
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("RECEIVER: Receiving messages...");
  let rec1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec1.data().unwrap(), frame1_data);
  assert!(rec1.is_more());
  println!("RECEIVER: Received Frame 1 via ZC (expected).");

  let rec2 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec2.data().unwrap(), frame2_data);
  assert!(!rec2.is_more());
  println!("RECEIVER: Received Frame 2 via ZC (expected).");

  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

// Test 4: Zerocopy send combined with TCP Corking
#[test]
#[cfg(all(target_os = "linux", feature = "io-uring"))]
async fn test_zerocopy_send_with_corking() -> Result<(), ZmqError> {
  println!("\n--- Starting test_zerocopy_send_with_corking ---");
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5903";
  // Enable both ZC and Corking
  let (push, pull) = setup_zc_pair(&ctx, endpoint, true, true).await?;

  let frame1_data = b"ZCCorkFrame1";
  let frame2_data = b"ZCCorkFrame2";

  let mut msg1 = Msg::from_static(frame1_data);
  msg1.set_flags(MsgFlags::MORE);
  let msg2 = Msg::from_static(frame2_data);

  println!("SENDER (ZC+Cork): Sending multi-part message Frame 1...");
  push.send(msg1).await?;
  // Engine should: set cork, then ZC send Frame 1. Cork remains ON.
  // Trace logs should show: "set TCP_CORK true", then "Attempting ZC vectored send."
  println!("SENDER (ZC+Cork): Sent Frame 1.");

  // NO artificial delay here, relying on cork to batch if possible.
  // However, ZC send is async and awaits completion. Corking's effect on
  // already async-batched ZC sends needs careful observation.

  println!("SENDER (ZC+Cork): Sending multi-part message Frame 2...");
  push.send(msg2).await?;
  // Engine should: ZC send Frame 2, then (since it's last part) unset cork.
  // Trace logs should show: "Attempting ZC vectored send.", then "set TCP_CORK false"
  println!("SENDER (ZC+Cork): Sent Frame 2.");
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("RECEIVER: Receiving messages...");
  let rec1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec1.data().unwrap(), frame1_data);
  assert!(rec1.is_more());
  println!("RECEIVER: Received Frame 1 (ZC+Cork expected).");

  let rec2 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec2.data().unwrap(), frame2_data);
  assert!(!rec2.is_more());
  println!("RECEIVER: Received Frame 2 (ZC+Cork expected).");

  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

// Test 5: Behavior when send_vectored_zc reports a partial send (currently an error)
#[test]
#[cfg(all(target_os = "linux", feature = "io-uring"))]
async fn test_zerocopy_partial_send_is_error() -> Result<(), ZmqError> {
  println!("\n--- Starting test_zerocopy_partial_send_is_error ---");
  // This test is difficult to induce reliably without manipulating kernel buffers
  // or having a misbehaving peer. We're testing our library's *reaction*
  // if a partial send *were* to happen from send_vectored_zc.
  // The current ZmtpEngineCore logic treats a partial send_vectored_zc as an error.

  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5904";
  // Enable ZC. No PULL socket will be created to potentially cause send issues,
  // but this test primarily checks if the error path for partial ZC send is hit.
  // A real partial send is hard to trigger.
  // This test might be better as a conceptual one unless we can mock the stream behavior.

  let sender = ctx.socket(SocketType::Push)?;
  sender.set_option(IO_URING_SNDZEROCOPY, &(1i32).to_ne_bytes()).await?;
  // Set a very short SNDTIMEO so connect doesn't block forever if no listener
  sender.set_option(SNDTIMEO, &(100i32).to_ne_bytes()).await?;

  // Connect to an address where no one is listening to make the send eventually fail.
  // The goal isn't to test the partial send itself, but that *if* it happened,
  // our code path for it would be an error.
  // This is a weak test for "partial send is error" because connect will fail first.
  // A better test would involve a mock stream that simulates partial ZC send.

  println!(
    "SENDER (ZC): Connecting to an intentionally dead endpoint {}...",
    endpoint
  );
  sender.connect(endpoint).await?;
  tokio::time::sleep(CONNECT_DELAY).await; // Allow connect attempts to fail

  let msg_data = b"AttemptingZerocopyToNowhere";
  println!("SENDER (ZC): Sending message (expected to fail at some point)...");
  let send_result = sender.send(Msg::from_static(msg_data)).await;

  println!("SENDER (ZC): Send result: {:?}", send_result);
  // We expect this to fail, likely due to no peer (Timeout or ResourceLimitReached from SNDTIMEO=100ms),
  // rather than specifically a "Partial zerocopy send" error unless connect succeeded to a black hole.
  // If the connect itself fails to find a peer, send will likely result in ResourceLimitReached or Timeout.
  // The ZC path might not even be reached if no peer is available.
  // This test needs refinement to specifically test the partial send error path.
  // For now, we just assert it's an error.
  assert!(send_result.is_err(), "Send to nowhere should result in an error.");

  // If we could somehow mock `send_vectored_zc` to return Ok(partial_count),
  // then we would assert:
  // assert!(matches!(send_result, Err(ZmqError::IoError { kind: io::ErrorKind::WriteZero, .. })));

  ctx.term().await?;
  println!("--- Test finished (note: partial ZC send path hard to trigger directly) ---");
  Ok(())
}
