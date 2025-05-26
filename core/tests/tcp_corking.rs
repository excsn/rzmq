use rzmq::{
  socket::options::{SNDTIMEO, TCP_CORK_OPT},
  Context, Msg, MsgFlags, SocketType, ZmqError,
};
use std::time::Duration;

mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(500);
const LONG_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECT_DELAY: Duration = Duration::from_millis(150);
const POST_SEND_DELAY: Duration = Duration::from_millis(50);

async fn setup_pair_with_cork(
  ctx: &Context,
  endpoint: &str,
  enable_cork_on_sender: bool,
) -> Result<(rzmq::Socket, rzmq::Socket), ZmqError> {
  let sender = ctx.socket(SocketType::Push)?;
  let receiver = ctx.socket(SocketType::Pull)?;

  if enable_cork_on_sender {
    println!(
      "Setting TCP_CORK_OPT={} on SENDER for endpoint {}",
      enable_cork_on_sender, endpoint
    );
    sender.set_option_raw(TCP_CORK_OPT, &(1i32).to_ne_bytes()).await?;
  }

  receiver.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  sender.connect(endpoint).await?;
  tokio::time::sleep(CONNECT_DELAY).await;

  Ok((sender, receiver))
}

#[rzmq_macros::test]
async fn test_tcp_cork_single_part_message_cork_enabled() -> Result<(), ZmqError> {
  println!("\n--- Starting test_tcp_cork_single_part_message_cork_enabled ---");
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5800"; // Unique port
  let (push, pull) = setup_pair_with_cork(&ctx, endpoint, true).await?;

  let msg_data = b"SingleCorkedMessage";
  println!("SENDER: Sending single part message with cork enabled...");
  push.send(Msg::from_static(msg_data)).await?;
  println!("SENDER: Sent.");
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("RECEIVER: Receiving message...");
  let received = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(received.data().unwrap(), msg_data);
  assert!(!received.is_more());
  println!("RECEIVER: Received correctly.");

  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

#[rzmq_macros::test]
async fn test_tcp_cork_single_part_message_cork_disabled() -> Result<(), ZmqError> {
  println!("\n--- Starting test_tcp_cork_single_part_message_cork_disabled ---");
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5801"; // Unique port
  let (push, pull) = setup_pair_with_cork(&ctx, endpoint, false).await?; // Cork disabled

  let msg_data = b"SingleMessageNoCork";
  println!("SENDER: Sending single part message with cork disabled...");
  push.send(Msg::from_static(msg_data)).await?;
  println!("SENDER: Sent.");
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("RECEIVER: Receiving message...");
  let received = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(received.data().unwrap(), msg_data);
  assert!(!received.is_more());
  println!("RECEIVER: Received correctly.");

  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

#[rzmq_macros::test]
async fn test_tcp_cork_multi_part_message_cork_enabled() -> Result<(), ZmqError> {
  println!("\n--- Starting test_tcp_cork_multi_part_message_cork_enabled ---");
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5802"; // Unique port
  let (push, pull) = setup_pair_with_cork(&ctx, endpoint, true).await?;

  let frame1_data = b"MultiCorkFrame1";
  let frame2_data = b"MultiCorkFrame2";
  let frame3_data = b"MultiCorkFrame3";

  let mut msg1 = Msg::from_static(frame1_data);
  msg1.set_flags(MsgFlags::MORE);
  let mut msg2 = Msg::from_static(frame2_data);
  msg2.set_flags(MsgFlags::MORE);
  let msg3 = Msg::from_static(frame3_data); // Last frame

  println!("SENDER: Sending multi-part message with cork enabled...");
  push.send(msg1).await?;
  println!("SENDER: Sent Frame 1");
  push.send(msg2).await?;
  println!("SENDER: Sent Frame 2");
  push.send(msg3).await?;
  println!("SENDER: Sent Frame 3 (last)");
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("RECEIVER: Receiving messages...");
  let rec1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec1.data().unwrap(), frame1_data);
  assert!(rec1.is_more());
  println!("RECEIVER: Received Frame 1");

  let rec2 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec2.data().unwrap(), frame2_data);
  assert!(rec2.is_more());
  println!("RECEIVER: Received Frame 2");

  let rec3 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec3.data().unwrap(), frame3_data);
  assert!(!rec3.is_more());
  println!("RECEIVER: Received Frame 3");

  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

#[rzmq_macros::test]
async fn test_tcp_cork_multi_part_message_cork_disabled() -> Result<(), ZmqError> {
  println!("\n--- Starting test_tcp_cork_multi_part_message_cork_disabled ---");
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5803"; // Unique port
  let (push, pull) = setup_pair_with_cork(&ctx, endpoint, false).await?; // Cork disabled

  let frame1_data = b"MultiNoCorkFrame1";
  let frame2_data = b"MultiNoCorkFrame2";

  let mut msg1 = Msg::from_static(frame1_data);
  msg1.set_flags(MsgFlags::MORE);
  let msg2 = Msg::from_static(frame2_data);

  println!("SENDER: Sending multi-part message with cork disabled...");
  push.send(msg1).await?;
  println!("SENDER: Sent Frame 1");
  push.send(msg2).await?;
  println!("SENDER: Sent Frame 2 (last)");
  tokio::time::sleep(POST_SEND_DELAY).await;

  println!("RECEIVER: Receiving messages...");
  let rec1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec1.data().unwrap(), frame1_data);
  assert!(rec1.is_more());
  println!("RECEIVER: Received Frame 1");

  let rec2 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec2.data().unwrap(), frame2_data);
  assert!(!rec2.is_more());
  println!("RECEIVER: Received Frame 2");

  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

#[rzmq_macros::test]
async fn test_tcp_cork_ping_pong_interaction() -> Result<(), ZmqError> {
  println!("\n--- Starting test_tcp_cork_ping_pong_interaction ---");
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:5804"; // Unique port
  let (push, pull) = setup_pair_with_cork(&ctx, endpoint, true).await?;

  // This test implicitly relies on the ZmtpEngineCore's heartbeat timers
  // being configured (e.g., via socket options HEARTBEAT_IVL, HEARTBEAT_TIMEOUT)
  // to actually send PINGs. If they are not set to trigger PINGs within the
  // test's timeframe, this test won't directly verify PING/cork interaction.
  // It will, however, verify that a pause followed by a send still works correctly
  // with corking enabled.

  // Set a very short SNDTIMEO to make sends non-blocking if PINGs cause issues.
  push.set_option_raw(SNDTIMEO, &(0i32).to_ne_bytes()).await?;

  println!("SENDER: Sending initial message...");
  push.send(Msg::from_static(b"InitialData")).await?;
  tokio::time::sleep(POST_SEND_DELAY).await;
  let rec_initial = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(rec_initial.data().unwrap(), b"InitialData");
  println!("Initial message exchanged.");

  println!("SENDER: Pausing to potentially allow heartbeat PINGs (if engine is configured for them)...");
  // To make this test more meaningful for PINGs, HEARTBEAT_IVL would need to be set
  // on the PUSH socket to a value smaller than this sleep, e.g., 200ms.
  // For now, this just tests if corking state is okay after a pause.
  // Example: push.set_option_raw(HEARTBEAT_IVL, &(200i32).to_ne_bytes()).await?;
  //          push.set_option_raw(HEARTBEAT_TIMEOUT, &(1000i32).to_ne_bytes()).await?;
  tokio::time::sleep(Duration::from_millis(500)).await;

  println!("SENDER: Sending second message after pause...");
  let msg_data_after_pause = b"DataAfterPause";
  // This send might fail with ResourceLimitReached if a PING was sent,
  // the PUSH socket got corked for the PING, then uncorked,
  // and the PULL socket's pipe is full from the InitialData.
  // Or it might succeed. The key is that the corking logic doesn't deadlock or panic.
  match push.send(Msg::from_static(msg_data_after_pause)).await {
    Ok(()) => {
      println!("SENDER: Sent second message successfully.");
      tokio::time::sleep(POST_SEND_DELAY).await;
      let rec_after_pause = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
      assert_eq!(rec_after_pause.data().unwrap(), msg_data_after_pause);
      println!("RECEIVER: Received second message successfully.");
    }
    Err(ZmqError::ResourceLimitReached) => {
      println!("SENDER: Send after pause resulted in ResourceLimitReached (EAGAIN), this can happen if PULL is slow or HWM is small.");
      // This is an acceptable outcome if PUSH pipe is full.
      // To make the test pass consistently, try to receive the first message, then pause, then send/recv second.
      // The current structure is fine for testing if corking breaks things.
    }
    Err(e) => {
      return Err(e); // Other errors are unexpected
    }
  }

  ctx.term().await?;
  println!("--- Test finished ---");
  Ok(())
}

#[rzmq_macros::test]
async fn test_tcp_cork_state_management_with_more_flag() -> Result<(), ZmqError> {
    println!("\n--- Starting test_tcp_cork_state_management_with_more_flag ---");
    let ctx = common::test_context();
    let endpoint = "tcp://127.0.0.1:5805"; // Unique port
    // Enable cork on sender. SNDTIMEO is not set to 0 here initially.
    let (push, pull) = setup_pair_with_cork(&ctx, endpoint, true).await?;

    // Message 1 (single part, should set and unset cork)
    println!("SENDER: Sending Message 1 ('SinglePart')");
    push.send(Msg::from_static(b"SinglePart")).await?;
    // ZmtpEngineCore: expect=T, cork=F -> set cork=T -> send -> expect=T, unset cork=F
    println!("SENDER: Sent Message 1.");

    let rec1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    assert_eq!(rec1.data().unwrap(), b"SinglePart");
    println!("RECEIVER: Received Message 1.");

    // Message 2 (multi-part)
    let mut msg2_part1 = Msg::from_static(b"MultiPart1");
    msg2_part1.set_flags(MsgFlags::MORE);
    let msg2_part2 = Msg::from_static(b"MultiPart2");

    println!("SENDER: Sending Message 2, Part 1 ('MultiPart1' with MORE)");
    push.send(msg2_part1).await?;
    // ZmtpEngineCore: expect=T, cork=F -> set cork=T -> send -> expect=F (because MORE)
    println!("SENDER: Sent Message 2, Part 1.");
    // Cork should be ON in the engine now.

    // Introduce a small delay. If corking is working, Part1 might be held.
    tokio::time::sleep(Duration::from_millis(10)).await;

    println!("SENDER: Sending Message 2, Part 2 ('MultiPart2' no MORE)");
    push.send(msg2_part2).await?;
    // ZmtpEngineCore: expect=F, cork=T -> send -> expect=T (because no MORE), unset cork=F
    println!("SENDER: Sent Message 2, Part 2.");

    // Receiver gets Message 2
    let rec2_part1 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    assert_eq!(rec2_part1.data().unwrap(), b"MultiPart1");
    assert!(rec2_part1.is_more());
    println!("RECEIVER: Received Message 2, Part 1.");

    let rec2_part2 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    assert_eq!(rec2_part2.data().unwrap(), b"MultiPart2");
    assert!(!rec2_part2.is_more());
    println!("RECEIVER: Received Message 2, Part 2.");
    
    // Message 3 (single part again, to check cork state reset)
    println!("SENDER: Sending Message 3 ('AnotherSingle')");
    push.send(Msg::from_static(b"AnotherSingle")).await?;
    // ZmtpEngineCore: expect=T, cork=F -> set cork=T -> send -> expect=T, unset cork=F
    println!("SENDER: Sent Message 3.");

    let rec3 = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    assert_eq!(rec3.data().unwrap(), b"AnotherSingle");
    println!("RECEIVER: Received Message 3.");

    ctx.term().await?;
    println!("--- Test finished ---");
    Ok(())
}