mod common;

use anyhow::Result;
use rzmq::{Msg, SocketType, context::context, socket::options};
use std::time::Duration;

/// Test A: rzmq ROUTER (Manual) <-> pyzmq DEALER (Raw)
///
/// rzmq ROUTER (Manual) should:
/// 1. Receive [Identity, Payload] from a raw DEALER (no empty delimiter inserted).
/// 2. Send [Identity, Payload] to a raw DEALER (no empty delimiter inserted).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_router_manual_vs_pyzmq_dealer() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  // 1. Setup rzmq ROUTER with MANUAL_FRAMING enabled
  let ctx = context()?;
  let router = ctx.socket(SocketType::Router)?;

  // Enable Manual Framing (disable Auto Delimiter)
  router.set_option(options::AUTO_DELIMITER, false).await?;

  router.bind(&endpoint).await?;
  tracing::info!("rzmq ROUTER (Manual): Bound to {}", endpoint);

  // 2. Start Python DEALER and wait for it to be ready.
  let (_guard, mut reader) =
    common::spawn_and_wait_ready("dealer_raw.py", &[endpoint.as_str()], Duration::from_secs(5))?;

  // 3. Receive from Python
  // Python sends b"Hello".
  // rzmq ROUTER (Manual) should receive [Identity, b"Hello"].
  // (If Auto were on, it would try to strip index 1, likely losing payload or erroring).
  tracing::info!("rzmq ROUTER: Waiting for message...");
  let frames = router.recv_multipart().await?;

  assert_eq!(frames.len(), 2, "Expected [Identity, Payload]");
  let identity = frames[0].clone();
  let payload = &frames[1];

  assert!(
    !identity.data().unwrap_or(&[]).is_empty(),
    "Identity should not be empty"
  );
  assert_eq!(payload.data().unwrap(), b"Hello", "Payload mismatch");
  tracing::info!("rzmq ROUTER: Received correct raw frames.");

  // 4. Send Reply
  // We must construct the full envelope: [Identity, Payload].
  // rzmq (Manual) will NOT insert an empty delimiter.
  tracing::info!("rzmq ROUTER: Sending reply...");
  router
    .send_multipart(vec![identity, Msg::from_static(b"Reply")])
    .await?;

  // 5. Verify Python Success
  common::wait_for_line(&mut reader, "SUCCESS", Duration::from_secs(5))?;
  tracing::info!("rzmq ROUTER: Python DEALER reported SUCCESS.");

  Ok(())
}

/// Test B: rzmq DEALER (Manual) <-> pyzmq ROUTER (Raw)
///
/// rzmq DEALER (Manual) should:
/// 1. Send [Payload] (no empty delimiter prepended).
/// 2. Receive [Payload] (no empty delimiter stripped/expected).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dealer_manual_vs_pyzmq_router() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  // 1. Start Python ROUTER (Server) and wait for it to bind.
  let (_guard, _reader) =
    common::spawn_and_wait_ready("router_raw.py", &[endpoint.as_str()], Duration::from_secs(5))?;

  // 2. Setup rzmq DEALER with MANUAL_FRAMING
  let ctx = context()?;
  let dealer = ctx.socket(SocketType::Dealer)?;
  dealer.set_option(options::AUTO_DELIMITER, false).await?;

  dealer.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(200)).await;

  // 3. Send Message
  // Manual mode: We send b"Hello". rzmq sends b"Hello".
  // (Auto mode would send [Empty, b"Hello"]).
  tracing::info!("rzmq DEALER: Sending raw message...");
  dealer.send(Msg::from_static(b"Hello")).await?;

  // 4. Receive Reply
  // Python ROUTER sends [Identity, b"Reply"].
  // rzmq DEALER (Manual) receives b"Reply".
  // (Auto mode would expect [Empty, b"Reply"] and strip the empty frame).
  tracing::info!("rzmq DEALER: Waiting for reply...");
  let reply = dealer.recv().await?;
  assert_eq!(reply.data().unwrap(), b"Reply");
  tracing::info!("rzmq DEALER: Received correct raw reply.");

  Ok(())
}
