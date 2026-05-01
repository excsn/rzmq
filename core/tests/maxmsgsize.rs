use rzmq::{
  socket::{options::MAXMSGSIZE, SocketEvent},
  Context, Msg, SocketType, ZmqError,
};
use std::time::Duration;
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(250);
const LONG_TIMEOUT: Duration = Duration::from_secs(3);

/// When the receiver has ZMQ_MAXMSGSIZE set and the sender sends a frame that exceeds it,
/// the receiver should close the connection. We verify via a monitor Disconnected event.
#[tokio::test]
async fn test_maxmsgsize_oversized_frame_drops_connection() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  let push = ctx.socket(SocketType::Push)?;

  pull.set_option_raw(MAXMSGSIZE, &64i64.to_ne_bytes()).await?;
  let monitor = pull.monitor_default().await?;

  let endpoint = "tcp://127.0.0.1:15722";
  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  push.connect(endpoint).await?;

  common::wait_for_monitor_event(&monitor, LONG_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .expect("handshake should complete before sending oversized frame");

  // 128 bytes exceeds the 64-byte limit on the receiver
  push.send(Msg::from_vec(vec![0u8; 128])).await?;

  common::wait_for_monitor_event(&monitor, LONG_TIMEOUT, SHORT_TIMEOUT, |e| {
    matches!(e, SocketEvent::Disconnected { .. })
  })
  .await
  .expect("oversized frame should cause the receiver to disconnect");

  ctx.term().await?;
  Ok(())
}

/// With the default ZMQ_MAXMSGSIZE of -1 (unlimited), large frames pass through normally.
#[tokio::test]
async fn test_maxmsgsize_default_unlimited_allows_large_frame() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  let push = ctx.socket(SocketType::Push)?;

  let endpoint = "tcp://127.0.0.1:15723";
  pull.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  let payload = vec![0u8; 128 * 1024]; // 128 KB
  push.send(Msg::from_vec(payload.clone())).await?;
  let msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
  assert_eq!(msg.data().unwrap().len(), payload.len());

  ctx.term().await?;
  Ok(())
}
