use rzmq::socket::options::{LINGER, RCVHWM, SNDHWM, SUBSCRIBE, XPUB_NODROP};
use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use serial_test::serial;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

mod common;

const WAKE_WITHIN: Duration = Duration::from_secs(2);

/// Sends 32 KiB messages until a send has made no progress for 300 ms, which
/// means it is blocked on the full pipe. Returns the task so the caller can
/// check that `close` wakes it.
async fn send_until_blocked(socket: Socket) -> tokio::task::JoinHandle<Result<(), ZmqError>> {
  let accepted = Arc::new(AtomicU64::new(0));
  let counter = accepted.clone();
  let task = tokio::spawn(async move {
    let payload = vec![0u8; 32 * 1024];
    loop {
      socket.send(Msg::from_vec(payload.clone())).await?;
      counter.fetch_add(1, Ordering::Relaxed);
    }
  });
  let mut last = u64::MAX;
  loop {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let now = accepted.load(Ordering::Relaxed);
    if now == last {
      break;
    }
    last = now;
  }
  assert!(!task.is_finished(), "send loop ended before it blocked");
  task
}

async fn assert_close_wakes(socket: &Socket, blocked: tokio::task::JoinHandle<Result<(), ZmqError>>) {
  socket.close().await.expect("close");
  let outcome = tokio::time::timeout(WAKE_WITHIN, blocked).await.expect("blocked send did not return after close");
  assert!(outcome.expect("send task").is_err(), "a send blocked across close must fail");
}

async fn stalled_peer(ctx: &Context, peer_type: SocketType) -> Result<(Socket, String), ZmqError> {
  let peer = ctx.socket(peer_type)?;
  peer.set_option(RCVHWM, 1i32).await?;
  if peer_type == SocketType::Sub {
    peer.set_option(SUBSCRIBE, b"").await?;
  }
  let endpoint = common::bind_and_resolve_tcp(&peer).await?;
  Ok((peer, endpoint))
}

/// PUSH with no peer: the connect pipe fills to SNDHWM and the next send blocks.
#[tokio::test]
#[serial]
async fn close_wakes_push_send_blocked_with_no_peer() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  push.set_option(SNDHWM, 8i32).await?;
  push.set_option(LINGER, 0i32).await?;
  push.connect("tcp://127.0.0.1:1").await?;

  let blocked = send_until_blocked(push.clone()).await;
  assert_close_wakes(&push, blocked).await;
  ctx.term().await
}

/// PUSH to a PULL that never reads: the pipe and the socket buffers fill.
#[tokio::test]
#[serial]
async fn close_wakes_push_send_blocked_on_stalled_peer() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let (_pull, endpoint) = stalled_peer(&ctx, SocketType::Pull).await?;
  let push = ctx.socket(SocketType::Push)?;
  push.set_option(SNDHWM, 8i32).await?;
  push.set_option(LINGER, 0i32).await?;
  push.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(300)).await;

  let blocked = send_until_blocked(push.clone()).await;
  assert_close_wakes(&push, blocked).await;
  ctx.term().await
}

/// PUB with XPUB_NODROP to a SUB that never reads: the send waits on the
/// subscriber's pipe credits.
#[tokio::test]
#[serial]
async fn close_wakes_pub_nodrop_send_blocked_on_stalled_subscriber() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let (_sub, endpoint) = stalled_peer(&ctx, SocketType::Sub).await?;
  let publisher = ctx.socket(SocketType::Pub)?;
  publisher.set_option(SNDHWM, 8i32).await?;
  publisher.set_option(LINGER, 0i32).await?;
  publisher.set_option(XPUB_NODROP, true).await?;
  publisher.connect(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(300)).await;

  let blocked = send_until_blocked(publisher.clone()).await;
  assert_close_wakes(&publisher, blocked).await;
  ctx.term().await
}
