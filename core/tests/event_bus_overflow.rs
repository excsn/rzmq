use rzmq::socket::options::RECONNECT_IVL;
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use std::time::Duration;

mod common;

// The context's event bus holds 256 events (DEFAULT_EVENT_BUS_CAPACITY).
const OVERFLOW_SOCKETS: usize = 1024;
const STEP_TIMEOUT: Duration = Duration::from_secs(3);
const MONITOR_POLL: Duration = Duration::from_millis(25);
const TERM_TIMEOUT: Duration = Duration::from_secs(5);

fn unused_tcp_endpoint() -> String {
  let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
  let port = listener.local_addr().expect("local addr").port();
  format!("tcp://127.0.0.1:{port}")
}

/// Each new socket publishes one event when its actor first runs. Created with
/// no await between them on a current-thread runtime, the actors publish back
/// to back before any earlier actor is polled.
fn overflow_event_bus(ctx: &Context) -> Result<Vec<Socket>, ZmqError> {
  (0..OVERFLOW_SOCKETS).map(|_| ctx.socket(SocketType::Push)).collect()
}

async fn wait_for(monitor: &MonitorReceiver, what: &str, pick: impl Fn(&SocketEvent) -> bool) {
  if let Err(e) = common::wait_for_monitor_event(monitor, STEP_TIMEOUT, MONITOR_POLL, pick).await {
    panic!("{what}: {e}");
  }
}

async fn assert_delivers(push: &Socket, pull: &Socket, payload: &'static [u8]) {
  common::send_timeout(push, Msg::from_static(payload), STEP_TIMEOUT)
    .await
    .unwrap_or_else(|e| panic!("send {:?}: {e}", String::from_utf8_lossy(payload)));
  let received = common::recv_timeout(pull, STEP_TIMEOUT)
    .await
    .unwrap_or_else(|e| panic!("recv {:?}: {e}", String::from_utf8_lossy(payload)));
  assert_eq!(received.data().unwrap(), payload);
}

async fn assert_terminates(ctx: &Context) {
  tokio::time::timeout(TERM_TIMEOUT, ctx.term())
    .await
    .unwrap_or_else(|_| panic!("term did not finish within {TERM_TIMEOUT:?}"))
    .expect("term");
}

async fn bound_socket_accepts_after_overflow(endpoint: Option<String>) -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let peer_ctx = common::test_context();

  let pull = ctx.socket(SocketType::Pull)?;
  let endpoint = match endpoint {
    Some(endpoint) => {
      pull.bind(&endpoint).await?;
      endpoint
    }
    None => common::bind_and_resolve_tcp(&pull).await?,
  };

  let _overflow = overflow_event_bus(&ctx)?;

  let push = peer_ctx.socket(SocketType::Push)?;
  let push_monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;
  wait_for(&push_monitor, "connect after the overflow", |e| {
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await;
  assert_delivers(&push, &pull, b"after-overflow").await;

  assert_terminates(&peer_ctx).await;
  assert_terminates(&ctx).await;
  Ok(())
}

/// A socket still retrying its endpoint connects once the peer binds, after
/// more events crossed the context's bus than it holds.
#[tokio::test(flavor = "current_thread")]
async fn reconnecting_socket_connects_after_event_bus_overflow() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let peer_ctx = common::test_context();
  let endpoint = unused_tcp_endpoint();

  let push = ctx.socket(SocketType::Push)?;
  push.set_option(RECONNECT_IVL, 100i32).await?;
  let push_monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;
  wait_for(&push_monitor, "first retry", |e| matches!(e, SocketEvent::ConnectRetried { .. })).await;

  let _overflow = overflow_event_bus(&ctx)?;

  let pull = peer_ctx.socket(SocketType::Pull)?;
  pull.bind(&endpoint).await?;
  wait_for(&push_monitor, "connect after the overflow", |e| {
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await;
  assert_delivers(&push, &pull, b"after-overflow").await;

  assert_terminates(&ctx).await;
  assert_terminates(&peer_ctx).await;
  Ok(())
}

/// An established connection keeps delivering after more events crossed the
/// context's bus than it holds.
#[tokio::test(flavor = "current_thread")]
async fn connected_socket_delivers_after_event_bus_overflow() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let peer_ctx = common::test_context();

  let pull = peer_ctx.socket(SocketType::Pull)?;
  let endpoint = common::bind_and_resolve_tcp(&pull).await?;

  let push = ctx.socket(SocketType::Push)?;
  let push_monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;
  wait_for(&push_monitor, "first connect", |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;
  assert_delivers(&push, &pull, b"before-overflow").await;

  let _overflow = overflow_event_bus(&ctx)?;
  tokio::task::yield_now().await;

  assert_delivers(&push, &pull, b"after-overflow").await;

  assert_terminates(&ctx).await;
  assert_terminates(&peer_ctx).await;
  Ok(())
}

/// A bound TCP socket accepts a new peer after more events crossed the
/// context's bus than it holds.
#[tokio::test(flavor = "current_thread")]
async fn bound_tcp_socket_accepts_after_event_bus_overflow() -> Result<(), ZmqError> {
  bound_socket_accepts_after_overflow(None).await
}

/// A bound IPC socket accepts a new peer after more events crossed the
/// context's bus than it holds.
#[cfg(feature = "ipc")]
#[tokio::test(flavor = "current_thread")]
async fn bound_ipc_socket_accepts_after_event_bus_overflow() -> Result<(), ZmqError> {
  bound_socket_accepts_after_overflow(Some(common::unique_ipc_endpoint())).await
}

/// `term()` finishes with a retrying socket and a bound socket in the context
/// after more events crossed its bus than it holds.
#[tokio::test(flavor = "current_thread")]
async fn context_terminates_after_event_bus_overflow() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let pull = ctx.socket(SocketType::Pull)?;
  common::bind_and_resolve_tcp(&pull).await?;

  let push = ctx.socket(SocketType::Push)?;
  push.set_option(RECONNECT_IVL, 100i32).await?;
  let push_monitor = push.monitor_default().await?;
  push.connect(&unused_tcp_endpoint()).await?;
  wait_for(&push_monitor, "first retry", |e| matches!(e, SocketEvent::ConnectRetried { .. })).await;

  let _overflow = overflow_event_bus(&ctx)?;
  tokio::task::yield_now().await;

  assert_terminates(&ctx).await;
  Ok(())
}
