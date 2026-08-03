use rzmq::socket::options::REUSE_PORT;
use rzmq::{Context, Socket, SocketType, ZmqError};
#[cfg(unix)]
use rzmq::Msg;
#[cfg(unix)]
use std::time::Duration;

mod common;

#[cfg(unix)]
const IDLE_WINDOW: Duration = Duration::from_millis(700);
#[cfg(unix)]
const SETTLE: Duration = Duration::from_millis(150);

async fn reuse_port_socket(ctx: &Context, socket_type: SocketType) -> Result<Socket, ZmqError> {
  let socket = ctx.socket(socket_type)?;
  socket.set_option_raw(REUSE_PORT, &1i32.to_ne_bytes()).await?;
  Ok(socket)
}

#[cfg(unix)]
async fn drain_count(socket: &Socket) -> usize {
  let mut count = 0;
  while common::recv_timeout(socket, IDLE_WINDOW).await.is_ok() {
    count += 1;
  }
  count
}

#[cfg(unix)]
async fn push_one(ctx: &Context, endpoint: &str, payload: &'static [u8]) -> Result<Socket, ZmqError> {
  let push = ctx.socket(SocketType::Push)?;
  push.connect(endpoint).await?;
  tokio::time::sleep(SETTLE).await;
  push.send(Msg::from_static(payload)).await?;
  Ok(push)
}

#[tokio::test]
async fn test_reuse_port_option_roundtrip() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;

  let raw = pull.get_option(REUSE_PORT).await?;
  let val = i32::from_ne_bytes(raw.try_into().expect("REUSE_PORT should be 4 bytes"));
  assert_eq!(val, 0, "default REUSE_PORT should be 0");

  pull.set_option_raw(REUSE_PORT, &1i32.to_ne_bytes()).await?;
  let raw2 = pull.get_option(REUSE_PORT).await?;
  let val2 = i32::from_ne_bytes(raw2.try_into().unwrap());
  assert_eq!(val2, 1);

  pull.set_option_raw(REUSE_PORT, &0i32.to_ne_bytes()).await?;
  let raw3 = pull.get_option(REUSE_PORT).await?;
  let val3 = i32::from_ne_bytes(raw3.try_into().unwrap());
  assert_eq!(val3, 0);

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_duplicate_bind_rejected_without_reuse_port() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let first = ctx.socket(SocketType::Pull)?;
  let endpoint = common::bind_and_resolve_tcp(&first).await?;

  match first.bind(&endpoint).await {
    Err(ZmqError::AddrInUse(_)) => {}
    other => panic!("same-socket rebind should be AddrInUse, got {:?}", other),
  }

  let second = ctx.socket(SocketType::Pull)?;
  assert!(
    second.bind(&endpoint).await.is_err(),
    "second socket should not be able to bind {} without REUSE_PORT",
    endpoint
  );

  ctx.term().await?;
  Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn test_reuse_port_across_sockets() -> Result<(), ZmqError> {
  const CONNECTIONS: usize = 16;

  let ctx = common::test_context();

  let pull_a = reuse_port_socket(&ctx, SocketType::Pull).await?;
  let endpoint = common::bind_and_resolve_tcp(&pull_a).await?;

  let pull_b = reuse_port_socket(&ctx, SocketType::Pull).await?;
  pull_b.bind(&endpoint).await?;
  tokio::time::sleep(SETTLE).await;

  let mut pushes = Vec::with_capacity(CONNECTIONS);
  for _ in 0..CONNECTIONS {
    pushes.push(push_one(&ctx, &endpoint, b"shard").await?);
  }
  tokio::time::sleep(SETTLE).await;

  let (a, b) = tokio::join!(drain_count(&pull_a), drain_count(&pull_b));
  assert_eq!(
    a + b,
    CONNECTIONS,
    "all messages should arrive across the two listeners (a={}, b={})",
    a,
    b
  );

  // Linux hashes the connection 4-tuple across the listener group. With 16 connections the
  // odds of every one landing on the same listener are ~2^-15.
  #[cfg(target_os = "linux")]
  assert!(
    a > 0 && b > 0,
    "Linux should distribute connections across listeners sharing a port (a={}, b={})",
    a,
    b
  );

  // Observed platform behavior, not an rzmq guarantee: macOS has no SO_REUSEPORT_LB, so one
  // listener takes every connection. A failure here means Darwin changed, not that rzmq broke.
  #[cfg(target_os = "macos")]
  assert!(
    a == 0 || b == 0,
    "macOS should not distribute connections across listeners (a={}, b={})",
    a,
    b
  );

  drop(pushes);
  ctx.term().await?;
  Ok(())
}

/// `REUSE_PORT` is stored but never applied where `SO_REUSEPORT` does not exist, so the OS
/// must still refuse the second bind.
#[cfg(not(unix))]
#[tokio::test]
async fn test_reuse_port_is_inert_without_so_reuseport() -> Result<(), ZmqError> {
  let ctx = common::test_context();

  let first = reuse_port_socket(&ctx, SocketType::Pull).await?;
  let endpoint = common::bind_and_resolve_tcp(&first).await?;

  match first.bind(&endpoint).await {
    Err(ZmqError::AddrInUse(_)) => {}
    other => panic!("same-socket rebind should be AddrInUse, got {:?}", other),
  }

  let second = reuse_port_socket(&ctx, SocketType::Pull).await?;
  match second.bind(&endpoint).await {
    Err(ZmqError::AddrInUse(_)) => {}
    other => panic!("second socket bind should be AddrInUse, got {:?}", other),
  }

  ctx.term().await?;
  Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn test_reuse_port_same_socket_shards() -> Result<(), ZmqError> {
  const SHARDS: usize = 3;
  const CONNECTIONS: usize = 6;

  let ctx = common::test_context();

  let pull = reuse_port_socket(&ctx, SocketType::Pull).await?;
  let endpoint = common::bind_and_resolve_tcp(&pull).await?;
  for _ in 1..SHARDS {
    pull.bind(&endpoint).await?;
  }
  tokio::time::sleep(SETTLE).await;

  let mut pushes = Vec::with_capacity(CONNECTIONS);
  for _ in 0..CONNECTIONS {
    pushes.push(push_one(&ctx, &endpoint, b"same-socket").await?);
  }
  tokio::time::sleep(SETTLE).await;

  assert_eq!(drain_count(&pull).await, CONNECTIONS);

  drop(pushes);
  ctx.term().await?;
  Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn test_unbind_stops_every_shard() -> Result<(), ZmqError> {
  const SHARDS: usize = 3;

  let ctx = common::test_context();

  let pull = reuse_port_socket(&ctx, SocketType::Pull).await?;
  let endpoint = common::bind_and_resolve_tcp(&pull).await?;
  for _ in 1..SHARDS {
    pull.bind(&endpoint).await?;
  }
  tokio::time::sleep(SETTLE).await;

  let addr = endpoint.strip_prefix("tcp://").unwrap().to_string();
  tokio::net::TcpStream::connect(&addr)
    .await
    .expect("port should accept while bound");

  pull.unbind(&endpoint).await?;
  tokio::time::sleep(Duration::from_millis(500)).await;

  // A single surviving shard would still accept, so one refusal covers all of them.
  assert!(
    tokio::net::TcpStream::connect(&addr).await.is_err(),
    "no listener should remain on {} after unbind",
    addr
  );

  ctx.term().await?;
  Ok(())
}
