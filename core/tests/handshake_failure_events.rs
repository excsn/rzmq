use rzmq::socket::options::{HANDSHAKE_IVL, PLAIN_PASSWORD, PLAIN_SERVER, PLAIN_USERNAME};
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Socket, SocketType, ZmqError};
use serial_test::serial;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

mod common;

const EVENT_TIMEOUT: Duration = Duration::from_secs(3);

/// Monitor events up to and including the first `Disconnected`. Without one it
/// returns everything seen before `EVENT_TIMEOUT`.
async fn events_through_disconnect(monitor: &MonitorReceiver) -> Vec<SocketEvent> {
  let mut events = Vec::new();
  let deadline = tokio::time::Instant::now() + EVENT_TIMEOUT;
  while let Ok(Ok(event)) = tokio::time::timeout_at(deadline, monitor.recv()).await {
    let done = matches!(event, SocketEvent::Disconnected { .. });
    events.push(event);
    if done {
      break;
    }
  }
  events
}

/// libzmq `stream_engine_base_t::error`: a connection that ends while its
/// handshake is running reports handshake failed, then disconnected.
fn check_handshake_failed_then_disconnected(side: &str, events: &[SocketEvent]) -> Result<(), String> {
  let failed = events.iter().position(|e| matches!(e, SocketEvent::HandshakeFailed { .. }));
  let disconnected = events.iter().position(|e| matches!(e, SocketEvent::Disconnected { .. }));
  match (failed, disconnected) {
    (Some(f), Some(d)) if f < d => Ok(()),
    _ => Err(format!("{side}: expected HandshakeFailed then Disconnected, got {events:?}")),
  }
}

fn assert_handshake_failed_then_disconnected(side: &str, events: &[SocketEvent]) {
  if let Err(failure) = check_handshake_failed_then_disconnected(side, events) {
    panic!("{failure}");
  }
}

fn assert_both_sides(server_events: &[SocketEvent], client_events: &[SocketEvent]) {
  let failures: Vec<String> = [
    check_handshake_failed_then_disconnected("server", server_events),
    check_handshake_failed_then_disconnected("client", client_events),
  ]
  .into_iter()
  .filter_map(Result::err)
  .collect();
  assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A TCP listener that runs `on_accept` on every connection.
async fn raw_peer<F, Fut>(on_accept: F) -> String
where
  F: Fn(tokio::net::TcpStream) -> Fut + Send + Sync + 'static,
  Fut: std::future::Future<Output = ()> + Send + 'static,
{
  let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind raw peer");
  let endpoint = format!("tcp://{}", listener.local_addr().expect("local addr"));
  tokio::spawn(async move {
    while let Ok((stream, _)) = listener.accept().await {
      tokio::spawn(on_accept(stream));
    }
  });
  endpoint
}

async fn monitored_dealer(ctx: &rzmq::Context) -> Result<(Socket, MonitorReceiver), ZmqError> {
  let dealer = ctx.socket(SocketType::Dealer)?;
  let monitor = dealer.monitor_default().await?;
  Ok((dealer, monitor))
}

#[tokio::test]
#[serial]
async fn peer_closing_during_handshake_reports_handshake_failed_then_disconnected() -> Result<(), ZmqError> {
  let endpoint = raw_peer(|mut stream| async move {
    let mut greeting = [0u8; 10];
    let _ = stream.read_exact(&mut greeting).await;
  })
  .await;
  let ctx = common::test_context();
  let (dealer, monitor) = monitored_dealer(&ctx).await?;
  dealer.connect(&endpoint).await?;

  assert_handshake_failed_then_disconnected("client", &events_through_disconnect(&monitor).await);
  ctx.term().await
}

#[tokio::test]
#[serial]
async fn handshake_timeout_reports_handshake_failed_then_disconnected() -> Result<(), ZmqError> {
  let endpoint = raw_peer(|stream| async move {
    tokio::time::sleep(Duration::from_secs(10)).await;
    drop(stream);
  })
  .await;
  let ctx = common::test_context();
  let (dealer, monitor) = monitored_dealer(&ctx).await?;
  dealer.set_option(HANDSHAKE_IVL, 200i32).await?;
  dealer.connect(&endpoint).await?;

  assert_handshake_failed_then_disconnected("client", &events_through_disconnect(&monitor).await);
  ctx.term().await
}

#[tokio::test]
#[serial]
async fn peer_reset_during_handshake_reports_handshake_failed_then_disconnected() -> Result<(), ZmqError> {
  let endpoint = raw_peer(|mut stream| async move {
    let mut greeting = [0u8; 10];
    let _ = stream.read_exact(&mut greeting).await;
    let _ = stream.set_linger(Some(Duration::ZERO));
  })
  .await;
  let ctx = common::test_context();
  let (dealer, monitor) = monitored_dealer(&ctx).await?;
  dealer.connect(&endpoint).await?;

  assert_handshake_failed_then_disconnected("client", &events_through_disconnect(&monitor).await);
  ctx.term().await
}

#[tokio::test]
#[serial]
async fn plain_auth_failure_reports_handshake_failed_then_disconnected_on_both_sides() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let server = ctx.socket(SocketType::Router)?;
  server.set_option(PLAIN_SERVER, true).await?;
  server.set_option(PLAIN_USERNAME, "admin").await?;
  server.set_option(PLAIN_PASSWORD, "secret").await?;
  let server_monitor = server.monitor_default().await?;
  let endpoint = common::bind_and_resolve_tcp(&server).await?;

  let (client, client_monitor) = monitored_dealer(&ctx).await?;
  client.set_option(PLAIN_USERNAME, "admin").await?;
  client.set_option(PLAIN_PASSWORD, "wrong").await?;
  client.connect(&endpoint).await?;

  let (server_events, client_events) =
    tokio::join!(events_through_disconnect(&server_monitor), events_through_disconnect(&client_monitor));
  assert_both_sides(&server_events, &client_events);
  ctx.term().await
}

#[cfg(feature = "curve")]
#[tokio::test]
#[serial]
async fn curve_wrong_server_key_reports_handshake_failed_then_disconnected_on_both_sides() -> Result<(), ZmqError> {
  use dryoc::{keypair::StackKeyPair as Keypair, types::Bytes};
  use rzmq::socket::options::{CURVE_SECRET_KEY, CURVE_SERVER, CURVE_SERVER_KEY};

  let server_keys = Keypair::r#gen();
  let client_keys = Keypair::r#gen();
  let impostor_keys = Keypair::r#gen();

  let ctx = common::test_context();
  let server = ctx.socket(SocketType::Router)?;
  server.set_option(CURVE_SERVER, true).await?;
  server.set_option_raw(CURVE_SECRET_KEY, server_keys.secret_key.as_slice()).await?;
  let server_monitor = server.monitor_default().await?;
  let endpoint = common::bind_and_resolve_tcp(&server).await?;

  let (client, client_monitor) = monitored_dealer(&ctx).await?;
  client.set_option_raw(CURVE_SECRET_KEY, client_keys.secret_key.as_slice()).await?;
  client.set_option_raw(CURVE_SERVER_KEY, impostor_keys.public_key.as_slice()).await?;
  client.connect(&endpoint).await?;

  let (server_events, client_events) =
    tokio::join!(events_through_disconnect(&server_monitor), events_through_disconnect(&client_monitor));
  assert_both_sides(&server_events, &client_events);
  ctx.term().await
}

fn variant(event: &SocketEvent) -> String {
  let debug = format!("{event:?}");
  debug.split([' ', '{']).next().unwrap_or_default().to_string()
}

/// `expected` appear in `events` in this order, other events allowed between them.
fn assert_in_order(side: &str, events: &[SocketEvent], expected: &[&str]) {
  let names: Vec<String> = events.iter().map(variant).collect();
  let mut rest = names.iter();
  for want in expected {
    assert!(rest.any(|name| name == want), "{side}: expected {expected:?} in order, got {events:?}");
  }
}

async fn plain_server(ctx: &rzmq::Context) -> Result<Socket, ZmqError> {
  let server = ctx.socket(SocketType::Router)?;
  server.set_option(PLAIN_SERVER, true).await?;
  server.set_option(PLAIN_USERNAME, "admin").await?;
  server.set_option(PLAIN_PASSWORD, "secret").await?;
  Ok(server)
}

async fn plain_client_with_wrong_password(ctx: &rzmq::Context) -> Result<Socket, ZmqError> {
  let client = ctx.socket(SocketType::Dealer)?;
  client.set_option(PLAIN_USERNAME, "admin").await?;
  client.set_option(PLAIN_PASSWORD, "wrong").await?;
  Ok(client)
}

/// A monitor attached after `bind` receives the events of a connection the
/// listener accepts afterwards.
#[tokio::test]
#[serial]
async fn monitor_attached_after_bind_receives_events_of_later_connections() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let server = plain_server(&ctx).await?;
  let endpoint = common::bind_and_resolve_tcp(&server).await?;
  let server_monitor = server.monitor_default().await?;

  let client = plain_client_with_wrong_password(&ctx).await?;
  client.connect(&endpoint).await?;

  let events = events_through_disconnect(&server_monitor).await;
  assert_in_order("server", &events, &["Accepted", "HandshakeFailed", "Disconnected"]);
  ctx.term().await
}

/// A monitor attached after `connect` receives the events of a connection that
/// is established afterwards.
#[tokio::test]
#[serial]
async fn monitor_attached_after_connect_receives_events_of_later_connections() -> Result<(), ZmqError> {
  let free = TcpListener::bind("127.0.0.1:0").await.expect("reserve port");
  let endpoint = format!("tcp://{}", free.local_addr().expect("local addr"));
  drop(free);

  let ctx = common::test_context();
  let client = plain_client_with_wrong_password(&ctx).await?;
  client.connect(&endpoint).await?;
  let client_monitor = client.monitor_default().await?;

  let server = plain_server(&ctx).await?;
  server.bind(&endpoint).await?;

  let events = events_through_disconnect(&client_monitor).await;
  assert_in_order("client", &events, &["Connected", "HandshakeFailed", "Disconnected"]);
  ctx.term().await
}
