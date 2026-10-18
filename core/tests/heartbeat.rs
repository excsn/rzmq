use rzmq::socket::options::{HEARTBEAT_IVL, HEARTBEAT_TIMEOUT};
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Msg, SocketType, ZmqError};
use serial_test::serial;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

mod common;

const EVENT_TIMEOUT: Duration = Duration::from_secs(3);

/// ZMTP 3.1 greeting for the NULL mechanism, as a client.
fn null_greeting() -> Vec<u8> {
  let mut g = vec![0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x7F, 3, 1];
  let mut mechanism = [0u8; 20];
  mechanism[..4].copy_from_slice(b"NULL");
  g.extend_from_slice(&mechanism);
  g.push(0);
  g.extend_from_slice(&[0u8; 31]);
  g
}

fn ready_command(socket_type: &str) -> Vec<u8> {
  let mut body = vec![5];
  body.extend_from_slice(b"READY");
  body.push(11);
  body.extend_from_slice(b"Socket-Type");
  body.extend_from_slice(&(socket_type.len() as u32).to_be_bytes());
  body.extend_from_slice(socket_type.as_bytes());
  let mut frame = vec![0x04, body.len() as u8];
  frame.extend_from_slice(&body);
  frame
}

/// A ROUTER peer that completes the NULL handshake and then never writes again.
/// Everything it reads is forwarded on the returned channel.
async fn silent_router_peer() -> (String, mpsc::UnboundedReceiver<Vec<u8>>) {
  let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind raw peer");
  let endpoint = format!("tcp://{}", listener.local_addr().expect("local addr"));
  let (tx, rx) = mpsc::unbounded_channel();
  tokio::spawn(async move {
    while let Ok((mut stream, _)) = listener.accept().await {
      let tx = tx.clone();
      tokio::spawn(async move {
        let mut handshake = null_greeting();
        handshake.extend_from_slice(&ready_command("ROUTER"));
        if stream.write_all(&handshake).await.is_err() {
          return;
        }
        let mut buf = [0u8; 4096];
        while let Ok(n) = stream.read(&mut buf).await {
          if n == 0 || tx.send(buf[..n].to_vec()).is_err() {
            return;
          }
        }
      });
    }
  });
  (endpoint, rx)
}

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

fn contains_ping(bytes: &[u8]) -> bool {
  bytes.windows(5).any(|w| w == b"\x04PING")
}

#[tokio::test]
#[serial]
async fn peer_that_stops_answering_is_disconnected_by_heartbeat_timeout() -> Result<(), ZmqError> {
  let (endpoint, mut peer_reads) = silent_router_peer().await;
  let ctx = common::test_context();
  let dealer = ctx.socket(SocketType::Dealer)?;
  let monitor = dealer.monitor_default().await?;
  dealer.set_option(HEARTBEAT_IVL, 100i32).await?;
  dealer.set_option(HEARTBEAT_TIMEOUT, 300i32).await?;
  dealer.connect(&endpoint).await?;

  let events = events_through_disconnect(&monitor).await;
  let succeeded = events.iter().position(|e| matches!(e, SocketEvent::HandshakeSucceeded { .. }));
  let disconnected = events.iter().position(|e| matches!(e, SocketEvent::Disconnected { .. }));
  assert!(
    matches!((succeeded, disconnected), (Some(s), Some(d)) if s < d),
    "expected HandshakeSucceeded then Disconnected, got {events:?}"
  );
  assert!(!events.iter().any(|e| matches!(e, SocketEvent::HandshakeFailed { .. })), "{events:?}");

  let mut read = Vec::new();
  while let Ok(chunk) = peer_reads.try_recv() {
    read.extend_from_slice(&chunk);
  }
  assert!(contains_ping(&read), "peer never received a PING");
  ctx.term().await
}

#[tokio::test]
#[serial]
async fn heartbeating_peers_stay_connected_while_idle() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  router.set_option(HEARTBEAT_IVL, 50i32).await?;
  router.set_option(HEARTBEAT_TIMEOUT, 200i32).await?;
  let endpoint = common::bind_and_resolve_tcp(&router).await?;

  let dealer = ctx.socket(SocketType::Dealer)?;
  let monitor = dealer.monitor_default().await?;
  dealer.set_option(HEARTBEAT_IVL, 50i32).await?;
  dealer.set_option(HEARTBEAT_TIMEOUT, 200i32).await?;
  dealer.connect(&endpoint).await?;
  common::wait_for_monitor_event(&monitor, EVENT_TIMEOUT, Duration::from_millis(50), |e| {
    matches!(e, SocketEvent::HandshakeSucceeded { .. })
  })
  .await
  .expect("handshake");

  tokio::time::sleep(Duration::from_secs(1)).await;
  while let Ok(event) = monitor.try_recv() {
    assert!(!matches!(event, SocketEvent::Disconnected { .. }), "idle connection dropped: {event:?}");
  }

  dealer.send(Msg::from_static(b"after idle")).await?;
  let frames = tokio::time::timeout(EVENT_TIMEOUT, router.recv_multipart())
    .await
    .map_err(|_| ZmqError::Timeout)??;
  assert_eq!(frames.last().and_then(|m| m.data()), Some(&b"after idle"[..]));
  ctx.term().await
}
