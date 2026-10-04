use rzmq::socket::SocketEvent;
use rzmq::socket::options::{LINGER, SNDHWM, SNDTIMEO};
use rzmq::{Msg, Socket, SocketType, ZmqError};
use serial_test::serial;
use std::time::Duration;

mod common;

const RECV_TIMEOUT: Duration = Duration::from_secs(3);
const RECONNECT_TIMEOUT: Duration = Duration::from_secs(6);
const MONITOR_POLL: Duration = Duration::from_millis(100);

fn numbered(n: u32) -> Msg {
  Msg::from_vec(n.to_string().into_bytes())
}

fn expected(range: std::ops::RangeInclusive<u32>) -> Vec<String> {
  range.map(|n| n.to_string()).collect()
}

/// Collects the payload frame of up to `n` messages, stopping early at the
/// first `timeout` with nothing received.
async fn collect_payloads(socket: &Socket, n: usize, timeout: Duration) -> Vec<String> {
  let mut got = Vec::with_capacity(n);
  while got.len() < n {
    match tokio::time::timeout(timeout, socket.recv_multipart()).await {
      Ok(Ok(frames)) => {
        let payload = frames.last().and_then(|f| f.data()).unwrap_or_default();
        got.push(String::from_utf8_lossy(payload).into_owned());
      }
      _ => break,
    }
  }
  got
}

async fn wait_for(monitor: &rzmq::socket::MonitorReceiver, timeout: Duration, pick: impl Fn(&SocketEvent) -> bool) {
  common::wait_for_monitor_event(monitor, timeout, MONITOR_POLL, pick)
    .await
    .expect("monitor event");
}

/// Every message a DEALER sends between `connect` returning and the connection
/// attaching reaches the peer, in send order.
#[tokio::test]
#[serial]
async fn dealer_burst_sent_right_after_connect_arrives_complete_and_in_order() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let endpoint = common::bind_and_resolve_tcp(&router).await?;

  let dealer = ctx.socket(SocketType::Dealer)?;
  dealer.connect(&endpoint).await?;
  for n in 1..=100 {
    dealer.send(numbered(n)).await?;
  }

  let got = collect_payloads(&router, 100, RECV_TIMEOUT).await;
  assert_eq!(got, expected(1..=100));
  Ok(())
}

/// Every message a PUSH sends between `connect` returning and the connection
/// attaching reaches the peer, in send order.
#[tokio::test]
#[serial]
async fn push_burst_sent_right_after_connect_arrives_complete_and_in_order() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  let endpoint = common::bind_and_resolve_tcp(&pull).await?;

  let push = ctx.socket(SocketType::Push)?;
  push.connect(&endpoint).await?;
  let sender = tokio::spawn(async move {
    for n in 1..=100 {
      push.send(numbered(n)).await?;
    }
    Ok::<_, ZmqError>(push)
  });

  let got = collect_payloads(&pull, 100, RECV_TIMEOUT).await;
  assert_eq!(got, expected(1..=100));
  sender.await.expect("sender task")?;
  Ok(())
}

/// A REQ request sent right after `connect` is answered.
#[tokio::test]
#[serial]
async fn req_request_sent_right_after_connect_is_answered() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let rep = ctx.socket(SocketType::Rep)?;
  let endpoint = common::bind_and_resolve_tcp(&rep).await?;
  let responder = tokio::spawn(async move {
    let request = common::recv_timeout(&rep, RECV_TIMEOUT).await?;
    assert_eq!(request.data(), Some(b"ping".as_slice()));
    rep.send(Msg::from_static(b"pong")).await?;
    Ok::<_, ZmqError>(rep)
  });

  let req = ctx.socket(SocketType::Req)?;
  req.connect(&endpoint).await?;
  req.send(Msg::from_static(b"ping")).await?;
  let reply = common::recv_timeout(&req, RECV_TIMEOUT).await?;
  assert_eq!(reply.data(), Some(b"pong".as_slice()));
  responder.await.expect("responder task")?;
  Ok(())
}

/// Messages a DEALER sends while its peer is down are delivered, in order, once
/// the peer is back and the connection has been re-established.
#[tokio::test]
#[serial]
async fn dealer_burst_sent_during_reconnect_gap_arrives_complete_and_in_order() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let endpoint = common::bind_and_resolve_tcp(&router).await?;

  let dealer = ctx.socket(SocketType::Dealer)?;
  let monitor = dealer.monitor_default().await?;
  dealer.connect(&endpoint).await?;
  wait_for(&monitor, RECV_TIMEOUT, |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;
  for n in 1..=10 {
    dealer.send(numbered(n)).await?;
  }
  assert_eq!(collect_payloads(&router, 10, RECV_TIMEOUT).await, expected(1..=10));

  router.close().await?;
  wait_for(&monitor, RECV_TIMEOUT, |e| matches!(e, SocketEvent::Disconnected { .. })).await;
  for n in 11..=30 {
    dealer.send(numbered(n)).await?;
  }

  let router = ctx.socket(SocketType::Router)?;
  let mut bound = router.bind(&endpoint).await;
  for _ in 0..20 {
    if bound.is_ok() {
      break;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    bound = router.bind(&endpoint).await;
  }
  bound?;
  wait_for(&monitor, RECONNECT_TIMEOUT, |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;

  let got = collect_payloads(&router, 20, RECV_TIMEOUT).await;
  assert_eq!(got, expected(11..=30));
  Ok(())
}

/// With LINGER -1, `close` on a DEALER delivers the messages it sent before its
/// connection attached, without waiting out a reconnect interval: the connecter
/// keeps its attempt alive through the socket's linger.
#[tokio::test]
#[serial]
async fn dealer_close_with_infinite_linger_delivers_burst_sent_before_attach() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let endpoint = common::bind_and_resolve_tcp(&router).await?;

  let dealer = ctx.socket(SocketType::Dealer)?;
  dealer.set_option(LINGER, -1i32).await?;
  dealer.connect(&endpoint).await?;
  for n in 1..=20 {
    dealer.send(numbered(n)).await?;
  }
  dealer.close().await?;

  let got = collect_payloads(&router, 20, Duration::from_millis(500)).await;
  assert_eq!(got, expected(1..=20));
  Ok(())
}

/// With no peer attached, a DEALER with SNDTIMEO 0 accepts exactly SNDHWM
/// messages and reports the next one as ResourceLimitReached.
#[tokio::test]
#[serial]
async fn dealer_with_no_peer_accepts_exactly_sndhwm_then_reports_full() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let dealer = ctx.socket(SocketType::Dealer)?;
  dealer.set_option(SNDHWM, 10i32).await?;
  dealer.set_option(SNDTIMEO, 0i32).await?;
  dealer.connect("tcp://127.0.0.1:1").await?;

  for n in 1..=10 {
    dealer.send(numbered(n)).await?;
  }
  assert!(matches!(dealer.send(numbered(11)).await, Err(ZmqError::ResourceLimitReached)));
  Ok(())
}
