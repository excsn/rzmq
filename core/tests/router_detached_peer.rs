use rzmq::socket::options::LAST_ENDPOINT;
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Msg, Socket, SocketType, ZmqError};
use std::time::Duration;

mod common;

const EVENT_TIMEOUT: Duration = Duration::from_secs(3);
const RECV_TIMEOUT: Duration = Duration::from_millis(500);
const MONITOR_POLL: Duration = Duration::from_millis(100);

async fn wait_for(monitor: &MonitorReceiver, pick: impl Fn(&SocketEvent) -> bool) {
  common::wait_for_monitor_event(monitor, EVENT_TIMEOUT, MONITOR_POLL, pick)
    .await
    .expect("monitor event");
}

/// Returns once the ROUTER core has run `pipe_detached` for the peer: the core
/// emits Disconnected and detaches the pipe in one step, so a command sent after
/// the event is processed after the detach.
async fn wait_until_detached(router: &Socket, monitor: &MonitorReceiver) {
  wait_for(monitor, |e| matches!(e, SocketEvent::Disconnected { .. })).await;
  router.get_option(LAST_ENDPOINT).await.expect("core round trip");
}

async fn recv_frames(router: &Socket) -> Option<Vec<Msg>> {
  tokio::time::timeout(RECV_TIMEOUT, router.recv_multipart()).await.ok()?.ok()
}

fn payload(frames: &[Msg]) -> String {
  String::from_utf8_lossy(frames.last().and_then(|f| f.data()).unwrap_or_default()).into_owned()
}

/// A ROUTER delivers the messages a DEALER sent before disconnecting, with the
/// same framing as while it was connected, when recv starts after the detach.
#[tokio::test]
async fn router_delivers_dealer_messages_queued_before_disconnect() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let monitor = router.monitor_default().await?;
  let endpoint = common::bind_and_resolve_tcp(&router).await?;

  let dealer = ctx.socket(SocketType::Dealer)?;
  dealer.connect(&endpoint).await?;
  wait_for(&monitor, |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;

  dealer.send(Msg::from_static(b"live")).await?;
  let live = recv_frames(&router).await.expect("live message");
  assert_eq!(payload(&live), "live");

  for n in 1..=5 {
    dealer.send(Msg::from_vec(n.to_string().into_bytes())).await?;
  }
  dealer.close().await?;
  wait_until_detached(&router, &monitor).await;

  let mut got = Vec::new();
  while let Some(frames) = recv_frames(&router).await {
    assert_eq!(frames.len(), live.len(), "framing after detach: {:?}", frames);
    assert_eq!(frames[0].data(), live[0].data(), "identity after detach");
    got.push(payload(&frames));
  }
  assert_eq!(got, ["1", "2", "3", "4", "5"]);
  ctx.term().await
}

/// A ROUTER delivers a REQ's request sent before the REQ disconnected, with the
/// REQ delimiter handled as it was while connected.
#[tokio::test]
async fn router_delivers_req_request_queued_before_disconnect() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let router = ctx.socket(SocketType::Router)?;
  let monitor = router.monitor_default().await?;
  let endpoint = common::bind_and_resolve_tcp(&router).await?;

  let req = ctx.socket(SocketType::Req)?;
  req.connect(&endpoint).await?;
  wait_for(&monitor, |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;

  req.send(Msg::from_static(b"live")).await?;
  let live = recv_frames(&router).await.expect("live request");
  assert_eq!(payload(&live), "live");
  let mut reply = live[..live.len() - 1].to_vec();
  reply.push(Msg::from_static(b"ok"));
  router.send_multipart(reply).await?;
  assert_eq!(common::recv_timeout(&req, EVENT_TIMEOUT).await?.data(), Some(b"ok".as_slice()));

  req.send(Msg::from_static(b"queued")).await?;
  req.close().await?;
  wait_until_detached(&router, &monitor).await;

  let frames = recv_frames(&router).await.expect("request queued before disconnect");
  assert_eq!(frames.len(), live.len(), "framing after detach: {:?}", frames);
  assert_eq!(payload(&frames), "queued");
  ctx.term().await
}
