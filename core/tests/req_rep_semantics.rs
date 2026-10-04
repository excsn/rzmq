use rzmq::socket::options::{AUTO_DELIMITER, RCVTIMEO, REQ_CORRELATE, REQ_RELAXED};
use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use std::time::Duration;
mod common;

const SAFETY: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum Transport {
  Tcp,
  Inproc,
}

async fn bind(socket: &Socket, transport: Transport) -> Result<String, ZmqError> {
  match transport {
    Transport::Tcp => common::bind_and_resolve_tcp(socket).await,
    Transport::Inproc => {
      let endpoint = common::unique_inproc_endpoint();
      socket.bind(&endpoint).await?;
      Ok(endpoint)
    }
  }
}

fn text(m: &Msg) -> String {
  String::from_utf8_lossy(m.data().unwrap_or(&[])).into_owned()
}

async fn req_with_rcvtimeo(ctx: &Context, endpoint: &str, rcvtimeo_ms: i32) -> Result<Socket, ZmqError> {
  let req = ctx.socket(SocketType::Req)?;
  req.set_option(RCVTIMEO, rcvtimeo_ms).await?;
  req.connect(endpoint).await?;
  Ok(req)
}

/// Answers `count` requests with `re:<request>`, holding the reply to `slow` for 400 ms.
fn spawn_echo_rep(rep: Socket, count: usize) -> tokio::task::JoinHandle<Result<Vec<String>, ZmqError>> {
  tokio::spawn(async move {
    let mut served = Vec::new();
    for _ in 0..count {
      let request = text(&rep.recv().await?);
      if request == "slow" {
        tokio::time::sleep(Duration::from_millis(400)).await;
      }
      rep.send(Msg::from_vec(format!("re:{}", request).into_bytes())).await?;
      served.push(request);
    }
    Ok(served)
  })
}

// --- Turn-taking after a timeout ---

/// By default a timed-out request stays outstanding: a new send is out of order and
/// a later recv still gets the late reply.
async fn strict_req_after_timeout_refuses_send_and_gets_late_reply(transport: Transport) -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, transport).await?;
    let server = spawn_echo_rep(rep, 1);

    let req = req_with_rcvtimeo(&ctx, &endpoint, 250).await?;
    req.send(Msg::from_static(b"slow")).await?;
    assert!(matches!(req.recv().await, Err(ZmqError::Timeout)));

    let refused = req.send(Msg::from_static(b"next")).await;
    assert!(matches!(refused, Err(ZmqError::InvalidState(_))), "got {:?}", refused);

    req.set_option(RCVTIMEO, 2000i32).await?;
    assert_eq!(text(&req.recv().await?), "re:slow");
    tokio::time::timeout(SAFETY, server).await.expect("server stuck").unwrap()?;
  }
  ctx.term().await
}

#[tokio::test]
async fn strict_req_after_timeout_refuses_send_and_gets_late_reply_tcp() -> Result<(), ZmqError> {
  strict_req_after_timeout_refuses_send_and_gets_late_reply(Transport::Tcp).await
}

#[tokio::test]
async fn strict_req_after_timeout_refuses_send_and_gets_late_reply_inproc() -> Result<(), ZmqError> {
  strict_req_after_timeout_refuses_send_and_gets_late_reply(Transport::Inproc).await
}

/// A recv after a timeout waits for the still-outstanding reply.
#[tokio::test]
async fn req_recv_after_timeout_waits_again() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, Transport::Tcp).await?;
    let req = req_with_rcvtimeo(&ctx, &endpoint, 200).await?;

    req.send(Msg::from_static(b"unanswered")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "unanswered");
    assert!(matches!(req.recv().await, Err(ZmqError::Timeout)));
    let started = std::time::Instant::now();
    let again = req.recv().await;
    assert!(matches!(again, Err(ZmqError::Timeout)), "got {:?}", again);
    assert!(started.elapsed() >= Duration::from_millis(150), "returned after {:?}", started.elapsed());
  }
  ctx.term().await
}

// --- REQ_RELAXED and REQ_CORRELATE ---

async fn relaxed_req(ctx: &Context, endpoint: &str, rcvtimeo_ms: i32, correlate: bool) -> Result<Socket, ZmqError> {
  let req = ctx.socket(SocketType::Req)?;
  req.set_option(RCVTIMEO, rcvtimeo_ms).await?;
  req.set_option(REQ_RELAXED, 1i32).await?;
  if correlate {
    req.set_option(REQ_CORRELATE, 1i32).await?;
  }
  req.connect(endpoint).await?;
  Ok(req)
}

/// The late reply to the abandoned request arrives after the new request went out.
/// Only the request id tells it apart.
async fn relaxed_correlated_req_discards_late_reply(transport: Transport) -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, transport).await?;
    let server = spawn_echo_rep(rep, 2);

    let req = relaxed_req(&ctx, &endpoint, 250, true).await?;
    req.send(Msg::from_static(b"slow")).await?;
    assert!(matches!(req.recv().await, Err(ZmqError::Timeout)));

    req.set_option(RCVTIMEO, 2000i32).await?;
    req.send(Msg::from_static(b"next")).await?;
    assert_eq!(text(&req.recv().await?), "re:next");
    tokio::time::timeout(SAFETY, server).await.expect("server stuck").unwrap()?;
  }
  ctx.term().await
}

#[tokio::test]
async fn relaxed_correlated_req_discards_late_reply_tcp() -> Result<(), ZmqError> {
  relaxed_correlated_req_discards_late_reply(Transport::Tcp).await
}

#[tokio::test]
async fn relaxed_correlated_req_discards_late_reply_inproc() -> Result<(), ZmqError> {
  relaxed_correlated_req_discards_late_reply(Transport::Inproc).await
}

/// A reply that arrived before the new request is discarded by the send itself,
/// with or without request ids.
async fn relaxed_req_discards_reply_queued_before_next_send(transport: Transport) -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, transport).await?;
    let server = spawn_echo_rep(rep, 2);

    let req = relaxed_req(&ctx, &endpoint, 250, false).await?;
    req.send(Msg::from_static(b"slow")).await?;
    assert!(matches!(req.recv().await, Err(ZmqError::Timeout)));

    tokio::time::sleep(Duration::from_millis(300)).await;
    req.set_option(RCVTIMEO, 2000i32).await?;
    req.send(Msg::from_static(b"next")).await?;
    assert_eq!(text(&req.recv().await?), "re:next");
    tokio::time::timeout(SAFETY, server).await.expect("server stuck").unwrap()?;
  }
  ctx.term().await
}

#[tokio::test]
async fn relaxed_req_discards_reply_queued_before_next_send_tcp() -> Result<(), ZmqError> {
  relaxed_req_discards_reply_queued_before_next_send(Transport::Tcp).await
}

#[tokio::test]
async fn relaxed_req_discards_reply_queued_before_next_send_inproc() -> Result<(), ZmqError> {
  relaxed_req_discards_reply_queued_before_next_send(Transport::Inproc).await
}

#[tokio::test]
async fn req_relaxed_and_correlate_options_round_trip() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let req = ctx.socket(SocketType::Req)?;
    let read = |raw: Vec<u8>| i32::from_ne_bytes(raw.try_into().unwrap());
    assert_eq!(read(req.get_option(REQ_RELAXED).await?), 0);
    assert_eq!(read(req.get_option(REQ_CORRELATE).await?), 0);
    req.set_option(REQ_RELAXED, 1i32).await?;
    req.set_option(REQ_CORRELATE, 1i32).await?;
    assert_eq!(read(req.get_option(REQ_RELAXED).await?), 1);
    assert_eq!(read(req.get_option(REQ_CORRELATE).await?), 1);

    let rep = ctx.socket(SocketType::Rep)?;
    let refused = rep.set_option(REQ_RELAXED, 1i32).await;
    assert!(matches!(refused, Err(ZmqError::InvalidOption(REQ_RELAXED))), "got {:?}", refused);
  }
  ctx.term().await
}

/// A correlated REQ against a ROUTER with manual framing: the ROUTER sees
/// `[identity, request id, delimiter, body]` and echoes the envelope back.
#[tokio::test]
async fn correlated_req_round_trips_through_router_with_manual_framing() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let router = ctx.socket(SocketType::Router)?;
    router.set_option(AUTO_DELIMITER, false).await?;
    let endpoint = bind(&router, Transport::Tcp).await?;

    let req = ctx.socket(SocketType::Req)?;
    req.set_option(RCVTIMEO, 2000i32).await?;
    req.set_option(REQ_CORRELATE, 1i32).await?;
    req.connect(&endpoint).await?;
    req.send(Msg::from_static(b"hello")).await?;

    let frames = tokio::time::timeout(SAFETY, router.recv_multipart()).await.expect("router stuck")?;
    assert_eq!(frames.len(), 4, "frames {:?}", frames);
    assert_eq!(frames[1].size(), 4);
    assert_eq!(frames[2].size(), 0);
    assert_eq!(text(&frames[3]), "hello");

    let mut reply = frames;
    reply[3] = Msg::from_static(b"world");
    router.send_multipart(reply).await?;
    assert_eq!(text(&req.recv().await?), "world");
  }
  ctx.term().await
}

/// A message from a peer other than the one the request went to is not the reply.
#[tokio::test]
async fn req_ignores_message_from_peer_that_did_not_get_the_request() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let dealer_a = ctx.socket(SocketType::Dealer)?;
    let dealer_b = ctx.socket(SocketType::Dealer)?;
    let endpoint_a = bind(&dealer_a, Transport::Tcp).await?;
    let endpoint_b = bind(&dealer_b, Transport::Tcp).await?;

    let req = ctx.socket(SocketType::Req)?;
    req.set_option(RCVTIMEO, 2000i32).await?;
    req.connect(&endpoint_a).await?;
    req.connect(&endpoint_b).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    req.send(Msg::from_static(b"q")).await?;
    let (holder, other) = tokio::select! {
      r = dealer_a.recv_multipart() => { r?; (dealer_a, dealer_b) }
      r = dealer_b.recv_multipart() => { r?; (dealer_b, dealer_a) }
    };

    other.send(Msg::from_static(b"intruder")).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    holder.send(Msg::from_static(b"answer")).await?;
    assert_eq!(text(&req.recv().await?), "answer");
  }
  ctx.term().await
}

// --- Routing ---

#[tokio::test]
async fn req_round_robins_across_connected_reps() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep_a = ctx.socket(SocketType::Rep)?;
    let rep_b = ctx.socket(SocketType::Rep)?;
    let endpoint_a = bind(&rep_a, Transport::Tcp).await?;
    let endpoint_b = bind(&rep_b, Transport::Tcp).await?;

    let tag = |rep: Socket, name: &'static str| {
      tokio::spawn(async move {
        let mut served = 0usize;
        while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(1500), rep.recv()).await {
          rep.send(Msg::from_static(name.as_bytes())).await?;
          served += 1;
        }
        Ok::<_, ZmqError>(served)
      })
    };
    let server_a = tag(rep_a, "a");
    let server_b = tag(rep_b, "b");

    let req = ctx.socket(SocketType::Req)?;
    req.set_option(RCVTIMEO, 2000i32).await?;
    req.connect(&endpoint_a).await?;
    req.connect(&endpoint_b).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut answers = Vec::new();
    for _ in 0..4 {
      req.send(Msg::from_static(b"q")).await?;
      answers.push(text(&req.recv().await?));
    }
    assert_eq!(answers.iter().filter(|a| *a == "a").count(), 2, "answers {:?}", answers);
    assert_eq!(answers.iter().filter(|a| *a == "b").count(), 2, "answers {:?}", answers);
    assert!(answers.windows(2).all(|w| w[0] != w[1]), "not alternating: {:?}", answers);

    assert_eq!(server_a.await.unwrap()?, 2);
    assert_eq!(server_b.await.unwrap()?, 2);
  }
  ctx.term().await
}

/// When the REP holding a request leaves, a relaxed REQ times out and its next
/// request goes to the REP that is still there.
#[tokio::test]
async fn relaxed_req_reaches_remaining_rep_after_one_leaves_mid_request() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep_a = ctx.socket(SocketType::Rep)?;
    let rep_b = ctx.socket(SocketType::Rep)?;
    let endpoint_a = bind(&rep_a, Transport::Tcp).await?;
    let endpoint_b = bind(&rep_b, Transport::Tcp).await?;

    let req = ctx.socket(SocketType::Req)?;
    req.set_option(RCVTIMEO, 250i32).await?;
    req.set_option(REQ_RELAXED, 1i32).await?;
    req.connect(&endpoint_a).await?;
    req.connect(&endpoint_b).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    req.send(Msg::from_static(b"first")).await?;
    let (holder, other) = tokio::select! {
      r = rep_a.recv() => { r?; (rep_a, rep_b) }
      r = rep_b.recv() => { r?; (rep_b, rep_a) }
    };
    holder.close().await?;
    let lost = req.recv().await;
    assert!(matches!(lost, Err(ZmqError::Timeout)), "got {:?}", lost);

    let server = spawn_echo_rep(other, 1);
    req.set_option(RCVTIMEO, 2000i32).await?;
    req.send(Msg::from_static(b"second")).await?;
    assert_eq!(text(&req.recv().await?), "re:second");
    tokio::time::timeout(SAFETY, server).await.expect("server stuck").unwrap()?;
  }
  ctx.term().await
}

/// Two requesters waiting on one REP each get the reply to their own request.
async fn rep_routes_each_reply_to_its_requester(transport: Transport) -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, transport).await?;
    let server = spawn_echo_rep(rep, 2);

    let one = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;
    let two = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    one.send(Msg::from_static(b"one")).await?;
    two.send(Msg::from_static(b"two")).await?;
    let (reply_one, reply_two) = tokio::join!(one.recv(), two.recv());
    assert_eq!(text(&reply_one?), "re:one");
    assert_eq!(text(&reply_two?), "re:two");
    tokio::time::timeout(SAFETY, server).await.expect("server stuck").unwrap()?;
  }
  ctx.term().await
}

#[tokio::test]
async fn rep_routes_each_reply_to_its_requester_tcp() -> Result<(), ZmqError> {
  rep_routes_each_reply_to_its_requester(Transport::Tcp).await
}

#[tokio::test]
async fn rep_routes_each_reply_to_its_requester_inproc() -> Result<(), ZmqError> {
  rep_routes_each_reply_to_its_requester(Transport::Inproc).await
}

// --- Delivery around a closing REP ---

/// A reply already sent when the REP closes still reaches the REQ.
async fn reply_sent_before_rep_closes_is_delivered(transport: Transport) -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, transport).await?;
    let req = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;

    req.send(Msg::from_static(b"request")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "request");
    rep.send(Msg::from_static(b"reply")).await?;
    rep.close().await?;
    assert_eq!(text(&req.recv().await?), "reply");
  }
  ctx.term().await
}

#[tokio::test]
async fn reply_sent_before_rep_closes_is_delivered_tcp() -> Result<(), ZmqError> {
  reply_sent_before_rep_closes_is_delivered(Transport::Tcp).await
}

#[tokio::test]
async fn reply_sent_before_rep_closes_is_delivered_inproc() -> Result<(), ZmqError> {
  reply_sent_before_rep_closes_is_delivered(Transport::Inproc).await
}

/// A REQ that was idle when its REP went away works with the REP that replaces it.
#[tokio::test]
async fn req_idle_through_rep_restart_works_with_new_rep() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, Transport::Tcp).await?;
    let req = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;

    req.send(Msg::from_static(b"before")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "before");
    rep.send(Msg::from_static(b"re:before")).await?;
    assert_eq!(text(&req.recv().await?), "re:before");
    rep.close().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rep2 = ctx.socket(SocketType::Rep)?;
    rep2.bind(&endpoint).await?;
    let server = spawn_echo_rep(rep2, 1);
    req.send(Msg::from_static(b"after")).await?;
    assert_eq!(text(&req.recv().await?), "re:after");
    tokio::time::timeout(SAFETY, server).await.expect("server stuck").unwrap()?;
  }
  ctx.term().await
}

// --- Multipart replies ---

async fn multipart_reply_arrives_intact(transport: Transport) -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, transport).await?;
    let req = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;

    req.send(Msg::from_static(b"parts")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "parts");
    rep
      .send_multipart(vec![Msg::from_static(b"one"), Msg::from_static(b"two"), Msg::from_static(b"three")])
      .await?;
    let frames = req.recv_multipart().await?;
    assert_eq!(frames.iter().map(text).collect::<Vec<_>>(), vec!["one", "two", "three"]);
  }
  ctx.term().await
}

#[tokio::test]
async fn multipart_reply_arrives_intact_tcp() -> Result<(), ZmqError> {
  multipart_reply_arrives_intact(Transport::Tcp).await
}

#[tokio::test]
async fn multipart_reply_arrives_intact_inproc() -> Result<(), ZmqError> {
  multipart_reply_arrives_intact(Transport::Inproc).await
}

// --- REP turn-taking around a dropped reply ---

/// Dropping the reply to a departed requester uses up the REP's turn, so a
/// second send without a recv is out of order.
#[tokio::test]
async fn rep_second_send_after_dropped_reply_is_invalid_state() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, Transport::Tcp).await?;
    let req = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;
    req.send(Msg::from_static(b"request")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "request");
    req.close().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dropped = rep.send(Msg::from_static(b"reply")).await;
    assert!(dropped.is_ok(), "expected the reply to be dropped, got {:?}", dropped);
    let second = rep.send(Msg::from_static(b"again")).await;
    assert!(matches!(second, Err(ZmqError::InvalidState(_))), "got {:?}", second);
  }
  ctx.term().await
}

// --- Shutdown while waiting ---

/// Closing a REQ wakes a recv that is waiting on it.
#[tokio::test]
async fn req_recv_returns_when_req_is_closed() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind(&rep, Transport::Tcp).await?;
    let req = ctx.socket(SocketType::Req)?;
    req.connect(&endpoint).await?;
    req.send(Msg::from_static(b"request")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "request");

    let waiting = req.clone();
    let pending = tokio::spawn(async move { waiting.recv().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    req.close().await?;
    let outcome = tokio::time::timeout(Duration::from_secs(2), pending)
      .await
      .expect("recv still waiting after close")
      .unwrap();
    assert!(outcome.is_err(), "recv returned {:?} after close", outcome);
  }
  ctx.term().await
}

/// Terminating the context wakes a recv that is waiting and lets term finish.
#[tokio::test]
async fn req_recv_returns_when_context_terminates() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let rep = ctx.socket(SocketType::Rep)?;
  let endpoint = bind(&rep, Transport::Tcp).await?;
  let req = ctx.socket(SocketType::Req)?;
  req.connect(&endpoint).await?;
  req.send(Msg::from_static(b"request")).await?;
  assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "request");

  let waiting = req.clone();
  let pending = tokio::spawn(async move { waiting.recv().await });
  tokio::time::sleep(Duration::from_millis(100)).await;
  tokio::time::timeout(Duration::from_secs(3), ctx.term())
    .await
    .expect("context term did not finish while a recv was waiting")?;
  let outcome = tokio::time::timeout(Duration::from_secs(2), pending)
    .await
    .expect("recv still waiting after term")
    .unwrap();
  assert!(outcome.is_err(), "recv returned {:?} after term", outcome);
  Ok(())
}
