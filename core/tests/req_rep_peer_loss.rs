use rzmq::socket::options::{RCVTIMEO, REQ_CORRELATE, REQ_RELAXED};
use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use std::time::{Duration, Instant};
mod common;

const SAFETY: Duration = Duration::from_secs(5);

fn text(m: &Msg) -> String {
  String::from_utf8_lossy(m.data().unwrap_or(&[])).into_owned()
}

async fn req_with_rcvtimeo(ctx: &Context, endpoint: &str, rcvtimeo_ms: i32) -> Result<Socket, ZmqError> {
  let req = ctx.socket(SocketType::Req)?;
  req.set_option(RCVTIMEO, rcvtimeo_ms).await?;
  req.connect(endpoint).await?;
  Ok(req)
}

async fn bind_inproc(socket: &Socket) -> Result<String, ZmqError> {
  let endpoint = common::unique_inproc_endpoint();
  socket.bind(&endpoint).await?;
  Ok(endpoint)
}

/// A REP server that stops at its first error survives a client that gives up
/// on a slow reply and leaves, and answers the next client.
#[tokio::test]
async fn rep_server_keeps_serving_after_requester_leaves_mid_request() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = common::bind_and_resolve_tcp(&rep).await?;

    let server = tokio::spawn(async move {
      let mut answered = Vec::new();
      for _ in 0..2 {
        let request = text(&rep.recv().await?);
        if request == "slow" {
          tokio::time::sleep(Duration::from_millis(400)).await;
        }
        rep.send(Msg::from_vec(format!("re:{}", request).into_bytes())).await?;
        answered.push(request);
      }
      Ok::<_, ZmqError>(answered)
    });

    let impatient = req_with_rcvtimeo(&ctx, &endpoint, 250).await?;
    impatient.send(Msg::from_static(b"slow")).await?;
    assert!(matches!(impatient.recv().await, Err(ZmqError::Timeout)));
    impatient.close().await?;

    let next = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    next.send(Msg::from_static(b"next")).await?;
    assert_eq!(text(&next.recv().await?), "re:next");

    let answered = tokio::time::timeout(SAFETY, server).await.expect("server stuck").unwrap()?;
    assert_eq!(answered, vec!["slow".to_string(), "next".to_string()]);
  }
  ctx.term().await
}

/// A REP holding a request keeps its turn when the requester leaves: recv is out of
/// order until the reply is sent, and that send drops the reply.
#[tokio::test]
async fn rep_keeps_its_turn_after_requester_leaves() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = common::bind_and_resolve_tcp(&rep).await?;

    let first = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;
    first.send(Msg::from_static(b"first")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "first");
    first.close().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let second = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    second.send(Msg::from_static(b"second")).await?;

    let out_of_turn = rep.recv().await;
    assert!(matches!(out_of_turn, Err(ZmqError::InvalidState(_))), "got {:?}", out_of_turn);
    rep.send(Msg::from_static(b"re:first")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "second");
    rep.send(Msg::from_static(b"re:second")).await?;
    assert_eq!(text(&second.recv().await?), "re:second");
  }
  ctx.term().await
}

#[tokio::test]
async fn rep_reply_to_departed_requester_is_dropped_inproc() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind_inproc(&rep).await?;

    let req = req_with_rcvtimeo(&ctx, &endpoint, 2000).await?;
    req.send(Msg::from_static(b"request")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "request");
    req.close().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let send_result = rep.send(Msg::from_static(b"reply")).await;
    assert!(send_result.is_ok(), "expected the reply to be dropped, got {:?}", send_result);
  }
  ctx.term().await
}

/// Without RCVTIMEO a REQ whose REP closed keeps waiting, as libzmq does.
#[tokio::test]
async fn req_recv_keeps_waiting_when_rep_closes_without_rcvtimeo() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = common::bind_and_resolve_tcp(&rep).await?;
    let req = ctx.socket(SocketType::Req)?;
    req.connect(&endpoint).await?;

    req.send(Msg::from_static(b"request")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "request");
    rep.close().await?;

    let outcome = tokio::time::timeout(Duration::from_millis(500), req.recv()).await;
    assert!(outcome.is_err(), "recv returned {:?} instead of waiting", outcome);
  }
  ctx.term().await
}

#[tokio::test]
async fn req_recv_times_out_when_rep_closes_inproc() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = bind_inproc(&rep).await?;
    let req = req_with_rcvtimeo(&ctx, &endpoint, 250).await?;

    req.send(Msg::from_static(b"request")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "request");
    rep.close().await?;

    let started = Instant::now();
    let recv_result = req.recv().await;
    assert!(matches!(recv_result, Err(ZmqError::Timeout)), "got {:?}", recv_result);
    assert!(started.elapsed() >= Duration::from_millis(200), "gave up after {:?}", started.elapsed());
  }
  ctx.term().await
}

/// A relaxed REQ that timed out on a vanished REP sends again and gets a reply
/// from the REP that replaces it.
#[tokio::test]
async fn relaxed_req_recovers_when_rep_restarts_on_same_endpoint() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let rep = ctx.socket(SocketType::Rep)?;
    let endpoint = common::bind_and_resolve_tcp(&rep).await?;
    let req = req_with_rcvtimeo(&ctx, &endpoint, 250).await?;
    req.set_option(REQ_RELAXED, 1i32).await?;
    req.set_option(REQ_CORRELATE, 1i32).await?;

    req.send(Msg::from_static(b"lost")).await?;
    assert_eq!(text(&common::recv_timeout(&rep, SAFETY).await?), "lost");
    rep.close().await?;
    let recv_result = req.recv().await;
    assert!(matches!(recv_result, Err(ZmqError::Timeout)), "got {:?}", recv_result);

    let rep2 = ctx.socket(SocketType::Rep)?;
    rep2.bind(&endpoint).await?;
    let server = tokio::spawn(async move {
      let request = text(&rep2.recv().await?);
      rep2.send(Msg::from_vec(format!("re:{}", request).into_bytes())).await?;
      Ok::<_, ZmqError>(request)
    });

    req.set_option(RCVTIMEO, 3000i32).await?;
    req.send(Msg::from_static(b"again")).await?;
    assert_eq!(text(&req.recv().await?), "re:again");
    let served = tokio::time::timeout(SAFETY, server).await.expect("server stuck").unwrap()?;
    assert_eq!(served, "again");
  }
  ctx.term().await
}
