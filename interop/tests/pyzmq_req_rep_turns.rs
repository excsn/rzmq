mod common;

use anyhow::Result;
use rzmq::socket::options::{RCVTIMEO, REQ_CORRELATE, REQ_RELAXED};
use rzmq::{Msg, Socket, SocketType};
use common::context;
use std::time::Duration;

/// Answers `count` requests with `re:<request>`, holding the reply to `slow` for 400 ms.
fn spawn_echo_rep(rep: Socket, count: usize) -> tokio::task::JoinHandle<Result<Vec<String>>> {
  tokio::spawn(async move {
    let mut served = Vec::new();
    for _ in 0..count {
      let request = String::from_utf8_lossy(rep.recv().await?.data().unwrap_or(&[])).into_owned();
      if request == "slow" {
        tokio::time::sleep(Duration::from_millis(400)).await;
      }
      rep.send(Msg::from_vec(format!("re:{}", request).into_bytes())).await?;
      served.push(request);
    }
    Ok(served)
  })
}

/// A libzmq REQ with ZMQ_REQ_CORRELATE relies on the REP echoing its request id
/// frame. rzmq's REP must echo it so libzmq can discard the late reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rzmq_rep_echoes_libzmq_request_id() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  let ctx = context()?;
  let rep = ctx.socket(SocketType::Rep)?;
  rep.bind(&endpoint).await?;
  let server = spawn_echo_rep(rep, 2);

  let (_guard, mut reader) =
    common::spawn_and_wait_ready("req_turns_client.py", &[endpoint.as_str(), "correlate"], Duration::from_secs(5))?;
  common::wait_for_line(&mut reader, "SUCCESS - re:next", Duration::from_secs(5))?;

  let served = tokio::time::timeout(Duration::from_secs(5), server).await??;
  assert_eq!(served?, vec!["slow".to_string(), "next".to_string()]);
  Ok(())
}

/// rzmq's REP drops the reply to a libzmq REQ that gave up and left, and serves
/// the next requester.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rzmq_rep_drops_reply_to_departed_libzmq_req() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  let ctx = context()?;
  let rep = ctx.socket(SocketType::Rep)?;
  rep.bind(&endpoint).await?;

  let (guard, mut reader) =
    common::spawn_and_wait_ready("req_turns_client.py", &[endpoint.as_str(), "giveup"], Duration::from_secs(5))?;
  let request = tokio::time::timeout(Duration::from_secs(5), rep.recv()).await??;
  assert_eq!(request.data().unwrap(), b"slow");
  common::wait_for_line(&mut reader, "Gave up.", Duration::from_secs(5))?;
  drop(guard);
  tokio::time::sleep(Duration::from_millis(300)).await;

  rep.send(Msg::from_static(b"re:slow")).await?;

  let req = ctx.socket(SocketType::Req)?;
  req.set_option(RCVTIMEO, 3000i32).await?;
  req.connect(&endpoint).await?;
  req.send(Msg::from_static(b"next")).await?;
  assert_eq!(rep.recv().await?.data().unwrap(), b"next");
  rep.send(Msg::from_static(b"re:next")).await?;
  assert_eq!(req.recv().await?.data().unwrap(), b"re:next");
  Ok(())
}

/// An rzmq REQ with REQ_CORRELATE against a libzmq REP, which echoes the request id
/// frame as part of the envelope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rzmq_correlated_req_against_libzmq_rep() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();

  let (_guard, _reader) =
    common::spawn_and_wait_ready("rep_server.py", &[endpoint.as_str()], Duration::from_secs(5))?;

  let ctx = context()?;
  let req = ctx.socket(SocketType::Req)?;
  req.set_option(RCVTIMEO, 3000i32).await?;
  req.set_option(REQ_RELAXED, 1i32).await?;
  req.set_option(REQ_CORRELATE, 1i32).await?;
  req.connect(&endpoint).await?;

  for word in ["one", "two"] {
    req.send(Msg::from_static(word.as_bytes())).await?;
    let reply = req.recv().await?;
    assert_eq!(reply.data().unwrap(), format!("{}-reply", word).as_bytes());
  }

  req.send(Msg::from_static(b"shutdown")).await?;
  assert_eq!(req.recv().await?.data().unwrap(), b"shutdown-reply");
  Ok(())
}
