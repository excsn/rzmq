mod common;

use anyhow::Result;
use rzmq::socket::options::{PLAIN_PASSWORD, PLAIN_SERVER, PLAIN_USERNAME, RCVTIMEO};
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Msg, Socket, SocketType};
use common::context;
use std::time::Duration;

const USER: &str = "testuser";
const PASS: &str = "testpassword";

async fn handshake_outcome(monitor: &MonitorReceiver) -> Result<SocketEvent> {
  let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
  loop {
    let event = tokio::time::timeout_at(deadline, monitor.recv()).await??;
    if matches!(event, SocketEvent::HandshakeFailed { .. } | SocketEvent::HandshakeSucceeded { .. }) {
      return Ok(event);
    }
  }
}

async fn plain_server(username: Option<&str>, password: Option<&str>) -> Result<(Socket, MonitorReceiver, String)> {
  let ctx = context()?;
  let rep = ctx.socket(SocketType::Rep)?;
  rep.set_option(PLAIN_SERVER, true).await?;
  if let Some(u) = username {
    rep.set_option(PLAIN_USERNAME, u).await?;
  }
  if let Some(p) = password {
    rep.set_option(PLAIN_PASSWORD, p).await?;
  }
  let monitor = rep.monitor_default().await?;
  let endpoint = common::alloc_endpoint();
  rep.bind(&endpoint).await?;
  Ok((rep, monitor, endpoint))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rzmq_plain_client_against_libzmq_plain_server() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();
  let (_guard, _reader) =
    common::spawn_and_wait_ready("plain_rep_server.py", &[endpoint.as_str(), USER, PASS], Duration::from_secs(5))?;

  let ctx = context()?;
  let req = ctx.socket(SocketType::Req)?;
  req.set_option(RCVTIMEO, 3000i32).await?;
  req.set_option(PLAIN_USERNAME, USER).await?;
  req.set_option(PLAIN_PASSWORD, PASS).await?;
  req.connect(&endpoint).await?;

  req.send(Msg::from_static(b"hello")).await?;
  assert_eq!(req.recv().await?.data().unwrap(), b"hello-reply");
  req.send(Msg::from_static(b"shutdown")).await?;
  assert_eq!(req.recv().await?.data().unwrap(), b"shutdown-reply");
  Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rzmq_plain_client_with_wrong_password_is_rejected_by_libzmq() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();
  let (_guard, _reader) =
    common::spawn_and_wait_ready("plain_rep_server.py", &[endpoint.as_str(), USER, PASS], Duration::from_secs(5))?;

  let ctx = context()?;
  let req = ctx.socket(SocketType::Req)?;
  req.set_option(RCVTIMEO, 1000i32).await?;
  req.set_option(PLAIN_USERNAME, USER).await?;
  req.set_option(PLAIN_PASSWORD, "wrong").await?;
  let monitor = req.monitor_default().await?;
  req.connect(&endpoint).await?;

  let outcome = handshake_outcome(&monitor).await?;
  match outcome {
    SocketEvent::HandshakeFailed { error_msg, .. } => {
      assert!(error_msg.contains("400"), "libzmq's ZAP status missing from {:?}", error_msg)
    }
    other => panic!("got {:?}", other),
  }
  Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn libzmq_plain_client_against_rzmq_plain_server() -> Result<()> {
  common::setup_logging();
  let (rep, monitor, endpoint) = plain_server(Some(USER), Some(PASS)).await?;
  let (_guard, mut reader) =
    common::spawn_and_wait_ready("plain_req_client.py", &[endpoint.as_str(), USER, PASS], Duration::from_secs(5))?;

  let outcome = handshake_outcome(&monitor).await?;
  assert!(matches!(outcome, SocketEvent::HandshakeSucceeded { .. }), "got {:?}", outcome);
  common::wait_for_line(&mut reader, "HANDSHAKE OK", Duration::from_secs(5))?;
  let request = tokio::time::timeout(Duration::from_secs(5), rep.recv()).await??;
  assert_eq!(request.data().unwrap(), b"hello");
  rep.send(Msg::from_static(b"hello-reply")).await?;
  common::wait_for_line(&mut reader, "SUCCESS", Duration::from_secs(5))?;
  Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn libzmq_plain_client_with_wrong_password_is_rejected_by_rzmq() -> Result<()> {
  common::setup_logging();
  let (_rep, monitor, endpoint) = plain_server(Some(USER), Some(PASS)).await?;
  let (_guard, mut reader) =
    common::spawn_and_wait_ready("plain_req_client.py", &[endpoint.as_str(), USER, "wrong"], Duration::from_secs(5))?;

  let outcome = handshake_outcome(&monitor).await?;
  assert!(matches!(outcome, SocketEvent::HandshakeFailed { .. }), "got {:?}", outcome);
  common::wait_for_line(&mut reader, "AUTH FAILED 400", Duration::from_secs(5))?;
  common::wait_for_line(&mut reader, "NO REPLY", Duration::from_secs(5))?;
  Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rzmq_plain_server_without_credentials_rejects_libzmq_client() -> Result<()> {
  common::setup_logging();
  let (_rep, monitor, endpoint) = plain_server(None, None).await?;
  let (_guard, mut reader) =
    common::spawn_and_wait_ready("plain_req_client.py", &[endpoint.as_str(), USER, PASS], Duration::from_secs(5))?;

  let outcome = handshake_outcome(&monitor).await?;
  assert!(matches!(outcome, SocketEvent::HandshakeFailed { .. }), "got {:?}", outcome);
  common::wait_for_line(&mut reader, "AUTH FAILED 400", Duration::from_secs(5))?;
  common::wait_for_line(&mut reader, "NO REPLY", Duration::from_secs(5))?;
  Ok(())
}
