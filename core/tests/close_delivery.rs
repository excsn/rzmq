use rzmq::socket::{PLAIN_PASSWORD, PLAIN_SERVER, PLAIN_USERNAME, RCVTIMEO};
use rzmq::{Msg, SocketType, ZmqError};
use std::time::Duration;
mod common;

/// A REP that sends its reply and closes at once, with default options, still
/// delivers the reply. Pins the default LINGER at -1: with LINGER 0 the reply is
/// dropped from the pipe or the session's egress on a multi-thread runtime.
async fn reply_before_close(plain: bool) -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let server = ctx.socket(SocketType::Rep)?;
    let client = ctx.socket(SocketType::Req)?;
    if plain {
      server.set_option(PLAIN_SERVER, true).await?;
      server.set_option(PLAIN_USERNAME, "testuser").await?;
      server.set_option(PLAIN_PASSWORD, "testpassword").await?;
      client.set_option(PLAIN_USERNAME, "testuser").await?;
      client.set_option(PLAIN_PASSWORD, "testpassword").await?;
    }
    let endpoint = common::bind_and_resolve_tcp(&server).await?;
    client.set_option(RCVTIMEO, 2000i32).await?;
    client.connect(&endpoint).await?;

    client.send(Msg::from_static(b"request")).await?;
    assert_eq!(common::recv_timeout(&server, Duration::from_secs(5)).await?.data().unwrap(), b"request");
    server.send(Msg::from_static(b"reply")).await?;
    server.close().await?;
    assert_eq!(client.recv().await?.data().unwrap(), b"reply");
  }
  ctx.term().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn null_multi_thread() -> Result<(), ZmqError> {
  reply_before_close(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_multi_thread() -> Result<(), ZmqError> {
  reply_before_close(true).await
}

#[tokio::test]
async fn null_current_thread() -> Result<(), ZmqError> {
  reply_before_close(false).await
}

#[tokio::test]
async fn plain_current_thread() -> Result<(), ZmqError> {
  reply_before_close(true).await
}

/// `term` on a context whose socket holds messages for a peer that never
/// appears finishes by forcing the linger, inside its cap, instead of waiting
/// for ever.
#[tokio::test]
async fn term_with_undeliverable_messages_and_infinite_linger_completes() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let dealer = ctx.socket(SocketType::Dealer)?;
  dealer.connect("tcp://127.0.0.1:1").await?;
  dealer.send(Msg::from_static(b"never delivered")).await?;

  let started = std::time::Instant::now();
  ctx.term().await?;
  let took = started.elapsed();
  assert!(took >= Duration::from_secs(10), "term returned before the linger cap: {took:?}");
  assert!(took < Duration::from_secs(13), "term did not force the linger: {took:?}");
  Ok(())
}

/// With blocky off, sockets start at LINGER 0 and the same `term` is prompt.
#[tokio::test]
async fn term_with_blocky_off_is_prompt() -> Result<(), ZmqError> {
  let ctx = common::test_context().with_blocky(false);
  let dealer = ctx.socket(SocketType::Dealer)?;
  dealer.connect("tcp://127.0.0.1:1").await?;
  dealer.send(Msg::from_static(b"dropped at term")).await?;

  let started = std::time::Instant::now();
  ctx.term().await?;
  assert!(started.elapsed() < Duration::from_secs(1));
  Ok(())
}
