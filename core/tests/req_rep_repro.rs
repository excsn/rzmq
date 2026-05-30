mod common;

use rzmq::{Msg, SocketType, ZmqError};
use std::time::Duration;

/// Regression test to verify that the REQ socket does not suffer from stale
/// notifications in its `reply_available_notifier`.
///
/// Running 1,000 rapid, sequential round-trips under a multi-threaded runtime
/// ensures that the notification signaling and message queue do not become
/// desynchronized, preventing premature "Receive operation interrupted" errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_req_socket_stale_notifier_regression() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let endpoint = "tcp://127.0.0.1:19876";

  let rep = ctx.socket(SocketType::Rep)?;
  rep.bind(endpoint).await?;

  // Spawn a REP echo server to immediately reply to incoming requests.
  let server_handle = tokio::spawn(async move {
    loop {
      match rep.recv().await {
        Ok(msg) => {
          if rep.send(msg).await.is_err() {
            break;
          }
        }
        Err(_) => break,
      }
    }
  });

  // Allow the listener to set up
  tokio::time::sleep(Duration::from_millis(100)).await;

  let req = ctx.socket(SocketType::Req)?;
  req.connect(endpoint).await?;

  // Allow the connection and handshake to fully complete
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Run 1,000 rapid sequential round-trips.
  let rounds = 1000;
  for i in 0..rounds {
    if let Err(e) = req.send(Msg::from_static(b"hello")).await {
      server_handle.abort();
      let _ = ctx.term().await;
      return Err(ZmqError::Internal(format!(
        "Failed on send at round {}: {}",
        i, e
      )));
    }

    if let Err(e) = req.recv().await {
      server_handle.abort();
      let _ = ctx.term().await;
      return Err(ZmqError::Internal(format!(
        "Failed on recv at round {}: {}",
        i, e
      )));
    }
  }

  // Cleanup
  server_handle.abort();
  ctx.term().await?;

  Ok(())
}
