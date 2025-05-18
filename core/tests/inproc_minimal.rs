// tests/inproc_minimal.rs (New file)

use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;
mod common;

const LONG_TIMEOUT: Duration = Duration::from_secs(5); // Increased timeout slightly

#[rzmq_macros::test]
async fn test_inproc_minimal_pipe() -> Result<(), ZmqError> {
  println!("Starting minimal inproc test...");
  let ctx = common::test_context();
  // Define sockets outside the inner scope so they live until after ctx.term()
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  {
    // Inner scope for operations
    let endpoint = "inproc://minimal-test-pipe";
    println!("Endpoint: {}", endpoint);

    println!("Binding PULL...");
    pull.bind(endpoint).await?;

    println!("Connecting PUSH...");
    push.connect(endpoint).await?;

    println!("Waiting for pipe setup...");
    // Give ample time for the connection and pipe setup in background
    tokio::time::sleep(Duration::from_millis(100)).await;

    let msg_data = b"Minimal Hello";
    println!("PUSH sending message...");
    push.send(Msg::from_static(msg_data)).await?;
    println!("PUSH message sent.");

    println!("PULL receiving message...");
    let received_msg = common::recv_timeout(&pull, LONG_TIMEOUT).await?;
    println!("PULL message received.");

    assert_eq!(received_msg.data().unwrap(), msg_data);
    println!("Assertion passed.");
  } // End of operational scope, but `push` and `pull` still live

  println!("Terminating context...");
  ctx.term().await?; // Sockets `push` and `pull` are alive during termination
  println!("Context terminated.");

  // `push` and `pull` handles are implicitly dropped here *after* term completes.
  println!("Minimal inproc test finished successfully.");
  Ok(())
}
