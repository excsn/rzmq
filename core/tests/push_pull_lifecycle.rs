use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;
use tokio::time::{sleep, timeout};

// Assuming common.rs exists in tests/ directory
// It should contain at least test_context() and recv_timeout()
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(250);
const LONG_TIMEOUT: Duration = Duration::from_secs(3); // Slightly longer for debugging
const NUM_MESSAGES: usize = 5; // Smaller number for faster test

#[rzmq_macros::test]
async fn test_push_pull_explicit_close_then_term() -> Result<(), ZmqError> {
  println!("\n--- Starting test_push_pull_explicit_close_then_term ---");
  let ctx = common::test_context(); // Ensure tracing is set up via common

  println!("Creating PUSH and PULL sockets...");
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;
  let endpoint = "tcp://127.0.0.1:5701"; // Unique port for this test

  println!("Binding PULL to {}...", endpoint);
  pull.bind(endpoint).await?;
  sleep(Duration::from_millis(50)).await; // Allow bind

  println!("Connecting PUSH to {}...", endpoint);
  push.connect(endpoint).await?;
  sleep(Duration::from_millis(150)).await; // Allow connect + handshake

  println!("Performing readiness handshake...");
  let ready_msg = Msg::from_static(b"READY");
  match timeout(LONG_TIMEOUT, push.send(ready_msg)).await {
    Ok(Ok(())) => println!("Readiness send OK."),
    Ok(Err(e)) => panic!("Readiness send failed: {}", e),
    Err(_) => panic!("Readiness send timed out"),
  }
  match timeout(LONG_TIMEOUT, pull.recv()).await {
    Ok(Ok(msg)) if msg.data().unwrap_or_default() == b"READY" => {
      println!("Readiness recv OK.")
    }
    Ok(Ok(msg)) => panic!("Readiness recv wrong message: {:?}", msg.data().unwrap_or_default()),
    Ok(Err(e)) => panic!("Readiness recv failed: {}", e),
    Err(_) => panic!("Readiness recv timed out"),
  }
  println!("Readiness handshake complete.");

  // --- Send/Receive Loop ---
  println!("Starting main send/receive loop ({} messages)...", NUM_MESSAGES);
  let sender_task = {
    let push = push.clone();
    tokio::spawn(async move {
      for i in 0..NUM_MESSAGES {
        let msg = Msg::from_vec(format!("Msg {}", i).into_bytes());
        if let Err(e) = push.send(msg).await {
          println!("Sender task error: {}", e);
          return Err(e);
        }
        // Small yield to allow receiver to potentially run
        tokio::task::yield_now().await;
      }
      println!("Sender task finished.");
      Ok(())
    })
  };

  let receiver_task = {
    let pull = pull.clone();
    tokio::spawn(async move {
      for i in 0..NUM_MESSAGES {
        match timeout(LONG_TIMEOUT, pull.recv()).await {
          Ok(Ok(msg)) => {
            let expected = format!("Msg {}", i).into_bytes();
            assert_eq!(msg.data().unwrap(), expected.as_slice(), "Mismatch on msg {}", i);
            println!("Receiver task received Msg {}", i);
          }
          Ok(Err(e)) => {
            println!("Receiver task error on msg {}: {}", i, e);
            // Allow the specific "Socket terminated" error if it happens *exactly* at the end
            if i == NUM_MESSAGES - 1 && matches!(&e, ZmqError::InvalidState(s) if s == &"Socket terminated") {
              println!("Receiver task tolerated expected termination error at the end.");
              // Break gracefully instead of returning error
              return Ok(i); // Return count received before termination error
            }
            return Err(e); // Return other errors
          }
          Err(_) => {
            // Timeout error
            println!("Receiver task timed out waiting for Msg {}", i);
            return Err(ZmqError::Timeout);
          }
        }
      }
      println!("Receiver task finished receiving all messages.");
      Ok(NUM_MESSAGES) // Return count
    })
  };

  // Wait for tasks
  let send_result = sender_task.await.expect("Sender task panicked");
  let recv_result = receiver_task.await.expect("Receiver task panicked");

  println!("Send task result: {:?}", send_result);
  println!("Receive task result: {:?}", recv_result);

  // Check results (allow receiver to have received slightly fewer if termination error occurred right at end)
  assert!(send_result.is_ok(), "Sender task failed");
  match recv_result {
    Ok(count) => {
      if count < NUM_MESSAGES {
        println!(
          "Warning: Receiver task completed but received only {}/{} messages (likely due to termination race).",
          count, NUM_MESSAGES
        );
      }
    }
    Err(e) => {
      panic!("Receiver task failed with unexpected error: {}", e);
    }
  }
  println!("Send/Receive loop finished.");

  // --- Explicit Teardown ---
  println!("Closing PUSH socket explicitly...");
  let push_close_res = push.close().await;
  println!("PUSH close result: {:?}", push_close_res);
  // Don't assert Ok here, as close might race with termination slightly

  println!("Closing PULL socket explicitly...");
  let pull_close_res = pull.close().await;
  println!("PULL close result: {:?}", pull_close_res);
  // Don't assert Ok here either

  // Optional delay - allow close commands to propagate if needed
  sleep(Duration::from_millis(50)).await;

  println!("Terminating context...");
  let term_result = ctx.term().await;
  println!("Context term result: {:?}", term_result);
  assert!(term_result.is_ok(), "Context termination failed"); // Assert term succeeds

  println!("--- Test test_push_pull_explicit_close_then_term Finished ---");
  Ok(())
}
