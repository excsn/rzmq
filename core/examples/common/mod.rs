use std::time::{Duration, Instant};

use futures::future::join_all;
use rzmq::socket::{MonitorReceiver, SocketEvent};

// Helper function to wait for HandshakeSucceeded events from multiple monitors
// This function is a general utility and not directly used by the oneshot-based
// handshake waiting in the main example, but provided as per your request.
pub async fn wait_for_handshake_events(
  monitors_info: Vec<(MonitorReceiver, String, String)>, // (receiver, expected_endpoint, description)
  timeout_duration: Duration,
  print_handshake_events: bool,
) -> Vec<Result<String, String>> {
  // Returns Ok(description) on success, Err(description + error) on failure
  let mut handshake_futures = Vec::new();

  for (monitor_rx, expected_endpoint, description) in monitors_info {
    let desc_clone_success = description.clone();
    let desc_clone_failure = description.clone();
    let expected_endpoint_clone = expected_endpoint.clone();

    handshake_futures.push(tokio::spawn(async move {
      let wait_start = Instant::now();
      loop {
        if wait_start.elapsed() > timeout_duration {
          let err_msg = format!(
            "[{}] Timeout (>{:?}) waiting for HandshakeSucceeded for endpoint {}",
            desc_clone_failure, timeout_duration, expected_endpoint_clone
          );
          eprintln!("{}", err_msg);
          return Err(err_msg);
        }

        match tokio::time::timeout(timeout_duration.saturating_sub(wait_start.elapsed()), monitor_rx.recv()).await {
          Ok(Ok(event)) => {
            if print_handshake_events {
              // Assuming PRINT_HANDSHAKE_EVENTS is a const bool available in scope
              println!("[{}] Monitor received event: {:?}", description, event);
            }
            if let SocketEvent::HandshakeSucceeded {
              endpoint: event_endpoint,
            } = event
            {
              if event_endpoint == expected_endpoint_clone {
                println!(
                  "[{}] HandshakeSucceeded for {} after {:?}.",
                  desc_clone_success,
                  event_endpoint,
                  wait_start.elapsed()
                );
                return Ok(desc_clone_success);
              } else {
                if print_handshake_events {
                  println!(
                    "[{}] HandshakeSucceeded for unexpected endpoint: {} (expected: {})",
                    description, event_endpoint, expected_endpoint_clone
                  );
                }
              }
            }
          }
          Ok(Err(recv_err)) => {
            // Monitor channel closed or other recv error
            let err_msg = format!(
              "[{}] Monitor channel error for endpoint {}: {:?}",
              desc_clone_failure, expected_endpoint_clone, recv_err
            );
            eprintln!("{}", err_msg);
            return Err(err_msg);
          }
          Err(_select_timeout_error) => {
            // Outer timeout (should be caught by wait_start.elapsed())
            // This case might not be strictly necessary if wait_start.elapsed() is the primary timeout mechanism
            // but kept for robustness.
            let err_msg = format!(
              "[{}] Outer timeout logic triggered for endpoint {}.",
              desc_clone_failure, expected_endpoint_clone
            );
            eprintln!("{}", err_msg);
            return Err(err_msg);
          }
        }
      }
    }));
  }

  join_all(handshake_futures)
    .await
    .into_iter()
    .map(|join_result| match join_result {
      Ok(Ok(desc)) => Ok(desc),
      Ok(Err(err_str)) => Err(err_str),
      Err(join_err) => Err(format!("Handshake monitoring task panicked: {}", join_err)),
    })
    .collect()
}
