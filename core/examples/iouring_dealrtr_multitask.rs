use bytes::Bytes;
use futures::future::join_all;
use rzmq::socket::{events::SocketEvent, options as zmq_opts};
use rzmq::uring::{initialize_uring_backend, shutdown_uring_backend, UringConfig};
use rzmq::{Context, Msg, MsgFlags, Socket, SocketType, ZmqError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit, Semaphore};

// This example now uses a common module for helpers like `wait_for_handshake_events`.
// Ensure `core/examples/common/mod.rs` and its contents are available.
mod common;

// --- Configuration Constants ---
const ROUTER_IO_URING_ENABLED: bool = true;
const DEALER_IO_URING_ENABLED: bool = true;
const SNDZEROCPY_IO_URING_ENABLED: bool = true;
const TCP_CORK_ENABLED: u32 = 1; // 0 for false, 1 for true
const NUM_DEALER_TASKS: usize = 8;
const MAX_CONCURRENT_REQUESTS: usize = 1000;
const NUM_MESSAGES_PER_DEALER: u64 = 250; // Total messages will be NUM_DEALER_TASKS * this
const PAYLOAD_SIZE_BYTES: usize = 1024;
const ROUTER_ENDPOINT: &'static str = "tcp://127.0.0.1:5558";

const HANDSHAKE_TIMEOUT_MS: u64 = 5000;
const ROUTER_RECV_TIMEOUT_MS: u64 = 10000;
// Timeout for the central receiver to wait for replies after all sends are done.
const CENTRAL_RECEIVER_OVERALL_TIMEOUT_MS: u64 = 30000;
// Timeout for central receiver if it's idle (no messages received) for this duration.
const CENTRAL_RECEIVER_IDLE_TIMEOUT_MS: u64 = 5000;
// Timeout on the dealer socket for the central receiver's recv_multipart() calls.
// Should be short to allow periodic checks of other conditions (e.g. overall timeout).
const CENTRAL_RECEIVER_SOCKET_RECV_TIMEOUT_MS: u64 = 200;

const TOTAL_MESSAGES_EXPECTED_BY_ROUTER: u64 = (NUM_DEALER_TASKS as u64) * NUM_MESSAGES_PER_DEALER;

// --- Logging Configuration ---
const PRINT_PER_TASK_SUMMARY: bool = true;
const PRINT_INDIVIDUAL_MESSAGES: bool = false;
const PRINT_HANDSHAKE_EVENTS: bool = true;

// --- Type Aliases for Clarity ---
type RequestId = u64;
type PendingRequestsMap = Arc<TokioMutex<HashMap<RequestId, OwnedSemaphorePermit>>>;

// --- Helper Functions ---

/// Generates a unique ID for a request based on the task and message sequence number.
fn generate_request_id(dealer_id: usize, message_seq_num: u64) -> RequestId {
  // Combine dealer ID and message number into a single u64 for a unique ID.
  ((dealer_id as u64) << 32) | (message_seq_num & 0xFFFFFFFF)
}

/// Creates the ZMTP frames for a request: [request_id, payload].
fn create_request_frames(request_id: RequestId, payload_template: &Bytes) -> Vec<Msg> {
  let mut req_id_msg = Msg::from_bytes(Bytes::copy_from_slice(&request_id.to_be_bytes()));
  req_id_msg.set_flags(MsgFlags::MORE);

  let mut payload_msg = Msg::from_bytes(payload_template.clone());
  payload_msg.set_flags(MsgFlags::empty()); // Last part has no MORE flag

  vec![req_id_msg, payload_msg]
}

/// Extracts the request ID and payload from a reply message from the router.
/// A reply from the router to the dealer will have the empty delimiter frame stripped by the DEALER socket,
/// so we expect [request_id_frame, payload_frame, ...].
fn extract_reply_data(mut frames: Vec<Msg>) -> Option<(RequestId, Vec<Msg>)> {
  if frames.is_empty() {
    if PRINT_INDIVIDUAL_MESSAGES {
      eprintln!("[DEALER Central Receiver] extract_reply_data: Received empty frame list.");
    }
    return None;
  }

  // The first frame should be the request_id echoed by the router.
  let request_id_frame = frames.remove(0);
  let request_id_bytes = match request_id_frame.data() {
    Some(d) if d.len() == 8 => d.try_into().ok(),
    _ => {
      if PRINT_INDIVIDUAL_MESSAGES {
        eprintln!("[DEALER Central Receiver] Malformed reply: request_id frame wrong size or missing.");
      }
      return None;
    }
  }?;
  
  let id = u64::from_be_bytes(request_id_bytes);
  Some((id, frames)) // Remaining frames are the echoed payload
}


// --- ROUTER Task (Server Side) ---
async fn run_router_task(
  router_socket: Socket,
  _endpoint: &'static str,
  total_expected_messages: u64,
) -> Result<(u64, u64), ZmqError> {
  // Returns (messages_echoed, bytes_echoed)
  println!(
    "[ROUTER] Bound successfully. Expecting {} total messages.",
    total_expected_messages
  );
  let mut messages_echoed = 0u64;
  let mut bytes_echoed = 0u64;
  let mut received_count = 0u64;

  // The router loop will exit after echoing all expected messages or if an error occurs.
  while received_count < total_expected_messages {
    match router_socket.recv_multipart().await {
      Ok(mut received_parts) => {
        received_count += 1;
        if received_parts.len() < 2 {
          if PRINT_INDIVIDUAL_MESSAGES {
            eprintln!("[ROUTER] Received malformed message ({} parts), expecting at least 2 (identity, payload).", received_parts.len());
          }
          continue; // Ignore malformed
        }

        // Echo the entire message back to the sender
        for part in &received_parts {
          bytes_echoed += part.size() as u64;
        }

        if let Err(e) = router_socket.send_multipart(received_parts).await {
          eprintln!("[ROUTER] Error sending reply: {}. Terminating task.", e);
          return Err(e);
        }
        messages_echoed += 1;
      }
      Err(ZmqError::Timeout) => {
        // Allow timeouts if the test runs long, but warn if no messages are coming.
        if received_count == 0 {
            eprintln!("[ROUTER] Recv timeout occurred before any messages were received. Check client connectivity.");
        }
        if received_count < total_expected_messages {
            continue; // Continue waiting if we haven't received everything yet.
        } else {
            break; // All messages accounted for, exit on timeout.
        }
      }
      Err(ZmqError::InvalidState(ref msg)) if msg.contains("Socket is closing") => {
        println!("[ROUTER] Socket is closing, terminating echo task.");
        break;
      }
      Err(e) => {
        eprintln!("[ROUTER] Unhandled recv_multipart error: {}. Terminating task.", e);
        return Err(e);
      }
    }
  }

  println!(
    "[ROUTER] Echo task finished. Total Received: {}. Messages Echoed: {}.",
    received_count, messages_echoed
  );
  Ok((messages_echoed, bytes_echoed))
}

// --- DEALER Side Tasks ---

#[derive(Debug)]
struct DealerTaskStats {
  id: usize,
  messages_sent: u64,
  bytes_sent: u64,
  send_errors: u64,
  duration: Duration,
}

/// Task for a single "dealer client" to send requests.
/// All these tasks share a single DEALER socket.
async fn run_dealer_sender_task(
  dealer_socket_shared_clone: rzmq::Socket,
  dealer_id: usize,
  num_messages_for_this_task: u64,
  message_payload_template: Bytes,
  pending_requests_map_clone: PendingRequestsMap,
  shared_semaphore: Arc<Semaphore>,
) -> Result<DealerTaskStats, ZmqError> {
  let mut task_messages_sent = 0u64;
  let mut task_bytes_sent = 0u64;
  let mut send_errors = 0u64;
  let task_start_time = Instant::now();

  for j in 0..num_messages_for_this_task {
    let permit = match shared_semaphore.clone().acquire_owned().await {
      Ok(p) => p,
      Err(_) => {
        eprintln!("[DEALER SENDER {}] Shared semaphore closed. Aborting.", dealer_id);
        break;
      }
    };

    let request_id = generate_request_id(dealer_id, j);
    pending_requests_map_clone.lock().await.insert(request_id, permit);

    let app_frames_to_send = create_request_frames(request_id, &message_payload_template);
    let current_payload_total_size: u64 = app_frames_to_send.iter().map(|m| m.size() as u64).sum();

    match dealer_socket_shared_clone.send_multipart(app_frames_to_send).await {
      Ok(_) => {
        task_messages_sent += 1;
        task_bytes_sent += current_payload_total_size;
      }
      Err(e) => {
        eprintln!("[DEALER SENDER {}] Send error for RequestId {}: {}", dealer_id, request_id, e);
        send_errors += 1;
        // If send fails, we must remove the permit from the map to avoid deadlock
        if let Some(permit_to_drop) = pending_requests_map_clone.lock().await.remove(&request_id) {
          drop(permit_to_drop);
        }
      }
    }
  }
  let task_duration = task_start_time.elapsed();
  if PRINT_PER_TASK_SUMMARY || send_errors > 0 {
    println!(
      "[DEALER SENDER {}] Task finished sending. Duration: {:?}. Sent: {} (err:{}).",
      dealer_id,
      task_duration,
      task_messages_sent,
      send_errors,
    );
  }
  Ok(DealerTaskStats {
    id: dealer_id,
    messages_sent: task_messages_sent,
    bytes_sent: task_bytes_sent,
    send_errors,
    duration: task_duration,
  })
}

/// A single, central task to receive all replies on the shared DEALER socket.
async fn run_dealer_central_receiver(
  dealer_socket: rzmq::Socket,
  pending_requests_map: PendingRequestsMap,
  total_expected_replies: u64,
) -> (u64, u64, u64, Option<Instant>) {
  // Returns (replies_processed, bytes_replied, extraction_errors, last_reply_timestamp)
  println!(
    "[DEALER Central Receiver] Task started. Expecting up to {} replies.",
    total_expected_replies
  );
  let mut replies_processed = 0u64;
  let mut bytes_replied = 0u64;
  let mut extraction_errors = 0u64;
  let mut last_message_activity_time = Instant::now();
  let mut last_valid_reply_timestamp: Option<Instant> = None;

  while replies_processed + extraction_errors < total_expected_replies {
    match dealer_socket.recv_multipart().await {
      Ok(frames_from_dealer_socket) => {
        last_message_activity_time = Instant::now();
        if let Some((request_id, reply_payload_parts)) = extract_reply_data(frames_from_dealer_socket) {
          if PRINT_INDIVIDUAL_MESSAGES {
              println!("[DEALER Central Receiver] Extracted RequestId: {}", request_id);
          }
          if let Some(_permit_to_drop) = pending_requests_map.lock().await.remove(&request_id) {
            // Drop the permit, releasing capacity in the semaphore.
            replies_processed += 1;
            for part in &reply_payload_parts {
              bytes_replied += part.size() as u64;
            }
            last_valid_reply_timestamp = Some(Instant::now());
          } else {
             // This can happen if the reply arrives after the main thread timed out waiting for it.
             if PRINT_INDIVIDUAL_MESSAGES {
                eprintln!("[DEALER Central Receiver] Received reply for untracked RequestId {}. Possibly a late reply.", request_id);
             }
          }
        } else {
          eprintln!("[DEALER Central Receiver] Failed to extract RequestId from reply. Frames might be malformed.");
          extraction_errors += 1;
        }
      }
      Err(ZmqError::InvalidState(ref msg)) if msg.contains("Socket is closing") => {
        println!("[DEALER Central Receiver] Main DEALER socket closing. Terminating.");
        break;
      }
      Err(ZmqError::Timeout) => {
        if last_message_activity_time.elapsed() > Duration::from_millis(CENTRAL_RECEIVER_IDLE_TIMEOUT_MS) {
          println!(
            "[DEALER Central Receiver] Idle for over {}ms. Terminating.",
            CENTRAL_RECEIVER_IDLE_TIMEOUT_MS,
          );
          break;
        }
      }
      Err(e) => {
        eprintln!("[DEALER Central Receiver] Unhandled error receiving from DEALER socket: {}. Terminating.", e);
        break;
      }
    }
  }

  println!(
    "[DEALER Central Receiver] Task finished. Replies processed: {}, Bytes replied: {}, Extraction Errors: {}",
    replies_processed, bytes_replied, extraction_errors
  );
  (
    replies_processed,
    bytes_replied,
    extraction_errors,
    last_valid_reply_timestamp,
  )
}

// --- Main Function ---

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "warn,rzmq=info");
  }
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .with_thread_ids(true)
    .with_thread_names(true)
    .with_target(true)
    .compact()
    .init();

  println!(
    "Starting DEALER-ROUTER Throughput Example ({} DEALER tasks, {} msgs/task, {} max concurrent)...",
    NUM_DEALER_TASKS, NUM_MESSAGES_PER_DEALER, MAX_CONCURRENT_REQUESTS
  );

  let uring_config = UringConfig {
    default_send_zerocopy: SNDZEROCPY_IO_URING_ENABLED && (DEALER_IO_URING_ENABLED || ROUTER_IO_URING_ENABLED),
    ring_entries: 1024,
    default_recv_multishot: true,
    default_recv_buffer_count: 64,
    default_recv_buffer_size: 4096,
    default_send_buffer_count: 64,
    default_send_buffer_size: 4096,
  };
  if let Err(e) = initialize_uring_backend(uring_config) {
      if !matches!(&e, ZmqError::InvalidState(s) if s.contains("already initialized")) {
          eprintln!("Failed to initialize io_uring backend: {:?}. Exiting.", e);
          return Err(e);
      }
      println!("io_uring backend was already initialized.");
  } else {
      println!("io_uring backend initialized explicitly.");
  }


  let ctx = Context::new().expect("Failed to create rzmq context");

  // --- Socket Setup ---
  let router_socket = ctx.socket(SocketType::Router)?;
  router_socket.set_option(zmq_opts::IO_URING_SESSION_ENABLED, ROUTER_IO_URING_ENABLED).await?;
  router_socket.set_option(zmq_opts::IO_URING_RCVMULTISHOT, ROUTER_IO_URING_ENABLED).await?;
  router_socket.set_option(zmq_opts::IO_URING_SNDZEROCOPY, SNDZEROCPY_IO_URING_ENABLED && ROUTER_IO_URING_ENABLED).await?;
  router_socket.set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED).await?;
  router_socket.set_option(zmq_opts::RCVHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32).max(5000)).await?;
  router_socket.set_option(zmq_opts::SNDHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32).max(5000)).await?;
  router_socket.set_option(zmq_opts::RCVTIMEO, ROUTER_RECV_TIMEOUT_MS as i32).await?;

  let router_monitor_rx = router_socket.monitor_default().await?;
  router_socket.bind(ROUTER_ENDPOINT).await?;

  let dealer_socket_main = ctx.socket(SocketType::Dealer)?;
  dealer_socket_main.set_option(zmq_opts::IO_URING_SESSION_ENABLED, DEALER_IO_URING_ENABLED).await?;
  dealer_socket_main.set_option(zmq_opts::IO_URING_RCVMULTISHOT, DEALER_IO_URING_ENABLED).await?;
  dealer_socket_main.set_option(zmq_opts::IO_URING_SNDZEROCOPY, SNDZEROCPY_IO_URING_ENABLED && DEALER_IO_URING_ENABLED).await?;
  dealer_socket_main.set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED).await?;
  dealer_socket_main.set_option(zmq_opts::SNDHWM, (MAX_CONCURRENT_REQUESTS * 2) as i32).await?;
  dealer_socket_main.set_option(zmq_opts::RCVHWM, (MAX_CONCURRENT_REQUESTS * 2) as i32).await?;
  dealer_socket_main.set_option(zmq_opts::SNDTIMEO, 5000i32).await?;
  dealer_socket_main.set_option(zmq_opts::RCVTIMEO, CENTRAL_RECEIVER_SOCKET_RECV_TIMEOUT_MS as i32).await?;

  let dealer_monitor_rx = dealer_socket_main.monitor_default().await?;
  dealer_socket_main.connect(ROUTER_ENDPOINT).await?;

  // --- Wait for Handshake ---
  common::wait_for_handshake_events(
    vec![
      (router_monitor_rx, ROUTER_ENDPOINT.to_string(), "ROUTER".to_string()),
      (dealer_monitor_rx, ROUTER_ENDPOINT.to_string(), "DEALER".to_string()),
    ],
    Duration::from_millis(HANDSHAKE_TIMEOUT_MS),
    PRINT_HANDSHAKE_EVENTS,
  ).await;
  println!("[Main] All handshakes complete.");

  // --- Shared State for Tasks ---
  let pending_requests_map: PendingRequestsMap = Arc::new(TokioMutex::new(HashMap::new()));
  let shared_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
  let message_payload_template = Bytes::from(vec![0u8; PAYLOAD_SIZE_BYTES]);

  // --- Spawn Tasks ---
  let router_join_handle = tokio::spawn(run_router_task(router_socket, ROUTER_ENDPOINT, TOTAL_MESSAGES_EXPECTED_BY_ROUTER));

  let central_receiver_handle = tokio::spawn(run_dealer_central_receiver(
    dealer_socket_main.clone(),
    pending_requests_map.clone(),
    TOTAL_MESSAGES_EXPECTED_BY_ROUTER,
  ));

  let mut sender_task_handles = Vec::new();
  println!("[Main] Spawning {} DEALER sender tasks...", NUM_DEALER_TASKS);

  let benchmark_active_phase_start_time = Instant::now();

  for i in 0..NUM_DEALER_TASKS {
    sender_task_handles.push(tokio::spawn(run_dealer_sender_task(
      dealer_socket_main.clone(),
      i,
      NUM_MESSAGES_PER_DEALER,
      message_payload_template.clone(),
      pending_requests_map.clone(),
      shared_semaphore.clone(),
    )));
  }

  // --- Await Sender Completion ---
  let all_dealer_sender_results = join_all(sender_task_handles).await;
  println!("[Main] All DEALER sender tasks finished their send loops.");

  // --- Aggregate Sender Stats ---
  let mut total_messages_successfully_sent = 0u64;
  let mut total_bytes_sent_agg = 0u64;
  for join_result in all_dealer_sender_results {
    match join_result {
      Ok(Ok(stats)) => {
        total_messages_successfully_sent += stats.messages_sent;
        total_bytes_sent_agg += stats.bytes_sent;
      }
      Ok(Err(e)) => eprintln!("[Main] A DEALER sender task failed with ZmqError: {}", e),
      Err(e) => eprintln!("[Main] A DEALER sender task panicked: {}", e),
    }
  }

  // --- Wait for Replies or Timeout ---
  let receive_phase_monitor_start_time = Instant::now();
  loop {
    let num_pending_now = pending_requests_map.lock().await.len();
    if num_pending_now == 0 {
      println!("[Main] All {} sent requests have been accounted for.", total_messages_successfully_sent);
      break;
    }
    if receive_phase_monitor_start_time.elapsed() >= Duration::from_millis(CENTRAL_RECEIVER_OVERALL_TIMEOUT_MS) {
      eprintln!("[Main] Overall receive timeout reached. Pending requests: {}", num_pending_now);
      break;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
  }

  // --- Shutdown and Final Results ---
  println!("[Main] Signaling central receiver to stop by closing main dealer socket...");
  if let Err(e) = dealer_socket_main.close().await {
      eprintln!("[Main] Error closing main dealer socket: {}", e);
  }

  let (central_replies_processed, central_bytes_replied, _, last_reply_ts) = central_receiver_handle.await.unwrap();
  
  let active_phase_end_time = last_reply_ts.unwrap_or_else(|| benchmark_active_phase_start_time + receive_phase_monitor_start_time.elapsed());
  let active_phase_duration = active_phase_end_time.duration_since(benchmark_active_phase_start_time);
  let active_seconds = active_phase_duration.as_secs_f64().max(1e-9);

  println!("\n--- Overall Results (Active Phase Duration: {:?}) ---", active_phase_duration);
  println!("Total messages successfully sent: {}", total_messages_successfully_sent);
  println!("Total replies processed: {}", central_replies_processed);
  let lost_messages = total_messages_successfully_sent.saturating_sub(central_replies_processed);
  if lost_messages > 0 {
      eprintln!("WARNING: Lost/timed-out messages: {}", lost_messages);
  }

  let agg_recv_msg_rate = central_replies_processed as f64 / active_seconds;
  let agg_recv_byte_rate_mb = (central_bytes_replied as f64 / active_seconds) / (1024.0 * 1024.0);
  println!("Effective System Throughput: {:.2} req/sec ({:.2} MB/s)", agg_recv_msg_rate, agg_recv_byte_rate_mb);

  // --- Final Teardown ---
  println!("[Main] Waiting for ROUTER task to complete...");
  let _ = router_join_handle.await;

  println!("[Main] Terminating context...");
  ctx.term().await?;
  shutdown_uring_backend().await.unwrap_or_else(|e| eprintln!("Error shutting down io_uring backend: {:?}", e));
  
  println!("[Main] Example finished.");
  Ok(())
}