use bytes::Bytes;
use futures::future::join_all;
use futures::{stream::FuturesUnordered, StreamExt};
use rzmq::socket::{events::SocketEvent, options as zmq_opts};
use rzmq::uring::{initialize_uring_backend, shutdown_uring_backend, UringConfig};
use rzmq::{Context, Msg, MsgFlags, Socket, SocketType, ZmqError};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::{sleep, timeout};

mod common;
use common::capacity_gate::{CapacityGate, OwnedPermitGuard};

// --- Configuration Constants ---
const ROUTER_IO_URING_ENABLED: bool = true;
const DEALER_IO_URING_ENABLED: bool = true;
const SNDZEROCPY_IO_URING_ENABLED: bool = false;
const TCP_CORK_ENABLED: bool = true;
const USE_MULTIPART_API: bool = true;
const NUM_DEALER_TASKS: usize = 8;
const MAX_CONCURRENT_REQUESTS: usize = 10_000;
const NUM_MESSAGES_PER_DEALER: u64 = 500_000;
const PAYLOAD_SIZE_BYTES: usize = 1024;
const ROUTER_ENDPOINT: &'static str = "tcp://127.0.0.1:5558";
const HANDSHAKE_TIMEOUT_MS: u64 = 5000;
const CENTRAL_RECEIVER_IDLE_TIMEOUT_MS: u64 = 15000;
const TOTAL_MESSAGES_EXPECTED_BY_ROUTER: u64 = (NUM_DEALER_TASKS as u64) * NUM_MESSAGES_PER_DEALER;

// --- MODIFIED: Implemented PrintLogLevel system ---
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PrintLogLevel {
  Off,
  Info,
  Verbose,
}
const PRINT_LEVEL: PrintLogLevel = PrintLogLevel::Info;

// --- Type Aliases for Clarity ---
type RequestId = u64;
type PendingRequestsMap = Arc<TokioMutex<HashMap<RequestId, OwnedPermitGuard>>>;

// --- Helper Functions ---
fn generate_request_id(dealer_id: usize, message_seq_num: u64) -> RequestId {
  ((dealer_id as u64) << 32) | (message_seq_num & 0xFFFFFFFF)
}

fn extract_reply_data(mut frames: Vec<Msg>) -> Option<(RequestId, Vec<Msg>)> {
  if frames.is_empty() {
    return None;
  }
  let request_id_frame = frames.remove(0);
  let request_id_bytes = request_id_frame.data()?.try_into().ok()?;
  let id = u64::from_be_bytes(request_id_bytes);
  Some((id, frames))
}

// --- ROUTER Task ---
async fn run_router_task(
  router_socket: Socket,
  total_expected_messages: u64,
) -> Result<u64, ZmqError> {
  let mut messages_echoed = 0u64;
  for _ in 0..total_expected_messages {
    let frames = router_socket.recv_multipart().await?;
    router_socket.send_multipart(frames).await?;
    messages_echoed += 1;
  }
  Ok(messages_echoed)
}

// --- DEALER Side Tasks ---
#[derive(Debug)]
struct DealerTaskStats {
  id: usize,
  messages_sent: u64,
  send_errors: u64,
}

async fn run_dealer_sender_task(
  dealer_socket_shared_clone: rzmq::Socket,
  dealer_id: usize,
  num_messages_for_this_task: u64,
  message_payload_template: Bytes,
  pending_requests_map_clone: PendingRequestsMap,
  capacity_gate: Arc<CapacityGate>,
) -> Result<DealerTaskStats, ZmqError> {
  let mut task_messages_sent = 0u64;
  let mut send_errors = 0u64;
  for j in 0..num_messages_for_this_task {
    let permit = capacity_gate.clone().acquire_owned().await;
    let request_id = generate_request_id(dealer_id, j);
    pending_requests_map_clone
      .lock()
      .await
      .insert(request_id, permit);
    let mut id_msg = Msg::from_bytes(Bytes::copy_from_slice(&request_id.to_be_bytes()));
    let payload_msg = Msg::from_bytes(message_payload_template.clone());
    let send_result = if USE_MULTIPART_API {
      id_msg.set_flags(MsgFlags::MORE);
      dealer_socket_shared_clone
        .send_multipart(vec![id_msg, payload_msg])
        .await
    } else {
      id_msg.set_flags(MsgFlags::MORE);
      dealer_socket_shared_clone.send(id_msg).await?;
      dealer_socket_shared_clone.send(payload_msg).await
    };
    if let Err(e) = send_result {
      eprintln!(
        "[DEALER SENDER {}] Send error for RequestId {}: {}",
        dealer_id, request_id, e
      );
      send_errors += 1;
      if let Some(permit_to_drop) = pending_requests_map_clone.lock().await.remove(&request_id) {
        drop(permit_to_drop);
      }
      return Err(e);
    }
    task_messages_sent += 1;
  }
  // --- MODIFIED: Conditional logging ---
  if PRINT_LEVEL >= PrintLogLevel::Verbose {
    println!(
      "[DEALER SENDER {}] Task finished sending. Sent: {} (err:{}).",
      dealer_id, task_messages_sent, send_errors
    );
  }
  Ok(DealerTaskStats {
    id: dealer_id,
    messages_sent: task_messages_sent,
    send_errors,
  })
}

// --- Helper for receiving logical messages ---
async fn receive_reply(dealer_socket: &Socket) -> Result<Vec<Msg>, ZmqError> {
  if USE_MULTIPART_API {
    dealer_socket.recv_multipart().await
  } else {
    let frame1 = dealer_socket.recv().await?;
    let frame2 = dealer_socket.recv().await?;
    Ok(vec![frame1, frame2])
  }
}

// --- Central Receiver ---
type RecvFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<Msg>, ZmqError>> + Send + 'a>>;
async fn run_dealer_central_receiver(
  dealer_socket: rzmq::Socket,
  pending_requests_map: PendingRequestsMap,
  total_expected_replies: u64,
) -> Result<u64, ZmqError> {
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!(
      "[DEALER Central Receiver] Task started. Expecting {} replies.",
      total_expected_replies
    );
  }
  let mut replies_processed = 0u64;
  let mut recv_futures: FuturesUnordered<RecvFuture> = FuturesUnordered::new();
  recv_futures.push(Box::pin(async { receive_reply(&dealer_socket).await }));

  while replies_processed < total_expected_replies {
    match timeout(
      Duration::from_millis(CENTRAL_RECEIVER_IDLE_TIMEOUT_MS),
      recv_futures.next(),
    )
    .await
    {
      Ok(Some(Ok(frames))) => {
        recv_futures.push(Box::pin(async { receive_reply(&dealer_socket).await }));
        if let Some((request_id, _)) = extract_reply_data(frames) {
          if let Some(permit) = pending_requests_map.lock().await.remove(&request_id) {
            drop(permit);
            replies_processed += 1;
          }
        }
      }
      Ok(Some(Err(e))) => return Err(e),
      Ok(None) | Err(_) => {
        if PRINT_LEVEL >= PrintLogLevel::Info {
          println!(
            "[DEALER Central Receiver] Timed out or channel closed. Processed {} replies.",
            replies_processed
          );
        }
        break;
      }
    }
  }
  Ok(replies_processed)
}

// --- Main Function ---
#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  // --- Setup ---
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .compact()
    .init();
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!(
      "Starting DEALER-ROUTER Throughput Example ({} DEALER tasks, {} msgs/task, {} max concurrent)...",
      NUM_DEALER_TASKS, NUM_MESSAGES_PER_DEALER, MAX_CONCURRENT_REQUESTS
    );
  }
  let uring_config = UringConfig {
    default_recv_multishot: DEALER_IO_URING_ENABLED || ROUTER_IO_URING_ENABLED,
    default_send_zerocopy: SNDZEROCPY_IO_URING_ENABLED,
    ring_entries: 10240,
    default_recv_buffer_count: 16,
    default_recv_buffer_size: 65536,
    default_send_buffer_count: 16,
    default_send_buffer_size: 65536,
  };
  if let Err(e) = initialize_uring_backend(uring_config) {
    if !matches!(&e, ZmqError::InvalidState(s) if s.contains("already initialized")) {
      eprintln!(
        "[Main] Failed to initialize io_uring backend: {:?}. Exiting.",
        e
      );
      return Err(e);
    }
  }
  let ctx = Context::new().expect("Failed to create rzmq context");

  // --- Socket Setup ---
  let router_socket = ctx.socket(SocketType::Router)?;
  router_socket
    .set_option(zmq_opts::IO_URING_SESSION_ENABLED, ROUTER_IO_URING_ENABLED)
    .await?;
  router_socket
    .set_option(
      zmq_opts::IO_URING_RCVMULTISHOT,
      ROUTER_IO_URING_ENABLED as i32,
    )
    .await?;
  router_socket
    .set_option(
      zmq_opts::IO_URING_SNDZEROCOPY,
      SNDZEROCPY_IO_URING_ENABLED as i32,
    )
    .await?;
  router_socket
    .set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED)
    .await?;
  router_socket
    .set_option(
      zmq_opts::SNDHWM,
      (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32).max(5000),
    )
    .await?;
  router_socket
    .set_option(
      zmq_opts::RCVHWM,
      (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32).max(5000),
    )
    .await?;
  let router_monitor_rx = router_socket.monitor_default().await?;
  router_socket.bind(ROUTER_ENDPOINT).await?;
  let dealer_socket_main = ctx.socket(SocketType::Dealer)?;
  dealer_socket_main
    .set_option(zmq_opts::IO_URING_SESSION_ENABLED, DEALER_IO_URING_ENABLED)
    .await?;
  dealer_socket_main
    .set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED)
    .await?;
  dealer_socket_main
    .set_option(
      zmq_opts::IO_URING_RCVMULTISHOT,
      DEALER_IO_URING_ENABLED as i32,
    )
    .await?;
  dealer_socket_main
    .set_option(
      zmq_opts::IO_URING_SNDZEROCOPY,
      SNDZEROCPY_IO_URING_ENABLED as i32,
    )
    .await?;
  dealer_socket_main
    .set_option(zmq_opts::SNDHWM, (MAX_CONCURRENT_REQUESTS * 2) as i32)
    .await?;
  dealer_socket_main
    .set_option(zmq_opts::RCVHWM, (MAX_CONCURRENT_REQUESTS * 2) as i32)
    .await?;
  let dealer_monitor_rx = dealer_socket_main.monitor_default().await?;
  dealer_socket_main.connect(ROUTER_ENDPOINT).await?;

  // --- MODIFIED: Conditional logging for handshake ---
  let print_handshake_events = PRINT_LEVEL >= PrintLogLevel::Info;
  common::wait_for_handshake_events(
    vec![
      (
        router_monitor_rx,
        ROUTER_ENDPOINT.to_string(),
        "ROUTER".to_string(),
      ),
      (
        dealer_monitor_rx,
        ROUTER_ENDPOINT.to_string(),
        "DEALER".to_string(),
      ),
    ],
    Duration::from_millis(HANDSHAKE_TIMEOUT_MS),
    print_handshake_events,
  )
  .await;
  if print_handshake_events {
    println!("[Main] All handshakes complete.");
  }

  sleep(Duration::from_secs(2)).await;
  // --- Shared State & Task Spawning ---
  let pending_requests_map: PendingRequestsMap = Arc::new(TokioMutex::new(HashMap::new()));
  let capacity_gate = Arc::new(CapacityGate::new(MAX_CONCURRENT_REQUESTS));
  let message_payload_template = Bytes::from(vec![0u8; PAYLOAD_SIZE_BYTES]);
  let benchmark_start_time = Instant::now();

  let router_join_handle = tokio::spawn(run_router_task(
    router_socket,
    TOTAL_MESSAGES_EXPECTED_BY_ROUTER,
  ));
  let central_receiver_handle = tokio::spawn(run_dealer_central_receiver(
    dealer_socket_main.clone(),
    pending_requests_map.clone(),
    TOTAL_MESSAGES_EXPECTED_BY_ROUTER,
  ));

  let mut sender_task_handles = Vec::new();
  for i in 0..NUM_DEALER_TASKS {
    sender_task_handles.push(tokio::spawn(run_dealer_sender_task(
      dealer_socket_main.clone(),
      i,
      NUM_MESSAGES_PER_DEALER,
      message_payload_template.clone(),
      pending_requests_map.clone(),
      capacity_gate.clone(),
    )));
  }

  // --- Await Completion & Report Results ---
  let sender_results = join_all(sender_task_handles).await;
  let mut total_sent = 0u64;
  let mut total_errors = 0u64;
  for res in sender_results {
    match res {
      Ok(Ok(stats)) => {
        total_sent += stats.messages_sent;
        total_errors += stats.send_errors;
      }
      Ok(Err(e)) => eprintln!("[Main] Dealer sender task failed with ZmqError: {}", e),
      Err(e) => eprintln!("[Main] Dealer sender task panicked: {}", e),
    }
  }

  let router_echoed = router_join_handle.await.unwrap().unwrap_or(0);
  let total_received = central_receiver_handle.await.unwrap().unwrap_or(0);
  let total_duration = benchmark_start_time.elapsed();

  // --- MODIFIED: Conditional logging for summary ---
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!("\n--- Test Finished in {:?} ---", total_duration);
    println!("Total Requests Sent by Tasks: {}", total_sent);
    println!("Total Replies Received by Tasks: {}", total_received);
    println!("Total Messages Echoed by Router: {}", router_echoed);
    if total_sent > 0 && total_received > 0 && total_duration.as_secs_f64() > 0.0 {
      let throughput = total_received as f64 / total_duration.as_secs_f64();
      println!("Throughput: {:.2} req/sec", throughput);
    }
  }

  if total_sent != total_received || total_sent != router_echoed {
    eprintln!(
      "**WARNING: Message loss detected! Sent: {}, Received: {}, Router Echoed: {}**",
      total_sent, total_received, router_echoed
    );
  }

  // --- Teardown ---
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!("[Main] Closing sockets and terminating context...");
  }
  dealer_socket_main.close().await?;
  ctx.term().await?;
  shutdown_uring_backend().await?;
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!("[Main] Example finished.");
  }
  Ok(())
}
