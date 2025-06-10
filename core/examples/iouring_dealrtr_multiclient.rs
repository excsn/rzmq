// core/examples/iouring_dealrtr_multiclient.rs

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
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit, Semaphore};

mod common;

// --- Configuration Constants ---
const ROUTER_IO_URING_ENABLED: bool = true;
const DEALER_IO_URING_ENABLED: bool = true;
const SNDZEROCPY_IO_URING_ENABLED: bool = false;
const TCP_CORK_ENABLED: u32 = 1;

const NUM_DEALERS: usize = 8;
const MAX_CONCURRENT_REQUESTS: usize = 1000;
const NUM_MSGS_PER_DEALER: u64 = 250_000;
const PAYLOAD_SIZE_BYTES: usize = 1024;
const ROUTER_ENDPOINT: &'static str = "tcp://127.0.0.1:5559";

const TOTAL_MESSAGES: u64 = (NUM_DEALERS as u64) * NUM_MSGS_PER_DEALER;

// --- Timeouts & Logging ---
const HANDSHAKE_TIMEOUT_MS: u64 = 10000;

// --- Type Aliases ---
type RequestId = u64;
// The PendingRequestsMap stores the permit and the time the request was sent for latency tracking
type PendingRequestsMap = Arc<TokioMutex<HashMap<RequestId, OwnedSemaphorePermit>>>;

// --- Helper Functions ---
fn generate_request_id(dealer_id: usize, message_seq_num: u64) -> RequestId {
  ((dealer_id as u64) << 32) | (message_seq_num & 0xFFFFFFFF)
}

fn create_request_frames(request_id: RequestId, payload_template: &Bytes) -> Vec<Msg> {
  vec![
    Msg::from_bytes(Bytes::copy_from_slice(&request_id.to_be_bytes())).with_flags(MsgFlags::MORE),
    Msg::from_bytes(payload_template.clone()),
  ]
}

fn extract_reply_data(frames: Vec<Msg>) -> Option<(RequestId, Vec<Msg>)> {
  if frames.len() < 2 {
    return None;
  }
  // The first frame from the ROUTER is the identity of the original DEALER, which ZMTP strips.
  // The second frame is our request ID.
  let request_id_bytes = frames[0].data()?.try_into().ok()?;
  let id = RequestId::from_be_bytes(request_id_bytes);
  Some((id, frames[1..].to_vec()))
}

// --- ROUTER Task (Server Side) ---
async fn run_router_task(router_socket: Socket) -> Result<u64, ZmqError> {
  let mut messages_echoed = 0u64;
  while messages_echoed < TOTAL_MESSAGES {
    let frames = router_socket.recv_multipart().await?;
    router_socket.send_multipart(frames).await?;
    messages_echoed += 1;
  }
  Ok(messages_echoed)
}

// --- DEALER Tasks (Client Side) ---
#[derive(Debug)]
struct DealerSenderStats {
  messages_sent: u64,
}

/// A task to send requests from a single DEALER socket.
async fn run_dealer_sender_task(
  dealer_socket: Socket,
  dealer_id: usize,
  num_messages: u64,
  payload: Bytes,
  pending_map: PendingRequestsMap,
  capacity_gate: Arc<Semaphore>,
) -> Result<DealerSenderStats, ZmqError> {
  for i in 0..num_messages {
    let permit = capacity_gate.clone().acquire_owned().await.unwrap();
    // **FIXED**: Generate a unique ID based on the dealer's own ID and its message sequence.
    // This removes the need for a shared atomic counter.
    let request_id = generate_request_id(dealer_id, i);

    pending_map.lock().await.insert(request_id, permit);

    let frames = create_request_frames(request_id, &payload);
    dealer_socket.send_multipart(frames).await?;
  }
  Ok(DealerSenderStats {
    messages_sent: num_messages,
  })
}

// --- NEW HELPER: Type alias for the complex future type in FuturesUnordered ---
type RecvFuture<'a> =
  Pin<Box<dyn Future<Output = (usize, Result<Vec<Msg>, ZmqError>)> + Send + 'a>>;

/// A central task to receive all replies from all DEALER sockets.
async fn run_dealer_central_receiver(
  dealer_sockets: Vec<Socket>,
  pending_map: PendingRequestsMap,
  total_replies_expected: u64,
) -> u64 {
  let mut replies_processed = 0u64;
  let mut recv_futures: FuturesUnordered<RecvFuture> = FuturesUnordered::new();

  for (idx, socket) in dealer_sockets.iter().enumerate() {
    let fut: RecvFuture = Box::pin(async move { (idx, socket.recv_multipart().await) });
    recv_futures.push(fut);
  }

  while replies_processed < total_replies_expected {
    match recv_futures.next().await {
      Some((socket_idx, Ok(frames))) => {
        // Re-arm the future for this socket to listen for the next reply
        let socket = &dealer_sockets[socket_idx];
        let fut: RecvFuture = Box::pin(async move { (socket_idx, socket.recv_multipart().await) });
        recv_futures.push(fut);

        // Process the received reply
        if let Some((request_id, _)) = extract_reply_data(frames) {
          if let Some(permit) = pending_map.lock().await.remove(&request_id) {
            drop(permit); // Release the semaphore permit
            replies_processed += 1;
          }
        }
      }
      Some((_, Err(e))) => {
        eprintln!("[Central Receiver] Recv error: {}. Shutting down.", e);
        break;
      }
      None => {
        // Stream is empty, this means all sockets have been closed or errored out.
        break;
      }
    }
  }
  replies_processed
}

trait MsgExt {
  fn with_flags(self, flags: MsgFlags) -> Self;
}
impl MsgExt for Msg {
  fn with_flags(mut self, flags: MsgFlags) -> Self {
    self.set_flags(flags);
    self
  }
}

// --- Main Function ---
#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .compact()
    .init();

  println!(
    "Starting io_uring DEALER-ROUTER Multi-Client Example ({} clients, {} msgs/client)...",
    NUM_DEALERS, NUM_MSGS_PER_DEALER
  );

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
      eprintln!("Failed to initialize io_uring backend: {:?}", e);
      return Err(e);
    }
  }

  let ctx = Context::new()?;

  // --- Setup ROUTER ---
  let router_socket = ctx.socket(SocketType::Router)?;
  router_socket
    .set_option(
      zmq_opts::IO_URING_SESSION_ENABLED,
      ROUTER_IO_URING_ENABLED as i32,
    )
    .await?;
  router_socket
    .set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED as i32)
    .await?;
  router_socket
    .set_option(zmq_opts::SNDHWM, (TOTAL_MESSAGES) as i32)
    .await?;
  router_socket
    .set_option(zmq_opts::RCVHWM, (TOTAL_MESSAGES) as i32)
    .await?;
  let mut router_monitor = router_socket.monitor_default().await?;
  router_socket.bind(ROUTER_ENDPOINT).await?;
  let router_handle = tokio::spawn(run_router_task(router_socket.clone()));

  // --- Setup DEALERs ---
  let mut dealer_sockets = Vec::new();
  for i in 0..NUM_DEALERS {
    let dealer = ctx.socket(SocketType::Dealer)?;
    dealer
      .set_option(zmq_opts::ROUTING_ID, format!("dealer_{}", i).as_bytes())
      .await?;
    dealer
      .set_option(
        zmq_opts::IO_URING_SESSION_ENABLED,
        DEALER_IO_URING_ENABLED as i32,
      )
      .await?;
    dealer
      .set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED as i32)
      .await?;
    dealer
      .set_option(zmq_opts::SNDHWM, MAX_CONCURRENT_REQUESTS as i32)
      .await?;
    dealer
      .set_option(zmq_opts::RCVHWM, MAX_CONCURRENT_REQUESTS as i32)
      .await?;
    dealer.connect(ROUTER_ENDPOINT).await?;
    dealer_sockets.push(dealer);
  }

  // --- Wait for All Handshakes ---
  for _ in 0..NUM_DEALERS {
    common::wait_for_event(
      &mut router_monitor,
      Duration::from_millis(HANDSHAKE_TIMEOUT_MS),
      |e| matches!(e, SocketEvent::HandshakeSucceeded { .. }),
    )
    .await
    .expect("Handshake timeout");
  }
  println!("[Main] All {} dealers connected to router.", NUM_DEALERS);

  // --- Shared State & Task Spawning ---
  let pending_requests_map = Arc::new(TokioMutex::new(HashMap::new()));
  let capacity_gate = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
  let payload_template = Bytes::from(vec![0u8; PAYLOAD_SIZE_BYTES]);

  let overall_start_time = Instant::now();

  let central_receiver_handle = tokio::spawn(run_dealer_central_receiver(
    dealer_sockets.clone(),
    pending_requests_map.clone(),
    TOTAL_MESSAGES,
  ));

  let mut sender_handles = Vec::new();
  for i in 0..NUM_DEALERS {
    sender_handles.push(tokio::spawn(run_dealer_sender_task(
      dealer_sockets[i].clone(),
      i,
      NUM_MSGS_PER_DEALER,
      payload_template.clone(),
      pending_requests_map.clone(),
      capacity_gate.clone(),
      // **FIXED**: No longer passing the shared atomic counter.
    )));
  }

  // --- Await Completion & Report ---
  println!("Sending all messages...");
  let sender_results = join_all(sender_handles).await;
  let total_sent: u64 = sender_results
    .into_iter()
    .map(|res| res.unwrap().unwrap().messages_sent)
    .sum();

  println!("Sending finished, awaiting receiver completion...");
  let receiver_count = central_receiver_handle.await.unwrap();

  println!("Now awaiting router completion...");
  let router_echoed = router_handle.await.unwrap().unwrap();

  let total_duration = overall_start_time.elapsed();

  println!("\n--- Test Finished in {:?} ---", total_duration);
  println!("Total Requests Sent by Dealers: {}", total_sent);
  println!("Total Replies Received by Dealers: {}", receiver_count);
  println!("Total Messages Echoed by Router: {}", router_echoed);

  if total_duration.as_secs_f64() > 0.0 {
    let throughput = receiver_count as f64 / total_duration.as_secs_f64();
    println!("Throughput: {:.2} req/sec", throughput);
  }

  if total_sent != receiver_count || total_sent != router_echoed {
    eprintln!(
      "**WARNING: Message loss detected! Sent: {}, Received: {}, Router Echoed: {}**",
      total_sent, receiver_count, router_echoed
    );
  }

  // --- Final Teardown ---
  println!("[Main] Closing sockets and terminating context...");
  for socket in dealer_sockets {
    let _ = socket.close().await;
  }
  let _ = router_socket.close().await;

  ctx.term().await?;
  shutdown_uring_backend().await?;
  Ok(())
}
