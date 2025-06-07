use futures::future::join_all;
use futures::{stream::FuturesUnordered, StreamExt};
use rzmq::{
  socket::{options::ROUTING_ID, MonitorReceiver, SocketEvent},
  Context, Msg, MsgFlags, Socket, SocketType, ZmqError,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

mod common;
use common::capacity_gate::{CapacityGate, OwnedPermitGuard};

// --- Constants (Unchanged) ---
const NUM_DEALERS_EXAMPLE: usize = 8;
const NUM_MSGS_PER_DEALER_EXAMPLE: u64 = 100_000;
const BIND_ADDR_EXAMPLE: &str = "tcp://127.0.0.1:8960";
const HWM_EXAMPLE: i32 = 2000;
const PAYLOAD_SIZE_BYTES: usize = 128;
const MAX_CONCURRENT_REQUESTS: usize = 1000;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const CENTRAL_RECEIVER_IDLE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PrintLogLevel {
  Off,
  Info,
  Verbose,
}
const PRINT_LEVEL: PrintLogLevel = PrintLogLevel::Info;
const USE_MULTIPART_API: bool = true;

type RequestId = u64;
type PendingRequestsMap = Arc<tokio::sync::Mutex<HashMap<RequestId, OwnedPermitGuard>>>;

// --- Helper Functions (Unchanged) ---
async fn wait_for_handshakes(
  router_monitor: &mut MonitorReceiver,
  num_expected: usize,
  timeout_duration: Duration,
) {
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!("[Setup] Waiting for {} handshake events...", num_expected);
  }
  let deadline = Instant::now() + timeout_duration;
  for i in 0..num_expected {
    loop {
      if Instant::now() > deadline {
        panic!("Timeout waiting for handshake event #{}", i + 1);
      }
      match timeout(deadline - Instant::now(), router_monitor.recv()).await {
        Ok(Ok(SocketEvent::HandshakeSucceeded { .. })) => {
          break;
        }
        Ok(Ok(_)) => {}
        Ok(Err(_)) => panic!("Router monitor channel closed unexpectedly"),
        Err(_) => continue,
      }
    }
  }
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!(
      "[Setup] All {} handshakes confirmed by monitor.",
      num_expected
    );
  }
  sleep(Duration::from_millis(100)).await;
}

async fn setup_dealer_router_example(
  ctx: &Context,
  num_dealers: usize,
) -> Result<(Vec<rzmq::Socket>, rzmq::Socket), ZmqError> {
  let router = ctx.socket(SocketType::Router)?;
  router
    .set_option_raw(rzmq::socket::options::SNDHWM, &HWM_EXAMPLE.to_ne_bytes())
    .await?;
  router
    .set_option_raw(rzmq::socket::options::RCVHWM, &HWM_EXAMPLE.to_ne_bytes())
    .await?;
  let mut router_monitor = router.monitor_default().await?;
  router.bind(BIND_ADDR_EXAMPLE).await?;
  let mut dealers = Vec::with_capacity(num_dealers);
  for i in 0..num_dealers {
    let dealer = ctx.socket(SocketType::Dealer)?;
    dealer
      .set_option_raw(ROUTING_ID, format!("dealer_{}", i).as_bytes())
      .await?;
    dealer
      .set_option_raw(rzmq::socket::options::SNDHWM, &HWM_EXAMPLE.to_ne_bytes())
      .await?;
    dealer
      .set_option_raw(rzmq::socket::options::RCVHWM, &HWM_EXAMPLE.to_ne_bytes())
      .await?;
    dealer.connect(BIND_ADDR_EXAMPLE).await?;
    dealers.push(dealer);
  }
  wait_for_handshakes(&mut router_monitor, num_dealers, HANDSHAKE_TIMEOUT).await;
  Ok((dealers, router))
}

async fn run_router_task(
  router_socket: rzmq::Socket,
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

#[derive(Debug)]
struct DealerSenderStats {
  id: usize,
  messages_sent: u64,
}
async fn run_dealer_sender_task(
  dealer_socket: Socket,
  dealer_id: usize,
  num_messages: u64,
  payload: Vec<u8>,
  pending_map: PendingRequestsMap,
  capacity_gate: Arc<CapacityGate>,
  request_id_counter: Arc<AtomicU64>,
) -> Result<DealerSenderStats, ZmqError> {
  let mut messages_sent_count = 0;
  for _ in 0..num_messages {
    let permit = capacity_gate.clone().acquire_owned().await;
    let request_id = request_id_counter.fetch_add(1, Ordering::Relaxed);
    pending_map.lock().await.insert(request_id, permit);
    let mut id_msg = Msg::from_vec(request_id.to_be_bytes().to_vec());
    let payload_msg = Msg::from_vec(payload.clone());
    let send_result = if USE_MULTIPART_API {
      id_msg.set_flags(MsgFlags::MORE);
      dealer_socket
        .send_multipart(vec![id_msg, payload_msg])
        .await
    } else {
      id_msg.set_flags(MsgFlags::MORE);
      dealer_socket.send(id_msg).await?;
      dealer_socket.send(payload_msg).await
    };
    if let Err(e) = send_result {
      eprintln!("[Dealer {} Sender] Send error: {}. Aborting.", dealer_id, e);
      pending_map.lock().await.remove(&request_id);
      return Err(e);
    }
    messages_sent_count += 1;
  }
  Ok(DealerSenderStats {
    id: dealer_id,
    messages_sent: messages_sent_count,
  })
}

// ====================================================================
// CORRECTED DEALER CENTRAL RECEIVER TASK
// ====================================================================

// NEW: Type alias for the future to solve the mismatched types error
type RecvFuture<'a> =
  Pin<Box<dyn Future<Output = (usize, Result<Vec<Msg>, ZmqError>)> + Send + 'a>>;

/// Receives one complete logical message from a dealer socket, using the
/// API specified by the `USE_MULTIPART_API` constant.
async fn receive_logical_message(dealer_socket: &Socket) -> Result<Vec<Msg>, ZmqError> {
  if USE_MULTIPART_API {
    dealer_socket.recv_multipart().await
  } else {
    let frame1 = dealer_socket.recv().await?;
    let frame2 = dealer_socket.recv().await?;
    Ok(vec![frame1, frame2])
  }
}

async fn run_dealer_central_receiver(
  dealer_sockets: Vec<Socket>,
  pending_map: PendingRequestsMap,
  total_expected_replies: u64,
) -> Result<u64, ZmqError> {
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!(
      "[Dealer Central Receiver] Task started. Listening on {} sockets.",
      dealer_sockets.len()
    );
  }
  let mut replies_processed = 0u64;

  // Use the type alias for the FuturesUnordered collection
  let mut recv_futures: FuturesUnordered<RecvFuture> = FuturesUnordered::new();
  for (idx, socket) in dealer_sockets.iter().enumerate() {
    // Explicitly cast the boxed future to the trait object type
    let fut: RecvFuture = Box::pin(async move { (idx, receive_logical_message(socket).await) });
    recv_futures.push(fut);
  }

  while replies_processed < total_expected_replies {
    match timeout(CENTRAL_RECEIVER_IDLE_TIMEOUT, recv_futures.next()).await {
      Ok(Some((socket_idx, Ok(frames)))) => {
        // Re-arm the future for this socket
        let socket = &dealer_sockets[socket_idx];
        let fut: RecvFuture =
          Box::pin(async move { (socket_idx, receive_logical_message(socket).await) });
        recv_futures.push(fut);

        // Process the received frames
        if frames.len() < 2 {
          continue;
        }

        // The router's identity is stripped by the dealer, so frames[0] is our request_id
        let request_id_bytes_opt = frames[0].data().and_then(|d| d.try_into().ok());

        if let Some(id_bytes) = request_id_bytes_opt {
          let request_id = RequestId::from_be_bytes(id_bytes);
          if let Some(permit) = pending_map.lock().await.remove(&request_id) {
            drop(permit);
            replies_processed += 1;
          }
        }
      }
      Ok(Some((_, Err(e)))) => return Err(e),
      Ok(None) | Err(_) => break,
    }
  }
  Ok(replies_processed)
}

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .compact()
    .init();
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!(
      "Starting Advanced Dealer-Router Example ({} Dealers, {} Msgs/Dealer)...",
      NUM_DEALERS_EXAMPLE, NUM_MSGS_PER_DEALER_EXAMPLE
    );
  }
  let ctx = Context::new()?;
  let (dealer_sockets, router_socket) =
    setup_dealer_router_example(&ctx, NUM_DEALERS_EXAMPLE).await?;
  let total_messages = (NUM_DEALERS_EXAMPLE as u64) * NUM_MSGS_PER_DEALER_EXAMPLE;

  let router_handle = tokio::spawn(run_router_task(router_socket.clone(), total_messages));

  let pending_requests_map: PendingRequestsMap = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
  let concurrency_gate = Arc::new(CapacityGate::new(MAX_CONCURRENT_REQUESTS));
  let request_id_counter = Arc::new(AtomicU64::new(0));
  let payload_template = vec![0u8; PAYLOAD_SIZE_BYTES];

  let overall_start_time = Instant::now();

  let central_receiver_handle = tokio::spawn(run_dealer_central_receiver(
    dealer_sockets.clone(),
    pending_requests_map.clone(),
    total_messages,
  ));

  let mut sender_handles = Vec::new();
  for i in 0..NUM_DEALERS_EXAMPLE {
    sender_handles.push(tokio::spawn(run_dealer_sender_task(
      dealer_sockets[i].clone(),
      i,
      NUM_MSGS_PER_DEALER_EXAMPLE,
      payload_template.clone(),
      pending_requests_map.clone(),
      concurrency_gate.clone(),
      request_id_counter.clone(),
    )));
  }

  let sender_results = join_all(sender_handles).await;
  let receiver_result = central_receiver_handle.await;
  let router_result = router_handle.await;
  let total_duration = overall_start_time.elapsed();

  let mut total_sent = 0;
  for (i, res) in sender_results.into_iter().enumerate() {
    match res {
      Ok(Ok(stats)) => total_sent += stats.messages_sent,
      Ok(Err(e)) => eprintln!("[Main] Dealer sender task {} failed: {}", i, e),
      Err(e) => eprintln!("[Main] Dealer sender task {} panicked: {}", i, e),
    }
  }

  let total_received = match receiver_result {
    Ok(Ok(count)) => count,
    Ok(Err(e)) => {
      eprintln!("[Main] Central receiver task failed with ZmqError: {}", e);
      0
    }
    Err(e) => {
      eprintln!("[Main] Central receiver task panicked: {}", e);
      0
    }
  };
  let router_echoed = router_result.unwrap().unwrap_or(0);

  println!("\n--- Test Finished in {:?} ---", total_duration);
  println!("Total Requests Sent by Dealers: {}", total_sent);
  println!("Total Replies Received by Dealers: {}", total_received);
  println!("Total Messages Echoed by Router: {}", router_echoed);
  let throughput = total_received as f64 / total_duration.as_secs_f64();
  println!("Throughput: {:.2} req/sec", throughput);

  if total_sent != total_received || total_sent != router_echoed {
    eprintln!(
      "**WARNING: Message loss detected! Sent: {}, Received: {}, Router Echoed: {}**",
      total_sent, total_received, router_echoed
    );
  }

  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!("[Main] Closing sockets and terminating context...");
  }
  for socket in dealer_sockets {
    let _ = socket.close().await;
  }
  let _ = router_socket.close().await;

  tokio::time::timeout(SHUTDOWN_TIMEOUT, ctx.term())
    .await
    .expect("Context termination timed out!");
  Ok(())
}
