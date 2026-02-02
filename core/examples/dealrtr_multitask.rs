use futures::{future::join_all, stream::FuturesUnordered, StreamExt};
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

// --- Constants ---
const NUM_DEALER_TASKS: usize = 8;
const NUM_MSGS_PER_TASK: u64 = 100_000;
const BIND_ADDR_EXAMPLE: &str = "tcp://127.0.0.1:8961";
const HWM_EXAMPLE: i32 = 2000;
const PAYLOAD_SIZE_BYTES: usize = 128;
const MAX_CONCURRENT_REQUESTS: usize = 2000;
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

// --- Setup and Helper Functions ---
async fn wait_for_handshake(router_monitor: &mut MonitorReceiver, timeout_duration: Duration) {
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!("[Setup] Waiting for handshake event...");
  }
  let deadline = Instant::now() + timeout_duration;
  loop {
    if Instant::now() > deadline {
      panic!("Timeout waiting for handshake event");
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
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!("[Setup] Handshake confirmed by monitor.");
  }
  sleep(Duration::from_millis(100)).await;
}

async fn setup_dealer_router_example(ctx: &Context) -> Result<(Socket, Socket), ZmqError> {
  let router = ctx.socket(SocketType::Router)?;
  router
    .set_option_raw(rzmq::socket::options::SNDHWM, &HWM_EXAMPLE.to_ne_bytes())
    .await?;
  router
    .set_option_raw(rzmq::socket::options::RCVHWM, &HWM_EXAMPLE.to_ne_bytes())
    .await?;
  let mut router_monitor = router.monitor_default().await?;
  router.bind(BIND_ADDR_EXAMPLE).await?;
  let dealer = ctx.socket(SocketType::Dealer)?;
  dealer
    .set_option_raw(ROUTING_ID, b"shared-dealer-socket")
    .await?;
  dealer
    .set_option_raw(rzmq::socket::options::SNDHWM, &HWM_EXAMPLE.to_ne_bytes())
    .await?;
  dealer
    .set_option_raw(rzmq::socket::options::RCVHWM, &HWM_EXAMPLE.to_ne_bytes())
    .await?;
  dealer.connect(BIND_ADDR_EXAMPLE).await?;
  wait_for_handshake(&mut router_monitor, HANDSHAKE_TIMEOUT).await;
  Ok((dealer, router))
}

// --- ROUTER Task Logic ---
async fn run_router_task(
  router_socket: Socket,
  total_expected_messages: u64,
) -> Result<u64, ZmqError> {
  let mut messages_echoed = 0u64;
  for _ in 0..total_expected_messages {
    let frames = router_socket.recv_multipart().await?; // Router always gets [id, delimiter, app_frames...]
    router_socket.send_multipart(frames).await?;
    messages_echoed += 1;
  }
  Ok(messages_echoed)
}

// --- DEALER SENDER Task ---
#[derive(Debug)]
struct DealerSenderStats {
  id: usize,
  messages_sent: u64,
}
async fn run_dealer_sender_task(
  dealer_socket: Socket,
  task_id: usize,
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

    // This is correct: Dealer application always provides the logical payload.
    // The socket adds the delimiter. So the logical message is [request_id, payload].
    id_msg.set_flags(MsgFlags::MORE);
    dealer_socket
      .send_multipart(vec![id_msg, payload_msg])
      .await?;

    messages_sent_count += 1;
  }
  Ok(DealerSenderStats {
    id: task_id,
    messages_sent: messages_sent_count,
  })
}

// --- CORRECTED CENTRAL RECEIVER and HELPERS ---

type RecvFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<Msg>, ZmqError>> + Send + 'a>>;

/// Receives one complete logical message from the dealer socket.
async fn receive_reply(dealer_socket: &Socket) -> Result<Vec<Msg>, ZmqError> {
  if USE_MULTIPART_API {
    dealer_socket.recv_multipart().await
  } else {
    // A logical reply message is [request_id, payload]
    let frame1 = dealer_socket.recv().await?;
    let frame2 = dealer_socket.recv().await?;
    Ok(vec![frame1, frame2])
  }
}

async fn run_dealer_central_receiver(
  dealer_socket: Socket,
  pending_map: PendingRequestsMap,
  total_expected_replies: u64,
) -> Result<u64, ZmqError> {
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!("[Dealer Central Receiver] Task started.");
  }
  let mut replies_processed = 0u64;

  let mut recv_futures: FuturesUnordered<RecvFuture> = FuturesUnordered::new();
  // Prime the loop with the first receive future
  recv_futures.push(Box::pin(async { receive_reply(&dealer_socket).await }));

  while replies_processed < total_expected_replies {
    match timeout(CENTRAL_RECEIVER_IDLE_TIMEOUT, recv_futures.next()).await {
      Ok(Some(Ok(frames))) => {
        // Re-arm the future for the next receive operation
        recv_futures.push(Box::pin(async { receive_reply(&dealer_socket).await }));

        // Process the received logical message
        if frames.len() < 2 {
          continue;
        }
        let request_id_bytes_opt = frames[0].data().and_then(|d| d.try_into().ok());

        if let Some(id_bytes) = request_id_bytes_opt {
          let request_id = RequestId::from_be_bytes(id_bytes);
          if let Some(permit) = pending_map.lock().await.remove(&request_id) {
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

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .compact()
    .init();
  if PRINT_LEVEL >= PrintLogLevel::Info {
    println!(
      "Starting Single-Dealer Multi-Task Example ({} Tasks, {} Msgs/Task)...",
      NUM_DEALER_TASKS, NUM_MSGS_PER_TASK
    );
  }
  let ctx = Context::new()?;
  let (dealer_socket, router_socket) = setup_dealer_router_example(&ctx).await?;
  let total_messages = (NUM_DEALER_TASKS as u64) * NUM_MSGS_PER_TASK;

  let router_handle = tokio::spawn(run_router_task(router_socket.clone(), total_messages));

  let pending_requests_map: PendingRequestsMap = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
  let concurrency_gate = Arc::new(CapacityGate::new(MAX_CONCURRENT_REQUESTS));
  let request_id_counter = Arc::new(AtomicU64::new(0));
  let payload_template = vec![0u8; PAYLOAD_SIZE_BYTES];

  let overall_start_time = Instant::now();

  let central_receiver_handle = tokio::spawn(run_dealer_central_receiver(
    dealer_socket.clone(),
    pending_requests_map.clone(),
    total_messages,
  ));

  let mut sender_handles = Vec::new();
  for i in 0..NUM_DEALER_TASKS {
    sender_handles.push(tokio::spawn(run_dealer_sender_task(
      dealer_socket.clone(),
      i,
      NUM_MSGS_PER_TASK,
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
      Ok(Err(e)) => eprintln!("[Main] Dealer task {} failed: {}", i, e),
      Err(e) => eprintln!("[Main] Dealer task {} panicked: {}", i, e),
    }
  }

  let total_received = match receiver_result {
    Ok(Ok(count)) => count,
    Ok(Err(e)) => {
      eprintln!("[Main] Central receiver task failed: {}", e);
      0
    }
    Err(e) => {
      eprintln!("[Main] Central receiver task panicked: {}", e);
      0
    }
  };
  let router_echoed = router_result.unwrap().unwrap_or(0);

  println!("\n--- Test Finished in {:?} ---", total_duration);
  println!("Total Requests Sent by Tasks: {}", total_sent);
  println!("Total Replies Received by Tasks: {}", total_received);
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
  let _ = dealer_socket.close().await;
  let _ = router_socket.close().await;

  tokio::time::timeout(SHUTDOWN_TIMEOUT, ctx.term())
    .await
    .expect("Context termination timed out!");
  Ok(())
}