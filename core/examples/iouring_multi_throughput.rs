// examples/dealer_router_iouring_throughput.rs

use rzmq::{Context, Msg, MsgFlags, Socket, SocketType, ZmqError};
use rzmq::socket::{events::SocketEvent, options as zmq_opts};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::Bytes;
use futures::future::join_all;
use tokio::sync::{Mutex as TokioMutex, Semaphore, OwnedSemaphorePermit};

mod common;

// --- Configuration Constants ---
const ROUTER_IO_URING_ENABLED: bool = true;
const DEALER_IO_URING_ENABLED: bool = true;
const TCP_CORK_ENABLED: u32 = 1; // 0 for false, 1 for true
const NUM_DEALER_TASKS: usize = 2;
const MAX_CONCURRENT_REQUESTS: usize = 40000;
const NUM_MESSAGES_PER_DEALER: u64 = 50000; // Reverted for full test
const PAYLOAD_SIZE_BYTES: usize = 1024;
const ROUTER_ENDPOINT: &'static str = "tcp://127.0.0.1:5558";

const HANDSHAKE_TIMEOUT_MS: u64 = 5000;
const ROUTER_RECV_TIMEOUT_MS: u64 = 10000; // Router timeout for individual recvs
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

type RequestId = u64;
type PendingRequestsMap = Arc<TokioMutex<HashMap<RequestId, OwnedSemaphorePermit>>>;

fn generate_request_id(dealer_id: usize, message_seq_num: u64) -> RequestId {
  ((dealer_id as u64) << 32) | (message_seq_num & 0xFFFFFFFF)
}

fn create_request_frames(request_id: RequestId, payload_template: &Bytes) -> Vec<Msg> {
  let mut req_id_msg = Msg::from_bytes(Bytes::copy_from_slice(&request_id.to_be_bytes()));
  req_id_msg.set_flags(MsgFlags::MORE);
  
  let mut payload_msg = Msg::from_bytes(payload_template.clone());
  payload_msg.set_flags(MsgFlags::empty());

  vec![req_id_msg, payload_msg]
}

fn extract_reply_data(mut frames: Vec<Msg>) -> Option<(RequestId, Vec<Msg>)> {
    if frames.is_empty() {
        if PRINT_INDIVIDUAL_MESSAGES { eprintln!("[DEALER Central Receiver] extract_reply_data: Received empty frame list.");}
        return None;
    }
    if frames[0].size() == 0 { // ZMTP empty delimiter prepended by ROUTER socket logic
        if PRINT_INDIVIDUAL_MESSAGES { println!("[DEALER Central Receiver] extract_reply_data: Consuming leading empty delimiter frame from ROUTER."); }
        frames.remove(0);
        if frames.is_empty() {
             if PRINT_INDIVIDUAL_MESSAGES { eprintln!("[DEALER Central Receiver] extract_reply_data: No frames after empty delimiter."); }
            return None;
        }
    }
    if frames.is_empty() {
        if PRINT_INDIVIDUAL_MESSAGES { eprintln!("[DEALER Central Receiver] extract_reply_data: No request_id frame found after delimiter processing."); }
        return None;
    }

    let request_id_frame = frames.remove(0); // This should be the request_id echoed by the router
    let request_id_bytes = match request_id_frame.data() {
        Some(d) => d,
        None => {
            if PRINT_INDIVIDUAL_MESSAGES { eprintln!("[DEALER Central Receiver] extract_reply_data: request_id_frame has no data."); }
            return None;
        }
    };
    if request_id_bytes.len() != 8 {
        if PRINT_INDIVIDUAL_MESSAGES {
            eprintln!("[DEALER Central Receiver] Malformed reply: request_id frame wrong size (expected 8, got {}). Content (hex): {:02x?}",
            request_id_bytes.len(), request_id_bytes);
        }
        return None;
    }
    let id = u64::from_be_bytes(request_id_bytes.try_into().ok()?);
    Some((id, frames)) // Remaining frames are the echoed payload
}

// --- ROUTER Task (Server Side) ---
async fn run_router_task(
    router_socket: Socket,
    endpoint: &'static str,
    total_expected_messages: u64,
) -> Result<(u64, u64, u64), ZmqError> { // Returns (echoed_msgs, echoed_bytes, timeouts)

    println!("[ROUTER] Bound successfully. Expecting {} total messages.", total_expected_messages);
    let mut messages_echoed = 0u64;
    let mut bytes_echoed = 0u64;
    let mut timeouts_occurred = 0u64;
    let mut received_count = 0u64;

    while received_count < total_expected_messages {
        match router_socket.recv_multipart().await {
            Ok(mut received_parts) => {
                received_count += 1;
                if received_parts.len() < 2 {
                    if PRINT_INDIVIDUAL_MESSAGES {
                        eprintln!(
                            "[ROUTER] Received malformed message ({} parts), expecting at least 2 (identity, request_id, ...). Received count {}. Ignoring. Parts: {:?}",
                            received_parts.len(), received_count, received_parts.iter().map(|m| m.size()).collect::<Vec<_>>()
                        );
                    }
                    continue;
                }
                let client_identity_frame = received_parts.remove(0);
                if PRINT_INDIVIDUAL_MESSAGES {
                    let request_id_from_payload = if !received_parts.is_empty() && received_parts[0].data().map_or(0, |d| d.len()) == 8 {
                        let id_bytes: [u8; 8] = received_parts[0].data().unwrap()[0..8].try_into().unwrap();
                        Some(u64::from_be_bytes(id_bytes))
                    } else { None };
                    println!(
                        "[ROUTER] Received (idx {}): Identity='{:?}', RequestId={:?}, AppPayloadParts (after req_id)={}",
                        received_count - 1, String::from_utf8_lossy(client_identity_frame.data().unwrap_or_default()),
                        request_id_from_payload, received_parts.len().saturating_sub(1)
                    );
                }
                let mut reply_to_send: Vec<Msg> = Vec::with_capacity(1 + received_parts.len());
                let mut id_reply_frame = client_identity_frame;
                if !received_parts.is_empty() {
                    id_reply_frame.set_flags(id_reply_frame.flags() | MsgFlags::MORE);
                } else {
                    id_reply_frame.set_flags(id_reply_frame.flags() & !MsgFlags::MORE);
                }
                reply_to_send.push(id_reply_frame);
                let num_app_frames_to_echo = received_parts.len();
                for (idx, mut part) in received_parts.into_iter().enumerate() {
                    bytes_echoed += part.size() as u64;
                    if idx < num_app_frames_to_echo - 1 {
                        part.set_flags(part.flags() | MsgFlags::MORE);
                    } else {
                        part.set_flags(part.flags() & !MsgFlags::MORE);
                    }
                    reply_to_send.push(part);
                }
                if let Err(e) = router_socket.send_multipart(reply_to_send).await {
                    eprintln!("[ROUTER] Error sending reply for received message #{}: {}", received_count, e);
                    return Err(e);
                }
                messages_echoed += 1;
            }
            Err(ZmqError::Timeout) => {
                timeouts_occurred += 1;
                if PRINT_INDIVIDUAL_MESSAGES || timeouts_occurred % 100 == 0 {
                    eprintln!("[ROUTER] Recv timeout (total timeouts: {}). Still expecting {} of {} total messages.",
                        timeouts_occurred, total_expected_messages.saturating_sub(received_count), total_expected_messages);
                }
                if timeouts_occurred > (total_expected_messages / 100).max(20) && received_count < total_expected_messages {
                    eprintln!("[ROUTER] Excessive timeouts ({}). Terminating echo task prematurely. Received {}/{} messages.",
                        timeouts_occurred, received_count, total_expected_messages);
                    break;
                }
            }
            Err(ZmqError::InvalidState(ref msg)) if msg.contains("Socket is closing") => {
                println!("[ROUTER] Socket is closing, terminating echo task. Received {} messages.", received_count);
                break;
            }
            Err(e) => {
                eprintln!("[ROUTER] recv_multipart error after receiving {} messages: {}. Terminating echo task.", received_count, e);
                return Err(e);
            }
        }
    }
    println!(
        "[ROUTER] Echo task finished. Total Actually Received: {}. Messages Echoed: {}. Timeouts During Recv: {}. Bytes Echoed: {}.",
        received_count, messages_echoed, timeouts_occurred, bytes_echoed
    );
    Ok((messages_echoed, bytes_echoed, timeouts_occurred))
}

// --- DEALER Side ---

#[derive(Debug)]
struct DealerTaskStats {
    id: usize,
    messages_sent: u64,
    bytes_sent: u64,
    send_errors: u64,
    duration: Duration,
}

async fn run_dealer_central_receiver(
    dealer_socket: rzmq::Socket,
    pending_requests_map: PendingRequestsMap,
    total_expected_replies: u64,
) -> (u64, u64, u64, Option<Instant>) { // (replies_processed, bytes_replied, extraction_errors, last_reply_timestamp)
    println!("[DEALER Central Receiver] Task started. Expecting up to {} replies.", total_expected_replies);
    let mut replies_processed = 0u64;
    let mut bytes_replied = 0u64;
    let mut extraction_errors = 0u64;
    let mut last_message_activity_time = Instant::now();
    let mut last_valid_reply_timestamp: Option<Instant> = None;

    while replies_processed + extraction_errors < total_expected_replies {
        match dealer_socket.recv_multipart().await {
            Ok(mut frames_from_dealer_socket) => {
                last_message_activity_time = Instant::now();
                if PRINT_INDIVIDUAL_MESSAGES {
                    println!("[DEALER Central Receiver] Raw frames from socket (count: {}): sizes {:?}",
                       frames_from_dealer_socket.len(),
                       frames_from_dealer_socket.iter().map(|f| f.size()).collect::<Vec<_>>());
                }
                if let Some((request_id, reply_payload_parts)) = extract_reply_data(frames_from_dealer_socket) {
                    if PRINT_INDIVIDUAL_MESSAGES {
                        println!("[DEALER Central Receiver] Extracted RequestId: {}, with {} payload parts.", request_id, reply_payload_parts.len());
                    }
                    let mut map_guard = pending_requests_map.lock().await;
                    if let Some(_permit_to_drop) = map_guard.remove(&request_id) {
                        drop(map_guard);
                        replies_processed += 1;
                        for part in &reply_payload_parts {
                            bytes_replied += part.size() as u64;
                        }
                        last_valid_reply_timestamp = Some(Instant::now());
                        if PRINT_INDIVIDUAL_MESSAGES {
                            println!("[DEALER Central Receiver] Processed reply for RequestId {}, permit released.", request_id);
                        }
                    } else {
                        drop(map_guard);
                         if PRINT_INDIVIDUAL_MESSAGES {
                            println!("[DEALER Central Receiver] No pending permit for RequestId {} (already timed out by main logic or late/duplicate reply).", request_id);
                         }
                    }
                } else {
                    if PRINT_INDIVIDUAL_MESSAGES { eprintln!("[DEALER Central Receiver] Failed to extract RequestId from reply. Frames might be malformed."); }
                    extraction_errors += 1;
                }
            }
            Err(ZmqError::InvalidState(ref msg)) if msg.contains("Socket is closing") => {
                println!("[DEALER Central Receiver] Main DEALER socket closing (likely signaled by main). Terminating receiver task.");
                break;
            }
            Err(ZmqError::Timeout) => {
                if PRINT_INDIVIDUAL_MESSAGES { println!("[DEALER Central Receiver] Main DEALER socket recv timed out."); }
                if last_message_activity_time.elapsed() > Duration::from_millis(CENTRAL_RECEIVER_IDLE_TIMEOUT_MS) &&
                   (replies_processed + extraction_errors < total_expected_replies) { // Only break if idle AND still expecting more
                    println!("[DEALER Central Receiver] Idle for over {}ms. Processed {} of {} expected replies. Terminating.",
                        CENTRAL_RECEIVER_IDLE_TIMEOUT_MS, replies_processed + extraction_errors, total_expected_replies);
                    break;
                }
                if replies_processed + extraction_errors >= total_expected_replies { // All expected accounted for
                     println!("[DEALER Central Receiver] Socket recv timed out, but all expected replies/errors accounted for. Terminating.");
                    break;
                }
                // Otherwise, continue waiting if still expecting replies and not idle for too long.
            }
            Err(e) => {
                eprintln!("[DEALER Central Receiver] Error receiving from main DEALER socket: {}. Terminating.", e);
                break;
            }
        }
    }
    println!("[DEALER Central Receiver] Task finished. Replies processed: {}, Bytes replied: {}, Extraction Errors: {}", replies_processed, bytes_replied, extraction_errors);
    (replies_processed, bytes_replied, extraction_errors, last_valid_reply_timestamp)
}


async fn run_dealer_task(
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
                eprintln!("[DEALER {}] Shared semaphore closed while acquiring for send (idx {}). Aborting task.", dealer_id, j);
                send_errors += num_messages_for_this_task - j;
                break;
            }
        };

        let request_id = generate_request_id(dealer_id, j);
        pending_requests_map_clone.lock().await.insert(request_id, permit);

        let app_frames_to_send = create_request_frames(request_id, &message_payload_template);
        let current_payload_total_size: u64 = app_frames_to_send.iter().map(|m| m.size() as u64).sum();

        if PRINT_INDIVIDUAL_MESSAGES {
            println!("[DEALER {}] Sending RequestId: {}", dealer_id, request_id);
        }

        match dealer_socket_shared_clone.send_multipart(app_frames_to_send).await {
            Ok(_) => {
                task_messages_sent += 1;
                task_bytes_sent += current_payload_total_size;
            }
            Err(e) => {
                eprintln!("[DEALER {}] Send error for RequestId {}: {}", dealer_id, request_id, e);
                send_errors += 1;
                if let Some(permit_to_drop) = pending_requests_map_clone.lock().await.remove(&request_id) {
                    drop(permit_to_drop);
                     if PRINT_INDIVIDUAL_MESSAGES { println!("[DEALER {}] Send error path: Permit for RequestId {} released.", dealer_id, request_id); }
                } else {
                     if PRINT_INDIVIDUAL_MESSAGES { println!("[DEALER {}] Send error path: No permit found in map for failed RequestId {}.", dealer_id, request_id); }
                }
            }
        }
    }

    if PRINT_INDIVIDUAL_MESSAGES && task_messages_sent == num_messages_for_this_task {
        println!("[DEALER {}] All {} sends initiated by this task.", dealer_id, task_messages_sent);
    }

    let task_duration = task_start_time.elapsed();
    if PRINT_PER_TASK_SUMMARY || send_errors > 0 {
        println!(
            "[DEALER {}] Task finished sending. Duration: {:?}. Sent: {} (err:{}). Rate (sent): {:.2} req/s",
            dealer_id,
            task_duration,
            task_messages_sent, send_errors,
            task_messages_sent as f64 / task_duration.as_secs_f64().max(1e-9)
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
        "Starting DEALER-ROUTER Throughput Example ({} DEALER tasks, {} msgs/task, {} max concurrent requests)...",
        NUM_DEALER_TASKS, NUM_MESSAGES_PER_DEALER, MAX_CONCURRENT_REQUESTS
    );

    let ctx = Context::new().expect("Failed to create rzmq context");

    let router_socket = ctx.socket(SocketType::Router)?;
    router_socket.set_option(zmq_opts::IO_URING_SESSION_ENABLED, ROUTER_IO_URING_ENABLED).await?;
    router_socket.set_option(zmq_opts::IO_URING_RCVMULTISHOT, ROUTER_IO_URING_ENABLED).await?;
    // router_socket.set_option(zmq_opts::IO_URING_SNDZEROCOPY, ROUTER_IO_URING_ENABLED).await?;
    router_socket.set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED).await?;
    router_socket.set_option(zmq_opts::RCVHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32).max(5000)).await?;
    router_socket.set_option(zmq_opts::SNDHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32).max(5000)).await?;
    router_socket.set_option(zmq_opts::RCVTIMEO, ROUTER_RECV_TIMEOUT_MS as i32).await?;

    let router_monitor_rx = router_socket.monitor_default().await?;
    println!("[ROUTER] Binding to {}...", ROUTER_ENDPOINT);
    router_socket.bind(ROUTER_ENDPOINT).await?;
    let router_join_handle = tokio::spawn(run_router_task(
        router_socket,
        ROUTER_ENDPOINT,
        TOTAL_MESSAGES_EXPECTED_BY_ROUTER,
    ));

    tokio::time::sleep(Duration::from_millis(200)).await; // Give router time to bind

    let dealer_socket_main = ctx.socket(SocketType::Dealer)?;
    dealer_socket_main.set_option(zmq_opts::IO_URING_SESSION_ENABLED, DEALER_IO_URING_ENABLED).await?;
    dealer_socket_main.set_option(zmq_opts::IO_URING_RCVMULTISHOT, DEALER_IO_URING_ENABLED).await?;
    // dealer_socket_main.set_option(zmq_opts::IO_URING_SNDZEROCOPY, DEALER_IO_URING_ENABLED).await?;
    dealer_socket_main.set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED).await?;
    let hwm_val = (MAX_CONCURRENT_REQUESTS * NUM_DEALER_TASKS * 2).max(5000) as i32; // Ensure HWM is generous
    dealer_socket_main.set_option(zmq_opts::SNDHWM, hwm_val).await?;
    dealer_socket_main.set_option(zmq_opts::RCVHWM, hwm_val).await?;
    dealer_socket_main.set_option(zmq_opts::SNDTIMEO, 5000i32).await?;
    dealer_socket_main.set_option(zmq_opts::RCVTIMEO, CENTRAL_RECEIVER_SOCKET_RECV_TIMEOUT_MS as i32).await?;


    let dealer_monitor_rx = dealer_socket_main.monitor_default().await?;
    println!("[DEALER Main] Connecting to {}...", ROUTER_ENDPOINT);
    dealer_socket_main.connect(ROUTER_ENDPOINT).await?;
    println!("[DEALER Main] Connect call returned. Waiting for handshake...");

    common::wait_for_handshake_events(
        vec![
            (router_monitor_rx, ROUTER_ENDPOINT.to_string(), String::from("ROUTER MAIN")),
            (dealer_monitor_rx, ROUTER_ENDPOINT.to_string(), String::from("DEALER MAIN")),
        ],
        Duration::from_millis(HANDSHAKE_TIMEOUT_MS),
        PRINT_HANDSHAKE_EVENTS,
    ).await;
    println!("[DEALER Main] All expected handshakes complete.");

    let pending_requests_map: PendingRequestsMap = Arc::new(TokioMutex::new(HashMap::new()));
    let shared_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));

    let central_receiver_dealer_socket_clone = dealer_socket_main.clone();
    let central_receiver_map_clone = pending_requests_map.clone();
    // total_expected_replies will be updated after dealer tasks finish sending.
    let central_receiver_handle = tokio::spawn(run_dealer_central_receiver(
        central_receiver_dealer_socket_clone,
        central_receiver_map_clone,
        TOTAL_MESSAGES_EXPECTED_BY_ROUTER, // Initial high estimate, actual passed later or logic adapted
    ));

    let message_payload_template = Bytes::from(vec![0u8; PAYLOAD_SIZE_BYTES]);
    let mut dealer_task_handles = Vec::new();
    println!(
        "[Main] Spawning {} DEALER tasks, each sending {} messages of {} bytes (app payload). Max concurrent requests (total): {}.",
        NUM_DEALER_TASKS, NUM_MESSAGES_PER_DEALER, PAYLOAD_SIZE_BYTES, MAX_CONCURRENT_REQUESTS
    );

    let benchmark_active_phase_start_time = Instant::now();

    for i in 0..NUM_DEALER_TASKS {
        let task_dealer_socket_clone = dealer_socket_main.clone();
        let task_payload_template_clone = message_payload_template.clone();
        let task_pending_map_clone = pending_requests_map.clone();
        let task_shared_semaphore_clone = shared_semaphore.clone();

        let handle = tokio::spawn(run_dealer_task(
            task_dealer_socket_clone,
            i,
            NUM_MESSAGES_PER_DEALER,
            task_payload_template_clone,
            task_pending_map_clone,
            task_shared_semaphore_clone,
        ));
        dealer_task_handles.push(handle);
    }

    let mut total_messages_sent_agg = 0u64;
    let mut total_bytes_sent_agg = 0u64;
    let mut successful_dealer_tasks = 0;
    let mut total_send_errors_agg = 0u64;
    let mut max_dealer_task_send_finish_time: Option<Instant> = None;


    let all_dealer_results = join_all(dealer_task_handles).await;

    for (idx, join_result) in all_dealer_results.into_iter().enumerate() {
        match join_result {
            Ok(Ok(stats)) => {
                total_messages_sent_agg += stats.messages_sent;
                total_bytes_sent_agg += stats.bytes_sent;
                total_send_errors_agg += stats.send_errors;
                successful_dealer_tasks += 1;
                // Calculate absolute finish time for this task's sending phase
                let task_absolute_finish_time = benchmark_active_phase_start_time + stats.duration;
                if max_dealer_task_send_finish_time.map_or(true, |max_t| task_absolute_finish_time > max_t) {
                    max_dealer_task_send_finish_time = Some(task_absolute_finish_time);
                }
                if stats.send_errors > 0 && PRINT_PER_TASK_SUMMARY {
                    eprintln!(
                        "[DEALER {} SUMMARY] Sent: {} (err:{})",
                        stats.id, stats.messages_sent, stats.send_errors,
                    );
                }
            }
            Ok(Err(e)) => eprintln!("[DEALER {}] Task failed with ZmqError: {}", idx, e),
            Err(e) => eprintln!("[DEALER {}] Task panicked: {}", idx, e),
        }
    }
    let total_messages_successfully_sent = total_messages_sent_agg - total_send_errors_agg;
    println!("[DEALER Main] All dealer tasks finished SENDING. Total successfully sent: {}", total_messages_successfully_sent);

    // Wait for central receiver to process replies or for an overall timeout.
    let mut central_replies_processed = 0u64;
    let mut central_bytes_replied = 0u64;
    let mut central_extraction_errors = 0u64;
    let mut last_reply_timestamp_from_central: Option<Instant> = None;
    
    let overall_receive_timeout_duration = Duration::from_millis(CENTRAL_RECEIVER_OVERALL_TIMEOUT_MS);
    let receive_phase_monitor_start_time = Instant::now();

    if total_messages_successfully_sent > 0 {
        loop {
            let num_pending_now = pending_requests_map.lock().await.len();
            if num_pending_now == 0 {
                 println!("[Main] All {} successfully sent requests have been accounted for (0 pending in map).", total_messages_successfully_sent);
                break;
            }
            if receive_phase_monitor_start_time.elapsed() >= overall_receive_timeout_duration {
                println!("[Main] Overall receive timeout ({}ms) reached. Pending requests: {}",
                    overall_receive_timeout_duration.as_millis(), num_pending_now);
                break;
            }
            if central_receiver_handle.is_finished() { // Check if central receiver exited early
                println!("[Main] Central receiver task finished while main was monitoring. Pending requests: {}", num_pending_now);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await; // Poll status
        }
    } else {
        println!("[Main] No messages were successfully sent by dealers. Skipping receive wait phase.");
    }

    println!("[DEALER Main] Signaling central receiver to stop by closing main dealer socket...");
    if let Err(e) = dealer_socket_main.close().await {
        eprintln!("[DEALER Main] Error closing main dealer socket: {}", e);
    }
    println!("[DEALER Main] Main dealer socket close call returned.");

    match central_receiver_handle.await {
        Ok((replies, bytes, errors, last_ts)) => {
            central_replies_processed = replies;
            central_bytes_replied = bytes;
            central_extraction_errors = errors;
            last_reply_timestamp_from_central = last_ts;
        }
        Err(e) => {
            eprintln!("[Main] DEALER Central Receiver task panicked or was cancelled: {}", e);
        }
    };

    let active_phase_end_time = last_reply_timestamp_from_central
        .or(max_dealer_task_send_finish_time) // If no replies, end is when last send task finished
        .unwrap_or_else(|| benchmark_active_phase_start_time + overall_receive_timeout_duration); // Fallback

    let active_phase_duration = if active_phase_end_time > benchmark_active_phase_start_time {
        active_phase_end_time.duration_since(benchmark_active_phase_start_time)
    } else {
        // If no activity, use a small default to avoid division by zero in rates.
        // This can happen if benchmark_active_phase_start_time itself is the "latest" time.
        Duration::from_nanos(1)
    };
    let active_seconds = active_phase_duration.as_secs_f64().max(1e-9);


    println!("\n--- Overall Combined Results (Active Phase Duration: {:?}) ---", active_phase_duration);
    println!("Benchmark Full Duration (incl. setup/teardown): {:?}", benchmark_active_phase_start_time.elapsed());
    println!("Successful DEALER tasks (for sending): {}/{}", successful_dealer_tasks, NUM_DEALER_TASKS);
    println!("Total messages initiated by dealers: {} (send errors: {})", total_messages_sent_agg, total_send_errors_agg);
    println!("Total messages successfully sent by dealers: {}", total_messages_successfully_sent);
    println!("Total replies processed by central receiver: {}", central_replies_processed);
    println!("Total reply extraction errors by central receiver: {}", central_extraction_errors);

    let final_num_pending = pending_requests_map.lock().await.len();
    println!("Requests still pending in map at benchmark end: {}", final_num_pending);
    let lost_messages = total_messages_successfully_sent
        .saturating_sub(central_replies_processed)
        .saturating_sub(final_num_pending as u64);
    println!("Implied lost/timed-out messages (successfully_sent - replied - pending_at_end): {}", lost_messages);

    if successful_dealer_tasks > 0 && active_seconds > 0.0 {
        let agg_send_msg_rate = total_messages_successfully_sent as f64 / active_seconds;
        let agg_send_byte_rate_mb = (total_bytes_sent_agg as f64 / active_seconds) / (1024.0 * 1024.0);
        println!( "Aggregate Send Rate (during active phase): {:.2} msgs/sec, {:.2} MB/sec", agg_send_msg_rate, agg_send_byte_rate_mb );
        let agg_recv_msg_rate = central_replies_processed as f64 / active_seconds;
        let agg_recv_byte_rate_mb = (central_bytes_replied as f64 / active_seconds) / (1024.0 * 1024.0);
        println!( "Effective System Recv Rate (during active phase): {:.2} msgs/sec, {:.2} MB/sec", agg_recv_msg_rate, agg_recv_byte_rate_mb );
    }
     if total_messages_successfully_sent != central_replies_processed || total_send_errors_agg > 0 || central_extraction_errors > 0 || final_num_pending > 0 {
        eprintln!("WARNING: AGGREGATE counts show discrepancies, errors, or pending messages.");
    }

    println!("[Main] Waiting for ROUTER task to complete...");
    match router_join_handle.await {
        Ok(Ok((router_msgs_echoed, router_bytes_echoed, router_timeouts))) => {
            println!( "[ROUTER] Task completed. Msgs echoed: {}. Bytes: {}. Timeouts: {}", router_msgs_echoed, router_bytes_echoed, router_timeouts );
            if active_seconds > 0.0 {
                 let router_echo_msg_rate = router_msgs_echoed as f64 / active_seconds;
                 let router_echo_byte_rate_mb = (router_bytes_echoed as f64 / active_seconds) / (1024.0 * 1024.0);
                 println!("[ROUTER]: Effective echo rate (during active phase): {:.2} msgs/sec, {:.2} MB/sec", router_echo_msg_rate, router_echo_byte_rate_mb);
            }
            if router_msgs_echoed != total_messages_successfully_sent { eprintln!("[ROUTER] WARNING: Router echoed {} msgs, dealers successfully sent {}.", router_msgs_echoed, total_messages_successfully_sent); }
            if router_msgs_echoed != central_replies_processed { eprintln!("[ROUTER] WARNING: Router echoed {} msgs, central dealer received {} replies.", router_msgs_echoed, central_replies_processed); }
        }
        Ok(Err(e)) => eprintln!("[ROUTER] Task failed with ZmqError: {}", e),
        Err(e) => eprintln!("[ROUTER] Task panicked: {}", e),
    }
    println!("[Main] Terminating context...");
    ctx.term().await?;
    println!("[Main] Context terminated. Example finished.");
    Ok(())
}