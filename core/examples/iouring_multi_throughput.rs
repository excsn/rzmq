// examples/dealer_router_iouring_throughput.rs

use rzmq::{Context, Msg, MsgFlags, SocketType, ZmqError};
use rzmq::socket::{events::SocketEvent, options as zmq_opts}; // Added SocketEvent
use std::time::{Duration, Instant};
use bytes::Bytes;
use futures::future::join_all;
use tokio::task::JoinHandle;

// --- Configuration Constants ---
const ROUTER_IO_URING_ENABLED: bool = true;
const DEALER_IO_URING_ENABLED: bool = true;
const TCP_CORK_ENABLED: u32 = 0; // 0 for false, 1 for true
const NUM_DEALER_TASKS: usize = 2;
const NUM_MESSAGES_PER_DEALER: u64 = 100;
const PAYLOAD_SIZE_BYTES: usize = 1024; // 1KB payload (excluding sequence number)
const ROUTER_ENDPOINT: &'static str = "tcp://127.0.0.1:5558";
const CLIENT_PIPELINE_DEPTH: usize = 15000;
const HANDSHAKE_TIMEOUT_MS: u64 = 5000; // Timeout for waiting for handshake
const ROUTER_RECV_TIMEOUT_MS: u64 = 10000; // Router waits 10s for a message before timing out

const TOTAL_MESSAGES_EXPECTED_BY_ROUTER: u64 = (NUM_DEALER_TASKS as u64) * NUM_MESSAGES_PER_DEALER;

// --- Logging Configuration ---
const PRINT_PER_TASK_SUMMARY: bool = true;
const PRINT_INDIVIDUAL_MESSAGES: bool = false;
const PRINT_HANDSHAKE_EVENTS: bool = true;

// Helper to prepend sequence number
fn create_payload_with_seq(seq: u64, template: &Bytes) -> Bytes {
    let mut full_payload = Vec::with_capacity(8 + template.len());
    full_payload.extend_from_slice(&seq.to_be_bytes());
    full_payload.extend_from_slice(template);
    Bytes::from(full_payload)
}

// Helper to extract sequence number
fn extract_seq_from_payload(payload: &[u8]) -> Option<u64> {
    if payload.len() >= 8 {
        let seq_bytes: [u8; 8] = payload[0..8].try_into().ok()?;
        Some(u64::from_be_bytes(seq_bytes))
    } else {
        None
    }
}


// --- ROUTER Task (Server Side) ---
async fn run_router_task(
    ctx: Context,
    endpoint: &'static str,
    total_expected_messages: u64,
) -> Result<(u64, u64, u64), ZmqError> { // Added timeouts_occurred
    let router_socket = ctx.socket(SocketType::Router)?;
    router_socket.set_option(zmq_opts::IO_URING_SESSION_ENABLED, ROUTER_IO_URING_ENABLED).await?;
    router_socket.set_option(zmq_opts::IO_URING_RCVMULTISHOT, ROUTER_IO_URING_ENABLED).await?;
    router_socket.set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED).await?;
    router_socket.set_option(zmq_opts::RCVHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32 / 2).max(5000)).await?;
    router_socket.set_option(zmq_opts::SNDHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32 / 2).max(5000)).await?;
    // Set RCVTIMEO for the router
    router_socket.set_option(zmq_opts::RCVTIMEO, ROUTER_RECV_TIMEOUT_MS as i32).await?;


    println!("[ROUTER] Binding to {}...", endpoint);
    router_socket.bind(endpoint).await?;
    println!("[ROUTER] Bound successfully. Expecting {} total messages.", total_expected_messages);

    let mut messages_echoed = 0u64;
    let mut bytes_echoed = 0u64;
    let mut timeouts_occurred = 0u64;
    let mut received_count = 0u64; // Track how many we've actually received

    // Loop until we've processed the expected number or too many timeouts
    while received_count < total_expected_messages {
        match router_socket.recv_multipart().await {
            Ok(mut received_parts) => {
                received_count += 1;
                if received_parts.is_empty() {
                    eprintln!("[ROUTER] Received empty multipart message (recv_idx {}), ignoring.", received_count -1);
                    continue;
                }
                let client_identity_frame = received_parts.remove(0);
                
                // Extract sequence number from the first payload part
                let original_seq = if let Some(first_payload) = received_parts.first() {
                    extract_seq_from_payload(first_payload.data().unwrap_or_default())
                } else { None };

                if PRINT_INDIVIDUAL_MESSAGES {
                     println!(
                        "[ROUTER] Received (idx {}): Identity='{:?}', Seq={:?}, {} payload parts. First part size: {}",
                        received_count - 1,
                        String::from_utf8_lossy(client_identity_frame.data().unwrap_or_default()),
                        original_seq,
                        received_parts.len(),
                        received_parts.first().map_or(0, |p| p.size())
                    );
                }

                let mut reply_to_send = Vec::with_capacity(1 + received_parts.len());
                let mut id_reply_frame = client_identity_frame; // Use the original identity frame
                if !received_parts.is_empty() {
                    id_reply_frame.set_flags(id_reply_frame.flags() | MsgFlags::MORE);
                } else { // Should not happen if we expect payload
                    id_reply_frame.set_flags(id_reply_frame.flags() & !MsgFlags::MORE);
                }
                reply_to_send.push(id_reply_frame);

                let num_payload_parts = received_parts.len();
                for (idx, mut part) in received_parts.into_iter().enumerate() {
                    bytes_echoed += part.size() as u64;
                    if idx < num_payload_parts - 1 {
                        part.set_flags(part.flags() | MsgFlags::MORE);
                    } else {
                        part.set_flags(part.flags() & !MsgFlags::MORE);
                    }
                    reply_to_send.push(part);
                }

                if let Err(e) = router_socket.send_multipart(reply_to_send).await {
                    eprintln!("[ROUTER] Error sending reply for recv_idx {}: {}", received_count -1, e);
                    return Err(e); // Break on send error
                }
                messages_echoed += 1;
            }
            Err(ZmqError::Timeout) => {
                timeouts_occurred += 1;
                eprintln!("[ROUTER] Recv timeout (total timeouts: {}). Still expecting {} messages.", timeouts_occurred, total_expected_messages - received_count);
                if timeouts_occurred > (total_expected_messages / 1000).max(10) { // Example: if 0.1% or at least 10 timeouts occur
                    eprintln!("[ROUTER] Too many timeouts. Terminating echo task.");
                    break;
                }
                // Continue waiting for more messages
            }
            Err(ZmqError::InvalidState(ref msg)) if msg.contains("Socket is closing") => {
                println!("[ROUTER] Socket closing, terminating echo task (after {} received).", received_count);
                break;
            }
            Err(e) => {
                eprintln!("[ROUTER] recv_multipart error (after {} received): {}, terminating echo task.", received_count, e);
                return Err(e);
            }
        }
    }
    println!("[ROUTER] Echo task finished. Total Received: {}. Echoed: {}. Timeouts: {}. Bytes Echoed: {}.",
             received_count, messages_echoed, timeouts_occurred, bytes_echoed);
    Ok((messages_echoed, bytes_echoed, timeouts_occurred))
}

// --- DEALER Task (Client Side Worker) ---
#[derive(Debug)]
struct DealerTaskStats {
    id: usize,
    messages_sent: u64,
    bytes_sent: u64,
    messages_received: u64,
    bytes_received: u64,
    duration: Duration,
    send_errors: u64,
    recv_errors: u64,
    seq_errors: u64,
}

async fn run_dealer_task(
    dealer_socket: rzmq::Socket,
    dealer_id: usize,
    num_messages_for_this_task: u64,
    message_payload_template: Bytes,
) -> Result<DealerTaskStats, ZmqError> {
    let mut task_messages_sent = 0u64;
    let mut task_bytes_sent = 0u64;
    let mut task_messages_received = 0u64;
    let mut task_bytes_received = 0u64;
    let task_start_time = Instant::now();
    let mut send_errors = 0u64;
    let mut recv_errors = 0u64;
    let mut seq_errors = 0u64;

    if PRINT_INDIVIDUAL_MESSAGES {
        println!("[DEALER {}] Task started, will send {} messages.", dealer_id, num_messages_for_this_task);
    }

    let (permit_tx, mut permit_rx) = tokio::sync::mpsc::channel(CLIENT_PIPELINE_DEPTH);
    for _ in 0..CLIENT_PIPELINE_DEPTH {
        permit_tx.send(()).await.expect("Failed to fill permit channel");
    }

    let sender_socket_clone = dealer_socket.clone();
    let sender_payload_template_clone = message_payload_template.clone();

    let sender_handle: JoinHandle<Result<(u64, u64, u64), ZmqError>> = tokio::spawn(async move {
        let mut s_sent = 0u64;
        let mut b_sent = 0u64;
        let mut s_errors = 0u64;
        for j in 0..num_messages_for_this_task {
            if permit_rx.recv().await.is_none() {
                 eprintln!("[DEALER {} Sender] Permit channel closed. Stopping sends.", dealer_id);
                 break;
            }

            let payload_with_seq = create_payload_with_seq(j, &sender_payload_template_clone);
            let payload_size_with_seq = payload_with_seq.len();
            let msg_to_send = Msg::from_bytes(payload_with_seq);

            if PRINT_INDIVIDUAL_MESSAGES {
                // println!("[DEALER {}] Sending msg #{} (Seq: {})", dealer_id, j, j);
            }
            match sender_socket_clone.send(msg_to_send).await {
                Ok(_) => {
                    s_sent += 1;
                    b_sent += payload_size_with_seq as u64;
                }
                Err(e) => {
                    eprintln!("[DEALER {} Sender] Send error on message #{} (Seq {}): {}", dealer_id, j, j, e);
                    s_errors += 1;
                    // Don't return permit on error, let receiver side timeout or error out
                    // to prevent deadlock if send fails persistently.
                    // Alternatively, could try to return permit and let receiver handle less replies.
                    // For this test, we'll assume sender stops and receiver eventually times out or errors.
                    return Err(e);
                }
            }
        }
        if PRINT_INDIVIDUAL_MESSAGES {
            println!("[DEALER {} Sender] All sends initiated. Total errors: {}", dealer_id, s_errors);
        }
        Ok((s_sent, b_sent, s_errors))
    });

    let receiver_handle: JoinHandle<Result<(u64, u64, u64, u64), ZmqError>> = tokio::spawn(async move {
        let mut r_received = 0u64;
        let mut b_received = 0u64;
        let mut r_errors = 0u64;
        let mut s_errors_local = 0u64; // Sequence errors

        // Receiver attempts to receive one reply for each message the sender *intended* to send
        for i in 0..num_messages_for_this_task {
            match dealer_socket.recv().await {
                Ok(reply_msg) => {
                    if PRINT_INDIVIDUAL_MESSAGES {
                        // println!("[DEALER {} Receiver] Received reply for sent_idx #{} (Size: {})", dealer_id, i, reply_msg.size());
                    }
                    let reply_payload = reply_msg.data().unwrap_or_default();
                    let received_seq = extract_seq_from_payload(reply_payload);

                    // Basic check: does reply payload seem to contain our original payload?
                    // This is a bit simplified; a robust check would compare entire content after seq.
                    let expected_size_with_seq = 8 + message_payload_template.len();
                    if reply_msg.size() == expected_size_with_seq {
                        if received_seq == Some(i) { // Simple in-order check for this dealer's stream
                            r_received += 1;
                            b_received += reply_msg.size() as u64;
                        } else {
                            eprintln!(
                                "[DEALER {} Receiver] Reply for sent_idx #{} has unexpected sequence. Expected: Some({}), Got: {:?}.",
                                dealer_id, i, i, received_seq
                            );
                            s_errors_local += 1;
                            // Still count as received for pipeline permit
                        }
                    } else {
                        eprintln!(
                            "[DEALER {} Receiver] Reply for sent_idx #{} unexpected size. Expected: {}, Got: {}.",
                            dealer_id, i, expected_size_with_seq, reply_msg.size()
                        );
                        s_errors_local += 1; // Count as a form of sequence/payload error
                    }

                    if permit_tx.send(()).await.is_err() {
                        eprintln!("[DEALER {} Receiver] Failed to return permit, sender task may have exited early.", dealer_id);
                        break;
                    }
                }
                Err(ZmqError::Timeout) => {
                    eprintln!("[DEALER {} Receiver] Recv timeout on reply for sent_idx #{}. Assuming message lost or router slow.", dealer_id, i);
                    r_errors +=1;
                    // We MUST return the permit here, otherwise the sender will deadlock if it's still sending.
                    if permit_tx.send(()).await.is_err() {
                        eprintln!("[DEALER {} Receiver] Failed to return permit after timeout, sender task may have exited early.", dealer_id);
                        break;
                    }
                    // continue to try receiving next expected message
                }
                Err(e) => {
                    eprintln!("[DEALER {} Receiver] Recv error on reply for sent_idx #{}: {}", dealer_id, i, e);
                    r_errors += 1;
                    // Do not return permit here, as the send loop might be broken.
                    // If sender is fine, it will block. If sender also errored, both stop.
                    return Err(e);
                }
            }
        }
        if PRINT_INDIVIDUAL_MESSAGES {
            println!("[DEALER {} Receiver] All expected replies processed (or loop terminated). Errors: {}, Seq Errors: {}",
                     dealer_id, r_errors, s_errors_local);
        }
        Ok((r_received, b_received, r_errors, s_errors_local))
    });

    let (sent_res, recvd_res) = tokio::try_join!(sender_handle, receiver_handle)
        .map_err(|je| ZmqError::Internal(format!("Dealer task join error: {}", je)))?; // Handle JoinError

    let (s_sent, b_sent, s_errors_from_sender) = sent_res?;
    let (r_recvd, b_recvd, r_errors_from_receiver, s_errors_from_receiver) = recvd_res?;

    task_messages_sent = s_sent;
    task_bytes_sent = b_sent;
    send_errors = s_errors_from_sender;
    task_messages_received = r_recvd;
    task_bytes_received = b_recvd;
    recv_errors = r_errors_from_receiver;
    seq_errors = s_errors_from_receiver;

    let task_duration = task_start_time.elapsed();
    if PRINT_PER_TASK_SUMMARY || task_messages_sent != task_messages_received || send_errors > 0 || recv_errors > 0 || seq_errors > 0 {
        println!(
            "[DEALER {}] Task finished. Duration: {:?}. Sent: {} (err:{}). Received: {} (err:{}, seq_err:{}). Rate: {:.2} req/s",
            dealer_id,
            task_duration,
            task_messages_sent, send_errors,
            task_messages_received, recv_errors, seq_errors,
            task_messages_sent as f64 / task_duration.as_secs_f64() // Rate based on successful sends
        );
    }

    Ok(DealerTaskStats {
        id: dealer_id,
        messages_sent: task_messages_sent,
        bytes_sent: task_bytes_sent,
        messages_received: task_messages_received,
        bytes_received: task_bytes_received,
        duration: task_duration,
        send_errors,
        recv_errors,
        seq_errors,
    })
}

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "warn,rzmq=warn");
    }
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(true)
        .compact()
        .init();

    println!(
        "Starting DEALER-ROUTER Throughput Example ({} DEALER tasks, {} msgs/task)...",
        NUM_DEALER_TASKS, NUM_MESSAGES_PER_DEALER
    );

    let ctx = Context::new().expect("Failed to create rzmq context");

    let router_ctx_clone = ctx.clone();
    let router_join_handle = tokio::spawn(run_router_task(
        router_ctx_clone,
        ROUTER_ENDPOINT,
        TOTAL_MESSAGES_EXPECTED_BY_ROUTER,
    ));

    // Give router a moment to bind
    tokio::time::sleep(Duration::from_millis(200)).await; // Slightly longer for OS to release port if run back-to-back

    let dealer_socket_main = ctx.socket(SocketType::Dealer)?;
    dealer_socket_main.set_option(zmq_opts::IO_URING_SESSION_ENABLED, DEALER_IO_URING_ENABLED).await?;
    dealer_socket_main.set_option(zmq_opts::IO_URING_RCVMULTISHOT, DEALER_IO_URING_ENABLED).await?;
    dealer_socket_main.set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED).await?;
    dealer_socket_main.set_option(zmq_opts::SNDHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32 / 2).max(5000)).await?;
    dealer_socket_main.set_option(zmq_opts::RCVHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32 / 2).max(5000)).await?;
    // Set a SNDTIMEO for the dealer's send operations
    dealer_socket_main.set_option(zmq_opts::SNDTIMEO, 5000i32).await?; // 5 second send timeout
    // Set a RCVTIMEO for the dealer's recv operations
    dealer_socket_main.set_option(zmq_opts::RCVTIMEO, 5000i32).await?; // 5 second receive timeout


    println!("[DEALER Main] Connecting to {}...", ROUTER_ENDPOINT);
    dealer_socket_main.connect(ROUTER_ENDPOINT).await?;
    println!("[DEALER Main] Connect call returned. Waiting for handshake...");

    // Create and wait for monitor event for handshake
    let mut monitor_rx = dealer_socket_main.monitor_default().await?;
    let handshake_wait_start = Instant::now();
    let mut handshake_succeeded = false;
    loop {
        match tokio::time::timeout(Duration::from_millis(HANDSHAKE_TIMEOUT_MS), monitor_rx.recv()).await {
            Ok(Ok(event)) => {
                if PRINT_HANDSHAKE_EVENTS {
                    println!("[DEALER Main Monitor] Received event: {:?}", event);
                }
                if let SocketEvent::HandshakeSucceeded { endpoint } = event {
                    if endpoint == ROUTER_ENDPOINT {
                        println!("[DEALER Main] HandshakeSucceeded for {} after {:?}.", ROUTER_ENDPOINT, handshake_wait_start.elapsed());
                        handshake_succeeded = true;
                        break;
                    }
                }
            }
            Ok(Err(_)) => { // Channel closed
                eprintln!("[DEALER Main Monitor] Monitor channel closed before handshake. Error.");
                return Err(ZmqError::Internal("Monitor channel closed".into()));
            }
            Err(_) => { // Timeout
                eprintln!("[DEALER Main Monitor] Timeout waiting for HandshakeSucceeded event after {:?}.", handshake_wait_start.elapsed());
                return Err(ZmqError::Timeout);
            }
        }
    }

    if !handshake_succeeded {
        return Err(ZmqError::Internal("Handshake did not succeed with router.".into()));
    }

    let message_payload_template = Bytes::from(vec![0u8; PAYLOAD_SIZE_BYTES]);
    let mut dealer_task_handles = Vec::new();
    println!(
        "[Main] Spawning {} DEALER tasks, each sending {} messages of {} bytes (excluding seq num).",
        NUM_DEALER_TASKS, NUM_MESSAGES_PER_DEALER, PAYLOAD_SIZE_BYTES
    );

    let overall_start_time = Instant::now();

    for i in 0..NUM_DEALER_TASKS {
        let task_dealer_socket = dealer_socket_main.clone();
        let task_payload_template_clone = message_payload_template.clone();
        let handle = tokio::spawn(run_dealer_task(
            task_dealer_socket,
            i,
            NUM_MESSAGES_PER_DEALER,
            task_payload_template_clone,
        ));
        dealer_task_handles.push(handle);
    }

    let mut total_messages_sent_agg = 0u64;
    let mut total_bytes_sent_agg = 0u64;
    let mut total_messages_received_agg = 0u64;
    let mut total_bytes_received_agg = 0u64;
    let mut successful_dealer_tasks = 0;
    let mut total_send_errors_agg = 0u64;
    let mut total_recv_errors_agg = 0u64;
    let mut total_seq_errors_agg = 0u64;

    let all_dealer_results = join_all(dealer_task_handles).await;

    for (idx, join_result) in all_dealer_results.into_iter().enumerate() {
        match join_result {
            Ok(Ok(stats)) => {
                total_messages_sent_agg += stats.messages_sent;
                total_bytes_sent_agg += stats.bytes_sent;
                total_messages_received_agg += stats.messages_received;
                total_bytes_received_agg += stats.bytes_received;
                total_send_errors_agg += stats.send_errors;
                total_recv_errors_agg += stats.recv_errors;
                total_seq_errors_agg += stats.seq_errors;
                successful_dealer_tasks += 1;
                if stats.messages_sent != stats.messages_received || stats.send_errors > 0 || stats.recv_errors > 0 || stats.seq_errors > 0 {
                    eprintln!(
                        "[DEALER {} SUMMARY] Sent: {} (err:{}), Received: {} (err:{}, seq_err:{})",
                        stats.id, stats.messages_sent, stats.send_errors,
                        stats.messages_received, stats.recv_errors, stats.seq_errors
                    );
                }
            }
            Ok(Err(e)) => eprintln!("[DEALER {}] Task failed with ZmqError: {}", idx, e),
            Err(e) => eprintln!("[DEALER {}] Task panicked: {}", idx, e),
        }
    }

    let overall_duration = overall_start_time.elapsed();
    println!("\n--- Overall DEALER Results ---");
    println!("Duration: {:?}", overall_duration);
    println!("Successful DEALER tasks: {}/{}", successful_dealer_tasks, NUM_DEALER_TASKS);
    println!("Total messages sent (aggregate): {} (errors: {})", total_messages_sent_agg, total_send_errors_agg);
    println!("Total messages received (aggregate): {} (errors: {}, seq_errors: {})", total_messages_received_agg, total_recv_errors_agg, total_seq_errors_agg);

    let seconds = overall_duration.as_secs_f64();
    if seconds > 0.0 && successful_dealer_tasks > 0 {
        let agg_send_msg_rate = total_messages_sent_agg as f64 / seconds;
        let agg_send_byte_rate_mb = (total_bytes_sent_agg as f64 / seconds) / (1024.0 * 1024.0);
        println!(
            "Aggregate Send Rate (based on successful sends by dealers): {:.2} msgs/sec, {:.2} MB/sec",
            agg_send_msg_rate, agg_send_byte_rate_mb
        );

        let agg_recv_msg_rate = total_messages_received_agg as f64 / seconds;
        let agg_recv_byte_rate_mb = (total_bytes_received_agg as f64 / seconds) / (1024.0 * 1024.0);
        println!(
            "Aggregate Recv Rate (based on successful receives by dealers): {:.2} msgs/sec, {:.2} MB/sec",
            agg_recv_msg_rate, agg_recv_byte_rate_mb
        );
    }
     if total_messages_sent_agg != total_messages_received_agg || total_send_errors_agg > 0 || total_recv_errors_agg > 0 || total_seq_errors_agg > 0 {
        eprintln!("WARNING: AGGREGATE counts show discrepancies or errors.");
    }

    println!("[DEALER Main] Closing main dealer socket...");
    if let Err(e) = dealer_socket_main.close().await {
        eprintln!("[DEALER Main] Error closing main dealer socket: {}", e);
    }
    println!("[DEALER Main] Main dealer socket closed.");

    println!("[Main] Waiting for ROUTER task to complete...");
    match router_join_handle.await {
        Ok(Ok((router_msgs_echoed, router_bytes_echoed, router_timeouts))) => {
            println!(
                "[ROUTER] Task completed. Messages echoed: {}. Bytes echoed: {}. Timeouts: {}",
                router_msgs_echoed, router_bytes_echoed, router_timeouts
            );
            if seconds > 0.0 {
                 let router_echo_msg_rate = router_msgs_echoed as f64 / seconds;
                 let router_echo_byte_rate_mb = (router_bytes_echoed as f64 / seconds) / (1024.0 * 1024.0);
                 println!("[ROUTER]: Effective echo rate: {:.2} msgs/sec, {:.2} MB/sec", router_echo_msg_rate, router_echo_byte_rate_mb);
            }
            if router_msgs_echoed != total_messages_sent_agg { // Compare with dealer sends
                eprintln!("[ROUTER] WARNING: Router echoed {} msgs, but dealers sent {} msgs in total.", router_msgs_echoed, total_messages_sent_agg);
            }
        }
        Ok(Err(e)) => eprintln!("[ROUTER] Task failed with ZmqError: {}", e),
        Err(e) => eprintln!("[ROUTER] Task panicked or was cancelled: {}", e),
    }

    println!("[Main] Terminating context...");
    ctx.term().await?;
    println!("[Main] Context terminated. Example finished.");

    Ok(())
}