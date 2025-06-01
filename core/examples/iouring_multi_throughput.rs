// examples/dealer_router_iouring_throughput.rs

use rzmq::{Context, Msg, MsgFlags, SocketType, ZmqError};
use rzmq::socket::options as zmq_opts;
use std::time::{Duration, Instant};
use bytes::Bytes;
use futures::future::join_all;
use tokio::task::JoinHandle;

// --- Configuration Constants ---
const ROUTER_IO_URING_ENABLED: bool = true;
const DEALER_IO_URING_ENABLED: bool = true;
const TCP_CORK_ENABLED: u32 = 0;
const NUM_DEALER_TASKS: usize = 8; // Number of concurrent dealer tasks
const NUM_MESSAGES_PER_DEALER: u64 = 20000; // Messages per dealer task
const PAYLOAD_SIZE_BYTES: usize = 1024; // 1KB
const ROUTER_ENDPOINT: &str = "tcp://127.0.0.1:5558"; // Ensure a unique port
const CLIENT_PIPELINE_DEPTH: usize = 15000; // How many messages can be sent before waiting for a reply to free up a slot.

const TOTAL_MESSAGES_EXPECTED_BY_ROUTER: u64 = (NUM_DEALER_TASKS as u64) * NUM_MESSAGES_PER_DEALER;

// --- Logging Configuration ---
const PRINT_PER_TASK_SUMMARY: bool = true; // Print summary for each dealer task
const PRINT_INDIVIDUAL_MESSAGES: bool = false; // Detailed send/recv logs (can be very verbose)

// --- ROUTER Task (Server Side) ---
async fn run_router_task(
    ctx: Context, // Pass context for socket creation
    endpoint: &'static str,
    total_expected_messages: u64,
) -> Result<(u64, u64), ZmqError> {
    let router_socket = ctx.socket(SocketType::Router)?;
    router_socket.set_option(zmq_opts::IO_URING_SESSION_ENABLED, ROUTER_IO_URING_ENABLED).await?;
    router_socket.set_option(zmq_opts::IO_URING_RCVMULTISHOT, ROUTER_IO_URING_ENABLED).await?;
    router_socket.set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED).await?;
    router_socket.set_option(zmq_opts::RCVHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32 / 2).max(5000)).await?;
    router_socket.set_option(zmq_opts::SNDHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32 / 2).max(5000)).await?;

    println!("[ROUTER] Binding to {}...", endpoint);
    router_socket.bind(endpoint).await?;
    println!("[ROUTER] Bound successfully. Expecting {} total messages.", total_expected_messages);

    let mut messages_echoed = 0u64;
    let mut bytes_echoed = 0u64;

    for msg_idx in 0..total_expected_messages {
        match router_socket.recv_multipart().await {
            Ok(mut received_parts) => {
                if received_parts.is_empty() {
                    eprintln!("[ROUTER] Received empty multipart message (msg {}), ignoring.", msg_idx);
                    continue;
                }
                let client_identity_frame = received_parts.remove(0);
                if PRINT_INDIVIDUAL_MESSAGES {
                     println!(
                        "[ROUTER] Received (msg {}): Identity='{:?}', {} payload parts. First part size: {}",
                        msg_idx,
                        String::from_utf8_lossy(client_identity_frame.data().unwrap_or_default()),
                        received_parts.len(),
                        received_parts.first().map_or(0, |p| p.size())
                    );
                }

                let mut reply_to_send = Vec::with_capacity(1 + received_parts.len());
                let mut id_reply_frame = client_identity_frame;
                if !received_parts.is_empty() {
                    id_reply_frame.set_flags(id_reply_frame.flags() | MsgFlags::MORE);
                } else {
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
                    eprintln!("[ROUTER] Error sending reply for msg {}: {}", msg_idx, e);
                    // Decide if to break or continue. For a benchmark, maybe continue for other clients.
                    // For simplicity, let's break on send error.
                    return Err(e);
                }
                messages_echoed += 1;
            }
            Err(ZmqError::InvalidState(ref msg)) if msg.contains("Socket is closing") => {
                println!("[ROUTER] Socket closing, terminating echo task (msg {}).", msg_idx);
                break;
            }
            Err(e) => {
                eprintln!("[ROUTER] recv_multipart error (msg {}): {}, terminating echo task.", msg_idx, e);
                return Err(e);
            }
        }
    }
    println!("[ROUTER] Echo task finished after {} messages and {} bytes.", messages_echoed, bytes_echoed);
    Ok((messages_echoed, bytes_echoed))
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
}

async fn run_dealer_task(
    dealer_socket: rzmq::Socket, // Cloned socket for this task
    dealer_id: usize,
    num_messages_for_this_task: u64,
    message_payload_template: Bytes,
    payload_size: usize,
) -> Result<DealerTaskStats, ZmqError> {
    let mut task_messages_sent = 0u64;
    let mut task_bytes_sent = 0u64;
    let mut task_messages_received = 0u64;
    let mut task_bytes_received = 0u64;
    let task_start_time = Instant::now();

    if PRINT_INDIVIDUAL_MESSAGES {
        println!("[DEALER {}] Task started, will send {} messages.", dealer_id, num_messages_for_this_task);
    }

    // For true async, we need to manage outstanding requests if we don't want to wait for each reply.
    // A simple way is to use a "pipeline" depth or a fixed number of outstanding messages.
    // For this example, we'll use a bounded channel to limit outstanding messages.
    // Let's say we allow up to `PIPELINE_DEPTH` messages to be "in-flight" without a reply.
    let (permit_tx, mut permit_rx) = tokio::sync::mpsc::channel(CLIENT_PIPELINE_DEPTH);

    // Fill the permit channel initially
    for _ in 0..CLIENT_PIPELINE_DEPTH {
        permit_tx.send(()).await.expect("Failed to fill permit channel");
    }

    let sender_socket_clone = dealer_socket.clone();
    let sender_payload_clone = message_payload_template.clone();

    // Sender sub-task
    let sender_handle: JoinHandle<Result<(u64, u64), ZmqError>> = tokio::spawn(async move {
        let mut s_sent = 0u64;
        let mut b_sent = 0u64;
        for j in 0..num_messages_for_this_task {
            // Wait for a permit to send
            if let None = permit_rx.recv().await {
                 eprintln!("[DEALER {} Sender] Permit channel closed. Stopping sends.", dealer_id);
                 break; // Permit channel closed, likely receiver task errored
            }

            let msg_to_send = Msg::from_bytes(sender_payload_clone.clone());
            if PRINT_INDIVIDUAL_MESSAGES {
                // println!("[DEALER {}] Sending msg #{}", dealer_id, j);
            }
            match sender_socket_clone.send(msg_to_send).await {
                Ok(_) => {
                    s_sent += 1;
                    b_sent += payload_size as u64;
                }
                Err(e) => {
                    eprintln!("[DEALER {} Sender] Send error on message {}: {}", dealer_id, j, e);
                    return Err(e); // Propagate error
                }
            }
        }
        if PRINT_INDIVIDUAL_MESSAGES {
            println!("[DEALER {} Sender] All sends initiated.", dealer_id);
        }
        Ok((s_sent, b_sent))
    });

    // Receiver sub-task (runs concurrently)
    // It will try to receive replies for all messages the sender *attempts* to send.
    let receiver_handle: JoinHandle<Result<(u64,u64), ZmqError>> = tokio::spawn(async move {
        let mut r_received = 0u64;
        let mut b_received = 0u64;

        for i in 0..num_messages_for_this_task { // Expect one reply per sent message
            match dealer_socket.recv().await {
                Ok(reply_msg) => {
                    if PRINT_INDIVIDUAL_MESSAGES {
                        // println!("[DEALER {} Receiver] Received reply #{} (Size: {})", dealer_id, i, reply_msg.size());
                    }
                    if reply_msg.size() == payload_size {
                        r_received += 1;
                        b_received += reply_msg.size() as u64;
                    } else {
                        eprintln!(
                            "[DEALER {} Receiver] Reply #{} unexpected size. Expected: {}, Got: {}.",
                            dealer_id, i, payload_size, reply_msg.size()
                        );
                    }
                    // Return a permit to the sender
                    if permit_tx.send(()).await.is_err() {
                        eprintln!("[DEALER {} Receiver] Failed to return permit, sender task may have exited.", dealer_id);
                        // This might happen if sender errored and closed its end of permit_rx.
                        // Receiver should probably also stop if it can't return permits.
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("[DEALER {} Receiver] Recv error on reply #{}: {}", dealer_id, i, e);
                    // Don't return permit here as the send loop might be broken.
                    return Err(e); // Propagate error
                }
            }
        }
        if PRINT_INDIVIDUAL_MESSAGES {
            println!("[DEALER {} Receiver] All expected replies processed (or loop terminated).", dealer_id);
        }
        Ok((r_received, b_received))
    });

    // Wait for sender and receiver to finish
    let (sent_res, recvd_res) = match tokio::try_join!(sender_handle, receiver_handle) {
      Ok((value, value2)) => {
        (value, value2)
      }
      Err(_) => {
        return Err(ZmqError::Internal("Failed to join".to_string()));
      }
    };
    
    let (s_sent, b_sent) = sent_res?;
    let (r_recvd, b_recvd) = recvd_res?;

    task_messages_sent = s_sent;
    task_bytes_sent = b_sent;
    task_messages_received = r_recvd;
    task_bytes_received = b_recvd;

    let task_duration = task_start_time.elapsed();
    if PRINT_PER_TASK_SUMMARY || task_messages_sent != task_messages_received {
        println!(
            "[DEALER {}] Task finished. Duration: {:?}. Sent: {}, Received: {}. Rate: {:.2} req/s",
            dealer_id,
            task_duration,
            task_messages_sent,
            task_messages_received,
            task_messages_sent as f64 / task_duration.as_secs_f64()
        );
    }

    Ok(DealerTaskStats {
        id: dealer_id,
        messages_sent: task_messages_sent,
        bytes_sent: task_bytes_sent,
        messages_received: task_messages_received,
        bytes_received: task_bytes_received,
        duration: task_duration,
    })
}


#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "warn,rzmq=warn"); // Reduce default log spam for benchmarks
    }
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO) // Default to WARN
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

    // Spawn ROUTER Task
    let router_ctx_clone = ctx.clone();
    let router_join_handle = tokio::spawn(run_router_task(
        router_ctx_clone,
        ROUTER_ENDPOINT,
        TOTAL_MESSAGES_EXPECTED_BY_ROUTER,
    ));
    
    // Give router a moment to bind
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- Main DEALER Socket Setup ---
    let dealer_socket_main = ctx.socket(SocketType::Dealer)?;
    println!("[DEALER Main] Enabling IO_URING_SESSION_ENABLED...");
    dealer_socket_main.set_option(zmq_opts::IO_URING_SESSION_ENABLED, DEALER_IO_URING_ENABLED).await?;
    dealer_socket_main.set_option(zmq_opts::IO_URING_RCVMULTISHOT, DEALER_IO_URING_ENABLED).await?;
    dealer_socket_main.set_option(zmq_opts::TCP_CORK, TCP_CORK_ENABLED).await?;
    dealer_socket_main.set_option(zmq_opts::SNDHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32 / 2).max(5000)).await?;
    dealer_socket_main.set_option(zmq_opts::RCVHWM, (TOTAL_MESSAGES_EXPECTED_BY_ROUTER as i32 / 2).max(5000)).await?;


    println!("[DEALER Main] Connecting to {}...", ROUTER_ENDPOINT);
    dealer_socket_main.connect(ROUTER_ENDPOINT).await?;
    println!("[DEALER Main] Connect call returned. Waiting for connection to establish...");
    tokio::time::sleep(Duration::from_millis(500)).await; // Increased time for handshake with multiple dealers potentially starting up
    println!("[DEALER Main] Assumed connected and handshake complete for all tasks.");

    // --- Throughput Test Logic ---
    let message_payload = Bytes::from(vec![0u8; PAYLOAD_SIZE_BYTES]);

    let mut dealer_task_handles: Vec<JoinHandle<Result<DealerTaskStats, ZmqError>>> = Vec::new();

    println!(
        "[Main] Spawning {} DEALER tasks, each sending {} messages of {} bytes.",
        NUM_DEALER_TASKS, NUM_MESSAGES_PER_DEALER, PAYLOAD_SIZE_BYTES
    );

    let overall_start_time = Instant::now();

    for i in 0..NUM_DEALER_TASKS {
        let task_dealer_socket = dealer_socket_main.clone(); // Clone the socket handle for each task
        let task_payload_template = message_payload.clone();
        let handle = tokio::spawn(run_dealer_task(
            task_dealer_socket,
            i,
            NUM_MESSAGES_PER_DEALER,
            task_payload_template,
            PAYLOAD_SIZE_BYTES,
        ));
        dealer_task_handles.push(handle);
    }

    let mut total_messages_sent_agg = 0u64;
    let mut total_bytes_sent_agg = 0u64;
    let mut total_messages_received_agg = 0u64;
    let mut total_bytes_received_agg = 0u64;
    let mut successful_dealer_tasks = 0;

    let all_dealer_results = join_all(dealer_task_handles).await;

    for (idx, join_result) in all_dealer_results.into_iter().enumerate() {
        match join_result {
            Ok(Ok(stats)) => {
                total_messages_sent_agg += stats.messages_sent;
                total_bytes_sent_agg += stats.bytes_sent;
                total_messages_received_agg += stats.messages_received;
                total_bytes_received_agg += stats.bytes_received;
                successful_dealer_tasks += 1;
                if stats.messages_sent != stats.messages_received {
                    eprintln!(
                        "[DEALER {} SUMMARY] Mismatch: Sent {}, Received {}",
                        stats.id, stats.messages_sent, stats.messages_received
                    );
                }
            }
            Ok(Err(e)) => {
                eprintln!("[DEALER {}] Task failed with ZmqError: {}", idx, e);
            }
            Err(e) => {
                eprintln!("[DEALER {}] Task panicked: {}", idx, e);
            }
        }
    }

    let overall_duration = overall_start_time.elapsed();
    println!("\n--- Overall DEALER Results ---");
    println!("Duration: {:?}", overall_duration);
    println!("Successful DEALER tasks: {}/{}", successful_dealer_tasks, NUM_DEALER_TASKS);
    println!("Total messages sent (aggregate): {}", total_messages_sent_agg);
    println!("Total messages received (aggregate): {}", total_messages_received_agg);

    let seconds = overall_duration.as_secs_f64();
    if seconds > 0.0 && successful_dealer_tasks > 0 {
        let agg_send_msg_rate = total_messages_sent_agg as f64 / seconds;
        let agg_send_byte_rate_mb = (total_bytes_sent_agg as f64 / seconds) / (1024.0 * 1024.0);
        println!(
            "Aggregate Send Rate: {:.2} msgs/sec, {:.2} MB/sec",
            agg_send_msg_rate, agg_send_byte_rate_mb
        );

        let agg_recv_msg_rate = total_messages_received_agg as f64 / seconds;
        let agg_recv_byte_rate_mb = (total_bytes_received_agg as f64 / seconds) / (1024.0 * 1024.0);
        println!(
            "Aggregate Recv Rate: {:.2} msgs/sec, {:.2} MB/sec",
            agg_recv_msg_rate, agg_recv_byte_rate_mb
        );
    }
     if total_messages_sent_agg != total_messages_received_agg {
        eprintln!("WARNING: AGGREGATE message sent count ({}) does not match received count ({}).", total_messages_sent_agg, total_messages_received_agg);
    }


    // --- Cleanup ---
    println!("[DEALER Main] Closing main dealer socket...");
    if let Err(e) = dealer_socket_main.close().await {
        eprintln!("[DEALER Main] Error closing main dealer socket: {}", e);
    }
    println!("[DEALER Main] Main dealer socket closed.");

    println!("[Main] Waiting for ROUTER task to complete...");
    match router_join_handle.await {
        Ok(Ok((router_msgs, router_bytes))) => {
            println!(
                "[ROUTER] Task completed. Messages echoed: {}. Bytes echoed: {}",
                router_msgs, router_bytes
            );
            if seconds > 0.0 {
                 let router_echo_msg_rate = router_msgs as f64 / seconds;
                 let router_echo_byte_rate_mb = (router_bytes as f64 / seconds) / (1024.0 * 1024.0);
                 println!("[ROUTER]: Effective echo rate: {:.2} msgs/sec, {:.2} MB/sec", router_echo_msg_rate, router_echo_byte_rate_mb);
            }
            if router_msgs != total_messages_sent_agg {
                eprintln!("[ROUTER] WARNING: Router echoed {} msgs, but dealers sent {} msgs in total.", router_msgs, total_messages_sent_agg);
            }
        }
        Ok(Err(e)) => {
            eprintln!("[ROUTER] Task failed with ZmqError: {}", e);
        }
        Err(e) => {
            eprintln!("[ROUTER] Task panicked or was cancelled: {}", e);
        }
    }

    println!("[Main] Terminating context...");
    ctx.term().await?;
    println!("[Main] Context terminated. Example finished.");

    Ok(())
}