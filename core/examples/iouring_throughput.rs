use bytes::Bytes;
use rzmq::socket::options as zmq_opts; // For socket option constants
use rzmq::uring::{initialize_uring_backend, shutdown_uring_backend, UringConfig};
use rzmq::{Context, Msg, MsgFlags, SocketType, ZmqError};
use std::time::{Duration, Instant}; // For efficient Bytes cloning

const PRINT_SEND_RECV_MESSAGES: bool = false;
const COMPACT_SEND_RECV_MESSAGES: bool = true;

// Ensure rzmq::main is used if your library's io_uring feature might switch Tokio runtimes.
// For this example, we'll use tokio::main and assume the library handles internal setup.
#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  // Initialize tracing (optional, but good for seeing rzmq logs)
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "info,rzmq=debug");
  }

  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO) // Adjust log level (INFO, DEBUG, TRACE)
    .with_thread_ids(true)
    .with_thread_names(true)
    .with_target(true)
    .compact()
    .init();

  println!("Starting DEALER-ROUTER example (potential io_uring usage)...");

  let uring_config = UringConfig {
    default_send_zerocopy: false,
    ring_entries: 1024,
    default_recv_multishot: false,
    default_recv_buffer_count: 64,
    default_recv_buffer_size: 4096,
    default_send_buffer_count: 64,  // For ZC pool if default_send_zerocopy is true
    default_send_buffer_size: 4096, // For ZC pool
  };
  match initialize_uring_backend(uring_config) {
    Ok(_) => println!("io_uring backend initialized explicitly."),
    Err(ZmqError::InvalidState(msg)) if msg.contains("already initialized") => {
      println!("io_uring backend was already initialized (perhaps by another test or context).");
    }
    Err(e) => {
      eprintln!("Failed to initialize io_uring backend: {:?}. Falling back.", e);
    }
  }

  let ctx = Context::new().expect("Failed to create rzmq context");

  // --- ROUTER Socket (Server Side) ---
  let router_socket = ctx.socket(SocketType::Router)?;
  router_socket
    .set_option(zmq_opts::IO_URING_SESSION_ENABLED, true)
    .await?;
  router_socket.set_option(zmq_opts::RCVHWM, 5000).await?;
  router_socket.set_option(zmq_opts::SNDHWM, 5000).await?;

  let endpoint = "tcp://127.0.0.1:5558"; // Ensure a unique port
  println!("ROUTER: Binding to {}...", endpoint);
  router_socket.bind(endpoint).await?;
  println!("ROUTER: Bound successfully.");

  let router_socket_for_task = router_socket.clone();
  // Spawn a task for ROUTER to echo messages
  let router_task_handle = tokio::spawn(async move {
    let router_socket = router_socket_for_task;
    println!("ROUTER: Echo task started, waiting for messages...");
    let mut messages_echoed = 0u64;
    let mut bytes_echoed = 0u64;
    loop {
      // ROUTER receives [identity, delimiter (if from DEALER), payload_frame_1, ...]
      match router_socket.recv_multipart().await {
        Ok(mut received_parts) => {
          if received_parts.is_empty() {
            eprintln!("ROUTER: Received empty multipart message, ignoring.");
            continue;
          }

          // First frame is the client's identity
          let client_identity_frame = received_parts.remove(0);
          // `received_parts` now contains the payload sent by the DEALER.
          // If DEALER sent [payload1, payload2], ROUTER's `recv_multipart` after stripping identity
          // should yield [payload1, payload2].
          // The DEALER socket itself prepends an empty delimiter before the payload.
          // The ROUTER socket's `ISocket` logic (specifically `process_raw_message_for_router`
          // via `IncomingMessageOrchestrator`) should strip this delimiter if present,
          // so `received_parts` here should be the actual application payload.

          if received_parts.is_empty() {
            // This could happen if DEALER sends only an empty message after delimiter,
            // or if the stripping logic leads to no payload.
            println!(
              "ROUTER: Received message from {:?} with no payload after identity/delimiter, echoing identity back.",
              client_identity_frame.data()
            );
          } else {
            if PRINT_SEND_RECV_MESSAGES {
              println!(
                "ROUTER: Received message from {:?} with {} payload parts. First part size: {}",
                client_identity_frame.data(),
                received_parts.len(),
                received_parts[0].size()
              );
            }
          }

          // Construct reply: [identity_frame_WITH_MORE, payload_part_1_WITH_MORE, ..., payload_part_N_NO_MORE]
          let mut reply_to_send = Vec::with_capacity(1 + received_parts.len());

          let mut id_reply_frame = client_identity_frame; // client_identity_frame already has its original flags
                                                          // Ensure identity frame has MORE if payload follows
          if !received_parts.is_empty() {
            id_reply_frame.set_flags(id_reply_frame.flags() | MsgFlags::MORE);
          } else {
            // If no payload, identity is the only (and last) frame
            id_reply_frame.set_flags(id_reply_frame.flags() & !MsgFlags::MORE);
          }
          reply_to_send.push(id_reply_frame);

          let num_payload_parts = received_parts.len();
          for (idx, mut part) in received_parts.into_iter().enumerate() {
            bytes_echoed += part.size() as u64;
            if idx < num_payload_parts - 1 {
              part.set_flags(part.flags() | MsgFlags::MORE);
            } else {
              // Last part of payload, ensure NO MORE
              part.set_flags(part.flags() & !MsgFlags::MORE);
            }
            reply_to_send.push(part);
          }

          if router_socket.send_multipart(reply_to_send).await.is_err() {
            eprintln!("ROUTER: Error sending reply, terminating echo task.");
            break;
          }
          messages_echoed += 1;
        }
        Err(ZmqError::InvalidState(ref msg)) if msg.contains("Socket is closing") => {
          println!("ROUTER: Socket closing, terminating echo task.");
          break;
        }
        Err(e) => {
          eprintln!("ROUTER: recv_multipart error: {}, terminating echo task.", e);
          break;
        }
      }
    }
    println!("ROUTER: Echo task finished after {} messages.", messages_echoed);
    (messages_echoed, bytes_echoed)
  });

  // --- DEALER Socket (Client Side) ---
  let dealer_socket = ctx.socket(SocketType::Dealer)?;
  // Enable io_uring for DEALER's connection
  println!("DEALER: Enabling IO_URING_SESSION_ENABLED...");
  dealer_socket
    .set_option(zmq_opts::IO_URING_SESSION_ENABLED, true)
    .await?;
  // dealer_socket.set_option(zmq_opts::SNDHWM, 1000i32).await?; // Optional: adjust HWMs
  // dealer_socket.set_option(zmq_opts::RCVHWM, 1000i32).await?;

  println!("DEALER: Connecting to {}...", endpoint);
  dealer_socket.connect(endpoint).await?;
  println!("DEALER: Connect call returned. Waiting for connection to establish...");
  // Allow time for TCP connection and ZMTP handshake (especially if io_uring path has its own handshake)
  tokio::time::sleep(Duration::from_millis(250)).await;
  println!("DEALER: Assumed connected and handshake complete.");

  
  // --- Throughput Test Logic ---
  let num_messages_to_send: u64 = 100_000; // Number of logical messages
  let payload_size_bytes: usize = 1024; // 1KB
  let message_payload = Bytes::from(vec![0u8; payload_size_bytes]); // Create Bytes once

  let mut messages_sent = 0u64;
  let mut messages_received = 0u64;
  let mut bytes_sent = 0u64;
  let mut bytes_received = 0u64;

  println!(
    "DEALER: Starting throughput test: {} messages of {} bytes each.",
    num_messages_to_send, payload_size_bytes
  );
  let start_time = Instant::now();

  for i in 0..num_messages_to_send {
    // DEALER send: The DEALER socket implementation should prepend the empty delimiter.
    // We just send the payload.
    let msg_to_send = Msg::from_bytes(message_payload.clone()); // Clone Bytes (cheap)

    match dealer_socket.send(msg_to_send).await {
      Ok(_) => {
        messages_sent += 1;
        bytes_sent += payload_size_bytes as u64;
      }
      Err(e) => {
        eprintln!("DEALER: Send error on message {}: {}, stopping.", i, e);
        break;
      }
    }

    // DEALER receive: Expects only the payload, identity/delimiter stripped by socket.
    match dealer_socket.recv().await {
      Ok(reply_msg) => {
        if PRINT_SEND_RECV_MESSAGES {
          if COMPACT_SEND_RECV_MESSAGES {
            println!(
              "[DEALER] Received reply (msg #{}) (Size: {})",
              i, // assuming i is the loop counter for messages
              reply_msg.size()
            );
          } else {
            println!(
              "[DEALER] Received reply (msg #{}): '{}' (Size: {})",
              i, // assuming i is the loop counter for messages
              String::from_utf8_lossy(reply_msg.data().unwrap_or_default()),
              reply_msg.size()
            );
          }
        }

        // Basic validation, could be more thorough
        if reply_msg.size() == payload_size_bytes {
          messages_received += 1;
          bytes_received += reply_msg.size() as u64;
        } else {
          eprintln!(
            "DEALER: Received reply with unexpected size. Expected: {}, Got: {}. Message #{}",
            payload_size_bytes,
            reply_msg.size(),
            i
          );
        }
      }
      Err(ZmqError::InvalidState(ref msg)) if msg.contains("Socket is closing") => {
        println!("DEALER: Socket closing during recv on message #{}. Stopping.", i);
        break;
      }
      Err(e) => {
        eprintln!("DEALER: Recv error on message {}: {}, stopping.", i, e);
        break;
      }
    }

    if (i + 1) % (num_messages_to_send / 10).max(1) == 0 {
      // Log progress every 10%
      println!("DEALER: Sent/Received {} messages...", i + 1);
    }
    // tokio::time::sleep(Duration::from_millis(1500)).await;
  }

  let duration = start_time.elapsed();
  println!("\nDEALER: Test loop finished. Duration: {:?}", duration);
  println!("DEALER: Total logical messages sent: {}", messages_sent);
  println!("DEALER: Total logical messages received: {}", messages_received);

  // --- Calculate and Print Rates ---
  let seconds = duration.as_secs_f64();
  if seconds > 0.0 {
    let send_msg_rate = messages_sent as f64 / seconds;
    let send_byte_rate_mb = (bytes_sent as f64 / seconds) / (1024.0 * 1024.0);
    println!(
      "DEALER: Send rate: {:.2} msgs/sec, {:.2} MB/sec",
      send_msg_rate, send_byte_rate_mb
    );

    let recv_msg_rate = messages_received as f64 / seconds;
    let recv_byte_rate_mb = (bytes_received as f64 / seconds) / (1024.0 * 1024.0);
    println!(
      "DEALER: Recv rate: {:.2} msgs/sec, {:.2} MB/sec",
      recv_msg_rate, recv_byte_rate_mb
    );
  }
  if messages_sent != messages_received {
    eprintln!(
      "WARNING: Message sent count ({}) does not match received count ({}).",
      messages_sent, messages_received
    );
  }

  // --- Cleanup ---
  println!("DEALER: Closing dealer socket...");
  if let Err(e) = dealer_socket.close().await {
    eprintln!("DEALER: Error closing dealer socket: {}", e);
  }
  println!("DEALER: Dealer socket closed.");


  println!("ROUTER: Closing router socket...");
  if let Err(e) = router_socket.close().await {
      eprintln!("ROUTER: Error closing router socket: {}", e);
  }
  println!("ROUTER: Router socket closed.");

  // Router task will be implicitly asked to stop when its socket is closed by ctx.term()
  // Or we could explicitly close router_socket here too.
  // For now, let ctx.term() handle ROUTER socket closure.
  // Wait for router task to finish and get its stats
  println!("ROUTER: Waiting for echo task to complete...");
  match router_task_handle.await {
    Ok((router_msgs, router_bytes_val)) => {
      println!(
        "ROUTER: Echo task completed. Messages echoed: {}. Bytes echoed: {}",
        router_msgs, router_bytes_val
      );
      if seconds > 0.0 {
        let router_echo_msg_rate = router_msgs as f64 / seconds;
        let router_echo_byte_rate_mb = (router_bytes_val as f64 / seconds) / (1024.0 * 1024.0);
        println!(
          "ROUTER: Effective echo rate: {:.2} msgs/sec, {:.2} MB/sec",
          router_echo_msg_rate, router_echo_byte_rate_mb
        );
      }
    }
    Err(e) => {
      eprintln!("ROUTER: Echo task panicked or was cancelled: {}", e);
    }
  }

  println!("Terminating context...");
  ctx.term().await?;


  println!("Terminated context...");
  match shutdown_uring_backend().await {
    Ok(_) => println!("io_uring backend shutdown."),
    Err(e) => eprintln!("Error shutting down io_uring backend: {:?}", e),
  }

  println!("Context terminated. Example finished.");

  Ok(())
}
