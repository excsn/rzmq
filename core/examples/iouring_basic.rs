use std::time::Duration;

use rzmq::socket::options as rzmq_options;
use rzmq::{
  uring::{initialize_uring_backend, shutdown_uring_backend, UringConfig}, // Import new items
  Context,
  Msg,
  SocketType,
  ZmqError,
}; // Alias for clarity

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::TRACE) // Adjust log level (INFO, DEBUG, TRACE)
    .with_thread_ids(true)
    .with_thread_names(true)
    .with_target(true)
    .compact()
    .init();
  // --- Explicitly initialize the io_uring backend with custom settings ---
  // Here, we enable default_send_zerocopy to true globally for the io_uring backend.
  // This means sends (especially zero-copy ones) handled by the UringWorker
  // will use the deferred ACK strategy (ACK after kernel completion).
  // If this was false (or UringConfig::default() was used and default_send_zerocopy
  // remained false), sends via UringWorker would get an immediate ACK.
  let uring_config = UringConfig {
    default_send_zerocopy: true,
    ring_entries: 256,
    default_recv_multishot: true,
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
      // Decide if to proceed without io_uring or exit.
      // For this example, we might choose to proceed, and sockets requesting io_uring
      // might fail or fallback gracefully if the library supports that.
      // Or, simply return the error: return Err(e);
    }
  }
  // -----------------------------------------------------------------------

  let ctx = Context::new()?; // Context::new() will now use the already initialized backend

  // REP socket (standard Tokio TCP for simplicity, or could also be io_uring)
  let rep = ctx.socket(SocketType::Rep)?;
  let rep_endpoint = "tcp://127.0.0.1:5555";
  rep.bind(rep_endpoint).await?;

  // Spawn a task for REP to handle requests
  let rep_handle = tokio::spawn(async move {
    println!("REP: Waiting for requests on {}", rep_endpoint);
    loop {
      match rep.recv().await {
        // Assuming REP expects single part messages
        Ok(req_msg) => {
          let req_payload = String::from_utf8_lossy(req_msg.data().unwrap_or_default());
          println!("REP: Received '{}'", req_payload);
          // REP echoes the payload
          println!("REP: Sending reply for '{}'", req_payload);
          if let Err(e) = rep.send(req_msg).await {
            // Echo back the received Msg
            eprintln!("REP: Error sending reply: {:?}", e);
            break;
          }
        }
        Err(e) => {
          eprintln!("REP: Error receiving: {:?}", e);
          break;
        }
      }
    }
    println!("REP: Task finished.");
    // rep socket will be closed when its Arc is dropped or context is termed.
  });

  // REQ socket, configured to use io_uring for its session
  let req = ctx.socket(SocketType::Req)?;
  // Option to enable io_uring for this socket's sessions
  // This tells the socket that when it connects, it should attempt to
  // create a session that uses the io_uring backend.
  req.set_option(rzmq_options::IO_URING_SESSION_ENABLED, true).await?;
  // Potentially other io_uring options for this specific socket could be set here,
  // though global defaults are now handled by UringConfig. Socket-specific overrides
  // for ZC, multishot for *this specific socket* would be new options if desired.

  println!("REQ: Connecting to {}", rep_endpoint);
  req.connect(rep_endpoint).await?;
  println!("REQ: Connected. Waiting a bit for connection to establish fully...");
  tokio::time::sleep(Duration::from_millis(200)).await; // Allow connection

  for i in 0..3 {
    let request_payload = format!("Request-Payload-{}", i);
    println!("REQ: Sending '{}'", request_payload);
    // For REQ socket, send expects a single message part.
    req.send(Msg::from_vec(request_payload.into_bytes())).await?;

    println!("REQ: Receiving reply for request {}...", i);
    match req.recv().await {
      Ok(reply) => {
        println!(
          "REQ: Received reply '{}'",
          String::from_utf8_lossy(reply.data().unwrap_or_default())
        );
      }
      Err(e) => {
        eprintln!("REQ: Error receiving reply for request {}: {:?}", i, e);
        break; // Exit loop on error
      }
    }
    tokio::time::sleep(Duration::from_millis(50)).await; // Small delay between requests
  }

  println!("REQ: Closing REQ socket.");
  req.close().await?; // Close the REQ socket explicitly

  // Context termination will signal the REP socket's parent context to shut down.
  // The REP socket itself doesn't need an explicit close call here if `ctx.term()` is used,
  // as actor shutdown is managed by the context.
  // However, the REP task needs to handle recv errors due to context termination to exit its loop.
  println!("Main: Terminating context...");
  ctx.term().await?;
  println!("Main: Context terminated.");

  println!("Waiting for rep socket");
  // Wait for REP task to finish if it hasn't already due to ctx.term()
  if let Err(e) = rep_handle.await {
    eprintln!("Error joining REP task: {:?}", e);
  }

  println!("Shutting down iouring");
  // --- Explicitly shutdown the io_uring backend (optional but good practice for cleanup) ---
  // This is more relevant if your application has a very specific point where it no longer
  // needs the io_uring backend and wants to release its global resources.
  // In many cases, letting OS clean up on process exit is fine if `ctx.term()` already joined worker.
  // However, for libraries or long-running apps with re-init needs, this is useful.
  match shutdown_uring_backend().await {
    Ok(_) => println!("io_uring backend shutdown."),
    Err(e) => eprintln!("Error shutting down io_uring backend: {:?}", e),
  }
  // -------------------------------------------------------------------------------------

  println!("Main: Example finished.");
  Ok(())
}
