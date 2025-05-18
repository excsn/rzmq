// tests/common.rs
#![allow(dead_code)] // Allow unused helpers for now

use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Context, Socket, ZmqError};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;
use std::time::Duration;

use tokio::time::timeout;

static IPC_ENDPOINT_COUNTER: AtomicUsize = AtomicUsize::new(0);
static INPROC_ENDPOINT_COUNTER: AtomicUsize = AtomicUsize::new(0);

// Use std::sync::Once for one-time initialization
static TRACING_INIT: Once = Once::new();

// Setup function to initialize tracing
fn setup_tracing() {
  TRACING_INIT.call_once(|| {
    // Default level filter (e.g., info for rzmq, warn for others)
    // Can be overridden by RUST_LOG env variable
    let default_filter = "rzmq=trace,debug,info,warn";
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    let subscriber = FmtSubscriber::builder()
      .with_max_level(tracing::Level::TRACE) // Allow all levels down to TRACE
      .with_env_filter(env_filter)
      .with_target(true) // Show module path
      .with_line_number(true) // Show line numbers
      .with_span_events(FmtSpan::CLOSE) // Log when spans close
      .with_test_writer() // Write to test output capture
      .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global tracing subscriber");

    println!("Tracing subscriber initialized."); // Optional: confirm init
  });
}

// Helper to create a context
pub fn test_context() -> Context {
  setup_tracing(); // Ensure tracing is initialized before creating context
  rzmq::Context::new().expect("Failed to create test context")
}

// Helper to generate unique IPC endpoints for tests
pub fn unique_ipc_endpoint() -> String {
  // Use process ID and a counter/random number to avoid collisions
  // between parallel tests. Needs a more robust unique name generator
  // for production-level testing.
  let pid = std::process::id();
  let count = IPC_ENDPOINT_COUNTER.fetch_add(1, Ordering::Relaxed);
  format!("ipc:///tmp/rzmq_test_{}_{}", pid, count)
}

// Helper to generate unique Inproc endpoints
pub fn unique_inproc_endpoint() -> String {
  let pid = std::process::id();
  let count = INPROC_ENDPOINT_COUNTER.fetch_add(1, Ordering::Relaxed);
  format!("inproc://rzmq_test_{}_{}", pid, count)
}

// Helper for recv with timeout assertion
pub async fn recv_timeout(socket: &Socket, duration: Duration) -> Result<rzmq::Msg, ZmqError> {
  match timeout(duration, socket.recv()).await {
    Ok(Ok(msg)) => Ok(msg),
    Ok(Err(e)) => Err(e),             // Propagate ZmqError from recv
    Err(_) => Err(ZmqError::Timeout), // Map timeout error
  }
}

// Helper for send with short timeout assertion (e.g., for non-blocking checks)
pub async fn send_timeout(socket: &Socket, msg: rzmq::Msg, duration: Duration) -> Result<(), ZmqError> {
  match timeout(duration, socket.send(msg)).await {
    Ok(Ok(())) => Ok(()),
    Ok(Err(e)) => Err(e),
    Err(_) => Err(ZmqError::Timeout),
  }
}

// Function to bind a socket and return the chosen endpoint (useful for tcp://*)
pub async fn bind_socket_resolve(socket: &Socket, base_endpoint: &str) -> Result<String, ZmqError> {
  socket.bind(base_endpoint).await?;
  // Retrieve the actual bound endpoint if using wildcard
  // NOTE: This requires ZMQ_LAST_ENDPOINT option support, which isn't implemented yet.
  // For now, if base_endpoint contains '*', we might have to guess or use fixed ports.
  // Let's assume fixed ports or non-wildcard for initial tests.
  // ZMQ_LAST_ENDPOINT = 52
  if base_endpoint.contains('*') {
    // TODO: Implement ZMQ_LAST_ENDPOINT to get actual bound port
    panic!("Cannot resolve wildcard endpoint without ZMQ_LAST_ENDPOINT support");
    // Or return a fixed known port used in tests for now
    // Ok("tcp://127.0.0.1:5555".to_string()) // Example
  } else {
    Ok(base_endpoint.to_string())
  }
}

// --- Helper function to wait for specific monitor event ---
pub async fn wait_for_monitor_event(
  monitor_rx: &MonitorReceiver,
  timeout: Duration,
  short_recv_timeout: Duration,
  check_event: impl Fn(&SocketEvent) -> bool, // Closure to check if event matches
) -> Result<SocketEvent, String> {
  let start_time = tokio::time::Instant::now();
  loop {
    // Check elapsed time first
    if start_time.elapsed() > timeout {
      return Err(format!(
        "Timeout waiting for specific monitor event after {:?}",
        timeout
      ));
    }

    // Try receiving with a short interval timeout
    match tokio::time::timeout(short_recv_timeout, monitor_rx.recv()).await {
      Ok(Ok(event)) => {
        // Received an event
        println!("Monitor received: {:?}", event); // Log received event
        if check_event(&event) {
          return Ok(event); // Found the event we want
        }
        // Event received, but not the one we're looking for, continue loop
      }
      Ok(Err(_recv_err)) => {
        // Channel closed
        return Err("Monitor channel closed unexpectedly".to_string());
      }
      Err(_elapsed) => { // Timeout for this recv attempt
         // Continue loop to check overall timeout
      }
    }
  }
}
