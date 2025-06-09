// core/src/uring.rs

#![cfg(feature = "io-uring")]

pub mod global_state;

use crate::error::ZmqError;
use crate::io_uring_backend::connection_handler::{HandlerUpstreamEvent, ProtocolHandlerFactory};
use crate::io_uring_backend::worker::UringWorker;
use crate::io_uring_backend::zmtp_handler::ZmtpHandlerFactory;

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use fibre::mpmc::unbounded;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tokio::task::{spawn_blocking, JoinHandle as TokioTaskJoinHandle};
use tracing::{debug, error, info, warn};

#[cfg(feature = "io-uring")]
pub const DEFAULT_IO_URING_SND_BUFFER_COUNT: usize = 16;
/// Default size (in bytes) for each buffer in the io_uring send buffer pool.
#[cfg(feature = "io-uring")]
pub const DEFAULT_IO_URING_SND_BUFFER_SIZE: usize = 65536;

#[cfg(feature = "io-uring")]
pub const DEFAULT_IO_URING_RECV_BUFFER_COUNT: usize = 16;
/// Default size (in bytes) for each buffer in the io_uring multishot receive pool.
#[cfg(feature = "io-uring")]
pub const DEFAULT_IO_URING_RECV_BUFFER_SIZE: usize = 65536; // 64KB

#[derive(Debug, Clone, Copy)]
pub struct UringConfig {
  pub ring_entries: u32,
  pub default_send_zerocopy: bool,
  pub default_recv_multishot: bool,
  pub default_recv_buffer_count: usize,
  pub default_recv_buffer_size: usize,
  pub default_send_buffer_count: usize,
  pub default_send_buffer_size: usize,
}

impl Default for UringConfig {
  fn default() -> Self {
    Self {
      ring_entries: 256,
      default_send_zerocopy: false,
      default_recv_multishot: true,
      default_recv_buffer_count: DEFAULT_IO_URING_RECV_BUFFER_COUNT,
      default_recv_buffer_size: DEFAULT_IO_URING_RECV_BUFFER_SIZE,
      default_send_buffer_count: DEFAULT_IO_URING_SND_BUFFER_COUNT,
      default_send_buffer_size: DEFAULT_IO_URING_SND_BUFFER_SIZE,
    }
  }
}

static URING_INIT_RESULT: OnceCell<Result<(), ZmqError>> = OnceCell::new();
pub static URING_BACKEND_INITIALIZED: AtomicBool = AtomicBool::new(false);

const NOT_INITIALIZED_ERROR_MSG: &str = "io_uring backend not initialized.";

pub fn initialize_uring_backend(config: UringConfig) -> Result<(), ZmqError> {
  let init_result_ref: Result<&Result<(), ZmqError>, ZmqError> = URING_INIT_RESULT.get_or_try_init(|| -> Result<Result<(), ZmqError>, ZmqError> {
    info!(
      "Initializing global io_uring backend with config: {:?}",
      config
    );

    let (upstream_event_tx, upstream_event_rx) = unbounded::<(RawFd, HandlerUpstreamEvent)>();
    *global_state::get_global_parsed_msg_tx_mutex().lock() = Some(upstream_event_tx.clone());
    *global_state::get_global_parsed_msg_rx_mutex().lock() = Some(upstream_event_rx.to_async());
    debug!("io_uring::initialize: Upstream message channel set in global state.");

    let factories: Vec<Arc<dyn ProtocolHandlerFactory>> = vec![Arc::new(ZmtpHandlerFactory {})];
    let (signaling_op_tx, worker_join_handle) =
      UringWorker::spawn_with_config(config, factories, upstream_event_tx).map_err(|e| {
        error!("Failed to spawn UringWorker: {}", e);
        *global_state::get_global_parsed_msg_tx_mutex().lock() = None;
        *global_state::get_global_parsed_msg_rx_mutex().lock() = None;
        e
      })?;

    *global_state::get_uring_worker_op_tx_mutex().lock() = Some(signaling_op_tx);
    *global_state::get_uring_worker_join_handle_mutex().lock() = Some(worker_join_handle);
    debug!("io_uring::initialize: Global UringWorker spawned and its handles stored.");

    let fd_mailbox_map_oncecell =
      global_state::get_uring_fd_to_socket_core_mailbox_map_oncecell();
    let fd_mailbox_map = fd_mailbox_map_oncecell
      .get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
      .clone();
    debug!("io_uring::initialize: URING_FD_TO_SOCKET_CORE_MAILBOX_MAP ensured/retrieved.");

    let processor_rx = global_state::get_global_parsed_msg_rx_mutex().lock().take()
      .ok_or_else(|| ZmqError::Internal("Upstream RX channel was unexpectedly None for processor task".into()))?;

    let processor_join_handle: TokioTaskJoinHandle<()> = tokio::spawn(
      global_state::run_global_uring_upstream_processor(processor_rx, fd_mailbox_map),
    );
    *global_state::get_uring_upstream_processor_join_handle_mutex().lock() =
      Some(processor_join_handle);
    debug!("io_uring::initialize: Global Uring Upstream Message Processor task spawned and handle stored.");

    URING_BACKEND_INITIALIZED.store(true, Ordering::SeqCst);
    info!("Global io_uring backend successfully initialized.");
    
    Ok(Ok(()))
  });

  match init_result_ref {
      Ok(stored_result_ref) => (*stored_result_ref).clone(),
      Err(init_error) => Err(init_error.clone()),
  }
}

pub async fn shutdown_uring_backend() -> Result<(), ZmqError> {
  // Check if initialization ever happened.
  if URING_INIT_RESULT.get().is_none() || !URING_BACKEND_INITIALIZED.load(Ordering::SeqCst) {
    warn!("{}", NOT_INITIALIZED_ERROR_MSG);
    return Ok(());
  }

  // Prevent multiple shutdowns from causing issues.
  if !URING_BACKEND_INITIALIZED.swap(false, Ordering::SeqCst) {
    warn!("io_uring backend shutdown already in progress or completed.");
    return Ok(());
  }

  info!("Shutting down global io_uring backend...");

  // 1. Signal the worker to stop by closing its op channel.
  let taken_op_tx = global_state::get_uring_worker_op_tx_mutex().lock().take();
  if taken_op_tx.is_some() {
    debug!("io_uring::shutdown: Signaled UringWorker to stop by closing its op channel.");
  }
  drop(taken_op_tx);

  // 2. Join the worker thread. Use spawn_blocking to avoid stalling the async runtime.
  if let Some(worker_handle) = global_state::get_uring_worker_join_handle_mutex().lock().take() {
    debug!("io_uring::shutdown: Joining UringWorker thread...");
    // This moves the blocking join to a dedicated thread pool.
    spawn_blocking(move || {
      match worker_handle.join() {
        Ok(Ok(())) => info!("UringWorker thread joined successfully."),
        Ok(Err(e)) => error!("UringWorker thread exited with error: {}", e),
        Err(e) => error!("Failed to join UringWorker thread (panic): {:?}", e),
      }
    }).await.map_err(|e| ZmqError::Internal(format!("spawn_blocking for worker join failed: {}", e)))?;
  } else {
    warn!("io_uring::shutdown: UringWorker JoinHandle was None. Cannot join.");
  }

  // 3. The UpstreamProcessor will have been signaled by the worker dropping the TX side.
  //    Now we just need to await its completion.
  if let Some(processor_handle) = global_state::get_uring_upstream_processor_join_handle_mutex().lock().take() {
    debug!("io_uring::shutdown: Awaiting UringUpstreamProcessor task...");
    if let Err(e) = processor_handle.await {
        if !e.is_cancelled() {
            error!("UringUpstreamProcessor task panicked or failed: {:?}", e);
        }
    }
    info!("UringUpstreamProcessor task finished.");
  } else {
    warn!("io_uring::shutdown: UringUpstreamProcessor JoinHandle was None. Cannot await.");
  }
  
  // 4. Final cleanup of global maps.
  if let Some(map_arc) = global_state::get_uring_fd_to_socket_core_mailbox_map_oncecell().get() {
    map_arc.write().clear();
    debug!("io_uring::shutdown: Cleared contents of global FD-to-mailbox map.");
  }
  
  let _ = global_state::get_global_parsed_msg_tx_mutex().lock().take();
  let _ = global_state::get_global_parsed_msg_rx_mutex().lock().take();

  info!("Global io_uring backend shutdown complete.");
  Ok(())
}