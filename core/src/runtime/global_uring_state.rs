// core/src/runtime/global_uring_state.rs

#![cfg(feature = "io-uring")]

use crate::error::ZmqError;
use crate::io_uring_backend::ops::UringOpRequest;
use crate::io_uring_backend::worker::UringWorker;
use crate::io_uring_backend::zmtp_handler::ZmtpHandlerFactory;
use crate::io_uring_backend::UringOpCompletion;
use crate::io_uring_backend::one_shot_sender::OneShotSender as WorkerOneShotSender;
use crate::message::{Msg, Blob};
use crate::runtime::MailboxSender as SocketCoreMailboxSender;
use crate::runtime::command::Command;
use crate::socket::options::{DEFAULT_IO_URING_RECV_BUFFER_COUNT, DEFAULT_IO_URING_RECV_BUFFER_SIZE};

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::thread::JoinHandle as StdThreadJoinHandle;
use std::time::Duration;

use once_cell::sync::OnceCell;
use parking_lot::RwLock; 
use tokio::sync::oneshot as tokio_oneshot;
use tokio::task::JoinHandle as TokioTaskJoinHandle;
use kanal::{Sender as KanalSender, Receiver as KanalReceiver, ReceiveError as KanalReceiveError}; 
use tracing::{debug, error, info, warn, trace};

// --- Global Static Cells ---
static URING_WORKER_OP_TX: OnceCell<KanalSender<UringOpRequest>> = OnceCell::new();
static URING_WORKER_JOIN_HANDLE: OnceCell<StdThreadJoinHandle<Result<(), ZmqError>>> = OnceCell::new();

static PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR: OnceCell<KanalSender<(RawFd, Result<Msg, ZmqError>)>> = OnceCell::new();
static PARSED_MSG_RX_FOR_PROCESSOR: OnceCell<KanalReceiver<(RawFd, Result<Msg, ZmqError>)>> = OnceCell::new();
static URING_UPSTREAM_PROCESSOR_JOIN_HANDLE: OnceCell<TokioTaskJoinHandle<()>> = OnceCell::new();

static URING_FD_TO_SOCKET_CORE_MAILBOX_MAP: OnceCell<Arc<RwLock<HashMap<RawFd, SocketCoreMailboxSender>>>> = OnceCell::new();

pub(crate) fn ensure_global_uring_systems_started() -> Result<(), ZmqError> {
  URING_FD_TO_SOCKET_CORE_MAILBOX_MAP.get_or_init(|| {
    debug!("GlobalUringState: Initializing URING_FD_TO_SOCKET_CORE_MAILBOX_MAP.");
    Arc::new(RwLock::new(HashMap::new()))
  });

  PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR.get_or_try_init(|| {
    let (tx, rx) = kanal::unbounded::<(RawFd, Result<Msg, ZmqError>)>();
    PARSED_MSG_RX_FOR_PROCESSOR.set(rx)
        .map_err(|_| ZmqError::Internal("Failed to set PARSED_MSG_RX_FOR_PROCESSOR more than once".into()))?;
    debug!("GlobalUringState: Initialized upstream message channel (PARSED_MSG_TX/RX).");
    Ok::<_, ZmqError>(tx)
  })?; 

  // Initialize UringWorker first
  let worker_op_tx = URING_WORKER_OP_TX.get_or_try_init(|| {
    info!("GlobalUringState: Initializing and spawning global UringWorker thread.");
    let factories: Vec<Arc<dyn crate::io_uring_backend::connection_handler::ProtocolHandlerFactory>> =
        vec![Arc::new(ZmtpHandlerFactory {})];
    
    let parsed_msg_tx_for_worker = PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR.get()
        .expect("PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR should be initialized by now")
        .clone();

    let (op_tx_internal, join_handle_internal) = UringWorker::spawn(
        256, 
        factories,
        parsed_msg_tx_for_worker,
    )?; 

    if URING_WORKER_JOIN_HANDLE.set(join_handle_internal).is_err() {
        warn!("GlobalUringState: UringWorker spawned, but its JoinHandle could not be stored globally (already set?). This is unexpected.");
    }
    debug!("GlobalUringState: Global UringWorker spawned successfully.");
    Ok::<_, ZmqError>(op_tx_internal)
  })?.clone(); // Clone the sender for our initialization request.

  // --- NEW: Initialize Default Buffer Ring ---
  // Check if buffer ring is already initialized conceptually (e.g. if ensure_global_uring_systems_started is called multiple times)
  // We can't directly check UringWorker's state here.
  // A simple way is to make InitializeBufferRing idempotent in the worker, or just send it once.
  // For OnceCell, this whole block only runs once. So, sending it here is fine.

  if !worker_op_tx.is_closed() { // Ensure worker is likely running
      info!("GlobalUringState: Sending InitializeBufferRing request to UringWorker.");
      let (reply_tx, reply_rx) = tokio_oneshot::channel();
      let init_buf_ring_req = UringOpRequest::InitializeBufferRing {
          user_data: u64::MAX - 1, // Special user_data for this internal op
          bgid: 0, // Default buffer group ID
          num_buffers: DEFAULT_IO_URING_RECV_BUFFER_COUNT as u16,
          buffer_capacity: DEFAULT_IO_URING_RECV_BUFFER_SIZE,
          reply_tx: WorkerOneShotSender::new(reply_tx),
      };

      // Send the request. Since this is critical setup, block on send.
      // Use a blocking send from Kanal if needed, or ensure the channel has capacity.
      // For now, assume unbounded or sufficient capacity for this one message.
      // If using `kanal::bounded`, this might need to be `send_blocking` or async send in a spawned task.
      // Since `ensure_global_uring_systems_started` is not async, we must use a blocking send
      // or refactor `ensure_global_uring_systems_started` to be async.
      // Let's use `try_send` and if it fails due to full (unlikely for unbounded), it's an error.
      // Or better, make `ensure_global_uring_systems_started` fully async.
      // For now, let's assume a short timeout for the reply.
      // To await the reply, this function will need to be async or run in a blocking context.
      // For now, let's use a blocking receive on the oneshot for simplicity in this sync function.
      // THIS WILL BLOCK THE CALLING THREAD (e.g. Context::new())
      // A better solution is to make ensure_global_uring_systems_started async if possible.
      // If it must remain sync, one might spawn a short-lived tokio task to do this async send/recv.
      //
      // Simplest blocking approach (will block current thread):
      // Runtime for this blocking part:

      if let Err(e) = worker_op_tx.send(init_buf_ring_req) { // Kanal send is async
          error!("GlobalUringState: Failed to send InitializeBufferRing request: {}", e);
          return Err(ZmqError::Internal(format!("Failed to send InitializeBufferRing: {}", e)));
      }

      futures::executor::block_on(async {
          match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
              Ok(Ok(Ok(UringOpCompletion::InitializeBufferRingSuccess { .. }))) => {
                  info!("GlobalUringState: Default buffer ring initialized successfully in UringWorker.");
                  Ok(())
              }
              Ok(Ok(Ok(other_completion))) => {
                  error!("GlobalUringState: UringWorker returned unexpected success for InitializeBufferRing: {:?}", other_completion);
                  Err(ZmqError::Internal("InitializeBufferRing failed: unexpected completion".into()))
              }
              Ok(Ok(Err(e))) => {
                  error!("GlobalUringState: UringWorker failed to initialize buffer ring: {}", e);
                  Err(e)
              }
              Ok(Err(oneshot_err)) => { // oneshot::RecvError
                  error!("GlobalUringState: Failed to receive reply for InitializeBufferRing: {}", oneshot_err);
                  Err(ZmqError::Internal(format!("InitializeBufferRing reply channel error: {}", oneshot_err)))
              }
              Err(_timeout_elapsed) => { // tokio::time::error::Elapsed
                  error!("GlobalUringState: Timeout waiting for InitializeBufferRing reply from UringWorker.");
                  Err(ZmqError::Timeout)
              }
          }
      })?; // Propagate error from block_on
  } else {
      error!("GlobalUringState: UringWorker op_tx channel is closed. Cannot initialize buffer ring.");
      return Err(ZmqError::Internal("UringWorker channel closed before buffer ring init".into()));
  }
  // --- END NEW ---

  URING_UPSTREAM_PROCESSOR_JOIN_HANDLE.get_or_try_init::<_, ZmqError>(|| { 
    info!("GlobalUringState: Initializing and spawning global Uring Upstream Message Processor task.");
    let msg_rx = PARSED_MSG_RX_FOR_PROCESSOR.get()
        .expect("PARSED_MSG_RX_FOR_PROCESSOR should be initialized by now")
        .clone(); 
    
    let fd_to_mailbox_map = URING_FD_TO_SOCKET_CORE_MAILBOX_MAP.get()
        .expect("URING_FD_TO_SOCKET_CORE_MAILBOX_MAP should be initialized by now")
        .clone();

    let processor_join_handle: TokioTaskJoinHandle<()> = tokio::spawn( 
        run_global_uring_upstream_processor(msg_rx, fd_to_mailbox_map)
    );
    debug!("GlobalUringState: Global Uring Upstream Message Processor task spawned.");
    Ok(processor_join_handle) 
  })?;
  
  Ok(())
}

pub(crate) fn get_global_uring_worker_op_tx() -> KanalSender<UringOpRequest> {
  URING_WORKER_OP_TX.get()
    .expect("Global UringWorker OP_TX not initialized. Call ensure_global_uring_systems_started() first.")
    .clone()
}

pub(crate) fn register_uring_fd_socket_core_mailbox(fd: RawFd, core_mailbox: SocketCoreMailboxSender) {
  if let Some(map) = URING_FD_TO_SOCKET_CORE_MAILBOX_MAP.get() {
    debug!(raw_fd = fd, "GlobalUringState: Registering uring FD with a SocketCore mailbox.");
    map.write().insert(fd, core_mailbox);
  } else {
    warn!(raw_fd = fd, "GlobalUringState: URING_FD_TO_SOCKET_CORE_MAILBOX_MAP not initialized. Cannot register FD.");
  }
}

pub(crate) fn unregister_uring_fd_socket_core_mailbox(fd: RawFd) {
  if let Some(map) = URING_FD_TO_SOCKET_CORE_MAILBOX_MAP.get() {
    debug!(raw_fd = fd, "GlobalUringState: Unregistering uring FD.");
    if map.write().remove(&fd).is_some() {
        trace!(raw_fd = fd, "GlobalUringState: Successfully unregistered uring FD.");
    } else {
        trace!(raw_fd = fd, "GlobalUringState: Attempted to unregister uring FD that was not in map.");
    }
  } else {
      warn!(raw_fd = fd, "GlobalUringState: URING_FD_TO_SOCKET_CORE_MAILBOX_MAP not initialized. Cannot unregister FD.");
  }
}

async fn run_global_uring_upstream_processor(
  msg_rx: KanalReceiver<(RawFd, Result<Msg, ZmqError>)>,
  fd_to_mailbox_map: Arc<RwLock<HashMap<RawFd, SocketCoreMailboxSender>>>,
) {
  info!("Global io_uring upstream message processor task started.");
  loop {
    match msg_rx.as_async().recv().await {
      Ok((fd, Ok(msg))) => {
        trace!(raw_fd = fd, msg_size = msg.size(), "UringUpstreamProcessor: Received data Msg for FD.");
        let socket_core_mailbox_clone: Option<SocketCoreMailboxSender> = {
            let map_read = fd_to_mailbox_map.read();
            map_read.get(&fd).cloned() // Clone the MailboxSender if found
        }; // map_read (RwLockReadGuard) is dropped here

        if let Some(socket_core_mailbox) = socket_core_mailbox_clone {
          let cmd = Command::UringFdMessage { fd, msg };
          if let Err(e) = socket_core_mailbox.send(cmd).await { 
            error!(raw_fd = fd, "UringUpstreamProcessor:s Failed to send UringFdMessage to SocketCore for FD {}: {}. Unregistering FD.", fd, e);
            fd_to_mailbox_map.write().remove(&fd); 
          }
        } else {
          warn!(raw_fd = fd, "UringUpstreamProcessor: Received message for unregistered FD. Discarding.");
        }
      }
      Ok((fd, Err(original_zmq_error))) => {
        info!(raw_fd = fd, error = %original_zmq_error, "UringUpstreamProcessor: Received Error/Signal for FD.");
        
        let command_to_send_to_core: Command; 
        let error_message_content = original_zmq_error.to_string(); // Bind to a variable with longer lifetime

        debug!(raw_fd = fd, "Checking error_message_content: '{}'", error_message_content);
        if error_message_content.contains("ZMTP_HANDSHAKE_COMPLETE_SIGNAL_FD_") {
          // It's our special signal, extract details
          let signal_part = error_message_content
              .split("ZMTP_HANDSHAKE_COMPLETE_SIGNAL_FD_")
              .nth(1) // Get the part after the prefix
              .unwrap_or(""); // Should always exist if contains worked

          let parts: Vec<&str> = signal_part.split("_PEER_ID_").collect();
          // The first part after "ZMTP_HANDSHAKE_COMPLETE_SIGNAL_FD_" should be the FD, 
          // but we already have `fd`. We are interested in the PEER_ID part.
          let peer_id_str_opt = parts.get(1).map(|s| s.trim_end_matches(')')); 
          
          let peer_identity: Option<Blob> = peer_id_str_opt.and_then(|s| {
              if s == "None" { // Explicitly check for "None" string from Debug format of Option<Blob>
                  None
              } else {
                  // Attempt to strip "Some(" and potential quote variants if Blob's Debug includes them
                  let inner_id_str = s
                      .trim_start_matches("Some(\"")
                      .trim_start_matches("Some(b\"")
                      .trim_end_matches('\"')
                      .trim_end_matches(')'); // Also trim closing parenthesis from Some(...)
                  
                  if inner_id_str.is_empty() && s != "None" { // If after stripping it's empty, but wasn't "None" string
                      Some(Blob::new()) 
                  } else if s != "None" { // If it wasn't "None" and isn't empty after stripping
                      Some(Blob::from(inner_id_str.as_bytes().to_vec()))
                  } else { // Was "None" or became empty in a way that implies None
                      None
                  }
              }
          });

          debug!(raw_fd = fd, ?peer_identity, parsed_signal_part = signal_part, "UringUpstreamProcessor: Parsed HANDSHAKE_COMPLETE signal for FD.");
          command_to_send_to_core = Command::UringFdHandshakeComplete { fd, peer_identity };

      } else {
          // This is a genuine error from ZmtpUringHandler, not our signal
          warn!(raw_fd = fd, error = %original_zmq_error, "UringUpstreamProcessor: Forwarding genuine ZmqError for FD.");
          command_to_send_to_core = Command::UringFdError { fd, error: original_zmq_error };
      }

        let socket_core_mailbox_clone: Option<SocketCoreMailboxSender> = {
            let map_read = fd_to_mailbox_map.read();
            map_read.get(&fd).cloned()
        };
        if let Some(socket_core_mailbox) = socket_core_mailbox_clone {
              // Use try_send for signals/errors to avoid blocking processor if SocketCore mailbox is full
              if socket_core_mailbox.try_send(command_to_send_to_core).is_err() { 
                warn!(raw_fd=fd, "UringUpstreamProcessor: Failed to send command (HandshakeComplete/Error) to SocketCore for FD {}. Mailbox might be full or closed.", fd);
                // If SocketCore mailbox is closed, its entry should eventually be removed from the map.
              }
        } else {
            warn!(raw_fd=fd, "UringUpstreamProcessor: Received signal/error for unregistered FD {}. Discarding signal.", fd);
        }
        
      }
      Err(KanalReceiveError::Closed) => { 
        info!("UringUpstreamProcessor: Upstream message channel (msg_rx) closed because all senders dropped. Terminating task.");
        break;
      }
      Err(KanalReceiveError::SendClosed) => { 
        info!("UringUpstreamProcessor: Upstream message channel (msg_rx) explicitly closed by sender. Terminating task.");
        break;
      }
    }
  }
  warn!("Global io_uring upstream message processor task has exited.");
}