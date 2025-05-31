// core/src/runtime/global_uring_state.rs

#![cfg(feature = "io-uring")]

use crate::error::ZmqError;
use crate::io_uring_backend::ops::UringOpRequest;
use crate::io_uring_backend::worker::UringWorker;
use crate::io_uring_backend::zmtp_handler::ZmtpHandlerFactory;
use crate::message::{Msg, Blob};
use crate::runtime::MailboxSender as SocketCoreMailboxSender;
use crate::runtime::command::Command;

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::thread::JoinHandle as StdThreadJoinHandle;

use once_cell::sync::OnceCell;
use parking_lot::RwLock; 
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

  URING_WORKER_OP_TX.get_or_try_init(|| {
    info!("GlobalUringState: Initializing and spawning global UringWorker thread.");
    let factories: Vec<Arc<dyn crate::io_uring_backend::connection_handler::ProtocolHandlerFactory>> =
        vec![Arc::new(ZmtpHandlerFactory {})];
    
    let parsed_msg_tx_for_worker = PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR.get()
        .expect("PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR should be initialized by now")
        .clone();

    let (op_tx, join_handle) = UringWorker::spawn(
        256, 
        factories,
        parsed_msg_tx_for_worker,
    )?; 

    if URING_WORKER_JOIN_HANDLE.set(join_handle).is_err() {
        warn!("GlobalUringState: UringWorker spawned, but its JoinHandle could not be stored globally (already set?). This is unexpected.");
    }
    debug!("GlobalUringState: Global UringWorker spawned successfully.");
    Ok::<_, ZmqError>(op_tx)
  })?; 

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
      Ok((fd, Err(zmq_error))) => {
        info!(raw_fd = fd, error = %zmq_error, "UringUpstreamProcessor: Received Error/Signal for FD.");
        
        let command_to_send_to_core: Option<Command>;
        let error_string = zmq_error.to_string(); // Bind to a variable with longer lifetime
        if error_string.starts_with("ZMTP_HANDSHAKE_COMPLETE_SIGNAL_FD_") {
            let parts: Vec<&str> = error_string.split("_PEER_ID_").collect();
            let peer_id_str_opt = parts.get(1).map(|s| s.trim_end_matches(')')); 
            
            let peer_identity: Option<Blob> = peer_id_str_opt.and_then(|s| {
                if s == "None" {
                    None
                } else {
                    let inner_id_str = s.trim_start_matches("Some(\"").trim_start_matches("Some(b\"").trim_end_matches('\"');
                    if inner_id_str.is_empty() && s != "None" { 
                        Some(Blob::new()) 
                    } else if s != "None" {
                        Some(Blob::from(inner_id_str.as_bytes().to_vec()))
                    } else {
                        None
                    }
                }
            });

            debug!(raw_fd = fd, ?peer_identity, "UringUpstreamProcessor: Parsed HANDSHAKE_COMPLETE for FD.");
            
            command_to_send_to_core = Some(Command::UringFdHandshakeComplete { fd, peer_identity });
        } else {
            warn!(raw_fd = fd, error = %error_string, "UringUpstreamProcessor: Non-handshake error/signal received for FD.");
            command_to_send_to_core = Some(Command::UringFdError { fd, error: zmq_error });
        }

        if let Some(cmd) = command_to_send_to_core {
            let socket_core_mailbox_clone: Option<SocketCoreMailboxSender> = {
                let map_read = fd_to_mailbox_map.read();
                map_read.get(&fd).cloned()
            };
            if let Some(socket_core_mailbox) = socket_core_mailbox_clone {
                 // Use try_send for signals/errors to avoid blocking processor if SocketCore mailbox is full
                 if socket_core_mailbox.try_send(cmd).is_err() { 
                    warn!(raw_fd=fd, "UringUpstreamProcessor: Failed to send command (HandshakeComplete/Error) to SocketCore for FD {}. Mailbox might be full or closed.", fd);
                    // If SocketCore mailbox is closed, its entry should eventually be removed from the map.
                 }
            } else {
                warn!(raw_fd=fd, "UringUpstreamProcessor: Received signal/error for unregistered FD {}. Discarding signal.", fd);
            }
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