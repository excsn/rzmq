#![cfg(feature = "io-uring")]

use crate::io_uring_backend::connection_handler::HandlerUpstreamEvent;
use crate::io_uring_backend::signaling_op_sender::SignalingOpSender;
use crate::message::Blob;
use crate::runtime::command::Command;
use crate::runtime::MailboxSender as SocketCoreMailboxSender;
use crate::{error::ZmqError, uring::URING_BACKEND_INITIALIZED};

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::thread::JoinHandle as StdThreadJoinHandle;

#[cfg(feature = "io-uring")]
use fibre::mpmc::{AsyncReceiver, Receiver as SyncReceiver, Sender as SyncSender};
use once_cell::sync::OnceCell;
#[cfg(feature = "io-uring")]
use parking_lot::Mutex;
use parking_lot::RwLock;
use tokio::task::JoinHandle as TokioTaskJoinHandle;
use tracing::{debug, error, info, trace, warn};

// --- Global Static Cells (Modified) ---
static URING_WORKER_OP_TX: OnceCell<Mutex<Option<SignalingOpSender>>> = OnceCell::new();
static URING_WORKER_JOIN_HANDLE: OnceCell<Mutex<Option<StdThreadJoinHandle<Result<(), ZmqError>>>>> = OnceCell::new();

static PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR: OnceCell<Mutex<Option<SyncSender<(RawFd, HandlerUpstreamEvent)>>>> =
  OnceCell::new();
static PARSED_MSG_RX_FOR_PROCESSOR: OnceCell<Mutex<Option<AsyncReceiver<(RawFd, HandlerUpstreamEvent)>>>> =
  OnceCell::new();
static URING_UPSTREAM_PROCESSOR_JOIN_HANDLE: OnceCell<Mutex<Option<TokioTaskJoinHandle<()>>>> = OnceCell::new();

// The FD map itself is already Arc<RwLock<...>>, so putting that inside OnceCell<Mutex<Option<...>>> is excessive.
// For the map, if we need to "clear" it, we'd clear the HashMap's contents, not the Arc/RwLock itself.
// Or, if the goal is to ensure it's not used after shutdown, we can keep it as is and rely on URING_BACKEND_INITIALIZED.
// Let's keep FD_TO_SOCKET_CORE_MAILBOX_MAP as OnceCell<Arc<RwLock<HashMap<...>>>> for now.
// "Taking" it means preventing new registrations/lookups.
static URING_FD_TO_SOCKET_CORE_MAILBOX_MAP: OnceCell<Arc<RwLock<HashMap<RawFd, SocketCoreMailboxSender>>>> =
  OnceCell::new();

// --- Getters for OnceCells (Modified to reflect Mutex<Option<T>>) ---
// These now return a reference to the Mutex, allowing guarded access to the Option<T>.

#[doc(hidden)]
#[cfg(feature = "io-uring")]
pub(crate) fn get_uring_worker_op_tx_mutex() -> &'static Mutex<Option<SignalingOpSender>> {
  URING_WORKER_OP_TX.get_or_init(Default::default)
}
#[doc(hidden)]
#[cfg(feature = "io-uring")]
pub(crate) fn get_uring_worker_join_handle_mutex() -> &'static Mutex<Option<StdThreadJoinHandle<Result<(), ZmqError>>>>
{
  URING_WORKER_JOIN_HANDLE.get_or_init(Default::default)
}
#[doc(hidden)]
#[cfg(feature = "io-uring")]
pub(crate) fn get_global_parsed_msg_tx_mutex() -> &'static Mutex<Option<SyncSender<(RawFd, HandlerUpstreamEvent)>>>
{
  PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR.get_or_init(Default::default)
}
#[doc(hidden)]
#[cfg(feature = "io-uring")]
pub(crate) fn get_global_parsed_msg_rx_mutex() -> &'static Mutex<Option<AsyncReceiver<(RawFd, HandlerUpstreamEvent)>>> {
  PARSED_MSG_RX_FOR_PROCESSOR.get_or_init(Default::default)
}
#[doc(hidden)]
#[cfg(feature = "io-uring")]
pub(crate) fn get_uring_upstream_processor_join_handle_mutex() -> &'static Mutex<Option<TokioTaskJoinHandle<()>>> {
  URING_UPSTREAM_PROCESSOR_JOIN_HANDLE.get_or_init(Default::default)
}

#[doc(hidden)]
#[cfg(feature = "io-uring")]
pub(crate) fn get_uring_fd_to_socket_core_mailbox_map_oncecell(
) -> &'static OnceCell<Arc<RwLock<HashMap<RawFd, SocketCoreMailboxSender>>>> {
  &URING_FD_TO_SOCKET_CORE_MAILBOX_MAP // This one we still init directly in uring.rs
}

pub(crate) fn ensure_global_uring_systems_started() -> Result<(), ZmqError> {
  if !URING_BACKEND_INITIALIZED.load(std::sync::atomic::Ordering::SeqCst) {
    info!("GlobalUringState: io_uring backend not yet initialized by user. Initializing with default configuration.");
    crate::uring::initialize_uring_backend(UringConfig::default())?;
  } else {
    debug!("GlobalUringState: io_uring backend already initialized.");
  }
  Ok(())
}

// --- Helper: get_global_uring_worker_op_tx ---
pub(crate) fn get_global_uring_worker_op_tx() -> Result<SignalingOpSender, ZmqError> {
  let guard = get_uring_worker_op_tx_mutex().lock();
  guard.as_ref().cloned().ok_or_else(|| {
    error!("Global UringWorker OP_TX not available or already taken. Ensure backend is initialized and not shut down.");
    ZmqError::Internal("UringWorker OP_TX unavailable".into())
  })
}

// --- register/unregister for FD map (Modified to handle OnceCell better) ---
pub(crate) fn register_uring_fd_socket_core_mailbox(fd: RawFd, core_mailbox: SocketCoreMailboxSender) {
  // Use get_or_init for the OnceCell itself, ensuring it contains the Arc<RwLock<...>>
  let map_arc = URING_FD_TO_SOCKET_CORE_MAILBOX_MAP.get_or_init(|| Arc::new(RwLock::new(HashMap::new())));
  debug!(
    raw_fd = fd,
    "GlobalUringState: Registering uring FD with a SocketCore mailbox."
  );
  map_arc.write().insert(fd, core_mailbox);
}

pub(crate) fn unregister_uring_fd_socket_core_mailbox(fd: RawFd) {
  if let Some(map_arc) = URING_FD_TO_SOCKET_CORE_MAILBOX_MAP.get() {
    debug!(raw_fd = fd, "GlobalUringState: Unregistering uring FD.");
    if map_arc.write().remove(&fd).is_some() {
      trace!(raw_fd = fd, "GlobalUringState: Successfully unregistered uring FD.");
    } else {
      trace!(
        raw_fd = fd,
        "GlobalUringState: Attempted to unregister uring FD that was not in map."
      );
    }
  } else {
    // This case should be less likely if register always initializes the OnceCell.
    warn!(
      raw_fd = fd,
      "GlobalUringState: URING_FD_TO_SOCKET_CORE_MAILBOX_MAP OnceCell not initialized. Cannot unregister FD."
    );
  }
}

// --- run_global_uring_upstream_processor (remains pub(crate)) ---
#[cfg(feature = "io-uring")]
pub(crate) async fn run_global_uring_upstream_processor(
  msg_rx: AsyncReceiver<(RawFd, HandlerUpstreamEvent)>,
  fd_to_mailbox_map: Arc<RwLock<HashMap<RawFd, SocketCoreMailboxSender>>>,
) {
  info!("Global io_uring upstream message processor task started.");
  loop {
    use fibre::RecvError;

    match msg_rx.recv().await {
      Ok((fd, upstream_event)) => {
        trace!(raw_fd = fd, event_type = ?upstream_event_variant_name(&upstream_event), "UringUpstreamProcessor: Received event for FD.");
        let socket_core_mailbox_clone: Option<SocketCoreMailboxSender> = { fd_to_mailbox_map.read().get(&fd).cloned() };

        if let Some(socket_core_mailbox) = socket_core_mailbox_clone {
          let command_to_send_to_core: Option<Command> = match upstream_event {
            HandlerUpstreamEvent::Data(msg) => Some(Command::UringFdMessage { fd, msg }),
            HandlerUpstreamEvent::HandshakeComplete { peer_identity } => {
              Some(Command::UringFdHandshakeComplete { fd, peer_identity })
            }
            HandlerUpstreamEvent::Error(error) => Some(Command::UringFdError { fd, error }),
          };
          if let Some(cmd) = command_to_send_to_core {
            if let Err(e) = socket_core_mailbox.send(cmd).await {
              error!(
                raw_fd = fd,
                "UringUpstreamProcessor: Failed to send Command to SocketCore for FD {}: {}. Unregistering FD.", fd, e
              );
              fd_to_mailbox_map.write().remove(&fd);
            }
          }
        } else {
          warn!(
            raw_fd = fd,
            "UringUpstreamProcessor: Received event for unregistered FD. Discarding."
          );
        }
      }
      Err(RecvError::Disconnected) => {
        info!("UringUpstreamProcessor: Upstream message channel closed. Terminating task.");
        break;
      }
    }
  }
  warn!("Global io_uring upstream message processor task has exited.");
}

#[cfg(feature = "io-uring")]
use crate::uring::UringConfig; // Moved import here

#[cfg(feature = "io-uring")]
fn upstream_event_variant_name(event: &HandlerUpstreamEvent) -> &'static str {
  match event {
    HandlerUpstreamEvent::Data(_) => "Data",
    HandlerUpstreamEvent::HandshakeComplete { .. } => "HandshakeComplete",
    HandlerUpstreamEvent::Error(_) => "Error",
  }
}
