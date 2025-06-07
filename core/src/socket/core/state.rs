// core/src/socket/core/state.rs

use crate::runtime::MailboxSender; // Command, SystemEvent not directly used in structs but for context
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::events::MonitorSender;
use crate::socket::options::SocketOptions;
use crate::socket::types::SocketType;
use crate::socket::SocketEvent;
use crate::Msg; // For AsyncSender<Msg>

use fibre::mpmc::AsyncSender;
use std::collections::{HashMap, HashSet};
#[cfg(feature = "io-uring")]
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;

/// Stores information about an active endpoint (Listener or Session) managed by `SocketCore`.
#[derive(Debug)]
pub(crate) struct EndpointInfo {
  /// - For Listeners/Connecters: Command mailbox to the Listener/Connecter actor.
  /// - For Session (ViaUringFd): Could be SocketCore's own sender or a dummy; not used for direct commands.
  pub mailbox: MailboxSender,
  /// - For Listeners/Connecters: `JoinHandle` for their main actor task.
  /// - For Session (ViaUringFd): `None`, as SocketCore doesn't directly manage their task handles.
  pub task_handle: Option<JoinHandle<()>>,
  /// Type of the endpoint (Listener or active Session).
  pub endpoint_type: EndpointType,
  /// The resolved URI of this endpoint (e.g., actual bound TCP address or peer's address).
  pub endpoint_uri: String,
  /// - For Session (ViaUringFd): `Some((synthetic_write_id, synthetic_read_id))` for ISocket interaction.
  /// - For Listener: `None`.
  pub pipe_ids: Option<(usize, usize)>, // (core_writes_here, core_reads_from_here)
  /// A unique identifier for this endpoint entry.
  /// - For Listener/Connecter: The handle_id of their primary actor.
  /// - For Session (ViaUringFd): The RawFd cast to usize.
  pub handle_id: usize,
  /// For connected endpoints (Sessions), this stores the original target URI if it was an outgoing connect.
  pub target_endpoint_uri: Option<String>,
  /// True if this endpoint represents an outbound connection initiated by this SocketCore.
  pub is_outbound_connection: bool,
  /// The unified interface for sending messages and closing the connection.
  pub connection_iface: Arc<dyn ISocketConnection>,
}

/// Enum to differentiate between Listener endpoints and active Session (connection) endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointType {
  Listener,
  Session, // Represents an active connection, regardless of underlying tech (session actor or uring FD)
           // Consider adding Connecter if we store pending connect attempts in endpoints map with a JoinHandle
}

/// Holds the mutable state for a `SocketCore` actor.
#[derive(Debug)]
pub(crate) struct CoreState {
  pub(crate) handle: usize,
  pub options: Arc<SocketOptions>,
  pub socket_type: SocketType,
  // For Session-based path: Map Core's pipe_write_id -> Sender to Session's data pipe
  pub pipes_tx: HashMap<usize, AsyncSender<Msg>>,
  // For Session-based path: Map Core's pipe_read_id -> JoinHandle of PipeReaderTask
  pub pipe_reader_task_handles: HashMap<usize, JoinHandle<()>>,
  // Main map of active endpoints, keyed by resolved endpoint_uri
  pub endpoints: HashMap<String, EndpointInfo>,

  // --- Mappings for ISocket interaction and event routing ---
  /// Maps a pipe_read_id (actual for sessions, or synthetic for uring FDs) to the endpoint_uri.
  /// Used to find the `EndpointInfo` when a message/event arrives on a "pipe" (actual or conceptual).
  pub pipe_read_id_to_endpoint_uri: HashMap<usize, String>,

  /// For io_uring path: Maps a RawFd directly to the endpoint_uri.
  /// Used when UringFd* commands/events (which carry RawFd) arrive at SocketCore.
  #[cfg(feature = "io-uring")]
  pub uring_fd_to_endpoint_uri: HashMap<RawFd, String>,

  #[cfg(feature = "inproc")]
  pub(crate) bound_inproc_names: HashSet<String>,
  pub(crate) monitor_tx: Option<MonitorSender>,
  pub(crate) last_bound_endpoint: Option<String>,
}

impl CoreState {
  pub(crate) fn new(handle: usize, socket_type: SocketType, options: SocketOptions) -> Self {
    Self {
      handle,
      options: Arc::new(options), // Wrap in Arc here
      socket_type,
      pipes_tx: HashMap::new(),
      pipe_reader_task_handles: HashMap::new(),
      endpoints: HashMap::new(),
      pipe_read_id_to_endpoint_uri: HashMap::new(),
      #[cfg(feature = "io-uring")]
      uring_fd_to_endpoint_uri: HashMap::new(),
      #[cfg(feature = "inproc")]
      bound_inproc_names: HashSet::new(),
      monitor_tx: None,
      last_bound_endpoint: None,
    }
  }

  pub(crate) fn get_pipe_sender(&self, pipe_write_id: usize) -> Option<AsyncSender<Msg>> {
    self.pipes_tx.get(&pipe_write_id).cloned()
  }

  #[allow(dead_code)]
  pub(crate) fn get_reader_task_handle(&self, pipe_read_id: usize) -> Option<&JoinHandle<()>> {
    self.pipe_reader_task_handles.get(&pipe_read_id)
  }

  /// Removes pipe sender, aborts/removes pipe reader task, and clears mapping.
  /// Returns true if any state was actually removed.
  pub(crate) fn remove_pipe_state(&mut self, pipe_write_id: usize, pipe_read_id: usize) -> bool {
    let tx_removed = self.pipes_tx.remove(&pipe_write_id).is_some();
    if tx_removed {
      tracing::trace!(
        core_handle = self.handle,
        pipe_id = pipe_write_id,
        "CoreState: Removed pipe sender"
      );
    }

    let reader_removed = if let Some(handle) = self.pipe_reader_task_handles.remove(&pipe_read_id) {
      tracing::trace!(
        core_handle = self.handle,
        pipe_id = pipe_read_id,
        "CoreState: Aborting pipe reader task"
      );
      handle.abort();
      true
    } else {
      false
    };
    if reader_removed {
      tracing::trace!(
        core_handle = self.handle,
        pipe_id = pipe_read_id,
        "CoreState: Removed pipe reader task handle"
      );
    }

    let map_removed = self.pipe_read_id_to_endpoint_uri.remove(&pipe_read_id).is_some();
    if map_removed {
      tracing::trace!(
        core_handle = self.handle,
        pipe_id = pipe_read_id,
        "CoreState: Removed pipe_read_id_to_endpoint_uri mapping"
      );
    }

    tx_removed || reader_removed || map_removed
  }

  pub(crate) fn send_monitor_event(&self, event: SocketEvent) {
    if let Some(ref tx) = self.monitor_tx {
      if tx.clone().to_sync().send(event).is_err() {
        // Non-blocking send
        tracing::warn!(
          socket_handle = self.handle,
          "Failed to send event to monitor channel (full or closed)"
        );
      }
    }
  }

  pub(crate) fn get_monitor_sender_clone(&self) -> Option<MonitorSender> {
    self.monitor_tx.clone()
  }
}

// --- Shutdown Coordinator ---
// Stays here for now, can be moved to shutdown.rs later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownPhase {
  Running,
  StoppingChildren,
  Lingering,
  CleaningPipes,
  Finished,
}

#[derive(Debug)]
pub(crate) struct ShutdownCoordinator {
  pub(crate) state: ShutdownPhase,
  /// Maps child actor handle_id (Listener/Connecter handle) to its endpoint URI for logging.
  /// This tracks actors SocketCore is directly responsible for stopping via their mailboxes.
  pub(crate) pending_child_actors: HashMap<usize, String>,
  /// Tracks connection_instance_ids (Session handle or RawFd) of active connections
  /// that need to be closed via ISocketConnection::close_connection().
  pub(crate) pending_connections_to_close: HashMap<usize, (String, Arc<dyn ISocketConnection>)>, // ID -> (URI, Interface)
  #[cfg(feature = "inproc")]
  pub(crate) inproc_connections_to_cleanup: Vec<(usize, usize, String)>, // (CoreWriteID, CoreReadID, ConnectorURI)
  pub(crate) linger_deadline: Option<Instant>,
}

impl Default for ShutdownCoordinator {
  fn default() -> Self {
    Self {
      state: ShutdownPhase::Running,
      pending_child_actors: HashMap::new(),
      pending_connections_to_close: HashMap::new(),
      #[cfg(feature = "inproc")]
      inproc_connections_to_cleanup: Vec::new(),
      linger_deadline: None,
    }
  }
}
