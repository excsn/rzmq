use crate::message::FrameBatch;
use crate::runtime::MailboxSender;
use crate::socket::SocketEvent;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::events::MonitorSender;
use crate::socket::options::SocketOptions;
use crate::socket::types::SocketType;

use fibre::mpmc::AsyncSender;
use std::collections::{HashMap, HashSet};
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
  pub peer_socket_type: Option<String>,
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

#[derive(Debug, Clone)]
pub(crate) struct ReconnectState {
  pub current_attempts: u32,
  pub next_attempt_at: Option<Instant>,
}

impl Default for ReconnectState {
  fn default() -> Self {
    Self {
      current_attempts: 0,
      next_attempt_at: None,
    }
  }
}

impl ReconnectState {
  /// Resets state when a connection is successfully established
  pub fn on_connection_success(&mut self) {
    self.current_attempts = 0;
    self.next_attempt_at = None;
  }

  /// Calculates the delay for the next attempt and updates state
  pub fn on_connection_failure(
    &mut self,
    base_ivl: std::time::Duration,
    max_ivl: std::time::Duration,
  ) -> std::time::Duration {
    // 1. Calculate Backoff: base * 2 ^ attempts
    // Cap power at 31 to prevent u32 overflow
    let multiplier = 2u32.saturating_pow(self.current_attempts.min(31));
    let mut delay = base_ivl.saturating_mul(multiplier);

    // 2. Cap at max interval
    if max_ivl > std::time::Duration::ZERO {
      delay = delay.min(max_ivl);
    }

    // 3. Update state
    self.current_attempts = self.current_attempts.saturating_add(1);
    self.next_attempt_at = Some(Instant::now() + delay);

    delay
  }

  /// Checks if a retry is due
  pub fn is_due(&self, now: Instant) -> bool {
    match self.next_attempt_at {
      Some(time) => now >= time,
      None => false,
    }
  }
}

/// Holds the mutable state for a `SocketCore` actor.
#[derive(Debug)]
pub(crate) struct CoreState {
  pub(crate) handle: usize,
  pub options: Arc<SocketOptions>,
  pub socket_type: SocketType,
  // For Session-based path: Map Core's pipe_write_id -> Sender to Session's data pipe
  pub pipes_tx: HashMap<usize, AsyncSender<FrameBatch>>,
  // For Session-based path: Map Core's pipe_read_id -> JoinHandle of PipeReaderTask
  pub pipe_reader_task_handles: HashMap<usize, JoinHandle<()>>,
  // Main map of active endpoints, keyed by resolved endpoint_uri
  pub endpoints: HashMap<String, EndpointInfo>,

  // --- Reconnection State ---
  /// Maps Target URI -> ReconnectState
  pub(crate) reconnect_states: HashMap<String, ReconnectState>,

  // --- Mappings for ISocket interaction and event routing ---
  /// Maps a pipe_read_id (actual for sessions, or synthetic for uring FDs) to the endpoint_uri.
  /// Used to find the `EndpointInfo` when a message/event arrives on a "pipe" (actual or conceptual).
  pub pipe_read_id_to_endpoint_uri: HashMap<usize, String>,

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
      reconnect_states: HashMap::new(),
      pipe_read_id_to_endpoint_uri: HashMap::new(),
      #[cfg(feature = "inproc")]
      bound_inproc_names: HashSet::new(),
      monitor_tx: None,
      last_bound_endpoint: None,
    }
  }

  pub(crate) fn get_pipe_sender(&self, pipe_write_id: usize) -> Option<AsyncSender<FrameBatch>> {
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

    let map_removed = self
      .pipe_read_id_to_endpoint_uri
      .remove(&pipe_read_id)
      .is_some();
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
      // Strip internal `#<id>` uniqueness suffixes from URIs before exposing them.
      // Inproc connections use `inproc://name#<handle>` as map keys internally;
      // external observers always see the clean logical URI.
      let clean = |s: String| -> String {
        match s.find('#') {
          Some(pos) => s[..pos].to_string(),
          None => s,
        }
      };
      let event = match event {
        SocketEvent::Listening { endpoint } =>
          SocketEvent::Listening { endpoint: clean(endpoint) },
        SocketEvent::BindFailed { endpoint, error_msg } =>
          SocketEvent::BindFailed { endpoint: clean(endpoint), error_msg },
        SocketEvent::Accepted { endpoint, peer_addr } =>
          SocketEvent::Accepted { endpoint: clean(endpoint), peer_addr: clean(peer_addr) },
        SocketEvent::AcceptFailed { endpoint, error_msg } =>
          SocketEvent::AcceptFailed { endpoint: clean(endpoint), error_msg },
        SocketEvent::Connected { endpoint, peer_addr } =>
          SocketEvent::Connected { endpoint: clean(endpoint), peer_addr: clean(peer_addr) },
        SocketEvent::ConnectDelayed { endpoint, error_msg } =>
          SocketEvent::ConnectDelayed { endpoint: clean(endpoint), error_msg },
        SocketEvent::ConnectRetried { endpoint, interval } =>
          SocketEvent::ConnectRetried { endpoint: clean(endpoint), interval },
        SocketEvent::ConnectFailed { endpoint, error_msg } =>
          SocketEvent::ConnectFailed { endpoint: clean(endpoint), error_msg },
        SocketEvent::Closed { endpoint } =>
          SocketEvent::Closed { endpoint: clean(endpoint) },
        SocketEvent::Disconnected { endpoint } =>
          SocketEvent::Disconnected { endpoint: clean(endpoint) },
        SocketEvent::HandshakeFailed { endpoint, error_msg } =>
          SocketEvent::HandshakeFailed { endpoint: clean(endpoint), error_msg },
        SocketEvent::HandshakeSucceeded { endpoint } =>
          SocketEvent::HandshakeSucceeded { endpoint: clean(endpoint) },
      };
      if tx.clone().to_sync().send(event).is_err() {
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

#[cfg(test)]
mod reconnect_tests {
  use super::*;
  use std::time::Duration;

  #[test]
  fn test_reconnect_backoff_doubling() {
    let mut state = ReconnectState::default();
    let base = Duration::from_millis(100);
    let max = Duration::from_millis(1000);

    // Call 1: 2^0 * 100ms = 100ms
    let delay1 = state.on_connection_failure(base, max);
    assert_eq!(delay1, Duration::from_millis(100));

    // Call 2: 2^1 * 100ms = 200ms
    let delay2 = state.on_connection_failure(base, max);
    assert_eq!(delay2, Duration::from_millis(200));

    // Call 3: 2^2 * 100ms = 400ms
    let delay3 = state.on_connection_failure(base, max);
    assert_eq!(delay3, Duration::from_millis(400));
  }

  #[test]
  fn test_reconnect_backoff_ceiling() {
    let mut state = ReconnectState::default();
    let base = Duration::from_millis(100);
    let max = Duration::from_millis(250);

    let _ = state.on_connection_failure(base, max); // 100ms
    let delay2 = state.on_connection_failure(base, max); // 200ms
    let delay3 = state.on_connection_failure(base, max); // 400ms → capped at 250ms

    assert_eq!(delay2, Duration::from_millis(200));
    assert_eq!(delay3, max);
  }

  #[test]
  fn test_reconnect_overflow_prevention() {
    let mut state = ReconnectState::default();
    let base = Duration::from_millis(100);
    let max = Duration::from_secs(60);

    for _ in 0..100 {
      let delay = state.on_connection_failure(base, max);
      // saturating_pow and saturating_mul prevent overflow; max cap keeps delay bounded
      assert!(delay <= max, "Delay {:?} must not exceed max {:?}", delay, max);
    }

    assert_eq!(state.current_attempts, 100);
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
