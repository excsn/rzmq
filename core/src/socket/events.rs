use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

/// Represents significant events occurring within a socket or its connections.
/// Inspired by libzmq socket monitor events.
#[derive(Debug, Clone)]
#[non_exhaustive] // Allow adding more events later
pub enum SocketEvent {
  // --- Listener Events ---
  /// Socket has started listening successfully on the endpoint.
  Listening { endpoint: String },
  /// Socket failed to bind to the endpoint.
  BindFailed { endpoint: String, error_msg: String },
  /// Accepted a new connection. `peer_addr` is the address of the remote peer.
  Accepted { endpoint: String, peer_addr: String },
  /// Failed to accept a new connection.
  AcceptFailed { endpoint: String, error_msg: String },

  // --- Connecter Events ---
  /// Connection attempt established (transport layer). Handshake may follow.
  /// Note: Often handshake success is the more useful event.
  Connected { endpoint: String, peer_addr: String },
  /// Initial connection attempt failed, retrying starts (if configured).
  ConnectDelayed { endpoint: String, error_msg: String },
  /// Retrying connection after delay.
  ConnectRetried {
    endpoint: String,
    interval: Duration,
  },
  /// Connection attempt failed definitively after retries or immediately.
  ConnectFailed { endpoint: String, error_msg: String },

  // --- General Connection/Session Events ---
  /// Connection closed (listener stopped, connection dropped).
  Closed { endpoint: String },
  /// Failed to close a connection cleanly (should be rare).
  // CloseFailed { endpoint: String, error: ZmqError },
  /// Peer disconnected or connection terminated. Endpoint identifies the peer connection URI.
  Disconnected { endpoint: String },

  // --- Handshake/Security Events (Often more useful than raw Connected/Accepted) ---
  /// ZMTP handshake (including security mechanism) failed.
  HandshakeFailed { endpoint: String, error_msg: String },
  /// ZMTP handshake (including security mechanism) succeeded.
  HandshakeSucceeded { endpoint: String },

  // --- Backpressure/Congestion Events ---
  /// The connection's egress pipe has reached its send HWM; the socket is
  /// actively skipping this peer via write-ready skip.
  ConnectionCongested { endpoint: String },
  /// The connection's egress pipe has drained below its send HWM; the peer
  /// re-enters the load-balancer rotation.
  ConnectionUncongested { endpoint: String },
}

/// Strip the internal `#<handle>` uniqueness suffix from a connection URI before
/// exposing it through the monitor channel. Inproc connections use
/// `inproc://name#<handle>` as internal map keys; external observers always see
/// the clean logical URI.
pub(crate) fn clean_endpoint_uri(uri: &str) -> &str {
  match uri.find('#') {
    Some(pos) => &uri[..pos],
    None => uri,
  }
}

impl SocketEvent {
  /// The event with its endpoint and peer address passed through `clean_endpoint_uri`.
  pub(crate) fn with_clean_endpoint(mut self) -> Self {
    let clean = |s: &mut String| {
      let cleaned = clean_endpoint_uri(s).len();
      s.truncate(cleaned);
    };
    match &mut self {
      SocketEvent::Accepted { endpoint, peer_addr } | SocketEvent::Connected { endpoint, peer_addr } => {
        clean(endpoint);
        clean(peer_addr);
      }
      SocketEvent::Listening { endpoint }
      | SocketEvent::BindFailed { endpoint, .. }
      | SocketEvent::AcceptFailed { endpoint, .. }
      | SocketEvent::ConnectDelayed { endpoint, .. }
      | SocketEvent::ConnectRetried { endpoint, .. }
      | SocketEvent::ConnectFailed { endpoint, .. }
      | SocketEvent::Closed { endpoint }
      | SocketEvent::Disconnected { endpoint }
      | SocketEvent::HandshakeFailed { endpoint, .. }
      | SocketEvent::HandshakeSucceeded { endpoint }
      | SocketEvent::ConnectionCongested { endpoint }
      | SocketEvent::ConnectionUncongested { endpoint } => clean(endpoint),
    }
    self
  }
}

/// A socket's monitor, shared by its core and every listener, connecter and
/// session it spawns. A clone refers to the same slot, so a monitor attached at
/// any time receives every event emitted after it.
#[derive(Clone, Default)]
pub(crate) struct MonitorHandle {
  slot: Arc<RwLock<Option<MonitorSender>>>,
}

impl MonitorHandle {
  /// Replaces any previous sender; its receiver closes once no clone of it is left.
  pub(crate) fn attach(&self, sender: MonitorSender) {
    *self.slot.write() = Some(sender);
  }

  /// Drops the event when no monitor is attached or its buffer is full.
  pub(crate) fn emit(&self, event: SocketEvent) {
    if let Some(tx) = self.slot.read().as_ref() {
      if tx.try_send(event.with_clean_endpoint()).is_err() {
        tracing::warn!("Failed to send event to monitor channel (full or closed)");
      }
    }
  }

  /// For a site that awaits its send; the slot's guard cannot be held across it.
  pub(crate) fn sender(&self) -> Option<MonitorSender> {
    self.slot.read().clone()
  }
}

impl std::fmt::Debug for MonitorHandle {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("MonitorHandle")
      .field("attached", &self.slot.read().is_some())
      .finish()
  }
}

// Type alias for the channel sender used for monitor events
pub type MonitorSender = fibre::mpsc::BoundedAsyncSender<SocketEvent>;
// Type alias for the channel receiver used for monitor events
pub type MonitorReceiver = fibre::mpsc::BoundedAsyncReceiver<SocketEvent>;

// Default capacity for monitor channel
pub const DEFAULT_MONITOR_CAPACITY: usize = 100;
