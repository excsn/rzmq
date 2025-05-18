// src/socket/events.rs

use crate::error::ZmqError;
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
  ConnectRetried { endpoint: String, interval: Duration },
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
  // /// A specific ZMTP protocol event occurred during handshake (e.g., ZAP needed).
  // HandshakeProtocol { endpoint: String, event_code: i32 }, // Example
}

// Type alias for the channel sender used for monitor events
pub type MonitorSender = async_channel::Sender<SocketEvent>;
// Type alias for the channel receiver used for monitor events
pub type MonitorReceiver = async_channel::Receiver<SocketEvent>;

// Default capacity for monitor channel
pub const DEFAULT_MONITOR_CAPACITY: usize = 100;