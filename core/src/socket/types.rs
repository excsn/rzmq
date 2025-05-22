// src/socket/types.rs

use crate::error::ZmqError; // For Result types in API methods.
use crate::message::Msg;    // For send/recv methods.
use crate::runtime::Command; // For UserMonitor command payload.
use crate::runtime::MailboxSender; // For storing the SocketCore's command sender.
use crate::socket::events::{MonitorReceiver, SocketEvent, DEFAULT_MONITOR_CAPACITY}; // For socket monitoring.
use crate::socket::ISocket; // The internal trait implemented by specific socket patterns.
use std::fmt;
use std::sync::Arc;
use tokio::sync::oneshot; // For reply channels in commands.

/// Represents the type of a ZeroMQ socket, defining its messaging pattern and behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SocketType {
  /// **PUB (Publish):** Distributes messages to all connected subscribers.
  /// Messages are topic-filtered on the subscriber side. PUB sockets do not receive messages.
  Pub,
  /// **SUB (Subscribe):** Receives messages from PUB sockets it's connected to.
  /// Must subscribe to specific topics (or all topics using an empty prefix) to receive messages.
  Sub,
  /// **REQ (Request):** Sends requests and receives replies in a strict alternating sequence.
  /// A REQ socket must `send()` then `recv()`, then `send()` again, and so on.
  Req,
  /// **REP (Reply):** Receives requests and sends replies in a strict alternating sequence.
  /// A REP socket must `recv()` then `send()`, then `recv()` again, and so on.
  Rep,
  /// **DEALER (Extended REQ):** Asynchronous request-reply pattern.
  /// Load-balances outgoing messages among connected peers and fair-queues incoming messages.
  /// Can send multiple messages before receiving and vice-versa. Often used with ROUTER.
  Dealer,
  /// **ROUTER (Extended REP):** Asynchronous request-reply pattern.
  /// Receives messages prefixed with the sender's identity and routes outgoing messages
  /// to specific peers based on their identity. Often used with DEALER.
  Router,
  /// **PUSH:** Distributes messages to a pool of connected PULL workers in a round-robin fashion.
  /// PUSH sockets do not receive messages.
  Push,
  /// **PULL:** Collects messages from a pool of connected PUSH distributors in a fair-queued manner.
  /// PULL sockets do not send messages.
  Pull,
}

/// The public handle for interacting with an rzmq socket.
/// This struct provides the user-facing API for socket operations.
/// Handles are cloneable (`Arc`-based), allowing them to be shared across tasks.
/// Operations on this handle are delegated to an underlying actor (`SocketCore`)
/// that manages the socket's state and pattern logic.
#[derive(Clone)]
pub struct Socket {
  // `inner` holds an `Arc` to the trait object implementing the specific socket pattern logic.
  // This allows `Socket` to be a generic handle for any `SocketType`.
  pub(crate) inner: Arc<dyn ISocket>,
  // Stores a clone of the command sender for the `SocketCore` actor associated with this socket.
  // This is used to send user-initiated commands (like bind, connect, send, recv) to the core actor.
  pub(crate) core_command_sender: MailboxSender,
}

impl Socket {
  /// Creates a new public `Socket` handle.
  /// This is typically called by `Context::socket()` after the internal socket machinery
  /// (`SocketCore` and the specific `ISocket` pattern implementation) has been set up.
  ///
  /// # Arguments
  /// * `socket_impl` - An `Arc` to the `ISocket` trait object that implements the socket's pattern logic.
  /// * `core_command_sender` - The `MailboxSender` for the `SocketCore` actor managing this socket.
  pub(crate) fn new(socket_impl: Arc<dyn ISocket>, core_command_sender: MailboxSender) -> Self {
    Self {
      inner: socket_impl,
      core_command_sender,
    }
  }

  // --- Public API Methods (Asynchronous) ---
  // These methods provide the primary interface for interacting with the socket.
  // They are asynchronous and delegate their operations to the `ISocket` implementation,
  // which typically involves sending a command to the `SocketCore` actor.

  /// Binds the socket to listen on a local endpoint (e.g., "tcp://127.0.0.1:5555", "ipc:///tmp/mysock").
  /// For connection-oriented transports like TCP, this allows incoming connections.
  pub async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
    self.inner.bind(endpoint).await
  }

  /// Connects the socket to a remote endpoint.
  /// For connection-oriented transports, this initiates a connection to a listening peer.
  pub async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
    self.inner.connect(endpoint).await
  }

  /// Disconnects from a specific endpoint that was previously connected using `connect()`.
  pub async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
    self.inner.disconnect(endpoint).await
  }

  /// Stops listening on a specific endpoint that was previously bound using `bind()`.
  pub async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError> {
    self.inner.unbind(endpoint).await
  }

  /// Sends a message asynchronously according to the socket's pattern.
  /// For example, a PUSH socket will distribute the message, while a REQ socket
  /// will send it as a request.
  pub async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    self.inner.send(msg).await
  }

  /// Receives a message asynchronously according to the socket's pattern.
  /// This call will block (asynchronously) until a message is available or a timeout occurs (if set).
  pub async fn recv(&self) -> Result<Msg, ZmqError> {
    self.inner.recv().await
  }

  /// Sends a sequence of message frames atomically as one logical message.
  /// The exact interpretation of "atomically" and how frames are handled
  /// (e.g., prepending identities or delimiters) depends on the socket type.
  ///
  /// For ROUTER: The first frame in `frames` MUST be the destination identity,
  ///             and it MUST have the MORE flag set if subsequent frames exist.
  ///             The implementation will insert the empty delimiter.
  ///             The payload frames follow.
  /// For DEALER: All frames are payload sent to a chosen peer, with an empty
  ///             delimiter prepended automatically by the DEALER implementation.
  /// Other types: May error or have specific behavior.
  ///
  /// The `frames` Vec should have MsgFlags::MORE set correctly on all but the last Msg.
  pub async fn send_multipart(&self, frames: Vec<Msg>) -> Result<(), ZmqError> {
    self.inner.send_multipart(frames).await
  }

  /// Sets a socket option asynchronously.
  /// Options control various aspects of the socket's behavior (e.g., high-water marks, timeouts).
  /// Refer to ZMQ documentation for standard option IDs and their meanings.
  pub async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    self.inner.set_option(option, value).await
  }

  /// Gets a socket option value asynchronously.
  pub async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    self.inner.get_option(option).await
  }

  /// Initiates a graceful shutdown of the socket asynchronously.
  /// This will close all connections and release resources associated with the socket.
  /// Further operations on the socket after calling `close()` may fail.
  pub async fn close(&self) -> Result<(), ZmqError> {
    self.inner.close().await
  }

  /// Creates a monitoring channel for this socket.
  ///
  /// Events detailing the socket's internal state changes (e.g., connections established,
  /// disconnections, bind failures, handshake events) will be sent to the returned `MonitorReceiver`.
  /// This is useful for observing the socket's lifecycle and network activity.
  ///
  /// # Arguments
  /// * `capacity` - Defines the buffer size of the monitoring channel. If the
  ///   receiver does not consume events quickly enough and the channel fills up,
  ///   subsequent events might be dropped, resulting in `RecvError::Lagged`.
  ///
  /// # Returns
  /// A `Result` containing the `MonitorReceiver` on success, or a `ZmqError` on failure
  /// (e.g., if the socket's internal actor communication fails).
  pub async fn monitor(&self, capacity: usize) -> Result<MonitorReceiver, ZmqError> {
    let (monitor_tx, monitor_rx) = async_channel::bounded(capacity.max(1)); // Ensure capacity is at least 1.
    let (reply_tx, reply_rx) = oneshot::channel(); // For acknowledging monitor setup.

    // Create a UserMonitor command to send to the SocketCore.
    let cmd = Command::UserMonitor { monitor_tx, reply_tx };

    // Send the command to the SocketCore's command mailbox.
    self
      .core_command_sender // Use the stored command sender for this socket's core.
      .send(cmd)
      .await
      .map_err(|_send_error| ZmqError::Internal("Mailbox send error during monitor setup".into()))?;

    // Wait for the SocketCore to acknowledge that the monitor has been set up.
    // The `??` propagates the `RecvError` from `reply_rx.await` and then the `Result<(), ZmqError>` inside.
    reply_rx
      .await
      .map_err(|_recv_error| ZmqError::Internal("Reply channel error during monitor setup".into()))??;

    Ok(monitor_rx) // Return the receiver end of the monitor channel to the user.
  }

  /// Creates a monitoring channel with a default capacity (`DEFAULT_MONITOR_CAPACITY`).
  /// This is a convenience wrapper around `monitor()`.
  pub async fn monitor_default(&self) -> Result<MonitorReceiver, ZmqError> {
    self.monitor(DEFAULT_MONITOR_CAPACITY).await
  }
}

impl fmt::Debug for Socket {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // Provide a basic Debug representation.
    // Avoid trying to debug the `dyn ISocket` directly as it can be complex.
    // Information like the socket's handle ID or type could be added if
    // `ISocket` provides accessors for them without needing to lock `SocketCore` state.
    f.debug_struct("Socket")
      // Example: .field("core_handle", &self.inner.core().handle) // If core().handle is accessible and cheap
      .finish_non_exhaustive() // Indicates that more fields might be added in the future.
  }
}