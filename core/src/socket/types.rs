use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::Command;
use crate::runtime::MailboxSender;
use crate::socket::ISocket;
use crate::socket::events::{DEFAULT_MONITOR_CAPACITY, MonitorReceiver};
use fibre::mpmc::bounded_async;
use fibre::oneshot;
use std::fmt;
use std::sync::Arc;

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

  /// Receives a complete multipart message from the socket.
  ///
  /// This method will read frames from the socket until a frame without
  /// the MORE flag is encountered, collecting them into a `Vec<Msg>`.
  /// It respects the `RCVTIMEO` socket option for the overall operation of
  /// receiving all parts of the message. If a timeout occurs mid-message,
  /// an error is returned and partial data is discarded.
  pub async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    self.inner.recv_multipart().await
  }

  /// Sets a socket option asynchronously.
  /// Options control various aspects of the socket's behavior (e.g., high-water marks, timeouts).
  /// Refer to ZMQ documentation for standard option IDs and their meanings.
  pub async fn set_option<T: ToBytes>(&self, option: i32, value: T) -> Result<(), ZmqError> {
    self.set_option_raw(option, &value.to_bytes()).await
  }

  /// Sets a socket option asynchronously.
  /// Options control various aspects of the socket's behavior (e.g., high-water marks, timeouts).
  /// Refer to ZMQ documentation for standard option IDs and their meanings.
  pub async fn set_option_raw(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
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

  /// Creates a monitoring channel for this socket. Must be called BEFORE connect.
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
    let (monitor_tx, monitor_rx) = bounded_async(capacity.max(1)); // Ensure capacity is at least 1.
    let (reply_tx, reply_rx) = oneshot::oneshot();

    // Create a UserMonitor command to send to the SocketCore.
    let cmd = Command::UserMonitor {
      monitor_tx,
      reply_tx,
    };

    // Send the command to the SocketCore's command mailbox.
    self
      .core_command_sender
      .send(cmd)
      .await
      .map_err(|_send_error| {
        ZmqError::Internal("Mailbox send error during monitor setup".into())
      })?;

    // Wait for the SocketCore to acknowledge that the monitor has been set up.
    // The `??` propagates the `RecvError` from `reply_rx.await` and then the `Result<(), ZmqError>` inside.
    reply_rx.recv().await.map_err(|_recv_error| {
      ZmqError::Internal("Reply channel error during monitor setup".into())
    })??;

    Ok(monitor_rx)
  }

  /// Creates a monitoring channel with a default capacity (`DEFAULT_MONITOR_CAPACITY`).
  /// This is a convenience wrapper around `monitor()`.
  pub async fn monitor_default(&self) -> Result<MonitorReceiver, ZmqError> {
    self.monitor(DEFAULT_MONITOR_CAPACITY).await
  }
}

impl fmt::Debug for Socket {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Socket").finish_non_exhaustive()
  }
}

pub trait ToBytes {
  fn to_bytes(&self) -> Vec<u8>;
}

impl ToBytes for Vec<u8> {
  fn to_bytes(&self) -> Vec<u8> {
    self.to_vec()
  }
}

impl ToBytes for &[u8] {
  fn to_bytes(&self) -> Vec<u8> {
    self.to_vec()
  }
}

impl<const N: usize> ToBytes for &[u8; N] {
  fn to_bytes(&self) -> Vec<u8> {
    self.to_vec()
  }
}

impl ToBytes for i32 {
  fn to_bytes(&self) -> Vec<u8> {
    self.to_ne_bytes().to_vec()
  }
}

impl ToBytes for u32 {
  fn to_bytes(&self) -> Vec<u8> {
    self.to_ne_bytes().to_vec()
  }
}

impl ToBytes for bool {
  fn to_bytes(&self) -> Vec<u8> {
    // Represent boolean as i32 (0 or 1) and then convert to bytes,
    // consistent with how integer options are typically handled.
    let int_val = if *self { 1i32 } else { 0i32 };
    int_val.to_ne_bytes().to_vec()
  }
}

impl ToBytes for String {
  fn to_bytes(&self) -> Vec<u8> {
    self.as_bytes().to_vec()
  }
}

impl ToBytes for &str {
  fn to_bytes(&self) -> Vec<u8> {
    self.as_bytes().to_vec()
  }
}
