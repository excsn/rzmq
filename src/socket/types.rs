// src/socket/types.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::Command;
use crate::socket::events::{MonitorReceiver, MonitorSender, SocketEvent, DEFAULT_MONITOR_CAPACITY};
use crate::socket::ISocket;
use std::fmt;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Represents the type of a ZeroMQ socket, defining its messaging pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SocketType {
  /// Publish messages to subscribers (Pub-Sub pattern).
  Pub,
  /// Subscribe to messages from publishers (Pub-Sub pattern).
  Sub,
  /// Send requests and receive replies (Req-Rep pattern).
  Req,
  /// Receive requests and send replies (Req-Rep pattern).
  Rep,
  /// Asynchronous request-reply, load-balancing outgoing (Dealer-Router pattern).
  Dealer,
  /// Asynchronous request-reply, routing incoming (Dealer-Router pattern).
  Router,
  /// Distribute messages to a pool of workers (Push-Pull pattern).
  Push,
  /// Collect messages from a pool of distributors (Push-Pull pattern).
  Pull,
}

/// The public handle for interacting with an rzmq socket.
/// Handles are cloneable and delegate operations to the underlying socket actor.
#[derive(Clone)] // Clone is cheap (Arc)
pub struct Socket {
  // Keep inner private to encapsulate implementation details
  pub(crate) inner: Arc<dyn ISocket>,
}

impl Socket {
  /// Creates a new public Socket handle wrapping the internal implementation.
  /// This is typically called by `Context::socket`.
  pub(crate) fn new(socket_impl: Arc<dyn ISocket>) -> Self {
    Self { inner: socket_impl }
  }

  // --- Public API Methods (Async) ---
  // These methods simply delegate to the ISocket trait implementation,
  // which in turn will likely send a command to the SocketCore actor.

  /// Binds the socket to listen on a local endpoint.
  pub async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
    self.inner.bind(endpoint).await
  }

  /// Connects the socket to a remote endpoint.
  pub async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
    self.inner.connect(endpoint).await
  }

  /// Disconnects from a specific endpoint.
  pub async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
    self.inner.disconnect(endpoint).await
  }

  /// Stops listening on a specific endpoint.
  pub async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError> {
    self.inner.unbind(endpoint).await
  }

  /// Sends a message asynchronously.
  pub async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    self.inner.send(msg).await
  }

  /// Receives a message asynchronously.
  pub async fn recv(&self) -> Result<Msg, ZmqError> {
    self.inner.recv().await
  }

  /// Sets a socket option asynchronously.
  pub async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    self.inner.set_option(option, value).await
  }

  /// Gets a socket option value asynchronously.
  pub async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    self.inner.get_option(option).await
  }

  /// Initiates graceful shutdown of the socket asynchronously.
  pub async fn close(&self) -> Result<(), ZmqError> {
    self.inner.close().await
  }

  /// Creates a monitoring channel for this socket.
  ///
  /// Events detailing the socket's internal state changes (connections,
  /// disconnections, errors, etc.) will be sent to the returned receiver.
  ///
  /// `capacity` defines the buffer size of the monitoring channel. If the
  /// receiver does not consume events quickly enough and the channel fills up,
  /// subsequent events might be dropped depending on the channel behavior.
  pub async fn monitor(&self, capacity: usize) -> Result<MonitorReceiver, ZmqError> {
    let (monitor_tx, monitor_rx) = async_channel::bounded(capacity.max(1));
    let (reply_tx, reply_rx) = oneshot::channel();

    let cmd = Command::UserMonitor { monitor_tx, reply_tx };

    // Send command to the socket's core actor
    self
      .inner
      .mailbox() // Use ISocket::mailbox() to get sender
      .send(cmd)
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error during monitor setup".into()))?;

    // Wait for the core actor to acknowledge setup
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error during monitor setup".into()))??; // Double ?? to propagate inner Result

    Ok(monitor_rx)
  }

  /// Creates a monitoring channel with a default capacity.
  pub async fn monitor_default(&self) -> Result<MonitorReceiver, ZmqError> {
    self.monitor(DEFAULT_MONITOR_CAPACITY).await
  }
}

impl fmt::Debug for Socket {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // Basic debug, avoid trying to debug the dyn ISocket directly
    f.debug_struct("Socket")
      // Optionally add socket type or handle ID if accessible?
      .finish_non_exhaustive()
  }
}
