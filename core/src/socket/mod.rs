// src/socket/mod.rs

// Declare sub-modules within the `socket` module.
// Each sub-module handles a specific aspect of socket functionality.

/// Contains the `SocketCore` actor implementation and related internal types.
pub(crate) mod core;
/// Defines `SocketEvent` for monitoring and related channel types.
pub mod events;
/// Defines socket option constants (e.g., SNDHWM) and parsing helpers.
pub mod options;
/// Contains helper patterns used by `ISocket` implementations (e.g., Distributor, FairQueue).
pub mod patterns;
/// Defines the public `Socket` handle and `SocketType` enum.
pub mod types;

pub(crate) mod connection_iface;

// Declare modules for each specific socket type implementation (ISocket trait implementors).
pub mod dealer_socket;
pub mod pub_socket;
pub mod pull_socket;
pub mod push_socket;
pub mod rep_socket;
pub mod req_socket;
pub mod router_socket;
pub mod sub_socket;

// Import necessary types from other modules within the crate.
use crate::context::Context; // For `create_socket_actor`.
use crate::error::{ZmqError, ZmqResult}; // For `Result` types in API methods.
use crate::message::Msg; // For `send`/`recv` message types.
use crate::runtime::{Command, MailboxSender}; // For actor communication.
use crate::socket::options::SocketOptions; // For initial socket configuration.

// Import specific socket pattern implementations.
use crate::socket::core::SocketCore; // The core actor logic.
use crate::Blob;

use async_trait::async_trait; // For defining asynchronous traits.
use std::sync::Arc; // For shared ownership of `SocketCore`.

/// Helper macro to implement API methods that send a command to `SocketCore`'s mailbox
/// and await a `oneshot` reply. This reduces boilerplate in `ISocket` implementations.
///
/// It requires the struct implementing `ISocket` to have a `mailbox()` method
/// that returns the `MailboxSender` for its associated `SocketCore`.
#[macro_export]
macro_rules! delegate_to_core {
  // Case for commands that have fields (other than the reply_tx).
  ($self:ident, $variant:ident, $($field:ident : $value:expr),+ $(,)?) => {
    {
      use fibre::oneshot;
      // Create a oneshot channel for the reply.
      let (reply_tx, mut reply_rx) = oneshot::channel();
      // Construct the command variant with its fields and the reply sender.
      // Ensure Command is accessible, e.g., via $crate::runtime::Command
      let cmd = $crate::runtime::Command::$variant { $($field : $value),+, reply_tx };
      // Send the command to the SocketCore's command mailbox.
      // ISocket::mailbox() provides the sender.
      $self.mailbox()
          .send(cmd)
          .await.map_err(|_send_error| $crate::error::ZmqError::Internal("Mailbox send error".into()))?;
      // Await the reply from SocketCore.
      // The `??` propagates both the channel error and the inner Result error.
      reply_rx.recv().await.map_err(|_recv_error| $crate::error::ZmqError::Internal("Reply channel error".into()))?
    }
  };
  // Case for commands that have NO fields (other than the reply_tx).
  ($self:ident, $variant:ident $(,)?) => {
      {
        use fibre::oneshot;
          let (reply_tx, mut reply_rx) = oneshot::channel();
          let cmd = $crate::runtime::Command::$variant { reply_tx };
          $self.mailbox()
              .send(cmd)
              .await.map_err(|_send_error| $crate::error::ZmqError::Internal("Mailbox send error".into()))?;
          reply_rx.recv().await.map_err(|_recv_error| $crate::error::ZmqError::Internal("Reply channel error".into()))?
      }
  };
}

/// Defines the internal behavior and pattern-specific logic for a ZeroMQ socket type.
/// Implementations of this trait (e.g., `PubSocket`, `ReqSocket`) encapsulate the
/// unique characteristics of each ZMQ pattern. They typically embed an `Arc<SocketCore>`
/// to interact with the underlying actor managing shared state and transport.
#[async_trait]
pub trait ISocket: Send + Sync + 'static {
  /// Returns a reference to the underlying `SocketCore` instance.
  /// This provides access to shared socket state and options if needed by the pattern logic.
  fn core(&self) -> &Arc<SocketCore>;

  /// Returns a clone of the command `MailboxSender` for the `SocketCore` actor
  /// associated with this socket. This sender is used by the public `Socket` handle
  /// (and the `delegate_to_core!` macro) to send user-initiated commands to the `SocketCore`.
  fn mailbox(&self) -> MailboxSender;

  // --- Methods mirroring the public API of `socket::types::Socket` ---
  // These methods are typically implemented using the `delegate_to_core!` macro.

  /// Handles the underlying logic for binding the socket to a local endpoint.
  async fn bind(&self, endpoint: &str) -> Result<(), ZmqError>;

  /// Handles the underlying logic for connecting the socket to a remote endpoint.
  async fn connect(&self, endpoint: &str) -> Result<(), ZmqError>;

  /// Handles the underlying logic for disconnecting from a specific endpoint.
  async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError>;

  /// Handles the underlying logic for unbinding from a specific endpoint.
  async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError>;

  /// Handles sending a message according to the socket's specific messaging pattern
  /// (e.g., distributing for PUSH, sending request for REQ).
  /// This method manages aspects like routing, sequencing, and high-water mark behavior.
  async fn send(&self, msg: Msg) -> Result<(), ZmqError>;

  /// Provides a message to the user, respecting the socket's messaging pattern.
  /// This typically involves reading from internal pipes or queues populated by `SocketCore`.
  async fn recv(&self) -> Result<Msg, ZmqError>;

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
  async fn send_multipart(&self, frames: Vec<Msg>) -> Result<(), ZmqError>;

  /// Receives all frames of a complete logical ZMQ message.
  /// Handles reading subsequent frames if the first frame has the MORE flag.
  /// Respects RCVTIMEO for the overall operation of receiving the full message.
  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError>;

  /// Applies a socket option. Some options might be handled by `SocketCore` directly,
  /// while others might require pattern-specific logic via `set_pattern_option`.
  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>;

  /// Retrieves a socket option's value. Similar to `set_option`, this might involve
  /// `SocketCore` or pattern-specific logic via `get_pattern_option`.
  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>;

  /// Initiates the shutdown sequence for this socket. This typically involves sending
  /// a `UserClose` command to the `SocketCore`.
  async fn close(&self) -> Result<(), ZmqError>;

  // --- Internal Methods called by SocketCore or its components ---

  /// Applies a socket option that is relevant *only* to this specific socket type/pattern.
  /// This is called by `SocketCore` after it has handled core-level options.
  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> ZmqResult<()>;

  /// Retrieves a socket option value that is relevant *only* to this specific socket type/pattern.
  /// Called by `SocketCore` if it doesn't handle the option itself.
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>;

  /// Processes a command received by the `SocketCore` that requires pattern-specific logic.
  /// This is rarely used now, as most specific logic is within `send`/`recv` or pipe events.
  ///
  /// # Returns
  /// * `Ok(true)` if the command was handled by the pattern logic.
  /// * `Ok(false)` if the command was not applicable and should be handled by `SocketCore` or ignored.
  /// * `Err(ZmqError)` if an error occurred during processing.
  async fn process_command(&self, command: Command) -> Result<bool, ZmqError>;

  /// Handles events originating from the data pipes connected to this socket.
  /// These events are sent by `PipeReaderTask`s to the `SocketCore`'s command mailbox.
  /// `SocketCore` then calls this method on the `ISocket` implementation.
  ///
  /// # Arguments
  /// * `pipe_id` - The ID of the pipe (from `SocketCore`'s perspective, usually its read ID) where the event originated.
  /// * `event_command` - The actual `Command` variant representing the pipe event (e.g., `PipeMessageReceived`, `PipeClosedByPeer`).
  async fn handle_pipe_event(&self, pipe_id: usize, event_command: Command) -> Result<(), ZmqError>;

  /// Called by `SocketCore` when a new connection (represented by a pair of data pipes)
  /// is successfully established and attached to this socket.
  /// This allows the pattern-specific logic (e.g., a `LoadBalancer` or `RouterMap`)
  /// to register the new pipe and make it available for sending/receiving.
  ///
  /// # Arguments
  /// * `pipe_read_id` - The ID `SocketCore` uses to read messages from this peer (Session writes to this).
  /// * `pipe_write_id` - The ID `SocketCore` uses to write messages to this peer (Session reads from this).
  /// * `peer_identity` - Optional identity of the peer, established during the ZMTP handshake (e.g., for ROUTER).
  async fn pipe_attached(&self, pipe_read_id: usize, pipe_write_id: usize, peer_identity: Option<&[u8]>);

  /// Called by `SocketCore` when the true ZMTP identity of a peer connected via
  /// the given `pipe_read_id` has been established (e.g., from `PeerIdentityEstablished` event).
  /// `RouterSocket` uses this to update its identity map.
  /// Other socket types might log a warning or do nothing.
  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>);

  /// Called by `SocketCore` just before a pipe is detached (e.g., due to disconnection,
  /// socket close, or context termination).
  /// This allows the pattern-specific logic to deregister the pipe and clean up any
  /// associated state (e.g., remove from a `LoadBalancer`).
  ///
  /// # Arguments
  /// * `pipe_read_id` - The ID of the pipe (from `SocketCore`'s perspective, its read ID) being detached.
  async fn pipe_detached(&self, pipe_read_id: usize);
}

// Re-export types from sub-modules for easier access at `rzmq::socket::*`.
pub use events::{MonitorReceiver, MonitorSender, SocketEvent, DEFAULT_MONITOR_CAPACITY};
pub use options::*; // Re-export all socket option constants (e.g., SNDHWM).
pub use types::{Socket, SocketType}; // Re-export the public Socket handle and SocketType enum.

/// Internal factory function to create and spawn the `SocketCore` actor and its
/// associated `ISocket` pattern implementation.
/// This is called by `Context::socket()`.
///
/// # Returns
/// A `Result` containing a tuple of (`Arc<dyn ISocket>`, `MailboxSender`) on success.
/// The `MailboxSender` is for the created `SocketCore`'s single command mailbox.
pub(crate) fn create_socket_actor(
  handle: usize,           // The unique handle ID for this new socket.
  ctx: Context,            // The parent context.
  socket_type: SocketType, // The desired ZMQ socket pattern.
) -> Result<(Arc<dyn ISocket>, MailboxSender), ZmqError> {
  // Create default options. These can be overridden later via `Socket::set_option`.
  let initial_options = SocketOptions::default();
  // Delegate to SocketCore's factory method.
  SocketCore::create_and_spawn(handle, ctx, socket_type, initial_options)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SourcePipeReadId(usize);
