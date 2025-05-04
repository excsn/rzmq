// src/socket/mod.rs

pub mod core; // Will contain SocketCore
pub mod options; // Will contain ZMQ option constants etc.
pub mod patterns;
pub mod types; // Will contain SocketType, public Socket handle // Will contain pattern helpers
               // Declare modules for each socket type impl
pub mod dealer_socket;
pub mod pub_socket;
pub mod pull_socket;
pub mod push_socket;
pub mod rep_socket;
pub mod req_socket;
pub mod router_socket;
pub mod sub_socket;

pub use crate::{Socket, SocketType};
// Re-export option constants when defined
// pub use options::*;

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender}; // Adjust imports as needed
use async_trait::async_trait;
use options::SocketOptions;
use std::sync::Arc; // Likely needed for Arc<SocketCore>

/// Defines the internal behavior and pattern logic for a specific socket type.
/// Implementations will typically embed `Arc<SocketCore>`.
#[async_trait]
pub trait ISocket: Send + Sync + 'static {
  // Ensure Send + Sync + 'static for Arc<dyn ...>

  /// Returns a reference to the underlying SocketCore actor's mailbox sender.
  /// Needed for the public `Socket` handle to send commands.
  fn mailbox(&self) -> &MailboxSender;

  // --- Methods mirroring public API (called by SocketCore after receiving Command) ---

  /// Handles the underlying logic for binding the socket.
  async fn bind(&self, endpoint: &str) -> Result<(), ZmqError>;

  /// Handles the underlying logic for connecting the socket.
  async fn connect(&self, endpoint: &str) -> Result<(), ZmqError>;

  /// Handles the underlying logic for disconnecting.
  async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError>;

  /// Handles the underlying logic for unbinding.
  async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError>;

  /// Handles sending a message according to the socket pattern.
  /// Manages routing, sequencing (REQ/REP), HWM.
  async fn send(&self, msg: Msg) -> Result<(), ZmqError>;

  /// Provides a message to the user, respecting socket pattern.
  /// Typically involves reading from internal pipes/queues.
  async fn recv(&self) -> Result<Msg, ZmqError>;

  /// Applies a socket option relevant to this socket type/pattern.
  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>;

  /// Retrieves a socket option value relevant to this socket type/pattern.
  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>;

  /// Initiates the shutdown sequence specific to this socket type.
  /// (Often delegates mostly to SocketCore::stop).
  async fn close(&self) -> Result<(), ZmqError>;

  // --- Internal Methods called by SocketCore ---

  /// Applies a socket option relevant ONLY to this socket type/pattern.
  /// Called by SocketCore after it handles core options.
  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>;

  /// Retrieves a socket option value relevant ONLY to this socket type/pattern.
  /// Called by SocketCore if it doesn't handle the option itself.
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>;

  /// Processes a command received by the SocketCore that requires pattern-specific logic.
  /// This might handle specific User* commands or internal coordination commands.
  /// Return `Ok(true)` if command was handled, `Ok(false)` if not applicable/should be handled by core.
  async fn process_command(&self, command: Command) -> Result<bool, ZmqError>;

  /// Handles events originating from pipes connected to this socket.
  /// Now includes messages received via PipeReaderTask.
  async fn handle_pipe_event(
    &self,
    pipe_id: usize,
    event_command: Command, // Pass the actual Command variant
  ) -> Result<(), ZmqError>;

  /// Called when a new connection (Pipe) is successfully established and attached.
  /// Allows the pattern logic to register the new pipe (e.g., add to distributor/load balancer).
  async fn pipe_attached(
    &self,
    pipe_read_id: usize,  // ID Core uses to read from session
    pipe_write_id: usize, // ID Core uses to write to session
    peer_identity: Option<&[u8]>,
  );

  /// Called just before a pipe is detached (e.g., due to disconnection or shutdown).
  /// Allows the pattern logic to deregister the pipe.
  async fn pipe_detached(&self, pipe_id: usize);
}

// --- Internal Helper ---
use crate::context::Context;
use core::SocketCore;
use crate::socket::{
  pull_socket::PullSocket, // Use real type
  // pub_socket::PubSocket, sub_socket::SubSocket,
  // req_socket::ReqSocket, rep_socket::RepSocket,
  // dealer_socket::DealerSocket, router_socket::RouterSocket,
  push_socket::PushSocket, // Use real type
};

/// Placeholder internal function to create and spawn the socket actor.
/// Will be replaced with actual logic calling SocketCore::create_and_spawn.
/// Marked pub(crate) so context.rs can call it.
pub(crate) fn create_socket_actor(
  handle: usize,
  ctx: Context,
  socket_type: SocketType,
) -> Result<(Arc<dyn ISocket>, MailboxSender), ZmqError> {
  let initial_options = SocketOptions::default();// TODO: Get from context?
  // Call the real factory, it now uses the concrete types directly
  SocketCore::create_and_spawn(handle, ctx, socket_type, initial_options)
}
