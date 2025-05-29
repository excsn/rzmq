// src/socket/push_socket.rs

use crate::error::ZmqError;
use crate::message::Msg; // For send method.
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, CoreState, SocketCore}; // Core components and send helper.
use crate::socket::options::SocketOptions; // Not directly used in `new` here, but core has it.
use crate::socket::patterns::LoadBalancer; // PUSH uses a LoadBalancer to distribute messages.
use crate::socket::ISocket; // The trait this struct implements.

use std::collections::HashMap; // For pipe_read_to_write_id map.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{RwLock, RwLockReadGuard};
use tokio::sync::{oneshot, Mutex, MutexGuard}; // oneshot for API replies.
use tokio::time::timeout as tokio_timeout; // For send timeout on peer wait.

// Import the delegate_to_core macro.
use crate::{delegate_to_core, Blob};

/// Implements the PUSH socket pattern.
/// PUSH sockets are used to distribute messages to a pool of PULL workers.
/// They send messages in a round-robin fashion to available connected PULL peers.
/// PUSH sockets do not receive messages.
#[derive(Debug)]
pub(crate) struct PushSocket {
  /// Arc to the shared `SocketCore` actor that manages common socket state and transport.
  core: Arc<SocketCore>,
  /// `LoadBalancer` to distribute outgoing messages to available PULL peer pipes.
  load_balancer: LoadBalancer,
  /// Maps a pipe's read ID (from SocketCore's perspective, PUSH doesn't read)
  /// to its corresponding write ID (which PUSH uses to send).
  /// This is needed during `pipe_detached` to correctly remove the pipe from the `load_balancer`.
  pipe_read_to_write_id: RwLock<HashMap<usize, usize>>,
}

impl PushSocket {
  /// Creates a new `PushSocket`.
  ///
  /// # Arguments
  /// * `core` - An `Arc` to the `SocketCore` managing this socket.
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      pipe_read_to_write_id: RwLock::new(HashMap::new()),
    }
  }

  /// Helper to get a locked guard for the `CoreState` within `SocketCore`.
  fn core_state(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }
}

#[async_trait]
impl ISocket for PushSocket {
  /// Returns a reference to the `SocketCore`.
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }

  /// Returns a clone of the `SocketCore`'s command mailbox sender.
  fn mailbox(&self) -> MailboxSender {
    self.core.command_sender()
  }

  // --- API Method Implementations (mostly delegated to SocketCore) ---
  async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserBind, endpoint: endpoint.to_string())
  }
  async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserConnect, endpoint: endpoint.to_string())
  }
  async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserDisconnect, endpoint: endpoint.to_string())
  }
  async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserUnbind, endpoint: endpoint.to_string())
  }

  /// Sends a message using the PUSH pattern.
  /// The message is sent to one of the connected PULL peers in a round-robin fashion.
  /// Behavior on full HWM or no available peers depends on the `SNDTIMEO` option.
  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    // Check if the socket is still considered running by its core.
    if !self.core.is_running().await {
      tracing::warn!(
        handle = self.core.handle,
        "PUSH send failed: Socket is not in Running state (likely closing or terminated)."
      );
      // Mimic libzmq: if socket is closing/closed, further sends should fail.
      // SNDTIMEO=0 typically results in EAGAIN (ResourceLimitReached).
      // If SNDTIMEO > 0 or -1, it might result in ETERM or other errors.
      return Err(ZmqError::ResourceLimitReached); // Or a more specific "socket closing" error.
    }

    // Get SNDTIMEO from options before potentially blocking on peer selection.
    let timeout_opt: Option<Duration> = { self.core_state().options.sndtimeo };

    // Select a peer pipe using the load balancer, waiting if necessary based on SNDTIMEO.
    let pipe_write_id = loop {
      if let Some(id) = self.load_balancer.get_next_pipe().await {
        break id; // Found an available peer pipe.
      }

      // No peer currently available. Check if the socket itself is still operational.
      if self.core.command_sender().is_closed() {
        tracing::warn!(
          handle = self.core.handle,
          "PUSH send failed: Core command mailbox closed (socket likely terminated)."
        );
        return Err(ZmqError::InvalidState("Socket terminated".into()));
      }

      // Behavior depends on timeout settings.
      match timeout_opt {
        Some(duration) if duration.is_zero() => {
          // Non-blocking send: if no peer, return ResourceLimitReached (EAGAIN).
          tracing::trace!(
            handle = self.core.handle,
            "PUSH send failed (non-blocking): No connected peers"
          );
          return Err(ZmqError::ResourceLimitReached);
        }
        None => {
          // Blocking send (infinite timeout): wait for a pipe to become available.
          tracing::trace!(
            handle = self.core.handle,
            "PUSH send blocking: Waiting for available peer..."
          );
          self.load_balancer.wait_for_pipe().await; // Wait for notification.
          continue; // Loop back to try getting a pipe again.
        }
        Some(duration) => {
          // Timed blocking send: wait for a pipe with a timeout.
          tracing::trace!(
            handle = self.core.handle,
            ?duration,
            "PUSH send timed wait: Waiting for available peer..."
          );
          match tokio_timeout(duration, self.load_balancer.wait_for_pipe()).await {
            Ok(()) => continue, // Wait succeeded, pipe might be available now.
            Err(_timeout_elapsed) => {
              tracing::debug!(
                handle = self.core.handle,
                ?duration,
                "PUSH send timed out waiting for peer"
              );
              return Err(ZmqError::Timeout); // Timeout elapsed.
            }
          }
        }
      }
    };

    // Get the sender channel for the selected pipe from CoreState.
    let pipe_tx = {
      let core_state_guard = self.core_state();
      core_state_guard.get_pipe_sender(pipe_write_id).ok_or_else(|| {
        // This indicates a consistency issue.
        tracing::error!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "PUSH send failed: Pipe sender disappeared after selection."
        );
        ZmqError::Internal("Pipe sender consistency error".into())
      })?
    }; // CoreState lock released here.

    // Send the message using the helper, which respects SNDTIMEO for HWM.
    match send_msg_with_timeout(&pipe_tx, msg, timeout_opt, self.core.handle, pipe_write_id).await {
      Ok(()) => Ok(()), // Message sent successfully.
      Err(ZmqError::ConnectionClosed) => {
        // Pipe was closed during send (e.g., peer disconnected).
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "PUSH send failed: Pipe channel closed"
        );
        self.load_balancer.remove_pipe(pipe_write_id).await; // Remove from load balancer.
                                                             // PUSH sockets typically don't return ConnectionClosed on send unless SNDTIMEO behavior dictates it.
                                                             // The helper `send_msg_with_timeout` might return this.
                                                             // For PUSH, it might be more idiomatic to retry or drop, but for now, propagate.
        Err(ZmqError::ConnectionClosed)
      }
      Err(ZmqError::ResourceLimitReached) => {
        // HWM reached and SNDTIMEO=0, or timed out waiting for HWM with SNDTIMEO>0.
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "PUSH send failed due to HWM (EAGAIN/Timeout)"
        );
        Err(ZmqError::ResourceLimitReached)
      }
      Err(ZmqError::Timeout) => {
        // Timed out waiting for HWM with SNDTIMEO > 0.
        tracing::debug!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "PUSH send timed out on HWM"
        );
        Err(ZmqError::Timeout)
      }
      Err(e) => Err(e), // Propagate other unexpected errors.
    }
  }

  /// PUSH sockets cannot receive messages. This method will always return an error.
  async fn recv(&self) -> Result<Msg, ZmqError> {
    Err(ZmqError::InvalidState("PUSH sockets cannot receive messages"))
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    unimplemented!("Not Implemented yet")
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    unimplemented!("Not implemented yet")
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    // Delegate to SocketCore for common options like SNDHWM, SNDTIMEO.
    delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
  }
  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // Delegate to SocketCore for common options.
    delegate_to_core!(self, UserGetOpt, option: option)
  }
  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  // --- Pattern-Specific Option Handling ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PUSH",
      option = option,
      "set_pattern_option called"
    );
    // PUSH sockets do not have specific pattern options like SUBSCRIBE.
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PUSH",
      option = option,
      "get_pattern_option called"
    );
    // PUSH sockets do not have readable pattern-specific options.
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks called by SocketCore ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    // PUSH sockets do not have special commands to process beyond user API calls.
    Ok(false) // Indicate command was not handled here.
  }

  /// PUSH sockets do not receive messages from peers, so they ignore `PipeMessageReceived` events.
  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    tracing::warn!(
      handle = self.core.handle,
      pipe_id = pipe_id,
      "PUSH socket received unexpected pipe event: {:?}",
      event.variant_name() // Log variant name.
    );
    Ok(())
  }

  /// Called by `SocketCore` when a new pipe (connection to a PULL peer) is attached.
  async fn pipe_attached(
    &self,
    pipe_read_id: usize,           // ID SocketCore uses to read from this peer (not used by PUSH).
    pipe_write_id: usize,          // ID SocketCore uses to write to this peer (PUSH uses this).
    _peer_identity: Option<&[u8]>, // Peer identity is not typically used by PUSH.
  ) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "PUSH attaching pipe"
    );
    // Store the mapping from read ID to write ID for cleanup during detachment.
    self
      .pipe_read_to_write_id
      .write()
      .insert(pipe_read_id, pipe_write_id);
    // Add the pipe's write ID to the load balancer so messages can be sent to it.
    self.load_balancer.add_pipe(pipe_write_id).await;
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "THEIR_SOCKET_TYPE", // e.g., "DEALER"
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but this socket type does not use peer identities. Ignoring."
    );
  }

  /// Called by `SocketCore` when a pipe is detached (PULL peer disconnected or socket closing).
  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PUSH detaching pipe"
    );
    // Remove the read ID -> write ID mapping.
    let maybe_write_id = self.pipe_read_to_write_id.write().remove(&pipe_read_id);
    if let Some(write_id) = maybe_write_id {
      // If a corresponding write ID was found, remove it from the load balancer.
      self.load_balancer.remove_pipe(write_id).await;
      tracing::trace!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        pipe_write_id = write_id,
        "PUSH removed detached pipe from load balancer"
      );
    } else {
      // This might occur if pipe_detached is called multiple times or if the pipe wasn't fully attached.
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        "PUSH detach: Write ID not found for read ID, load balancer may not be updated."
      );
    }
  }
}
