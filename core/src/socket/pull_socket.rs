// src/socket/pull_socket.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::options::SocketOptions;
use crate::socket::patterns::FairQueue; // PULL uses a FairQueue to collect messages.
use crate::socket::ISocket;

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, MutexGuard}; // oneshot for API replies.
use tokio::time::timeout; // For recv timeout.

// Import the delegate_to_core macro.
use crate::{delegate_to_core, Blob};

/// Implements the PULL socket pattern.
/// PULL sockets are used to collect messages distributed by PUSH sockets.
/// They receive messages in a fair-queued manner from all connected PUSH peers.
/// PULL sockets do not send messages.
#[derive(Debug)]
pub(crate) struct PullSocket {
  /// Arc to the shared `SocketCore` actor that manages common socket state and transport.
  core: Arc<SocketCore>,
  /// `FairQueue` to buffer incoming messages from all connected PUSH peers.
  /// `recv()` calls will pop messages from this queue.
  fair_queue: FairQueue,
}

impl PullSocket {
  /// Creates a new `PullSocket`.
  ///
  /// # Arguments
  /// * `core` - An `Arc` to the `SocketCore` managing this socket.
  /// * `options` - Initial socket options, used here to determine queue capacity (RCVHWM).
  pub fn new(core: Arc<SocketCore>, options: SocketOptions) -> Self {
    // Capacity for the incoming message queue, based on RCVHWM.
    let queue_capacity = options.rcvhwm.max(1); // Ensure capacity is at least 1.
    Self {
      core,
      fair_queue: FairQueue::new(queue_capacity),
    }
  }

  /// Helper to get a locked guard for the `CoreState` within `SocketCore`.
  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

#[async_trait]
impl ISocket for PullSocket {
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

  /// PULL sockets cannot send messages. This method will always return an error.
  async fn send(&self, _msg: Msg) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("PULL sockets cannot send messages"))
  }

  /// Receives a message using the PULL pattern.
  /// Messages are taken from an internal fair queue populated by all connected PUSH peers.
  /// This call will block (asynchronously) if no messages are currently available,
  /// unless a `RCVTIMEO` is set.
  async fn recv(&self) -> Result<Msg, ZmqError> {
    // Check if the socket (and its core actor) is still considered running.
    // This check uses the command_sender's status as a proxy for the core actor's liveness.
    // If the mailbox is closed, the core actor is likely terminated.
    if self.core.command_sender().is_closed() {
      tracing::warn!(
        handle = self.core.handle,
        "PULL recv failed: Core command mailbox detected as closed (socket terminated)."
      );
      return Err(ZmqError::InvalidState("Socket terminated".into()));
    }

    // Get RCVTIMEO from options.
    let rcvtimeo_opt: Option<Duration> = { self.core_state().await.options.rcvtimeo };

    // Attempt to pop a message from the fair queue.
    let pop_future = self.fair_queue.pop_message();

    let result = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => {
        // Apply timeout if RCVTIMEO is set to a positive value.
        tracing::trace!(handle = self.core.handle, ?duration, "Applying RCVTIMEO to PULL recv");
        match timeout(duration, pop_future).await {
          Ok(Ok(Some(msg))) => Ok(msg), // Message received within timeout.
          Ok(Ok(None)) => {
            // FairQueue::pop_message returning None usually means the internal channel was closed.
            tracing::error!(
              handle = self.core.handle,
              "PULL recv failed: FairQueue's internal channel closed."
            );
            Err(ZmqError::Internal("Receive queue closed unexpectedly".into()))
          }
          Ok(Err(e)) => Err(e), // Propagate internal errors from pop_message.
          Err(_timeout_elapsed) => {
            tracing::trace!(handle = self.core.handle, "PULL recv timed out due to RCVTIMEO.");
            Err(ZmqError::Timeout) // Timeout occurred.
          }
        }
      }
      _ => {
        // No timeout (RCVTIMEO is None, meaning infinite wait) or RCVTIMEO = 0.
        // Note: RCVTIMEO=0 usually implies non-blocking, but FairQueue::pop_message is inherently blocking
        // if the queue is empty. To achieve true non-blocking, `try_pop_message` would be used,
        // but the ZMQ API for recv with RCVTIMEO=0 is typically mapped to ResourceLimitReached if no message.
        // Here, we assume RCVTIMEO=0 still means "wait indefinitely if no message" for simplicity with pop_message.
        // A more precise RCVTIMEO=0 handling might involve `try_pop_message` and returning `ResourceLimitReached`.
        // For now, it blocks until a message or queue closure.
        match pop_future.await? {
          // Use ? to propagate errors from pop_message (e.g., queue closed).
          Some(msg) => Ok(msg),
          None => {
            tracing::error!(
              handle = self.core.handle,
              "PULL recv failed: FairQueue's internal channel closed (infinite wait)."
            );
            Err(ZmqError::Internal("Receive queue closed unexpectedly".into()))
          }
        }
      }
    };
    result
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    // Most options are handled by SocketCore. RCVHWM might affect FairQueue capacity,
    // but changing HWM on an active queue is complex and often not supported directly
    // without recreating the queue. For now, delegate all to core.
    delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
  }
  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // Delegate all option gets to SocketCore.
    // If RCVHWM needs to reflect FairQueue.capacity(), SocketCore's handler would need to query it.
    delegate_to_core!(self, UserGetOpt, option: option)
  }
  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  // --- Pattern-Specific Option Handling ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PULL",
      option = option,
      "set_pattern_option called"
    );
    // PULL sockets do not have specific pattern options like SUBSCRIBE.
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PULL",
      option = option,
      "get_pattern_option called"
    );
    // PULL sockets do not have readable pattern-specific options.
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks called by SocketCore ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    // PULL sockets do not have special commands to process beyond user API calls.
    Ok(false) // Indicate command was not handled here.
  }

  /// Handles messages received from a pipe by the `SocketCore`.
  /// For PULL sockets, any `PipeMessageReceived` contains a message from a PUSH peer.
  /// This message is pushed into the internal `fair_queue`.
  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_id,
          msg_size = msg.size(),
          "PULL handle_pipe_event: Received message from pipe, pushing to FairQueue."
        );
        // Push the received message into the internal fair queue.
        // `recv()` will then pick it up from this queue.
        self.fair_queue.push_message(msg).await?;
      }
      // PULL sockets typically ignore other pipe events like PipeClosedByPeer at this ISocket level,
      // as the pipe detachment is handled by SocketCore calling pipe_detached.
      _ => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_id,
          "PULL received unhandled pipe event: {:?}",
          event.variant_name() // Log variant name for brevity.
        );
      }
    }
    Ok(())
  }

  /// Called by `SocketCore` when a new pipe (connection to a PUSH peer) is attached.
  async fn pipe_attached(
    &self,
    pipe_read_id: usize,           // PULL uses the read ID to associate with the FairQueue.
    _pipe_write_id: usize,         // PULL does not send, so write ID is not directly used here.
    _peer_identity: Option<&[u8]>, // Peer identity is not typically used by PULL.
  ) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PULL attaching pipe"
    );
    // Notify the FairQueue that a new pipe is contributing messages.
    // This might be used for internal tracking or debugging within FairQueue.
    self.fair_queue.pipe_attached(pipe_read_id);
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

  /// Called by `SocketCore` when a pipe is detached (PUSH peer disconnected or socket closing).
  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PULL detaching pipe"
    );
    // Notify the FairQueue that a pipe is no longer contributing.
    // FairQueue might use this for cleanup or internal state adjustment if necessary.
    self.fair_queue.pipe_detached(pipe_read_id);
  }
}
