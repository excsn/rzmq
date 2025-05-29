use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, SocketCore};
use crate::socket::patterns::Distributor;
use crate::socket::ISocket;
use crate::{delegate_to_core, Blob, MsgFlags};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

/// Implements the PUB (Publish) socket pattern.
/// PUB sockets distribute messages to all connected SUB (Subscribe) peers.
/// They perform a "fan-out" of messages. PUB sockets do not receive messages.
/// Message filtering (based on topics) happens on the SUB side.
#[derive(Debug)]
pub(crate) struct PubSocket {
  /// Arc to the shared `SocketCore` actor that manages common socket state and transport.
  core: Arc<SocketCore>,
  /// `Distributor` to send copies of each message to all connected peer pipes.
  distributor: Distributor,
  /// Maps a pipe's read ID (from SocketCore's perspective) to its corresponding write ID.
  /// This is needed during `pipe_detached` to correctly remove the pipe from the `distributor`.
  pipe_read_to_write_id: RwLock<HashMap<usize, usize>>,
}

impl PubSocket {
  /// Creates a new `PubSocket`.
  ///
  /// # Arguments
  /// * `core` - An `Arc` to the `SocketCore` managing this socket.
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      distributor: Distributor::new(),
      pipe_read_to_write_id: RwLock::new(HashMap::new()),
    }
  }
}

#[async_trait]
impl ISocket for PubSocket {
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
  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
  }
  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    delegate_to_core!(self, UserGetOpt, option: option)
  }
  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  // --- Pattern-Specific Logic for send() and recv() ---

  /// Sends a message using the PUB pattern.
  /// The message is distributed to all currently connected and subscribed peers.
  /// If a peer's send queue (pipe) is full (HWM reached) and SNDTIMEO is 0,
  /// the message is silently dropped for that peer. If SNDTIMEO is > 0, it may block.
  /// PUB sockets do not report send errors like `ResourceLimitReached` or `Timeout`
  /// to the user; they either succeed in queuing or drop (for SNDTIMEO=0).
  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    let payload_preview_str = msg
      .data()
      .map(|d| String::from_utf8_lossy(&d.iter().take(20).copied().collect::<Vec<_>>()).into_owned())
      .unwrap_or_else(|| "<empty_payload>".to_string());

    tracing::debug!(
        handle = self.core.handle,
        msg_size = msg.size(),
        msg_payload_preview = %payload_preview_str, // Use the owned String
        "PubSocket::send preparing to distribute message"
    );
    // Use the distributor to send the message to all connected peer pipes.
    // `send_to_all` internally handles HWM and timeouts for each pipe.
    // It collects fatal errors (like ConnectionClosed) for pipes that need cleanup.
    match self
      .distributor
      .send_to_all(&msg, self.core.handle, &self.core.core_state)
      .await
    {
      Ok(()) => Ok(()), // Succeeded for all reachable peers (or message was dropped due to HWM for some).
      Err(errors) => {
        // Some peers disconnected or had fatal errors during the send attempt.
        // PUB sockets don't report these errors to the user application.
        // However, we should clean up the distributor by removing these failed pipes.
        for (pipe_write_id, error_detail) in errors {
          tracing::debug!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            error = %error_detail,
            "PUB removing disconnected/errored peer found during send"
          );
          // Remove the pipe directly from the distributor.
          // The pipe_detached hook will eventually be called by SocketCore for full cleanup,
          // but this preemptive removal from the distributor stops further send attempts to it.
          self.distributor.remove_pipe(pipe_write_id).await;
        }
        Ok(()) // Still return Ok(()) to the user, as per ZMQ PUB behavior.
      }
    }
  }

  /// PUB sockets cannot receive messages. This method will always return an error.
  async fn recv(&self) -> Result<Msg, ZmqError> {
    Err(ZmqError::InvalidState("PUB sockets cannot receive messages"))
  }

  async fn send_multipart(&self, mut frames: Vec<Msg>) -> Result<(), ZmqError> {
    if frames.is_empty() {
      // Sending an empty multipart message could be interpreted as sending
      // a single empty message without the MORE flag.
      // Or it could be an error. ZMQ PUB generally just sends what it's given.
      // If we send one empty message:
      // return self.send(Msg::new()).await;
      // For now, let's consider it an invalid operation or do nothing.
      tracing::warn!(
        handle = self.core.handle,
        "PUB send_multipart called with empty frames vector. Doing nothing."
      );
      return Ok(()); // Or perhaps Err(ZmqError::InvalidMessage("Cannot send empty multipart message".into()))
    }

    // Ensure MORE flags are set correctly on the frames to be sent.
    // The last frame must NOT have MORE. All preceding frames MUST have MORE.
    let num_frames = frames.len();
    for (i, frame) in frames.iter_mut().enumerate() {
      if i < num_frames - 1 {
        // This is not the last frame, ensure MORE is set.
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        // This is the last frame, ensure MORE is NOT set.
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
    }

    // Use the distributor to get peer pipe WRITE IDs
    let peer_pipe_write_ids = self.distributor.get_peer_ids().await;
    if peer_pipe_write_ids.is_empty() {
      tracing::trace!(
        handle = self.core.handle,
        "PUB send_multipart: No connected peers to send to."
      );
      return Ok(()); // No peers, so "success" in the sense of no error, but message not sent.
    }

    let timeout_opt = self.core.core_state.read().options.sndtimeo;
    let mut all_fatal_send_errors: Vec<(usize, ZmqError)> = Vec::new();

    for pipe_write_id in peer_pipe_write_ids {
      let pipe_sender = if let Some(pipe_sender) = self.core.core_state.read().get_pipe_sender(pipe_write_id) {
        pipe_sender
      } else {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "PUB send_multipart: Pipe sender not found in core state. Stale pipe ID in distributor?"
        );
        // This pipe_write_id is stale, should be removed from distributor.
        all_fatal_send_errors.push((
          pipe_write_id,
          ZmqError::Internal("Distributor had stale pipe ID".into()),
        ));
        continue;
      };

      tracing::trace!(
        handle = self.core.handle,
        target_pipe_id = pipe_write_id,
        num_frames = num_frames,
        "PUB send_multipart: Distributing multipart message to pipe."
      );
      let mut pipe_had_fatal_error = false;
      for (frame_idx, frame_to_send) in frames.iter().enumerate() {
        // Important: Each frame is cloned for sending to this specific pipe.
        // Distributor::send_to_all in the single `send` method also clones.
        match send_msg_with_timeout(
          &pipe_sender,
          frame_to_send.clone(),
          timeout_opt,
          self.core.handle,
          pipe_write_id,
        )
        .await
        {
          Ok(()) => { /* Frame sent successfully to this pipe */ }
          Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
            // HWM/Timeout for this specific pipe.
            // PUB sockets silently drop messages for congested peers.
            tracing::trace!(
              handle = self.core.handle,
              pipe_id = pipe_write_id,
              frame_index = frame_idx,
              "PUB send_multipart: Dropping frame (and subsequent for this peer) due to HWM/Timeout."
            );
            pipe_had_fatal_error = true; // Treat as if we can't send more to this peer for this logical message
            break; // Stop sending further parts of this logical message to *this* congested peer
          }
          Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::Internal(_)) => {
            // Fatal error for this pipe. Record it.
            tracing::debug!(
                handle = self.core.handle,
                pipe_id = pipe_write_id,
                frame_index = frame_idx,
                error = %e,
                "PUB send_multipart: Fatal error sending to pipe. Marking for cleanup."
            );
            all_fatal_send_errors.push((pipe_write_id, e));
            pipe_had_fatal_error = true;
            break; // Stop sending further parts to *this* dead peer
          }
          Err(e) => {
            // Other unexpected errors
            tracing::error!(
                handle = self.core.handle,
                pipe_id = pipe_write_id,
                frame_index = frame_idx,
                error = %e,
                "PUB send_multipart: Unexpected error sending to pipe."
            );
            all_fatal_send_errors.push((pipe_write_id, e));
            pipe_had_fatal_error = true;
            break;
          }
        }
      }
      if pipe_had_fatal_error {
        // If we broke sending to this peer, we might want to ensure it's
        // removed from distributor for subsequent messages if it was a ConnectionClosed type error.
        // The main error collection below will handle this.
      }
    }

    // After attempting to send to all peers, clean up any distributor entries for pipes
    // that had fatal errors (ConnectionClosed, Internal).
    if !all_fatal_send_errors.is_empty() {
      for (failed_pipe_id, error_detail) in all_fatal_send_errors {
        // Only remove for truly fatal errors, not just HWM/Timeout drops
        if matches!(error_detail, ZmqError::ConnectionClosed | ZmqError::Internal(_)) {
          tracing::debug!(
              handle = self.core.handle,
              pipe_id = failed_pipe_id,
              error = %error_detail,
              "PUB send_multipart: Removing disconnected/errored peer from distributor."
          );
          self.distributor.remove_pipe(failed_pipe_id).await;
        }
      }
    }

    Ok(()) // PUB send operations (single or multipart) generally return Ok(()) to the user,
           // even if messages were dropped for some subscribers due to HWM or disconnections.
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    Err(ZmqError::InvalidState("PUB sockets cannot receive messages"))
  }

  // --- Pattern-Specific Option Handling ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    // PUB sockets typically ignore options like SUBSCRIBE or UNSUBSCRIBE,
    // as these are relevant to SUB sockets.
    // Other pattern-specific options for PUB are not common in core ZMQ.
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PUB",
      option = option,
      "Ignoring pattern-specific option for PUB socket"
    );
    // Return UnsupportedOption to indicate it's not a recognized pattern option for PUB.
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // PUB sockets do not have readable pattern-specific options.
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks called by SocketCore ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    // PUB sockets do not typically handle special commands beyond user API calls.
    Ok(false) // Indicate command was not handled here.
  }

  /// PUB sockets do not receive messages from peers, so they ignore `PipeMessageReceived` events.
  async fn handle_pipe_event(&self, _pipe_id: usize, _event: Command) -> Result<(), ZmqError> {
    // Any message arriving on a pipe to a PUB socket is unexpected by the pattern.
    Ok(())
  }

  /// Called by `SocketCore` when a new pipe (connection to a peer) is attached.
  /// For PUB sockets, this means a new SUB peer (or other compatible socket) has connected.
  async fn pipe_attached(
    &self,
    pipe_read_id: usize,           // ID SocketCore uses to read from this peer (not used by PUB).
    pipe_write_id: usize,          // ID SocketCore uses to write to this peer (PUB uses this).
    _peer_identity: Option<&[u8]>, // Peer identity is not typically used by PUB.
  ) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "PUB attaching pipe"
    );
    // Store the mapping from read ID to write ID for cleanup during detachment.
    self.pipe_read_to_write_id.write().insert(pipe_read_id, pipe_write_id);
    // Add the pipe's write ID to the distributor so messages can be fanned out to it.
    self.distributor.add_pipe(pipe_write_id).await;
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

  /// Called by `SocketCore` when a pipe is detached (peer disconnected or socket closing).
  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PUB detaching pipe"
    );
    // Remove the read ID -> write ID mapping.
    let maybe_write_id = self.pipe_read_to_write_id.write().remove(&pipe_read_id);
    if let Some(write_id) = maybe_write_id {
      // If a corresponding write ID was found, remove it from the distributor.
      self.distributor.remove_pipe(write_id).await;
      tracing::trace!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        pipe_write_id = write_id,
        "PUB removed detached pipe from distributor"
      );
    } else {
      // This might happen if pipe_detached is called multiple times for the same pipe
      // or if the pipe was not properly added.
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        "PUB detach: Write ID not found for read ID, distributor may not be updated."
      );
    }
  }
}
