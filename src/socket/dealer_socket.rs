// src/socket/dealer_socket.rs

use crate::delegate_to_core; // Macro for delegating API calls to SocketCore.
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags}; // Blob not directly used here, but often with DEALER/ROUTER.
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, CoreState, SocketCore}; // Core components.
use crate::socket::options::SocketOptions;
use crate::socket::patterns::{FairQueue, LoadBalancer}; // DEALER uses LoadBalancer (out) and FairQueue (in).
use crate::socket::{ISocket, SourcePipeReadId}; // The trait this struct implements.

use async_trait::async_trait;
use std::collections::HashMap; // For pipe_read_to_write_id map.
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, MutexGuard}; // Mutex for internal state, oneshot for API replies.
use tokio::time::timeout; // For send/recv timeouts.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IncomingPipeState {
  /// Waiting for the first frame from a peer.
  Idle,
  /// Received an identity-like frame (non-empty, MORE).
  /// Now expecting either an empty delimiter (if from ROUTER-like peer)
  /// or the first payload frame.
  StrippedIdentityExpectDelimiterOrPayload,
  /// Received an empty delimiter frame (non-empty, MORE), from another DEALER.
  /// Now expecting the first payload frame.
  StrippedDelimiterExpectPayload,
  /// Actively receiving payload frames for the current logical message.
  ExpectingMorePayload,
}

/// Implements the DEALER socket pattern.
/// DEALER sockets provide asynchronous request-reply capabilities.
/// They load-balance outgoing messages among connected peers and fair-queue incoming messages.
/// DEALER sockets typically connect to ROUTER sockets.
#[derive(Debug)]
pub(crate) struct DealerSocket {
  /// Arc to the shared `SocketCore` actor that manages common socket state and transport.
  core: Arc<SocketCore>,
  /// `LoadBalancer` to distribute outgoing messages to available peer pipes.
  load_balancer: LoadBalancer,
  /// `FairQueue` to buffer incoming messages from all connected peers.
  incoming_queue: FairQueue,
  /// Maps a pipe's read ID (from SocketCore's perspective) to its corresponding write ID.
  /// This is needed during `pipe_detached` to correctly remove the pipe from the `load_balancer`.
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
  pipe_state: Mutex<HashMap<usize, IncomingPipeState>>,
}

impl DealerSocket {
  /// Creates a new `DealerSocket`.
  ///
  /// # Arguments
  /// * `core` - An `Arc` to the `SocketCore` managing this socket.
  /// * `options` - Initial socket options, used here to determine queue capacities.
  pub fn new(core: Arc<SocketCore>, options: SocketOptions) -> Self {
    // Capacity for the incoming message queue, based on RCVHWM.
    let queue_capacity = options.rcvhwm.max(1); // Ensure capacity is at least 1.
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      incoming_queue: FairQueue::new(queue_capacity),
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
      pipe_state: Mutex::new(HashMap::new()),
    }
  }

  /// Helper to get a locked guard for the `CoreState` within `SocketCore`.
  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

#[async_trait]
impl ISocket for DealerSocket {
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

  /// Sends a message using the DEALER pattern.
  /// Prepends an empty delimiter frame and then sends the user's message parts.
  /// Load-balances the message across available connected peers.
  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    // Get SNDTIMEO from options before potentially blocking on peer selection.
    let timeout_opt: Option<Duration> = { self.core_state().await.options.sndtimeo };

    // Select a peer pipe using the load balancer, waiting if necessary based on SNDTIMEO.
    let pipe_write_id = loop {
      if let Some(id) = self.load_balancer.get_next_pipe().await {
        break id; // Found an available peer pipe.
      }
      // No peer currently available, behavior depends on timeout settings.
      match timeout_opt {
        Some(duration) if duration.is_zero() => {
          // Non-blocking send: if no peer, return ResourceLimitReached (EAGAIN).
          tracing::trace!(
            handle = self.core.handle,
            "DEALER send failed (non-blocking): No connected peers"
          );
          return Err(ZmqError::ResourceLimitReached);
        }
        None => {
          // Blocking send (infinite timeout): wait for a pipe to become available.
          tracing::trace!(
            handle = self.core.handle,
            "DEALER send blocking: Waiting for available peer..."
          );
          self.load_balancer.wait_for_pipe().await; // Wait for notification.
          continue; // Loop back to try getting a pipe again.
        }
        Some(duration) => {
          // Timed blocking send: wait for a pipe with a timeout.
          tracing::trace!(
            handle = self.core.handle,
            ?duration,
            "DEALER send timed wait: Waiting for available peer..."
          );
          match timeout(duration, self.load_balancer.wait_for_pipe()).await {
            Ok(()) => continue, // Wait succeeded, pipe might be available now.
            Err(_timeout_elapsed) => {
              tracing::debug!(
                handle = self.core.handle,
                ?duration,
                "DEALER send timed out waiting for peer"
              );
              return Err(ZmqError::Timeout); // Timeout elapsed.
            }
          }
        }
      }
    };

    // Get the sender channel for the selected pipe from CoreState.
    let pipe_tx = {
      let core_state_guard = self.core_state().await;
      core_state_guard.get_pipe_sender(pipe_write_id).ok_or_else(|| {
        // This indicates a consistency issue if a pipe ID from LoadBalancer isn't in CoreState.
        tracing::error!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "DEALER send failed: Pipe sender disappeared after selection."
        );
        // Consider removing the stale pipe ID from LoadBalancer here if this happens.
        ZmqError::Internal("Pipe sender consistency error".into())
      })?
    }; // CoreState lock released here.

    // DEALER sockets traditionally prepend an empty delimiter frame when talking to ROUTER.
    tracing::trace!(
      handle = self.core.handle,
      pipe_id = pipe_write_id,
      "DEALER prepending empty delimiter"
    );
    let mut delimiter = Msg::new(); // Empty payload.
    delimiter.set_flags(MsgFlags::MORE); // Delimiter must have the MORE flag.

    // Send the delimiter frame, respecting SNDTIMEO for HWM.
    match send_msg_with_timeout(&pipe_tx, delimiter, timeout_opt, self.core.handle, pipe_write_id).await {
      Ok(()) => {} // Delimiter sent successfully, proceed to payload.
      Err(e) => {
        // If sending delimiter fails fatally, remove the pipe from load balancer.
        if matches!(e, ZmqError::ConnectionClosed | ZmqError::Internal(_)) {
          self.load_balancer.remove_pipe(pipe_write_id).await;
        }
        return Err(e); // Return the error (e.g., Timeout, ResourceLimitReached, ConnectionClosed).
      }
    }

    // Prepare the user's message payload.
    // Ensure MORE flag is correctly set on the final part of the user's message.
    let is_last_user_part = !msg.is_more();
    if is_last_user_part {
      msg.set_flags(msg.flags() & !MsgFlags::MORE); // Ensure MORE is unset if it's the last part.
    } else {
      msg.set_flags(msg.flags() | MsgFlags::MORE); // Ensure MORE is set if more user parts follow.
    }

    // Send the user's message payload, respecting SNDTIMEO for HWM.
    match send_msg_with_timeout(&pipe_tx, msg, timeout_opt, self.core.handle, pipe_write_id).await {
      Ok(()) => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "DEALER sent message payload"
        );
        Ok(()) // Entire message (delimiter + payload) sent successfully.
      }
      Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::Internal(_)) => {
        // Fatal error sending payload, remove pipe from load balancer.
        tracing::warn!(handle=self.core.handle, pipe_id=pipe_write_id, error=%e, "DEALER send (payload) failed fatally");
        self.load_balancer.remove_pipe(pipe_write_id).await;
        Err(e)
      }
      Err(e @ ZmqError::ResourceLimitReached) | Err(e @ ZmqError::Timeout) => {
        // Non-fatal error (HWM or timeout) sending payload. Delimiter was already sent.
        tracing::trace!(handle=self.core.handle, pipe_id=pipe_write_id, error=%e, "DEALER send (payload) failed HWM/Timeout");
        Err(e)
      }
      Err(e) => Err(e), // Propagate other unexpected errors.
    }
  }

  /// Receives a message using the DEALER pattern.
  /// Strips the leading empty delimiter frame if present (typically from a ROUTER).
  async fn recv(&self) -> Result<Msg, ZmqError> {
    // Get RCVTIMEO from options.
    let rcvtimeo_opt: Option<Duration> = { self.core_state().await.options.rcvtimeo };

    // Pop the next available message (or first part of it) from the incoming fair queue.
    let pop_future = self.incoming_queue.pop_message();
    let received_msg = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => {
        // Apply timeout if RCVTIMEO is set to a positive value.
        tracing::trace!(handle = self.core.handle, ?duration, "Applying RCVTIMEO to DEALER recv");
        match timeout(duration, pop_future).await {
          Ok(Ok(Some(msg))) => msg, // Message received within timeout.
          Ok(Ok(None)) => return Err(ZmqError::Internal("Receive queue closed unexpectedly".into())),
          Ok(Err(e)) => return Err(e), // Internal error from pop_message.
          Err(_timeout_elapsed) => return Err(ZmqError::Timeout), // Timeout occurred.
        }
      }
      _ => {
        // No timeout or RCVTIMEO = 0 (though pop_message itself is blocking if queue is empty).
        match pop_future.await? {
          // Use ? to propagate errors from pop_message.
          Some(msg) => msg,
          None => return Err(ZmqError::Internal("Receive queue closed unexpectedly".into())),
        }
      }
    };
    // The `handle_pipe_event` method is responsible for stripping the delimiter from ROUTER
    // and queuing only the payload parts. So, `received_msg` here should be the first
    // actual payload frame. The user then calls `recv` repeatedly if `is_more()` is true.
    Ok(received_msg)
  }

  // --- Pattern-Specific Option Handling ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    // DEALER sockets do not have specific pattern options like SUBSCRIBE/UNSUBSCRIBE.
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // DEALER sockets do not have readable pattern-specific options.
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks called by SocketCore ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    // DEALER sockets do not typically handle special commands beyond user API calls.
    Ok(false) // Indicate command was not handled here.
  }

  /// Handles messages received from a pipe by the `SocketCore`.
  /// For DEALER, this means a message part has arrived from a peer (likely ROUTER).
  /// It needs to strip the initial empty delimiter frame if present.
  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        let mut state_map_guard = self.pipe_state.lock().await;
        let mut current_pipe_state = state_map_guard.entry(pipe_read_id).or_insert(IncomingPipeState::Idle);
        let is_last_part_of_transport_msg = !msg.is_more(); // Whether this is the last ZMTP frame

        tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_read_id,
            state = ?*current_pipe_state,
            size = msg.size(),
            more = msg.is_more(),
            "DEALER handle_pipe_event received ZMTP frame"
        );

        match *current_pipe_state {
          IncomingPipeState::Idle => {
            // This is the first frame from this peer for a new logical message.
            if msg.size() > 0 && msg.is_more() {
              // Case 1: Non-empty frame with MORE flag. This is likely an identity from a ROUTER.
              // We strip this identity frame.
              tracing::trace!(
                handle = self.core.handle,
                pipe_id = pipe_read_id,
                "DEALER (Idle): Stripped identity-like frame."
              );
              *current_pipe_state = IncomingPipeState::StrippedIdentityExpectDelimiterOrPayload;
              // If this identity frame was the *only* frame (no MORE), it's an anomaly.
              // For now, StrippedIdentity... implies we expect more.
              // If is_last_part_of_transport_msg is true here, it means identity only, no payload.
              // The user won't see anything for this. State will reset on next msg or detach.
            } else if msg.size() == 0 && msg.is_more() {
              // Case 2: Empty frame with MORE flag. This is a delimiter from another DEALER.
              // We strip this delimiter frame.
              tracing::trace!(
                handle = self.core.handle,
                pipe_id = pipe_read_id,
                "DEALER (Idle): Stripped initial empty delimiter."
              );
              *current_pipe_state = IncomingPipeState::StrippedDelimiterExpectPayload;
            } else {
              // Case 3: Frame is payload (either single-part, or first part of payload if peer isn't ROUTER/DEALER).
              // Or an empty frame without MORE (unusual, but treat as full empty message).
              tracing::trace!(
                handle = self.core.handle,
                pipe_id = pipe_read_id,
                "DEALER (Idle): Queuing as first payload frame."
              );
              let mut payload_msg = msg;
              payload_msg
                .metadata_mut()
                .insert_typed(SourcePipeReadId(pipe_read_id))
                .await;
              self.incoming_queue.push_message(payload_msg).await?;

              if is_last_part_of_transport_msg {
                *current_pipe_state = IncomingPipeState::Idle; // Reset for next message
              } else {
                *current_pipe_state = IncomingPipeState::ExpectingMorePayload;
              }
            }
          }
          IncomingPipeState::StrippedIdentityExpectDelimiterOrPayload => {
            // We previously stripped an identity. Now check if this frame is an empty delimiter.
            if msg.size() == 0 && msg.is_more() {
              // Case 4: Yes, it's an empty delimiter after an identity. Strip it.
              tracing::trace!(
                handle = self.core.handle,
                pipe_id = pipe_read_id,
                "DEALER (StrippedId): Stripped empty delimiter."
              );
              *current_pipe_state = IncomingPipeState::ExpectingMorePayload; // Now expect actual payload
            } else {
              // Case 5: No, it's not an empty delimiter. This frame is the first payload part.
              tracing::trace!(
                handle = self.core.handle,
                pipe_id = pipe_read_id,
                "DEALER (StrippedId): Queuing as first payload frame."
              );
              let mut payload_msg = msg;
              payload_msg
                .metadata_mut()
                .insert_typed(SourcePipeReadId(pipe_read_id))
                .await;
              self.incoming_queue.push_message(payload_msg).await?;

              if is_last_part_of_transport_msg {
                *current_pipe_state = IncomingPipeState::Idle;
              } else {
                *current_pipe_state = IncomingPipeState::ExpectingMorePayload;
              }
            }
          }
          IncomingPipeState::StrippedDelimiterExpectPayload | IncomingPipeState::ExpectingMorePayload => {
            // Case 6: We are expecting payload (either after initial delimiter or during multi-part).
            tracing::trace!(
              handle = self.core.handle,
              pipe_id = pipe_read_id,
              "DEALER (ExpectPayload): Queuing payload frame."
            );
            let mut payload_msg = msg;
            payload_msg
              .metadata_mut()
              .insert_typed(SourcePipeReadId(pipe_read_id))
              .await;
            self.incoming_queue.push_message(payload_msg).await?;

            if is_last_part_of_transport_msg {
              *current_pipe_state = IncomingPipeState::Idle;
            } else {
              // Stay in ExpectingMorePayload (or transition from StrippedDelimiter)
              *current_pipe_state = IncomingPipeState::ExpectingMorePayload;
            }
          }
        }
      }
      _ => { /* DEALER sockets typically ignore other direct pipe events. */ }
    }
    Ok(())
  }

  /// Called by `SocketCore` when a new pipe (connection to a peer) is attached.
  async fn pipe_attached(&self, pipe_read_id: usize, pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "DEALER attaching pipe"
    );
    // Store the mapping from read ID to write ID for cleanup on detachment.
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    // Add the pipe's write ID to the load balancer so it can be used for sending.
    self.load_balancer.add_pipe(pipe_write_id).await;
    // Notify the incoming fair queue that a new pipe is attached (for potential internal tracking).
    self.incoming_queue.pipe_attached(pipe_read_id);
    // Initialize pipe state to Idle
    self.pipe_state.lock().await.insert(pipe_read_id, IncomingPipeState::Idle);
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
      "DEALER detaching pipe"
    );
    // Remove the read ID -> write ID mapping.
    let maybe_write_id = self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);
    if let Some(write_id) = maybe_write_id {
      // If a write ID was found, remove it from the load balancer.
      self.load_balancer.remove_pipe(write_id).await;
    }
    // Notify the incoming fair queue that the pipe is detached.
    self.incoming_queue.pipe_detached(pipe_read_id);
    // Remove state for the detached pipe
    self.pipe_state.lock().await.remove(&pipe_read_id);
  }
}
