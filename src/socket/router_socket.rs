// src/socket/router_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, CoreState, SocketCore};
use crate::socket::patterns::{FairQueue, RouterMap}; // Use RouterMap and FairQueue
use crate::socket::ISocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::timeout;

use super::options::SocketOptions;

#[derive(Debug)]
pub(crate) struct RouterSocket {
  core: Arc<SocketCore>,
  router_map: RouterMap,     // Maps Identity -> Write Pipe ID
  incoming_queue: FairQueue, // Buffers incoming messages (already prefixed with ID)
  // Store partial incoming messages per pipe if needed for multi-part identity prepending
  // Key: pipe_read_id
  partial_incoming: Mutex<HashMap<usize, Vec<Msg>>>,
  /// Set when the Identity frame is sent, cleared when the last payload frame is sent.
  current_send_target: Mutex<Option<usize>>,
  // Map Pipe Read ID -> Identity (needed for prepending on receive)
  // Duplicates RouterMap's reverse map but might be convenient here
  pipe_to_identity: Mutex<HashMap<usize, Blob>>,
}

impl RouterSocket {
  pub fn new(core: Arc<SocketCore>, options: SocketOptions) -> Self {
    let queue_capacity = options.rcvhwm.max(1);
    Self {
      core,
      router_map: RouterMap::new(),
      incoming_queue: FairQueue::new(queue_capacity),
      partial_incoming: Mutex::new(HashMap::new()),
      current_send_target: Mutex::new(None),
      pipe_to_identity: Mutex::new(HashMap::new()),
    }
  }

  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }

  /// Creates a temporary identity Blob from a pipe ID.
  /// TODO: Replace this with real identity management later.
  fn pipe_id_to_placeholder_identity(pipe_read_id: usize) -> Blob {
    Blob::from(pipe_read_id.to_be_bytes().to_vec())
  }
}

// --- ISocket Implementation ---
#[async_trait]
impl ISocket for RouterSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }
  fn mailbox(&self) -> &MailboxSender {
    self.core.mailbox_sender()
  }

  // --- API Method Implementations (Delegate to Core) ---
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

  // --- Pattern-Specific Logic ---
  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    // ROUTER send expects Identity frame first, then payload frame(s).
    // We implement stateful send based on current_send_target.

    // Lock the target state for the whole operation if possible,
    // or carefully manage its setting/clearing around awaits.
    let mut current_target_guard = self.current_send_target.lock().await;

    // 1. Get SNDTIMEO setting (needed for potentially sending first frame)
    let timeout_opt: Option<Duration> = { self.core_state().await.options.sndtimeo };

    if current_target_guard.is_none() {
      // --- First Frame: Must be the Destination Identity ---
      if !msg.is_more() {
        return Err(ZmqError::InvalidMessage(
          "ROUTER send expects Identity frame with MORE flag followed by payload".into(),
        ));
      }
      let destination_id = Blob::from(msg.data().unwrap_or(&[]).to_vec());
      if destination_id.is_empty() {
        return Err(ZmqError::InvalidMessage(
          "ROUTER send received empty Identity frame".into(),
        ));
      }

      // Find target pipe write ID
      let pipe_write_id = match self.router_map.get_pipe(&destination_id).await {
        Some(id) => id,
        None => {
          // Identity not found
          tracing::debug!(
            handle = self.core.handle,
            ?destination_id,
            "ROUTER send failed: Destination identity unknown"
          );

          return Err(ZmqError::HostUnreachable(format!(
            "Peer {:?} not connected or identity unknown",
            destination_id
          )));
        }
      };

      // Get sender channel
      let pipe_tx = {
        let core_state_guard = self.core_state().await;
        match core_state_guard.get_pipe_sender(pipe_write_id) {
          Some(tx) => tx,
          None => {
            // Peer likely disconnected between router_map lookup and getting sender
            tracing::error!(
              handle = self.core.handle,
              pipe_id = pipe_write_id,
              ?destination_id,
              "ROUTER send failed: Pipe sender disappeared after lookup."
            );
            // TODO: Cleanup RouterMap? Needs coordination.
            return Err(ZmqError::HostUnreachable("Peer disconnected".into())); // EHOSTUNREACH
          }
        }
      };

      // Ensure MORE flag IS set on the identity frame being sent
      msg.set_flags(msg.flags() | MsgFlags::MORE);

      // Send the identity frame using the timeout helper
      match send_msg_with_timeout(&pipe_tx, msg, timeout_opt, self.core.handle, pipe_write_id).await {
        Ok(()) => {
          // Store target for subsequent payload frames
          *current_target_guard = Some(pipe_write_id);
          tracing::trace!(
            handle = self.core.handle,
            ?destination_id,
            pipe_id = pipe_write_id,
            "ROUTER send target locked"
          );
          Ok(()) // Ready for payload frame(s)
        }
        Err(e @ ZmqError::ConnectionClosed) => {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            "ROUTER send (identity) failed: {}",
            e
          );
          // TODO: Remove peer from RouterMap? Needs read ID. Complex cleanup.
          Err(ZmqError::HostUnreachable("Peer disconnected during send".into()))
          // Return EHOSTUNREACH
        }
        Err(e @ ZmqError::Internal(_)) => {
          // Treat internal errors as fatal for this attempt
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            "ROUTER send (identity) failed: {}",
            e
          );
          Err(e)
        }
        Err(e @ ZmqError::ResourceLimitReached) | Err(e @ ZmqError::Timeout) => {
          tracing::debug!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            "ROUTER send (identity) failed HWM/Timeout: {}",
            e
          );
          Err(e) // Return EAGAIN or Timeout
        }
        Err(e) => Err(e), // Other errors
      }
      // Note: current_target_guard lock is held until this branch returns
    } else {
      // --- Subsequent Frame(s): Payload ---
      let target_pipe_id = current_target_guard.unwrap(); // We know it's Some

      // Get sender (potentially locking CoreState again)
      let pipe_tx = {
        let core_state_guard = self.core_state().await;
        match core_state_guard.get_pipe_sender(target_pipe_id) {
          Some(tx) => tx,
          None => {
            // Target pipe disappeared while we were holding the lock! Should be rare.
            tracing::error!(
              handle = self.core.handle,
              pipe_id = target_pipe_id,
              "ROUTER send (payload): Target pipe disappeared!"
            );
            *current_target_guard = None; // Clear target state
            return Err(ZmqError::HostUnreachable("Peer disconnected mid-message".into()));
            // EHOSTUNREACH
          }
        }
      };

      // Determine if this is the last part *intended by the user* for this message
      let is_last_user_part = !msg.is_more();

      // Send payload frame using the *same timeout setting* as the identity frame
      let send_result = send_msg_with_timeout(
        &pipe_tx,
        msg,         // msg already has correct MORE flag from user perspective
        timeout_opt, // Reuse timeout setting
        self.core.handle,
        target_pipe_id,
      )
      .await;

      // If this was the last part intended by the user, clear the target state
      if is_last_user_part {
        *current_target_guard = None;
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = target_pipe_id,
          "ROUTER send target released"
        );
      }

      // Process send result
      match send_result {
        Ok(()) => Ok(()),
        Err(e @ ZmqError::ConnectionClosed) => {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = target_pipe_id,
            "ROUTER send (payload) failed: Pipe channel closed"
          );
          *current_target_guard = None; // Ensure target is cleared on error too
                                        // TODO: Remove peer from RouterMap?
          Err(ZmqError::HostUnreachable("Peer disconnected during send".into()))
          // Return EHOSTUNREACH
        }
        Err(e @ ZmqError::Internal(_)) => {
          *current_target_guard = None; // Clear target
          Err(e)
        }
        Err(e @ ZmqError::ResourceLimitReached) | Err(e @ ZmqError::Timeout) => {
          // Don't clear target state here, user might retry the *same* payload part
          tracing::debug!(
            handle = self.core.handle,
            pipe_id = target_pipe_id,
            "ROUTER send (payload) failed HWM/Timeout: {}",
            e
          );
          Err(e) // Return EAGAIN or Timeout
        }
        Err(e) => {
          *current_target_guard = None; // Clear target on unknown errors
          Err(e)
        }
      }
      // Note: current_target_guard lock is held until this branch returns
    } // end else (payload frame)
  } // end send

  async fn recv(&self) -> Result<Msg, ZmqError> {
    let rcvtimeo_opt: Option<Duration> = { self.core_state().await.options.rcvtimeo };

    // Pop message frame (could be Identity or Payload part) from the fair queue
    let pop_future = self.incoming_queue.pop_message();

    let result = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => {
        tracing::trace!(handle = self.core.handle, ?duration, "Applying RCVTIMEO to ROUTER recv");
        match timeout(duration, pop_future).await {
          Ok(Ok(Some(msg))) => Ok(msg), // Got a frame
          Ok(Ok(None)) => Err(ZmqError::Internal("Receive queue closed".into())),
          Ok(Err(e)) => Err(e),
          Err(_timeout_elapsed) => Err(ZmqError::Timeout),
        }
      }
      _ => {
        // No timeout
        match pop_future.await? {
          Some(msg) => Ok(msg),
          None => Err(ZmqError::Internal("Receive queue closed".into())),
        }
      }
    };
    // User is responsible for reading frames (Identity first) and checking MORE flag.

    result
  }

  // --- Pattern Specific Options ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    // TODO: Handle ROUTER specific options like ZMQ_ROUTER_MANDATORY, ZMQ_ROUTER_HANDOVER?
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        let mut partial_guard = self.partial_incoming.lock().await;
        let buffer = partial_guard.entry(pipe_read_id).or_insert_with(Vec::new);

        let is_first_part_for_peer = buffer.is_empty();
        let mut current_msg = msg; // Shadow msg
        let is_last_part = !current_msg.is_more();

        if is_first_part_for_peer {
          // Handling the very first frame for this peer connection
          if current_msg.size() == 0 && current_msg.is_more() {
            // Consume the optional initial empty delimiter from DEALER
            tracing::trace!(
              handle = self.core.handle,
              pipe_id = pipe_read_id,
              "ROUTER consumed initial empty delimiter from DEALER"
            );
            // Do not add to buffer, do not change state, just return
            drop(partial_guard);
            return Ok(());
          } else {
            // First frame is actual data (e.g., identity or first payload part if no delimiter)
            buffer.push(current_msg);
          }
        } else {
          // Handling subsequent frames for this peer
          if current_msg.size() == 0 && current_msg.is_more() {
            // Consume subsequent empty delimiters from DEALER
            tracing::trace!(
              handle = self.core.handle,
              pipe_id = pipe_read_id,
              "ROUTER consumed subsequent empty delimiter from DEALER"
            );
            // Do not add to buffer, just return
            drop(partial_guard);
            return Ok(());
          } else {
            // It's a payload part
            buffer.push(current_msg);
          }
        }

        if is_last_part {
          // Message complete, process it
          let complete_message_parts = partial_guard.remove(&pipe_read_id).unwrap_or_default();
          drop(partial_guard); // Release lock

          // Get identity associated with this pipe

          let identity_blob = {
            let map_guard = self.pipe_to_identity.lock().await;
            map_guard.get(&pipe_read_id).cloned() // Clone the Blob
          };

          let identity_blob = match identity_blob {
            Some(id) => id,
            None => {
              // This case should ideally not happen if pipe_attached worked correctly,
              // but handle it defensively.
              tracing::error!(
                handle = self.core.handle,
                pipe_id = pipe_read_id,
                "ROUTER handle_pipe_event: Identity not found for pipe! Using placeholder."
              );
              // Maybe return error instead of using placeholder? Depends on desired strictness.
              // For now, use placeholder to avoid breaking receive loop.
              Self::pipe_id_to_placeholder_identity(pipe_read_id)
            }
          };

          // Construct the logical message to queue for user recv(): Identity + Payload(s)

          // 1. Create Identity Frame
          let mut id_msg = Msg::from_vec(identity_blob.to_vec());
          // Must set MORE flag if payload follows
          if !complete_message_parts.is_empty() {
            id_msg.set_flags(MsgFlags::MORE);
          }

          // Push identity frame FIRST
          if let Err(e) = self.incoming_queue.push_message(id_msg).await {
            tracing::error!(
                handle=self.core.handle,
                pipe_id=pipe_read_id,
                error=?e,
                "ROUTER failed to push Identity frame to queue. Dropping message."
            );
            // If we can't queue the identity, don't queue the payload either.
            // Return the error? Or just log and drop? Log and drop for now.
            return Ok(()); // Or return Err(e)? Decide on error propagation strategy.
          }

          // 2. Push Original Payload Frame(s)
          let num_payload_parts = complete_message_parts.len();
          for (i, mut part) in complete_message_parts.into_iter().enumerate() {
            // Ensure MORE flag is correct for the logical message being queued
            if i < num_payload_parts - 1 {
              part.set_flags(part.flags() | MsgFlags::MORE); // Set MORE on intermediate parts
            } else {
              part.set_flags(part.flags() & !MsgFlags::MORE); // Ensure MORE is unset on the very last part
            }

            if let Err(e) = self.incoming_queue.push_message(part).await {
              tracing::error!(
                  handle=self.core.handle,
                  pipe_id=pipe_read_id,
                  error=?e,
                  part_index=i+1, // 1-based index for logging
                  "ROUTER failed to push Payload frame to queue. Message incomplete."
              );
              // Message is now incomplete in the queue. This is problematic.
              // Maybe try to clear the queue for this peer? Complex.
              // For now, just log the error. User might get partial message if they recv before error.
              return Ok(()); // Or return Err(e)?
            }
          }

          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_read_id,
            "ROUTER pushed Identity + Payload parts to FairQueue"
          );
        } else {
          // More parts coming, keep buffered
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_read_id,
            "ROUTER buffering partial message part"
          );
        }
      }
      _ => { /* ROUTER ignores other pipe events */ }
    }
    Ok(())
  }

  async fn pipe_attached(&self, pipe_read_id: usize, pipe_write_id: usize, peer_identity_opt: Option<&[u8]>) {
    // Determine identity: Use provided identity, or generate placeholder
    let identity = match peer_identity_opt {
      // Use provided ID if available and non-empty
      Some(id_bytes) if !id_bytes.is_empty() => {
        tracing::debug!(
          handle = self.core.handle,
          pipe_read_id = pipe_read_id,
          "ROUTER using ZMTP peer identity from handshake"
        );
        Blob::from(id_bytes.to_vec())
      }
      _ => {
        // Fallback if no ZMTP ID provided or it's empty
        tracing::debug!(
          // Changed level to debug, this is expected if peer has no ID set
          handle = self.core.handle,
          pipe_read_id = pipe_read_id,
          "ROUTER Peer did not provide ZMTP identity, generating placeholder based on pipe ID."
        );
        Self::pipe_id_to_placeholder_identity(pipe_read_id)
      }
    };

    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      ?identity,
      "ROUTER attaching pipe"
    );
    // Add peer to router map AND local reverse map
    self
      .router_map
      .add_peer(identity.clone(), pipe_read_id, pipe_write_id)
      .await;
    self.pipe_to_identity.lock().await.insert(pipe_read_id, identity); // Store the chosen identity
                                                                       // Notify FairQueue
    self.incoming_queue.pipe_attached(pipe_read_id);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "ROUTER detaching pipe"
    );

    // 1. Try to find the identity and write pipe ID for the detaching read pipe ID
    let maybe_identity: Option<Blob> = {
      let id_map_guard = self.pipe_to_identity.lock().await;
      id_map_guard.get(&pipe_read_id).cloned()
      // id_map_guard lock released here
    };

    // We need the write_id associated with this identity to check against current_send_target
    let maybe_write_id: Option<usize> = if let Some(ref identity) = maybe_identity {
      // Use the public get_pipe method BEFORE removing the mapping
      self.router_map.get_pipe(identity).await
    } else {
      None
    };

    // 2. Remove peer from router map (uses read ID)
    self.router_map.remove_peer_by_read_pipe(pipe_read_id).await;

    // 3. Remove from local identity map
    let _removed_identity = self.pipe_to_identity.lock().await.remove(&pipe_read_id);

    // 4. Notify FairQueue helper
    self.incoming_queue.pipe_detached(pipe_read_id);

    // 5. Clear any partial messages from this pipe
    self.partial_incoming.lock().await.remove(&pipe_read_id);

    // 6. Clear pending send target if it matched the detached pipe's write ID
    if let Some(write_pipe_id) = maybe_write_id {
      let mut target_guard = self.current_send_target.lock().await;
      if *target_guard == Some(write_pipe_id) {
        tracing::warn!(
          handle = self.core.handle,
          pipe_read_id = pipe_read_id,
          pipe_write_id = write_pipe_id,
          "Current ROUTER send target pipe detached. Clearing target."
        );
        *target_guard = None;
      }
    } else {
      // If we couldn't find the identity or write_id (e.g., mapping was already gone), we can't reliably check the target.
      tracing::trace!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        "Could not verify write_id for detached pipe to clear send target."
      );
    }
  }
} // end impl ISocket for RouterSocket
