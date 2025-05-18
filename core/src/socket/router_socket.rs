// src/socket/router_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, CoreState, SocketCore};
use crate::socket::options::{SocketOptions, ROUTER_MANDATORY};
use crate::socket::patterns::{FairQueue, RouterMap}; // Use RouterMap and FairQueue
use crate::socket::ISocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::timeout;

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

  fn mailbox(&self) -> MailboxSender {
    self.core.command_sender()
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
    let mut current_target_guard = self.current_send_target.lock().await;
    let core_opts = self.core_state().await.options.clone(); // Clone options for use
    let timeout_opt: Option<Duration> = core_opts.sndtimeo;
    let router_mandatory_opt: bool = core_opts.router_mandatory;

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
          // Identity not found. Behavior depends on ROUTER_MANDATORY.
          if router_mandatory_opt {
            tracing::debug!(
              handle = self.core.handle,
              ?destination_id,
              "ROUTER send failed (mandatory=true): Destination identity unknown. Returning EHOSTUNREACH."
            );
            return Err(ZmqError::HostUnreachable(format!(
              "Peer {:?} not connected or identity unknown (ROUTER_MANDATORY=true)",
              destination_id
            )));
          } else {
            tracing::debug!(
              handle = self.core.handle,
              ?destination_id,
              "ROUTER send (mandatory=false): Destination identity unknown. Silently dropping message."
            );
            // current_target_guard remains None, message effectively dropped.
            return Ok(()); // Silently drop
          }
        }
      };

      // Get sender channel
      let pipe_tx = {
        // Re-lock core_state briefly if needed, or ensure it's still valid
        // If we already cloned options, maybe we don't need to lock again just for sender if it's cached
        // But get_pipe_sender typically needs the lock.
        let core_state_guard = self.core_state().await;
        match core_state_guard.get_pipe_sender(pipe_write_id) {
          Some(tx) => tx,
          None => {
            tracing::error!(
              handle = self.core.handle,
              pipe_id = pipe_write_id,
              ?destination_id,
              "ROUTER send failed: Pipe sender disappeared after lookup (peer likely disconnected)."
            );
            // Even if mandatory is false, if we found an ID then a pipe, but pipe is gone, this is an error.
            return Err(ZmqError::HostUnreachable("Peer disconnected".into()));
          }
        }
      };

      msg.set_flags(msg.flags() | MsgFlags::MORE);

      match send_msg_with_timeout(&pipe_tx, msg, timeout_opt, self.core.handle, pipe_write_id).await {
        Ok(()) => {
          *current_target_guard = Some(pipe_write_id);
          tracing::trace!(
            handle = self.core.handle,
            ?destination_id,
            pipe_id = pipe_write_id,
            "ROUTER send target locked"
          );
          Ok(())
        }
        Err(e @ ZmqError::ConnectionClosed) => {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            "ROUTER send (identity) failed: {}",
            e
          );
          Err(ZmqError::HostUnreachable("Peer disconnected during send".into()))
        }
        Err(e @ ZmqError::Internal(_)) => {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            "ROUTER send (identity) internal error: {}",
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
          Err(e)
        }
        Err(e) => Err(e),
      }
    } else {
      // --- Subsequent Frame(s): Payload ---
      let target_pipe_id = current_target_guard.unwrap();

      let pipe_tx = {
        let core_state_guard = self.core_state().await;
        match core_state_guard.get_pipe_sender(target_pipe_id) {
          Some(tx) => tx,
          None => {
            tracing::error!(
              handle = self.core.handle,
              pipe_id = target_pipe_id,
              "ROUTER send (payload): Target pipe disappeared!"
            );
            *current_target_guard = None;
            // If router_mandatory is false, we should still silently drop here.
            // If router_mandatory is true, EHOSTUNREACH is appropriate.
            return if router_mandatory_opt {
              Err(ZmqError::HostUnreachable("Peer disconnected mid-message".into()))
            } else {
              Ok(()) // Silently drop
            };
          }
        }
      };

      let is_last_user_part = !msg.is_more();
      let send_result = send_msg_with_timeout(&pipe_tx, msg, timeout_opt, self.core.handle, target_pipe_id).await;

      if is_last_user_part {
        *current_target_guard = None;
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = target_pipe_id,
          "ROUTER send target released"
        );
      }

      match send_result {
        Ok(()) => Ok(()),
        Err(e @ ZmqError::ConnectionClosed) => {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = target_pipe_id,
            "ROUTER send (payload) failed: Pipe channel closed"
          );
          *current_target_guard = None;
          // If router_mandatory is false, this should be a silent drop.
          // If router_mandatory is true, EHOSTUNREACH.
          if router_mandatory_opt {
            Err(ZmqError::HostUnreachable("Peer disconnected during send".into()))
          } else {
            Ok(()) // Silently drop
          }
        }
        Err(e @ ZmqError::Internal(_)) => {
          *current_target_guard = None;
          Err(e) // Internal errors are usually propagated
        }
        Err(e @ ZmqError::ResourceLimitReached) | Err(e @ ZmqError::Timeout) => {
          // These errors should be propagated regardless of router_mandatory.
          // The send attempt was made but blocked/timed out.
          tracing::debug!(
            handle = self.core.handle,
            pipe_id = target_pipe_id,
            "ROUTER send (payload) failed HWM/Timeout: {}",
            e
          );
          Err(e)
        }
        Err(e) => {
          // Other unexpected errors
          *current_target_guard = None;
          Err(e)
        }
      }
    }
  }

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
  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    if option == ROUTER_MANDATORY {
      let val = crate::socket::options::parse_bool_option(value)?;
      self.core_state().await.options.router_mandatory = val;
      tracing::debug!(handle = self.core.handle, "ROUTER_MANDATORY set to {}", val);
      Ok(())
    } else {
      Err(ZmqError::UnsupportedOption(option))
    }
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    if option == ROUTER_MANDATORY {
      let val = self.core_state().await.options.router_mandatory;
      Ok((val as i32).to_ne_bytes().to_vec())
    } else {
      Err(ZmqError::UnsupportedOption(option))
    }
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
    let placeholder_identity = Self::pipe_id_to_placeholder_identity(pipe_read_id);
    let initial_identity_to_use = match peer_identity_opt {
      Some(id_bytes) if !id_bytes.is_empty() => {
        tracing::debug!(
          handle = self.core.handle,
          pipe_read_id,
          "ROUTER pipe_attached using provided ZMTP identity"
        );
        Blob::from(id_bytes.to_vec())
      }
      _ => {
        tracing::debug!(
          handle = self.core.handle,
          pipe_read_id,
          "ROUTER pipe_attached using placeholder identity, awaiting true identity"
        );
        placeholder_identity.clone() // Use placeholder if no valid ZMTP ID yet
      }
    };

    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      initial_identity = ?initial_identity_to_use,
      "ROUTER attaching pipe"
    );

    self
      .router_map
      .add_peer(initial_identity_to_use.clone(), pipe_read_id, pipe_write_id)
      .await;
    self
      .pipe_to_identity
      .lock()
      .await
      .insert(pipe_read_id, initial_identity_to_use);
    self.incoming_queue.pipe_attached(pipe_read_id);
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, new_identity_opt: Option<Blob>) {
    let new_identity = match new_identity_opt {
      Some(id) if !id.is_empty() => id,
      _ => {
        tracing::warn!(
          handle = self.core.handle,
          pipe_read_id,
          "update_peer_identity called with None or empty identity, using placeholder."
        );
        Self::pipe_id_to_placeholder_identity(pipe_read_id)
      }
    };

    tracing::debug!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        new_identity = ?new_identity,
        "ROUTER updating peer identity"
    );

    let mut p_to_id_guard = self.pipe_to_identity.lock().await;
    let old_identity_opt = p_to_id_guard.get(&pipe_read_id).cloned(); // Get current/old identity

    if old_identity_opt.as_ref() == Some(&new_identity) {
      tracing::trace!(handle = self.core.handle, pipe_read_id, "Identity already up-to-date.");
      return;
    }

    // Get the write_id associated with this pipe_read_id
    // This requires looking up the old_identity in router_map
    let pipe_write_id_opt = if let Some(ref old_id) = old_identity_opt {
      self.router_map.get_pipe(old_id).await
    } else {
      // If there was no old identity, it means this pipe was not fully in router_map yet
      // or pipe_to_identity was somehow inconsistent.
      // We need a write_id to update router_map. If we don't have one, we can't update router_map correctly.
      // This implies pipe_attached might not have found a write_id, which is problematic.
      // For now, we'll try to find it via a reverse lookup in core_state if this is a real issue.
      // However, pipe_attached *should* have received a valid pipe_write_id.
      // Let's assume pipe_attached always sets a valid write_id for the initial placeholder.
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "No old identity found in pipe_to_identity map for update. Cannot find write_id via old identity."
      );
      None
    };

    if let Some(pipe_write_id) = pipe_write_id_opt {
      // If there was an old identity, remove its mapping from router_map
      if let Some(old_id) = old_identity_opt {
        // We can't directly call remove_peer_by_read_pipe as it also removes from pipe_to_identity
        // which we are currently holding a lock on (implicitly).
        // Let's refine this: RouterMap should probably handle updates more gracefully.
        // For now:
        let mut id_to_pipe_guard = self.router_map.identity_to_pipe.lock().await;
        id_to_pipe_guard.remove(&old_id);
        drop(id_to_pipe_guard);
        tracing::trace!(
          handle = self.core.handle,
          pipe_read_id,
          ?old_id,
          "Removed old identity from router_map."
        );
      }

      // Add the new identity to router_map, pointing to the same pipe_write_id
      let mut id_to_pipe_guard = self.router_map.identity_to_pipe.lock().await;
      id_to_pipe_guard.insert(new_identity.clone(), pipe_write_id);
      drop(id_to_pipe_guard);
      tracing::trace!(
        handle = self.core.handle,
        pipe_read_id,
        ?new_identity,
        pipe_write_id,
        "Added new identity to router_map."
      );
    } else {
      tracing::error!(
        handle = self.core.handle,
        pipe_read_id,
        "Could not find pipe_write_id for update_peer_identity. RouterMap may be inconsistent."
      );
      // If we don't have pipe_write_id, we cannot update router_map.
      // This is a significant issue.
    }

    // Update the pipe_to_identity map last
    p_to_id_guard.insert(pipe_read_id, new_identity);
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
