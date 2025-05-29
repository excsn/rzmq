// src/socket/router_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, CoreState, SocketCore};
use crate::socket::options::{SocketOptions, ROUTER_MANDATORY};
use crate::socket::patterns::{incoming_orchestrator::IncomingMessageOrchestrator, RouterMap}; // Use RouterMap and FairQueue
use crate::socket::ISocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{RwLock as ParkingLotRwLock, RwLockReadGuard};
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit};

use super::patterns::WritePipeCoordinator;

#[derive(Debug)]
struct ActiveFragmentedSend {
  target_pipe_id: usize,
  _permit: OwnedSemaphorePermit,
}

// Context for Router's static processing function
struct RouterMessageProcessCtx<'a> {
  // Provides synchronous read access to the identity map needed by the processor.
  identity_map_guard: RwLockReadGuard<'a, HashMap<usize, Blob>>,
  // Optional: Could pass core_handle for logging if the static fn needs it.
  // socket_core_handle: usize,
}

#[derive(Debug)]
pub(crate) struct RouterSocket {
  core: Arc<SocketCore>,
  router_map_for_send: RouterMap, // Maps Identity -> Write Pipe ID
  // The orchestrator handles all incoming logic: partial assembly, staging, main queue
  incoming_orchestrator: IncomingMessageOrchestrator,

  // Map Pipe Read ID -> Identity (needed for prepending on receive)
  // Duplicates RouterMap's reverse map but might be convenient here
  pipe_to_identity_shared_map: Arc<ParkingLotRwLock<HashMap<usize, Blob>>>,
  /// Set when the Identity frame is sent, cleared when the last payload frame is sent.
  current_send_target: TokioMutex<Option<ActiveFragmentedSend>>,
  pipe_send_coordinator: WritePipeCoordinator,
}

impl RouterSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let shared_pipe_to_identity_map = Arc::new(ParkingLotRwLock::new(HashMap::new()));

    let orchestrator = IncomingMessageOrchestrator::new(
      &core,
    );

    Self {
      core,
      router_map_for_send: RouterMap::new(), // This is for SENDING by identity
      incoming_orchestrator: orchestrator,
      pipe_to_identity_shared_map: shared_pipe_to_identity_map,
      current_send_target: TokioMutex::new(None),
      pipe_send_coordinator: WritePipeCoordinator::new(),
    }
  }

  fn core_state(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  /// Creates a temporary identity Blob from a pipe ID.
  /// TODO: Replace this with real identity management later.
  fn pipe_id_to_placeholder_identity(pipe_read_id: usize) -> Blob {
    Blob::from(pipe_read_id.to_be_bytes().to_vec())
  }

  // Static processing function for ROUTER messages
  fn process_raw_message_for_router(
    pipe_read_id: usize,
    mut raw_zmtp_message: Vec<Msg>, // e.g., [EMPTY_DELIM_MORE, PAYLOAD_NOMORE] from DEALER
    ctx: RouterMessageProcessCtx<'_>,
  ) -> Result<Vec<Msg>, ZmqError> {
    let identity_blob = ctx.identity_map_guard.get(&pipe_read_id).cloned().unwrap_or_else(|| {
      tracing::warn!(
        // handle = ctx.socket_core_handle, // If passed in ctx
        "Router transform: Identity for pipe {} not found in map, using placeholder.",
        pipe_read_id
      );
      Self::pipe_id_to_placeholder_identity(pipe_read_id)
    });

    let id_msg = Msg::from_vec(identity_blob.to_vec());
    // MORE flag for id_msg will be set by orchestrator.queue_application_message_frames later.

    // Strip leading empty delimiter if present (typical from DEALER)
    if !raw_zmtp_message.is_empty() && raw_zmtp_message[0].size() == 0 {
      tracing::trace!(
        // handle = ctx.socket_core_handle,
        "Router transform: Stripped empty ZMTP delimiter from pipe {}.",
        pipe_read_id
      );
      raw_zmtp_message.remove(0);
    }

    let mut application_message = Vec::with_capacity(1 + raw_zmtp_message.len());
    application_message.push(id_msg);
    application_message.extend(raw_zmtp_message);

    Ok(application_message) // Returns [IDENTITY, (stripped_delimiter_payload_frames)...]
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
    let core_opts = self.core_state().options.clone(); // Clone options for use
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
      let pipe_write_id = match self.router_map_for_send.get_pipe(&destination_id).await {
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

      let permit = self
        .pipe_send_coordinator
        .acquire_send_permit(pipe_write_id, timeout_opt)
        .await?;
      // Get sender channel
      let pipe_tx = {
        // Re-lock core_state briefly if needed, or ensure it's still valid
        // If we already cloned options, maybe we don't need to lock again just for sender if it's cached
        // But get_pipe_sender typically needs the lock.
        let core_state_guard = self.core_state();
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
          // Identity sent. Now send the empty delimiter.
          let mut delimiter_frame = Msg::new();
          delimiter_frame.set_flags(MsgFlags::MORE); // Delimiter always has MORE before payload.
          tracing::trace!(
            handle = self.core.handle,
            // destination_id is still in scope from outer block
            // ?destination_id, 
            pipe_id = pipe_write_id,
            "ROUTER sending empty delimiter after identity"
          );
          match send_msg_with_timeout(&pipe_tx, delimiter_frame, timeout_opt, self.core.handle, pipe_write_id).await {
            Ok(()) => {
              // Both identity and delimiter sent successfully.
              *current_target_guard = Some(ActiveFragmentedSend {
                target_pipe_id: pipe_write_id,
                _permit: permit, // Permit is moved here and will be held by current_target_guard
              });
              tracing::trace!(
                handle = self.core.handle,
                // ?destination_id, 
                pipe_id = pipe_write_id,
                "ROUTER send target locked (identity and delimiter sent)"
              );
              Ok(())
            }
            Err(e) => {
              tracing::warn!(handle = self.core.handle, pipe_id = pipe_write_id, "ROUTER send (delimiter) failed: {}", e);
              // Permit is dropped here implicitly as it's not moved into ActiveFragmentedSend.
              // current_target_guard remains None.
              if router_mandatory_opt {
                if matches!(e, ZmqError::ConnectionClosed) { Err(ZmqError::HostUnreachable("Peer disconnected during delimiter send".into())) } else { Err(e) }
              } else {
                // If not mandatory, and it failed (e.g. pipe gone), it's a silent drop for this message.
                Ok(())
              }
            }
          }
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
      let active_info = current_target_guard.as_ref().unwrap(); // is_some() check done
      let target_pipe_id = active_info.target_pipe_id;

      let pipe_tx = {
        let core_state_guard = self.core_state();
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
        Err(_e @ ZmqError::ConnectionClosed) => {
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
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };

    return self.incoming_orchestrator.recv_message(rcvtimeo_opt).await;
  }

  async fn send_multipart(&self, frames: Vec<Msg>) -> Result<(), ZmqError> {
    if frames.is_empty() {
      return Err(ZmqError::InvalidMessage("Cannot send an empty set of frames.".into()));
    }

    let destination_id_blob = Blob::from(frames[0].data().unwrap_or_default().to_vec());
    if destination_id_blob.is_empty() {
      return Err(ZmqError::InvalidMessage(
        "First frame (destination identity) cannot be empty.".into(),
      ));
    }

    let core_opts = self.core_state().options.clone();
    let timeout_opt: Option<Duration> = core_opts.sndtimeo;
    let router_mandatory_opt: bool = core_opts.router_mandatory;

    // Find target pipe write ID
    let pipe_write_id = match self.router_map_for_send.get_pipe(&destination_id_blob).await {
      Some(id) => id,
      None => {
        return if router_mandatory_opt {
          Err(ZmqError::HostUnreachable(format!(
            "Peer {:?} not connected or identity unknown (ROUTER_MANDATORY=true)",
            destination_id_blob
          )))
        } else {
          tracing::debug!(
            handle = self.core.handle,
            ?destination_id_blob,
            "ROUTER send_full_message (mandatory=false): Destination unknown. Silently dropping."
          );
          Ok(()) // Silently drop
        };
      }
    };

    // Check if the target pipe is busy with a fragmented send from the `send()` method.
    // The subsequent acquire_send_permit will handle the actual waiting if needed.
    {
      let current_send_guard = self.current_send_target.lock().await;
      if let Some(active_info) = &*current_send_guard {
        if active_info.target_pipe_id == pipe_write_id {
          tracing::debug!("send_multipart for pipe {} will wait on its per-pipe semaphore because a fragmented send() is active to it.", pipe_write_id);
        }
      }
    }

    // Acquire the per-pipe send permit. This blocks if the pipe is busy
    // from EITHER a fragmented send() OR another send_multipart().
    let _permit = self
      .pipe_send_coordinator
      .acquire_send_permit(pipe_write_id, timeout_opt)
      .await?;
    // If acquire_send_permit re  turns error, propagate it.

    let pipe_tx = {
      let core_state_guard = self.core_state();
      match core_state_guard.get_pipe_sender(pipe_write_id) {
        Some(tx) => tx,
        None => return Err(ZmqError::HostUnreachable("Peer disconnected before send".into())),
      }
    };

    // --- Frame 1: Must be the Destination Identity ---
    // We will consume the first frame from the input `frames` vector.
    // To do this without complex Vec manipulation if `frames` is large,
    // we iterate through an owned version or use an iterator.
    // For simplicity, let's assume frames isn't excessively large.
    let mut frames_iter = frames.into_iter(); // Consumes the input Vec

    let mut destination_identity_frame = match frames_iter.next() {
      Some(f) => f,
      None => return Err(ZmqError::Internal("frames vector became empty unexpectedly".into())), // Should be caught by initial is_empty
    };

    // Ensure the identity frame has the MORE flag, as delimiter and payload will follow.
    destination_identity_frame.set_flags(destination_identity_frame.flags() | MsgFlags::MORE);

    // --- Actually send Identity Frame (using the internal send_msg_with_timeout) ---
    tracing::trace!(handle = self.core.handle, dest_id = ?destination_id_blob, pipe_id = pipe_write_id, "Sending full_message IDENTITY");
    send_msg_with_timeout(
      &pipe_tx,
      destination_identity_frame,
      timeout_opt,
      self.core.handle,
      pipe_write_id,
    )
    .await?;

    // --- Send Empty Delimiter Frame ---
    let mut delimiter_frame = Msg::new();
    delimiter_frame.set_flags(MsgFlags::MORE); // Delimiter always has MORE before payload

    tracing::trace!(handle = self.core.handle, dest_id = ?destination_id_blob, pipe_id = pipe_write_id, "Sending full_message DELIMITER");
    send_msg_with_timeout(&pipe_tx, delimiter_frame, timeout_opt, self.core.handle, pipe_write_id).await?;

    // --- Send Remaining Frames (Actual Payload) ---
    // The remaining frames in `frames_iter` are the payload.
    // The caller of `send_full_message` is responsible for ensuring MORE flags are
    // correctly set on these payload frames (all but the last should have MORE).
    let remaining_payload_frames: Vec<Msg> = frames_iter.collect();

    if remaining_payload_frames.is_empty() {
      // If app_payload was empty, we must send one final frame (can be empty) without MORE.
      let last_empty_payload_frame = Msg::new();
      tracing::trace!(handle = self.core.handle, dest_id = ?destination_id_blob, pipe_id = pipe_write_id, "Sending full_message LAST EMPTY PAYLOAD");
      send_msg_with_timeout(
        &pipe_tx,
        last_empty_payload_frame,
        timeout_opt,
        self.core.handle,
        pipe_write_id,
      )
      .await?;
    } else {
      let num_payload_to_send = remaining_payload_frames.len();
      for (i, mut payload_frame) in remaining_payload_frames.into_iter().enumerate() {
        // Ensure the very last frame of the entire message sequence does not have MORE.
        if i == num_payload_to_send - 1 {
          payload_frame.set_flags(payload_frame.flags() & !MsgFlags::MORE);
        }
        // else: trust caller set MORE correctly on intermediate payload frames.

        tracing::trace!(handle = self.core.handle, dest_id = ?destination_id_blob, pipe_id = pipe_write_id, part=i, flags=?payload_frame.flags(), "Sending full_message PAYLOAD frame");
        send_msg_with_timeout(&pipe_tx, payload_frame, timeout_opt, self.core.handle, pipe_write_id).await?;
      }
    }

    // --- All parts sent successfully, unlock target ---
    tracing::trace!(handle = self.core.handle, dest_id = ?destination_id_blob, pipe_id = pipe_write_id, "Full message sent successfully");
    Ok(())
  }

  /// Receives all frames of a complete logical ZMQ message.
  /// Delegates to IncomingMessageOrchestrator.
  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };
    // Call the new method on the orchestrator that handles multipart logic.
    self.incoming_orchestrator.recv_logical_message(rcvtimeo_opt).await
  }

  // --- Pattern Specific Options ---
  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    if option == ROUTER_MANDATORY {
      let val = crate::socket::options::parse_bool_option(value)?;
      self.core.core_state.write().options.router_mandatory = val;
      tracing::debug!(handle = self.core.handle, "ROUTER_MANDATORY set to {}", val);
      Ok(())
    } else {
      Err(ZmqError::UnsupportedOption(option))
    }
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    if option == ROUTER_MANDATORY {
      let val = self.core_state().options.router_mandatory;
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
        // 1. Accumulate frame.
        if let Some(raw_zmtp_message_vec) = self.incoming_orchestrator.accumulate_pipe_frame(pipe_read_id, msg)? {
          // accumulate is sync now

          // 2. Process the raw ZMTP message using ROUTER's specific logic
          let app_logical_message = {
            // Scope for read guard
            let identity_map_guard = self.pipe_to_identity_shared_map.read(); // parking_lot read guard
            let router_ctx = RouterMessageProcessCtx {
              identity_map_guard,
              // socket_core_handle: self.core.handle, // If needed
            };
            Self::process_raw_message_for_router(pipe_read_id, raw_zmtp_message_vec, router_ctx)?
          }; // Guard dropped here

          // 3. Queue the processed application-level message frames
          self
            .incoming_orchestrator
            .queue_application_message_frames(pipe_read_id, app_logical_message)
            .await?;
        }
      }
      _ => { /* ROUTER ignores other direct pipe events */ }
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
      .router_map_for_send
      .add_peer(initial_identity_to_use.clone(), pipe_read_id, pipe_write_id)
      .await;

    // Add to router_map_for_send (for sending)
    self
      .router_map_for_send
      .add_peer(initial_identity_to_use.clone(), pipe_read_id, pipe_write_id)
      .await;

    // Add to shared pipe_to_identity_shared_map (for orchestrator's receive path)
    self
      .pipe_to_identity_shared_map
      .write()
      .insert(pipe_read_id, initial_identity_to_use);

    self.pipe_send_coordinator.add_pipe(pipe_write_id).await;
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

    tracing::debug!(handle = self.core.handle, pipe_read_id, new_identity = ?new_identity, "ROUTER updating peer identity");

    let old_identity_opt;
    {
      let p_to_id_guard = self.pipe_to_identity_shared_map.write(); // parking_lot RwLock write guard
      old_identity_opt = p_to_id_guard.get(&pipe_read_id).cloned();

      if old_identity_opt.as_ref() == Some(&new_identity) {
        return; // No change
      }
    }

    let pipe_write_id_opt = if let Some(ref old_id) = old_identity_opt {
      self.router_map_for_send.get_pipe(old_id).await
    } else {
      None
    };

    if let Some(pipe_write_id) = pipe_write_id_opt {
      if let Some(old_id) = old_identity_opt.as_ref() {
        let mut id_to_pipe_w_guard = self.router_map_for_send.identity_to_pipe.lock().await; // TokioMutex
        id_to_pipe_w_guard.remove(old_id);
        // drop(id_to_pipe_w_guard) happens implicitly
      }
      let mut id_to_pipe_w_guard = self.router_map_for_send.identity_to_pipe.lock().await; // TokioMutex
      id_to_pipe_w_guard.insert(new_identity.clone(), pipe_write_id);
    } else {
      tracing::error!(
        handle = self.core.handle,
        pipe_read_id,
        "Could not find pipe_write_id for update_peer_identity in router_map_for_send. Map may be inconsistent."
      );
    }

    // Re-acquire lock to update the shared map
    self
      .pipe_to_identity_shared_map
      .write()
      .insert(pipe_read_id, new_identity);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!("ROUTER detaching pipe_read_id: {}", pipe_read_id);

    let removed_identity_from_shared_map = self.pipe_to_identity_shared_map.write().remove(&pipe_read_id);

    let pipe_write_id_to_clean = if let Some(ref identity) = removed_identity_from_shared_map {
      self.router_map_for_send.get_pipe(identity).await // Read lock on router_map_for_send.identity_to_pipe
    } else {
      None
    };

    // This will remove from both maps inside router_map_for_send
    self.router_map_for_send.remove_peer_by_read_pipe(pipe_read_id).await;

    if let Some(write_id) = pipe_write_id_to_clean {
      if let Some(semaphore) = self.pipe_send_coordinator.remove_pipe(write_id).await {
        semaphore.close();
      }
      let mut active_frag_guard = self.current_send_target.lock().await; // TokioMutex
      if let Some(active_info) = &*active_frag_guard {
        if active_info.target_pipe_id == write_id {
          *active_frag_guard = None;
        }
      }
    }

    // Inform orchestrator to clear any partial messages for this pipe
    self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
  }
} // end impl ISocket for RouterSocket
