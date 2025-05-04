// src/socket/router_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::{FairQueue, RouterMap}; // Use RouterMap and FairQueue
use crate::socket::ISocket;
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, MutexGuard};

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
  pub fn new(core: Arc<SocketCore>) -> Self {
    // TODO: Get RCVHWM from core options
    let initial_hwm = 1000;
    Self {
      core,
      router_map: RouterMap::new(),
      incoming_queue: FairQueue::new(initial_hwm),
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
  fn pipe_id_to_identity(pipe_read_id: usize) -> Blob {
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

    let mut current_target_guard = self.current_send_target.lock().await;

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
      if let Some(pipe_write_id) = self.router_map.get_pipe(&destination_id).await {
        // Store target for subsequent payload frames
        *current_target_guard = Some(pipe_write_id);
        tracing::trace!(
          handle = self.core.handle,
          ?destination_id,
          pipe_id = pipe_write_id,
          "ROUTER send target locked"
        );
        // Send the identity frame itself down the pipe
        let pipe_tx = {
          let core_state_guard = self.core_state().await;
          match core_state_guard.get_pipe_sender(pipe_write_id) {
            Some(tx) => tx,
            None => {
              *current_target_guard = None; // Clear target
              return Err(ZmqError::Internal("Pipe sender consistency error".into()));
            }
          }
        };
        // Ensure MORE flag IS set on the identity frame being sent
        msg.set_flags(msg.flags() | MsgFlags::MORE);
        if let Err(_e) = pipe_tx.send(msg).await {
          *current_target_guard = None; // Clear target on error
          self
            .router_map
            .remove_peer_by_read_pipe(pipe_write_id)
            .await; // Need read ID? Complex cleanup.
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            "ROUTER send (identity) failed: Pipe channel closed"
          );
          return Err(ZmqError::ConnectionClosed);
        }
        Ok(()) // Ready for payload frame(s)
      } else {
        // Identity not found
        tracing::debug!(
          handle = self.core.handle,
          ?destination_id,
          "ROUTER send failed: Destination identity unknown"
        );
        // TODO: Handle ZMQ_ROUTER_MANDATORY etc. options
        Err(ZmqError::HostUnreachable(
          "Peer identity not connected".into(),
        ))
      }
    } else {
      // --- Subsequent Frame(s): Payload ---
      let target_pipe_id = current_target_guard.unwrap(); // We know it's Some

      // Get sender
      let pipe_tx = {
        let core_state_guard = self.core_state().await;
        match core_state_guard.get_pipe_sender(target_pipe_id) {
          Some(tx) => tx,
          None => {
            /* ... consistency error ... */
            *current_target_guard = None;
            return Err(ZmqError::Internal("Pipe sender consistency error".into()));
          }
        }
      };

      // Ensure MORE flag is correct for the *final* user payload part
      let is_last_part = !msg.is_more();
      // The ZMTP frame for this payload part should have the same MORE flag as the user provided msg.

      // Send the payload frame
      let send_result = pipe_tx.send(msg).await;

      // If this was the last part, clear the target pipe ID
      if is_last_part {
        *current_target_guard = None;
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = target_pipe_id,
          "ROUTER send target released"
        );
      }

      send_result.map_err(|_| {
        // Pipe likely closed by peer
        *current_target_guard = None; // Clear target
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = target_pipe_id,
          "ROUTER send (payload) failed: Pipe channel closed"
        );
        // TODO: Remove peer from RouterMap? Need better cleanup coordination.
        ZmqError::ConnectionClosed
      })
    } // end else (payload frame)
  } // end send

  async fn recv(&self) -> Result<Msg, ZmqError> {
    let mut message_parts: Vec<Msg> = Vec::new();

    // 1. Receive the first part - this MUST be the Identity
    let first_part = match self.incoming_queue.pop_message().await? {
      Some(msg) => msg,
      None => return Err(ZmqError::Internal("Receive queue closed".into())),
    };

    // Check if it looks like an identity (e.g., non-empty? maybe store metadata?)
    // We assume the first part IS the identity based on handle_pipe_event logic.
    message_parts.push(first_part);

    // 2. Receive subsequent parts until MORE flag is not set
    loop {
      let next_part = match self.incoming_queue.pop_message().await? {
        Some(msg) => msg,
        None => {
          // Queue closed unexpectedly after receiving only identity/partial message
          tracing::error!(
            handle = self.core.handle,
            "Receive queue closed mid-message"
          );
          return Err(ZmqError::ProtocolViolation(
            "Incomplete message received".into(),
          ));
        }
      };
      let is_last = !next_part.is_more();
      message_parts.push(next_part);
      if is_last {
        break; // End of logical message
      }
    }

    // Combine parts? No, ZMQ recv returns one part at a time usually.
    // RETHINK: The user calls recv() multiple times for multi-part.
    // Our FairQueue stores individual Msg parts.
    // `recv` should just return the *next available* part from the queue.

    // --- Corrected recv() Logic ---
    // Get the next frame (could be ID or payload part) from the queue
    match self.incoming_queue.pop_message().await? {
      Some(msg) => {
        tracing::trace!(handle=self.core.handle, msg_size=msg.size(), flags=?msg.flags(), "ROUTER recv returning message part");
        Ok(msg) // Return the single frame (could be ID or data)
      }
      None => Err(ZmqError::Internal("Receive queue closed".into())),
    }
    // The USER is responsible for calling recv() repeatedly and checking the MORE flag
    // to reconstruct the full message (Identity + Payload Parts).
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
        let is_last_part = !msg.is_more();
        buffer.push(msg);

        if is_last_part {
          // Message complete, process it
          let complete_message_parts = partial_guard.remove(&pipe_read_id).unwrap();
          drop(partial_guard); // Release lock

          // Get identity associated with this pipe
          let identity_blob = {
            let map_guard = self.pipe_to_identity.lock().await; // Use local map
            map_guard.get(&pipe_read_id).cloned().unwrap_or_else(|| {
              tracing::warn!(
                handle = self.core.handle,
                pipe_id = pipe_read_id,
                "Could not find identity for pipe, using placeholder."
              );
              Self::pipe_id_to_identity(pipe_read_id) // Fallback
            })
          };

          // --- Push "Identity frame" then "Payload frame(s)" to FairQueue ---
          // 1. Push Identity Frame
          let mut id_msg = Msg::from_vec(identity_blob.to_vec());
          id_msg.set_flags(MsgFlags::MORE); // Identity frame *always* has MORE flag for ROUTER recv
          self.incoming_queue.push_message(id_msg).await?;

          // 2. Push Original Payload Frame(s)
          let num_payload_parts = complete_message_parts.len(); // Get length first
          for (i, mut part) in complete_message_parts.into_iter().enumerate() {
            // Consumes vector
            // Ensure MORE flag is correct relative to *original* message parts
            if i < num_payload_parts - 1 {
              // Use stored length
              part.set_flags(part.flags() | MsgFlags::MORE);
            } else {
              part.set_flags(part.flags() & !MsgFlags::MORE);
            }
            self.incoming_queue.push_message(part).await?;
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

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    pipe_write_id: usize,
    peer_identity_opt: Option<&[u8]>,
  ) {
    // Determine identity
    let identity = match peer_identity_opt {
      Some(id_bytes) => Blob::from(id_bytes.to_vec()),
      None => {
        tracing::warn!(
          handle = self.core.handle,
          pipe_read_id = pipe_read_id,
          "Peer did not provide ROUTING_ID/Identity, generating one."
        );
        Self::pipe_id_to_identity(pipe_read_id) // Fallback to pipe ID
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
    self
      .pipe_to_identity
      .lock()
      .await
      .insert(pipe_read_id, identity);
    // Notify FairQueue
    self.incoming_queue.pipe_attached(pipe_read_id);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "ROUTER detaching pipe"
    );
    // Remove peer from router map and local reverse map
    self.router_map.remove_peer_by_read_pipe(pipe_read_id).await; // Removes both forward/reverse in helper
    self.pipe_to_identity.lock().await.remove(&pipe_read_id); // Remove from local map too
                                                              // Notify FairQueue
    self.incoming_queue.pipe_detached(pipe_read_id);
    // Clear any partial messages from this pipe
    self.partial_incoming.lock().await.remove(&pipe_read_id);
    // Clear pending send target if it was for this peer? Difficult without write_id here.
    // TODO: Improve cleanup of current_send_target on peer disconnect.
  }
} // end impl ISocket for RouterSocket
