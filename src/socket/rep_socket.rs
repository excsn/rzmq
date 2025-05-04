// src/socket/rep_socket.rs

use crate::delegate_to_core;
// <<< ADDED RepSocket IMPLEMENTATION >>>
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags}; // Use Blob for routing ID?
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::FairQueue; // Use FairQueue for incoming requests
use crate::socket::ISocket;
use async_trait::async_trait;
use bitflags::bitflags;
use std::collections::{HashMap, VecDeque}; // To temporarily store routing parts
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, MutexGuard};

/// Information about the peer from which the last request was received.
#[derive(Debug, Clone)]
struct PeerInfo {
  /// The pipe ID the request arrived on (Core reads from this pipe).
  source_pipe_read_id: usize,
  /// Routing prefix frames (e.g., identity frame from ROUTER/DEALER).
  /// Stored in reverse order for easy prepending later.
  routing_prefix: Vec<Msg>, // Use Msg to preserve flags etc.
}

/// Represents the state of the REP socket's receive/send cycle.
#[derive(Debug, Clone)] // Clone needed if we put PeerInfo inside?
enum RepState {
  ReadyToReceive,            // Waiting for the next request.
  ReceivedRequest(PeerInfo), // Request received, holding info needed for reply.
}

#[derive(Debug)]
pub(crate) struct RepSocket {
  core: Arc<SocketCore>,
  incoming_request_queue: FairQueue, // Buffers incoming requests
  // Track the state of the request-reply cycle
  state: Mutex<RepState>,
  // Map Read Pipe ID -> Write Pipe ID (needed for sending reply)
  // This mapping is still necessary, even if routing info is stored in state.
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
}

impl RepSocket {
  /// Creates a new RepSocket.
  pub fn new(core: Arc<SocketCore>) -> Self {
    // TODO: Get RCVHWM from core options
    let initial_hwm = 1000;
    Self {
      core,
      incoming_request_queue: FairQueue::new(initial_hwm),
      state: Mutex::new(RepState::ReadyToReceive),
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
    }
  }

  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

// --- ISocket Implementation ---
#[async_trait]
impl ISocket for RepSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }

  fn mailbox(&self) -> &MailboxSender {
    self.core.mailbox_sender()
  }

  // --- API Method Implementations ---
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
  async fn recv(&self) -> Result<Msg, ZmqError> {
    let mut current_state_guard = self.state.lock().await;
    if !matches!(*current_state_guard, RepState::ReadyToReceive) {
      return Err(ZmqError::InvalidState(
        "REP socket must call send() before receiving again",
      ));
    }

    // Pop the *full* incoming request (including routing prefix) from the queue
    let full_request = match self.incoming_request_queue.pop_message().await? {
      Some(msg) => msg,
      None => {
        return Err(ZmqError::Internal(
          "Request queue closed unexpectedly".into(),
        ))
      }
    };

    // Now, parse the received message(s) to separate routing info from payload
    // ZMTP guarantees multi-part messages arrive contiguously. We expect handle_pipe_event
    // to push one "logical" message (which might be multi-part internally) to the queue.
    // For now, assume handle_pipe_event pushes a Vec<Msg> or similar if multi-part needed.
    // Let's simplify: assume incoming is single Msg, routing info is in metadata? NO.
    // Backtrack: FairQueue holds Msg. handle_pipe_event needs to reconstruct logical requests.

    // --- RETHINK handle_pipe_event / FairQueue interaction for multi-part ---
    // Option 1: FairQueue stores Vec<Msg>. handle_pipe_event collects frames until MORE flag is unset.
    // Option 2: FairQueue stores Msg. recv() loops calling pop_message until MORE flag is unset.
    // Option 3: Pass Pipe ID with message? FQ stores (usize, Msg). recv gets ID.

    // Let's try Option 2: recv loops here. Requires FQ stores single Msgs.
    let mut request_parts: Vec<Msg> = Vec::new();
    let mut source_pipe_read_id: Option<usize> = None;

    // Assume FQ stores Msg, and metadata *might* contain source pipe ID added by handle_pipe_event
    // TODO: Ensure handle_pipe_event adds PipeReadId metadata!
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct PipeReadId(usize); // Metadata key

    loop {
      let part = match self.incoming_request_queue.pop_message().await? {
        Some(msg) => msg,
        None => {
          return Err(ZmqError::Internal(
            "Request queue closed unexpectedly".into(),
          ))
        }
      };

      // Store source pipe from the *first* part
      if source_pipe_read_id.is_none() {
        // Attempt to get source_pipe_read_id from metadata
        // This requires handle_pipe_event to add it!
        if let Some(id_meta) = part.metadata().get::<PipeReadId>().await {
          source_pipe_read_id = Some(id_meta.0);
        } else {
          tracing::error!(
            handle = self.core.handle,
            "Received message part without PipeReadId metadata!"
          );
          // Cannot proceed without knowing the source
          // Discard parts received so far and return error?
          return Err(ZmqError::Internal(
            "Missing source pipe ID on request".into(),
          ));
        }
      }

      let is_last_part = !part.is_more();
      request_parts.push(part);
      if is_last_part {
        break;
      }
    }

    let source_pipe_read_id = source_pipe_read_id.unwrap(); // Should be set now

    // Separate routing prefix (all but the last part) from payload (last part)
    let payload = request_parts
      .pop()
      .ok_or_else(|| ZmqError::ProtocolViolation("Received empty message".into()))?;
    let routing_prefix = request_parts; // Remaining parts are prefix

    // Store peer info and transition state
    let peer_info = PeerInfo {
      source_pipe_read_id,
      routing_prefix, // Already in reverse order if we push()? No, keep as received.
    };
    *current_state_guard = RepState::ReceivedRequest(peer_info);

    tracing::trace!(
      handle = self.core.handle,
      pipe_id = source_pipe_read_id,
      "REP received request, ready to send reply"
    );
    Ok(payload) // Return only the actual payload to the user
  } // end recv

  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    let mut current_state_guard = self.state.lock().await;
    let peer_info = match &*current_state_guard {
      RepState::ReceivedRequest(info) => info.clone(), // Clone needed info
      _ => {
        return Err(ZmqError::InvalidState(
          "REP socket must call recv() before sending",
        ))
      }
    };
    // Important: Transition state *before* async operations below to prevent races
    *current_state_guard = RepState::ReadyToReceive;
    drop(current_state_guard); // Release lock

    // 1. Find the Write Pipe ID corresponding to the Source Read Pipe ID
    let pipe_write_id = {
      let map_guard = self.pipe_read_to_write_id.lock().await;
      match map_guard.get(&peer_info.source_pipe_read_id).copied() {
        Some(id) => id,
        None => {
          tracing::error!(handle=self.core.handle, pipe_read_id=peer_info.source_pipe_read_id, "REP send failed: Write pipe ID not found for source read pipe (peer likely disconnected)");
          // Peer disconnected after we received request but before sending reply.
          // State was already reset, just return error.
          return Err(ZmqError::ConnectionClosed); // Or a different error?
        }
      }
    };

    // 2. Get the sender channel from CoreState
    let pipe_tx = {
      let core_state_guard = self.core_state().await;
      match core_state_guard.get_pipe_sender(pipe_write_id) {
        Some(tx) => tx,
        None => {
          tracing::error!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            "REP send failed: Pipe sender not found"
          );
          // State inconsistency. Reset state? Error already returned above likely.
          return Err(ZmqError::Internal("Pipe sender consistency error".into()));
        }
      }
    };

    // 3. Prepend routing prefix frames (if any)
    let mut frames_to_send: VecDeque<Msg> = VecDeque::new();
    // Ensure LAST part sent has MORE flag unset
    msg.set_flags(MsgFlags::MORE); // Need mutable flags access
    frames_to_send.push_back(msg); // Payload goes last

    // Prepend routing prefix in reverse order they were received
    for mut prefix_msg in peer_info.routing_prefix.into_iter().rev() {
      prefix_msg.set_flags(MsgFlags::MORE); // Ensure MORE flag is set on prefix parts
      frames_to_send.push_front(prefix_msg);
    }

    // 4. Send the message frame(s) via the channel
    for frame in frames_to_send {
      if let Err(_e) = pipe_tx.send(frame).await {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "REP send failed: Pipe channel closed during multi-part send"
        );
        // Don't change state back, it's already ReadyToReceive
        return Err(ZmqError::ConnectionClosed);
      }
    }

    tracing::trace!(
      handle = self.core.handle,
      pipe_id = pipe_write_id,
      "REP sent reply"
    );
    Ok(())
  } // end send

  // --- Pattern Specific Options ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { mut msg, .. } => {
        // Need to add source pipe ID metadata BEFORE pushing to queue
        // Ensure ISocket trait has method to get core handle? Yes, core().
        // let core_handle = self.core.handle; // Need handle access
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        struct PipeReadId(usize);
        msg.metadata_mut().insert_typed(PipeReadId(pipe_id)).await; // Add metadata

        // Push message part into the queue for recv() to pick up
        // recv() will loop until MORE flag is unset.
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_id,
          msg_size = msg.size(),
          "REP pushing request part to FairQueue"
        );
        self.incoming_request_queue.push_message(msg).await?;
      }
      _ => { /* REP ignores other pipe events */ }
    }
    Ok(())
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    pipe_write_id: usize,
    _peer_identity: Option<&[u8]>,
  ) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "REP attaching pipe"
    );
    // Store mapping
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    // Notify FairQueue
    self.incoming_request_queue.pipe_attached(pipe_read_id);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "REP detaching pipe"
    );
    // Remove from map
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .remove(&pipe_read_id);
    // Notify FairQueue
    self.incoming_request_queue.pipe_detached(pipe_read_id);

    // Check if we were waiting for a reply FROM this pipe
    let mut current_state = self.state.lock().await;
    if let RepState::ReceivedRequest(ref peer_info) = *current_state {
      if peer_info.source_pipe_read_id == pipe_read_id {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_read_id,
          "Peer disconnected while REP socket held its request. Resetting state."
        );
        // Peer disconnected before we could send reply. Discard request state.
        *current_state = RepState::ReadyToReceive;
      }
    }
  }
} // end impl ISocket for RepSocket
