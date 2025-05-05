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
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, MutexGuard};
use tokio::time::timeout;

use super::options::SocketOptions;

/// Metadata key type for associating the source pipe ID with a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SourcePipeReadId(usize);

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
  pub fn new(core: Arc<SocketCore>, options: SocketOptions) -> Self {
    let queue_capacity = options.rcvhwm.max(1);
    Self {
      core,
      incoming_request_queue: FairQueue::new(queue_capacity),
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

    let rcvtimeo_opt: Option<Duration> = { self.core_state().await.options.rcvtimeo };

    // Need to loop reading parts until last part received, applying timeout to EACH part?
    // ZMQ RCVTIMEO usually applies to the *whole logical message* arrival.
    // Our FairQueue stores individual frames. This makes timeout complex.

    // --- RETHINK: Timeout on the *first* part read? ---
    let first_part_pop_future = self.incoming_request_queue.pop_message();
    let first_part_result = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => {
        timeout(duration, first_part_pop_future)
          .await
          .map_err(|_| ZmqError::Timeout)? // Map timeout error
      }
      _ => first_part_pop_future.await, // No timeout
    };

    let first_part = match first_part_result? {
      Some(msg) => msg,
      None => return Err(ZmqError::Internal("Request queue closed".into())),
    };
    // <<< NOTE: Renamed first_part variable here for clarity >>>

    let mut request_parts: Vec<Msg> = Vec::new();
    let mut source_pipe_read_id: Option<usize> = None;

    // Process first part
    // Use the externally defined struct
    if let Some(id_meta) = first_part.metadata().get::<SourcePipeReadId>().await {
      source_pipe_read_id = Some(id_meta.0);
    } else {
      // Handle missing metadata gracefully
      tracing::error!(handle = self.core.handle, msg = ?first_part, "REP recv: Message part missing expected SourcePipeReadId metadata!");
      // Decide how to handle: Return error? Panic (like now)? Drop message?
      // Returning an error seems safest.
      return Err(ZmqError::Internal(
        "Missing pipe ID metadata on received message".into(),
      ));
    }
    let mut is_last = !first_part.is_more();
    request_parts.push(first_part);

    // Loop for subsequent parts (no timeout applied here)
    while !is_last {
      let next_part = match self.incoming_request_queue.pop_message().await? {
        Some(msg) => msg,
        None => return Err(ZmqError::ProtocolViolation("Incomplete message received".into())),
      };
      // Verify subsequent parts also have metadata? Optional but safer
      if next_part.metadata().get::<SourcePipeReadId>().await.is_none() {
        tracing::error!(handle = self.core.handle, msg = ?next_part, "REP recv: Subsequent message part missing expected SourcePipeReadId metadata!");
        return Err(ZmqError::Internal(
          "Missing pipe ID metadata on subsequent message part".into(),
        ));
      }
      is_last = !next_part.is_more();
      request_parts.push(next_part);
    }

    // <<< This unwrap should now succeed if metadata is present >>>
    let source_pipe_read_id = source_pipe_read_id.expect("source_pipe_read_id should be Some if metadata check passed");

    let payload = request_parts
      .pop()
      .ok_or_else(|| ZmqError::ProtocolViolation("Received empty message".into()))?;
    let routing_prefix = request_parts;

    let peer_info = PeerInfo {
      source_pipe_read_id,
      routing_prefix,
    };
    *current_state_guard = RepState::ReceivedRequest(peer_info);

    tracing::trace!(
      handle = self.core.handle,
      pipe_id = source_pipe_read_id,
      "REP received request, ready to send reply"
    );
    Ok(payload)
  } // end recv

  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    let mut current_state_guard = self.state.lock().await;
    let peer_info = match &*current_state_guard {
      RepState::ReceivedRequest(info) => info.clone(), // Clone needed info
      _ => return Err(ZmqError::InvalidState("REP socket must call recv() before sending")),
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
          tracing::error!(
            handle = self.core.handle,
            pipe_read_id = peer_info.source_pipe_read_id,
            "REP send failed: Write pipe ID not found for source read pipe (peer likely disconnected)"
          );
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

    // Add payload (last frame)
    msg.set_flags(msg.flags() & !MsgFlags::MORE); // Ensure MORE is unset on payload
    frames_to_send.push_back(msg);

    // Prepend stored prefix frames (already in correct order)
    for mut prefix_msg in peer_info.routing_prefix.into_iter().rev() {
      // Iterate stored prefix in reverse to prepend
      prefix_msg.set_flags(prefix_msg.flags() | MsgFlags::MORE); // Ensure MORE is set
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

    tracing::trace!(handle = self.core.handle, pipe_id = pipe_write_id, "REP sent reply");
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
        tracing::debug!(handle=self.core.handle, pipe_id=pipe_id, "REQ handle_pipe_event called for PipeMessageReceived");
        // Need to add source pipe ID metadata BEFORE pushing to queue
        msg.metadata_mut().insert_typed(SourcePipeReadId(pipe_id)).await; // Add metadata

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

  async fn pipe_attached(&self, pipe_read_id: usize, pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
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
    self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);
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
