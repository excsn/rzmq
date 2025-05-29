use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, CoreState, SocketCore};
use crate::socket::patterns::FairQueue;
use crate::socket::{ISocket, SourcePipeReadId};

use async_trait::async_trait;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex};
use tokio::time::timeout;

#[derive(Debug, Clone)]
struct PeerInfo {
  source_pipe_read_id: usize,
  routing_prefix: Vec<Msg>,
}

#[derive(Debug, Clone)]
enum RepState {
  ReadyToReceive,
  ReceivedRequest(PeerInfo),
}

#[derive(Debug)]
pub(crate) struct RepSocket {
  core: Arc<SocketCore>,
  incoming_request_queue: FairQueue,
  state: Mutex<RepState>,
  pipe_read_to_write_id: RwLock<HashMap<usize, usize>>,
}

impl RepSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let queue_capacity = core.core_state.read().options.rcvhwm.max(1);
    Self {
      core,
      incoming_request_queue: FairQueue::new(queue_capacity),
      state: Mutex::new(RepState::ReadyToReceive),
      pipe_read_to_write_id: RwLock::new(HashMap::new()),
    }
  }

  fn core_state(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }
}

#[async_trait]
impl ISocket for RepSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }

  fn mailbox(&self) -> MailboxSender {
    self.core.command_sender()
  }

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

  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    
    let mut current_state_guard = self.state.lock().await;
    let peer_to_reply_to = match &*current_state_guard {
      RepState::ReceivedRequest(info) => info.clone(),
      _ => {
        return Err(ZmqError::InvalidState(
          "REP socket must call recv() a request before sending a reply",
        ))
      }
    };
    *current_state_guard = RepState::ReadyToReceive;
    drop(current_state_guard);

    let pipe_write_id_for_reply = {
      let map_guard = self.pipe_read_to_write_id.read();
      match map_guard.get(&peer_to_reply_to.source_pipe_read_id).copied() {
        Some(id) => id,
        None => {
          tracing::error!(
            handle = self.core.handle,
            pipe_read_id = peer_to_reply_to.source_pipe_read_id,
            "REP send failed: Write pipe ID not found for source read pipe."
          );
          return Err(ZmqError::HostUnreachable(
            "Peer disconnected before reply could be sent".into(),
          ));
        }
      }
    };

    let pipe_tx = {
      let core_state_guard = self.core_state();
      match core_state_guard.get_pipe_sender(pipe_write_id_for_reply) {
        Some(tx) => tx,
        None => {
          tracing::error!(
            handle = self.core.handle,
            pipe_id = pipe_write_id_for_reply,
            "REP send failed: Pipe sender channel not found in CoreState."
          );
          return Err(ZmqError::Internal(
            "Pipe sender consistency error during REP send".into(),
          ));
        }
      }
    };

    let mut frames_to_send: VecDeque<Msg> = VecDeque::new();
    msg.set_flags(msg.flags() & !MsgFlags::MORE);
    frames_to_send.push_back(msg);

    for mut prefix_msg_part in peer_to_reply_to.routing_prefix.into_iter().rev() {
      prefix_msg_part.set_flags(prefix_msg_part.flags() | MsgFlags::MORE);
      frames_to_send.push_front(prefix_msg_part);
    }

    let timeout_opt: Option<Duration> = { self.core_state().options.sndtimeo };

    for frame_to_send in frames_to_send {
      if let Err(e) = send_msg_with_timeout(
        &pipe_tx,
        frame_to_send,
        timeout_opt,
        self.core.handle,
        pipe_write_id_for_reply,
      )
      .await
      {
        tracing::warn!(handle = self.core.handle, pipe_id = pipe_write_id_for_reply, error = %e, "REP send failed during multi-part reply.");
        return Err(e);
      }
    }

    tracing::trace!(
      handle = self.core.handle,
      pipe_id = pipe_write_id_for_reply,
      "REP sent complete reply."
    );
    Ok(())
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let current_state_guard = self.state.lock().await;
    if !matches!(*current_state_guard, RepState::ReadyToReceive) {
      return Err(ZmqError::InvalidState(
        "REP socket must call send() before receiving again",
      ));
    }
    drop(current_state_guard);

    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };

    let first_part;
    let mut source_pipe_read_id_from_meta: Option<usize> = None;

    let pop_first_future = self.incoming_request_queue.pop_message();
    match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => match timeout(duration, pop_first_future).await {
        Ok(Ok(Some(msg))) => first_part = msg,
        Ok(Ok(None)) => {
          return Err(ZmqError::Internal(
            "Request queue closed while waiting for first part".into(),
          ))
        }
        Ok(Err(e)) => return Err(e),
        Err(_timeout_elapsed) => return Err(ZmqError::Timeout),
      },
      _ => match pop_first_future.await? {
        Some(msg) => first_part = msg,
        None => {
          return Err(ZmqError::Internal(
            "Request queue closed while waiting for first part".into(),
          ))
        }
      },
    }

    if let Some(id_meta) = first_part.metadata().get::<SourcePipeReadId>().await {
      source_pipe_read_id_from_meta = Some(id_meta.0);
    } else {
      tracing::error!(handle = self.core.handle, msg = ?first_part, "REP recv: First message part missing SourcePipeReadId metadata!");
      return Err(ZmqError::Internal(
        "Missing pipe ID metadata on received message part".into(),
      ));
    }
    let source_pipe_id = source_pipe_read_id_from_meta.unwrap();

    let mut received_frames: Vec<Msg> = Vec::new();
    received_frames.push(first_part);
    let mut more_parts_expected = received_frames.last().unwrap().is_more();

    while more_parts_expected {
      match self.incoming_request_queue.pop_message().await? {
        Some(next_msg_part) => {
          if next_msg_part
            .metadata()
            .get::<SourcePipeReadId>()
            .await
            .map_or(true, |meta| meta.0 != source_pipe_id)
          {
            tracing::error!(handle = self.core.handle, msg = ?next_msg_part, expected_pipe_id = source_pipe_id, "REP recv: Subsequent message part from different/missing pipe ID!");
            return Err(ZmqError::ProtocolViolation(
              "Interleaved message parts from different peers".into(),
            ));
          }
          more_parts_expected = next_msg_part.is_more();
          received_frames.push(next_msg_part);
        }
        None => {
          tracing::error!(
            handle = self.core.handle,
            "REP recv: Request queue closed while expecting more message parts."
          );
          return Err(ZmqError::ProtocolViolation(
            "Incomplete multi-part message received: queue closed".into(),
          ));
        }
      }
    }

    let actual_payload = received_frames
      .pop()
      .ok_or_else(|| ZmqError::ProtocolViolation("Received an empty logical message (no frames)".into()))?;
    let routing_prefix_frames = received_frames; // Remaining frames are the prefix

    let mut current_state_guard = self.state.lock().await;
    if !matches!(*current_state_guard, RepState::ReadyToReceive) {
      tracing::error!(handle = self.core.handle, state = ?*current_state_guard, "REP state changed unexpectedly while assembling request. Protocol violation or internal error.");
      return Err(ZmqError::InvalidState(
        "REP socket state changed mid-receive; request discarded",
      ));
    }

    let peer_info = PeerInfo {
      source_pipe_read_id: source_pipe_id,
      routing_prefix: routing_prefix_frames.clone(), // Clone here for logging if needed later
    };
    *current_state_guard = RepState::ReceivedRequest(peer_info);
    drop(current_state_guard);

    // Pre-calculate the value that involves an await.
    let num_prefix_frames_for_log = actual_payload
      .metadata()
      .get::<SourcePipeReadId>() // This is an Arc<SourcePipeReadId>
      .await // Await the future to get Option<Arc<SourcePipeReadId>>
      .map_or(0, |_| routing_prefix_frames.len()); // Now this part is synchronous

    tracing::trace!(
      handle = self.core.handle,
      pipe_id = source_pipe_id,
      num_prefix_frames = num_prefix_frames_for_log, // Use the pre-calculated value
      "REP received complete request, ready to send reply"
    );
    Ok(actual_payload)
  }

  async fn send_multipart(&self, payload_frames: Vec<Msg>) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let mut user_payload_frames = payload_frames;
    if user_payload_frames.is_empty() {
      // Ensure at least one payload frame (can be empty) if user provides empty Vec
      user_payload_frames.push(Msg::new());
    }

    let peer_to_reply_to: PeerInfo;
    // Lock state to check if ready to send and to retrieve PeerInfo
    {
      let op_state_guard = self.state.lock().await;
      match &*op_state_guard {
        RepState::ReceivedRequest(info) => peer_to_reply_to = info.clone(),
        _ => {
          return Err(ZmqError::InvalidState(
            "REP socket must recv() a request before sending a reply",
          ))
        }
      }
    }

    let pipe_write_id_for_reply_result = self
      .pipe_read_to_write_id
      .read()
      .get(&peer_to_reply_to.source_pipe_read_id)
      .copied();
    
    let pipe_write_id_for_reply = match pipe_write_id_for_reply_result
    {
      Some(id) => id,
      None => {
        *self.state.lock().await = RepState::ReadyToReceive; // Reset state
        return Err(ZmqError::HostUnreachable(
          "Peer disconnected before reply could be sent".into(),
        ));
      }
    };


    let pipe_tx_result = self.core_state().get_pipe_sender(pipe_write_id_for_reply);
    let pipe_tx = match pipe_tx_result {
      Some(tx) => tx,
      None => {
        *self.state.lock().await = RepState::ReadyToReceive; // Reset state
        return Err(ZmqError::Internal(
          "Pipe sender consistency error during REP send".into(),
        ));
      }
    };

    // Construct the full message sequence: routing prefix + user payload
    let mut frames_to_send: Vec<Msg> =
      Vec::with_capacity(peer_to_reply_to.routing_prefix.len() + user_payload_frames.len());
    frames_to_send.extend(peer_to_reply_to.routing_prefix);
    frames_to_send.extend(user_payload_frames);

    // Set MORE flags correctly for the entire sequence
    let num_total_frames = frames_to_send.len();
    if num_total_frames == 0 {
      // Should not happen due to user_payload_frames logic, but defensive
      *self.state.lock().await = RepState::ReadyToReceive;
      return Ok(()); // Sending zero frames effectively
    }

    for (i, frame) in frames_to_send.iter_mut().enumerate() {
      if i < num_total_frames - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
    }

    let timeout_opt: Option<Duration> = { self.core_state().options.sndtimeo };

    // Attempt to send all frames
    let send_attempt_result: Result<(), ZmqError> = async {
      for frame_to_send in frames_to_send {
        // frames_to_send is moved into the async block
        send_msg_with_timeout(
          &pipe_tx,
          frame_to_send,
          timeout_opt,
          self.core.handle,
          pipe_write_id_for_reply,
        )
        .await?; // Propagate error immediately if a part fails
      }
      Ok(())
    }
    .await;

    // Regardless of send success or failure, REP transitions back to ReadyToReceive
    // after an attempt to send a reply.
    *self.state.lock().await = RepState::ReadyToReceive;

    if let Err(ref e) = send_attempt_result {
      tracing::warn!(handle = self.core.handle, pipe_id = pipe_write_id_for_reply, error = %e, "REP send_multipart: Send failed. State reset.");
    } else {
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_write_id_for_reply,
        "REP send_multipart: Sent complete reply. State reset."
      );
    }

    send_attempt_result // Return the result of the send attempt
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    // Initial state check: Must be ReadyToReceive.
    {
      // Scoped lock
      let op_state_guard = self.state.lock().await;
      if !matches!(*op_state_guard, RepState::ReadyToReceive) {
        return Err(ZmqError::InvalidState(
          "REP socket must call send() before receiving again",
        ));
      }
    } // Lock released

    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };

    // --- Special handling for RCVTIMEO = 0 (non-blocking) ---
    if rcvtimeo_opt == Some(Duration::ZERO) {
      match self.incoming_request_queue.try_pop_message() {
        Ok(Some(first_frame)) => {
          // Got the first frame non-blockingly.
          // Now, *all subsequent frames for this logical message must also be immediately available*.
          let mut routing_prefix_frames: Vec<Msg> = Vec::new();
          let mut received_payload_frames: Vec<Msg> = Vec::new();
          let mut source_pipe_id_from_meta: Option<usize> = None;
          let mut delimiter_found = false;
          let mut current_frame_for_nonblocking_path = first_frame;

          loop {
            // Collect subsequent frames non-blockingly
            let frame_source_id = match current_frame_for_nonblocking_path
              .metadata()
              .get::<SourcePipeReadId>()
              .await
            {
              Some(id_meta) => id_meta.0,
              None => {
                return Err(ZmqError::ProtocolViolation(
                  "Missing pipe ID on request frame (RCVTIMEO=0 path)".into(),
                ))
              }
            };
            if source_pipe_id_from_meta.is_none() {
              source_pipe_id_from_meta = Some(frame_source_id);
            } else if source_pipe_id_from_meta != Some(frame_source_id) {
              return Err(ZmqError::ProtocolViolation(
                "Interleaved request parts (RCVTIMEO=0 path)".into(),
              ));
            }

            let is_more = current_frame_for_nonblocking_path.is_more();

            if !delimiter_found {
              let is_delimiter = current_frame_for_nonblocking_path.size() == 0;
              routing_prefix_frames.push(current_frame_for_nonblocking_path);
              if is_delimiter {
                delimiter_found = true;
              }
              if !is_more {
                // End of ZMTP message
                if !delimiter_found {
                  received_payload_frames = std::mem::take(&mut routing_prefix_frames);
                }
                break;
              }
            } else {
              received_payload_frames.push(current_frame_for_nonblocking_path);
              if !is_more {
                break;
              }
            }

            // Try to get next frame non-blockingly for RCVTIMEO=0
            match self.incoming_request_queue.try_pop_message() {
              Ok(Some(next_frame)) => current_frame_for_nonblocking_path = next_frame,
              Ok(None) => {
                return Err(ZmqError::ProtocolViolation(
                  "Incomplete multipart request (RCVTIMEO=0, subsequent part missing)".into(),
                ))
              } // Incomplete
              Err(e) => return Err(e), // Queue error
            }
          }
          // If loop finished, complete message received non-blockingly
          let final_source_pipe_id = source_pipe_id_from_meta.unwrap(); // Must be Some if first_frame was processed
          let peer_info = PeerInfo {
            source_pipe_read_id: final_source_pipe_id,
            routing_prefix: routing_prefix_frames,
          };
          *self.state.lock().await = RepState::ReceivedRequest(peer_info);
          return Ok(received_payload_frames);
        }
        Ok(None) => return Err(ZmqError::ResourceLimitReached), // No message available non-blockingly
        Err(e) => return Err(e),                                // Error from try_pop_message (e.g., queue closed)
      }
    }

    // --- Handling for RCVTIMEO > 0 or RCVTIMEO = None (infinite) ---
    let core_logic_future = async {
      let mut routing_prefix_frames: Vec<Msg> = Vec::new();
      let mut received_payload_frames: Vec<Msg> = Vec::new();
      let mut source_pipe_id_from_meta: Option<usize> = None;
      let mut delimiter_found = false;

      loop {
        // Collect all frames of one logical request
        let mut current_frame = match self.incoming_request_queue.pop_message().await {
          Ok(Some(msg)) => msg,
          Ok(None) => {
            return Err(ZmqError::Internal(
              "REP: Request queue closed mid-request (unexpected)".into(),
            ))
          }
          Err(e) => return Err(e),
        };

        let frame_source_id = match current_frame.metadata().get::<SourcePipeReadId>().await {
          Some(id_meta) => id_meta.0,
          None => return Err(ZmqError::ProtocolViolation("Missing pipe ID on request frame".into())),
        };

        if source_pipe_id_from_meta.is_none() {
          source_pipe_id_from_meta = Some(frame_source_id);
        } else if source_pipe_id_from_meta != Some(frame_source_id) {
          // This indicates a bug in FairQueue or handle_pipe_event if messages from different sources get interleaved
          // for REQ, as handle_pipe_event pushes to FairQueue and REP pulls one logical message at a time.
          // However, if a peer disconnects and another connects quickly, SourcePipeReadId could change.
          // The state transition logic should handle this.
          tracing::error!(handle = self.core.handle, msg = ?current_frame, expected_pipe_id = ?source_pipe_id_from_meta, "REP: Interleaved request parts from different peers during blocking recv!");
          return Err(ZmqError::ProtocolViolation(
            "Interleaved request parts from different peers".into(),
          ));
        }

        let is_more = current_frame.is_more();

        if !delimiter_found {
          let is_delimiter = current_frame.size() == 0;
          routing_prefix_frames.push(current_frame);
          if is_delimiter {
            delimiter_found = true;
          }
          if !is_more {
            // End of ZMTP message (e.g. from REQ client, or DEALER sending only prefix)
            if !delimiter_found {
              // No delimiter found, all frames become payload
              received_payload_frames = std::mem::take(&mut routing_prefix_frames);
            }
            // If delimiter_found and !is_more, payload is empty.
            break; // Logical request complete
          }
        } else {
          // After delimiter, collecting payload
          received_payload_frames.push(current_frame);
          if !is_more {
            break; // Last frame of payload, logical request complete
          }
        }
      } // End frame collection loop

      let final_source_pipe_id = source_pipe_id_from_meta.ok_or_else(|| {
        ZmqError::Internal("REP: Source pipe ID not determined after processing frames (blocking path)".into())
      })?;

      // State transition and PeerInfo creation
      let peer_info = PeerInfo {
        source_pipe_read_id: final_source_pipe_id,
        routing_prefix: routing_prefix_frames,
      };
      *self.state.lock().await = RepState::ReceivedRequest(peer_info);

      Ok(received_payload_frames)
    };

    // Apply overall timeout if RCVTIMEO is Some(duration > 0)
    let result = if let Some(duration) = rcvtimeo_opt {
      // Note: RCVTIMEO=0 already handled
      match timeout(duration, core_logic_future).await {
        Ok(internal_result) => internal_result,
        Err(_elapsed) => Err(ZmqError::Timeout), // Overall RCVTIMEO expired
      }
    } else {
      // RCVTIMEO = None (infinite)
      core_logic_future.await
    };

    // If any error occurred (timeout or from core_logic_future), ensure state is reset
    // if it was no longer ReadyToReceive (implying we started processing).
    if result.is_err() {
      let mut op_state_guard = self.state.lock().await;
      // If state is ReceivedRequest, it means an error occurred after successfully processing
      // a full request OR an error occurred mid-request that wasn't caught by inner logic's return.
      // If an error occurred mid-request, the inner logic should have returned Err(...)
      // and this outer 'if' is just a fallback. The main case for this is timeout.
      if matches!(*op_state_guard, RepState::ReceivedRequest(_))
        || matches!(result.as_ref().err(), Some(&ZmqError::Timeout))
          && !matches!(*op_state_guard, RepState::ReadyToReceive)
      {
        tracing::warn!(
          handle = self.core.handle,
          "REP recv_multipart: Error/Timeout. Resetting state to ReadyToReceive."
        );
        *op_state_guard = RepState::ReadyToReceive;
      }
    }
    result
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { mut msg, .. } => {
        msg.metadata_mut().insert_typed(SourcePipeReadId(pipe_read_id)).await;
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_read_id,
          msg_size = msg.size(),
          more_flag = msg.is_more(),
          "REP handle_pipe_event: Received frame, pushing to FairQueue."
        );
        self.incoming_request_queue.push_message(msg).await?;
      }
      _ => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_read_id,
          "REP received unhandled pipe event: {:?}",
          event.variant_name()
        );
      }
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
    self
      .pipe_read_to_write_id
      .write()
      .insert(pipe_read_id, pipe_write_id);
    self.incoming_request_queue.pipe_attached(pipe_read_id);
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

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "REP detaching pipe"
    );
    self.pipe_read_to_write_id.write().remove(&pipe_read_id);
    self.incoming_request_queue.pipe_detached(pipe_read_id);

    let mut current_state_guard = self.state.lock().await;
    if let RepState::ReceivedRequest(ref peer_info) = *current_state_guard {
      if peer_info.source_pipe_read_id == pipe_read_id {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_read_id,
          "Peer disconnected while REP socket held its request. Resetting REP state."
        );
        *current_state_guard = RepState::ReadyToReceive;
      }
    }
  }
}
