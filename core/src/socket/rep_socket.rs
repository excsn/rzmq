// core/src/socket/rep_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::EndpointInfo;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::FairQueue; // REP uses FairQueue for incoming requests
use crate::socket::{ISocket, SourcePipeReadId};

use async_trait::async_trait;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::timeout;

#[derive(Debug, Clone)]
struct PeerInfo {
  source_pipe_read_id: usize,
  target_endpoint_uri: String,
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
  state: TokioMutex<RepState>,
  pipe_read_id_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl RepSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    // <<< REVERTED: Use rcvhwm for FairQueue capacity >>>
    let queue_capacity = core.core_state.read().options.rcvhwm.max(1);
    Self {
      core,
      incoming_request_queue: FairQueue::new(queue_capacity), // Directly use FairQueue
      state: TokioMutex::new(RepState::ReadyToReceive),
      pipe_read_id_to_endpoint_uri: RwLock::new(HashMap::new()),
    }
  }

  fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
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

    // REP application message should not have MORE flag set by user.
    // If it does, we clear it as it's the last part of the app payload.
    if msg.is_more() {
      msg.set_flags(msg.flags() & !MsgFlags::MORE);
    }
    
    // Delegate to send_multipart with a single payload frame
    self.send_multipart(vec![msg]).await
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    let frames = self.recv_multipart().await?;
    if frames.is_empty() {
      Err(ZmqError::InvalidMessage(
        "Received empty multipart message when single part expected".into(),
      ))
    } else if frames.len() > 1 {
      tracing::warn!(
        handle = self.core.handle,
        num_frames = frames.len(),
        "REP recv() got multipart message, returning last frame as payload."
      );
      Ok(frames.into_iter().last().unwrap())
    } else {
      Ok(frames.into_iter().next().unwrap())
    }
  }

  async fn send_multipart(&self, payload_frames: Vec<Msg>) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let mut user_payload_frames = payload_frames;
    if user_payload_frames.is_empty() { // ZMQ REP must send at least one frame (can be empty)
      user_payload_frames.push(Msg::new());
    }

    let peer_to_reply_to = {
      let mut current_state_guard = self.state.lock().await;
      match std::mem::replace(&mut *current_state_guard, RepState::ReadyToReceive) {
        RepState::ReceivedRequest(info) => info,
        RepState::ReadyToReceive => {
          // Restore state as we didn't consume it
          *current_state_guard = RepState::ReadyToReceive; 
          return Err(ZmqError::InvalidState(
            "REP socket must recv() a request before sending a reply",
          ));
        }
      }
    }; // current_state_guard is dropped

    let conn_iface: Arc<dyn ISocketConnection> = {
      let core_s_read = self.core_state_read();
      match core_s_read.endpoints.get(&peer_to_reply_to.target_endpoint_uri) {
        Some(ep_info) => ep_info.connection_iface.clone(),
        None => {
          tracing::error!(handle = self.core.handle, uri = %peer_to_reply_to.target_endpoint_uri, "REP send_multipart: ISocketConnection not found. Peer likely disconnected.");
          return Err(ZmqError::HostUnreachable(
            "Peer disconnected before reply could be sent".into(),
          ));
        }
      }
    }; // core_s_read is dropped

    // Assemble all ZMTP frames for the wire: routing_prefix + user_payload_frames
    let mut zmtp_wire_frames: Vec<Msg> = Vec::with_capacity(
      peer_to_reply_to.routing_prefix.len() + user_payload_frames.len()
    );
    zmtp_wire_frames.extend(peer_to_reply_to.routing_prefix); // routing_prefix frames should already have MORE flags set correctly from when they were received.
    zmtp_wire_frames.extend(user_payload_frames);

    // Ensure MORE flags are set correctly for the entire sequence
    let num_total_frames = zmtp_wire_frames.len();
    if num_total_frames == 0 { // Should not happen if user_payload_frames is guaranteed non-empty
      return Ok(());
    }

    for (i, frame) in zmtp_wire_frames.iter_mut().enumerate() {
      if i < num_total_frames - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE); // Last frame of the logical message
      }
    }
    
    // Call ISocketConnection::send_multipart once with all prepared ZMTP wire frames
    match conn_iface.send_multipart(zmtp_wire_frames).await {
      Ok(()) => {
        tracing::trace!(handle = self.core.handle, uri = %peer_to_reply_to.target_endpoint_uri, "REP send_multipart: Sent complete reply.");
        Ok(())
      }
      Err(e) => {
        tracing::warn!(handle = self.core.handle, uri = %peer_to_reply_to.target_endpoint_uri, error = %e, "REP send_multipart: Send failed.");
        // If send failed, the REP socket should ideally revert to ReadyToReceive state,
        // which it already did when `peer_to_reply_to` was taken.
        // The error should be propagated.
        Err(e)
      }
    }
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    {
      let current_state_guard = self.state.lock().await;
      if !matches!(*current_state_guard, RepState::ReadyToReceive) {
        return Err(ZmqError::InvalidState(
          "REP socket must call send() before receiving again",
        ));
      }
    }

    let rcvtimeo_opt: Option<Duration> = self.core_state_read().options.rcvtimeo;

    // This Vec will store the ZMTP frames of one logical request from a peer
    let mut current_request_frames: Vec<Msg> = Vec::new();
    let mut current_request_source_pipe_id: Option<usize> = None;

    loop {
      // Loop to pop frames from FairQueue until a full logical request is assembled
      let frame_from_queue = match rcvtimeo_opt {
        Some(duration) if !duration.is_zero() => {
          match timeout(duration, self.incoming_request_queue.pop_message()).await {
            Ok(Ok(Some(msg))) => msg,
            Ok(Ok(None)) => return Err(ZmqError::Internal("REP: Request queue closed".into())),
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(ZmqError::Timeout),
          }
        }
        _ => {
          // RCVTIMEO = 0 or None
          if rcvtimeo_opt == Some(Duration::ZERO) {
            // Non-blocking
            match self.incoming_request_queue.try_pop_message() {
              Ok(Some(msg)) => msg,
              Ok(None) => return Err(ZmqError::ResourceLimitReached),
              Err(e) => return Err(e),
            }
          } else {
            // Infinite
            match self.incoming_request_queue.pop_message().await? {
              Some(msg) => msg,
              None => return Err(ZmqError::Internal("REP: Request queue closed".into())),
            }
          }
        }
      };

      let frame_source_id = match frame_from_queue.metadata().get::<SourcePipeReadId>().await {
        Some(id_meta) => id_meta.0,
        None => return Err(ZmqError::ProtocolViolation("Missing pipe ID on request frame".into())),
      };

      if current_request_source_pipe_id.is_none() {
        current_request_source_pipe_id = Some(frame_source_id);
      } else if current_request_source_pipe_id != Some(frame_source_id) {
        // This implies message interleaving from different pipes if FairQueue isn't strict,
        // or a bug. REP should process one request fully before another.
        // For now, treat as error and potentially requeue `frame_from_queue` if possible.
        // Simplest is to error out.
        return Err(ZmqError::ProtocolViolation(
          "Interleaved ZMTP frames from different peers in REP queue".into(),
        ));
      }

      let is_last_zmtp_frame = !frame_from_queue.is_more();
      current_request_frames.push(frame_from_queue);

      if is_last_zmtp_frame {
        break; // Got all ZMTP frames for this logical request
      }
    } // End of ZMTP frame assembly loop

    // Now `current_request_frames` holds the complete ZMTP message (e.g., [empty_MORE, payload_NOMORE])
    let source_pipe_id = current_request_source_pipe_id.unwrap(); // Must be Some if loop executed

    let target_endpoint_uri_for_reply = {
      let core_s_read = self.core_state_read();
      core_s_read
        .pipe_read_id_to_endpoint_uri
        .get(&source_pipe_id)
        .cloned()
        .ok_or_else(|| ZmqError::Internal("REP: Endpoint URI lookup failed for reply routing".into()))?
    };

    let mut routing_prefix: Vec<Msg> = Vec::new();
    let mut payload_frames: Vec<Msg> = Vec::new();
    let mut delimiter_found = false;

    for frame in current_request_frames {
      if !delimiter_found && frame.size() == 0 {
        delimiter_found = true;
        routing_prefix.push(frame);
      } else if !delimiter_found {
        routing_prefix.push(frame);
      } else {
        payload_frames.push(frame);
      }
    }
    if !delimiter_found {
      payload_frames = routing_prefix; // If no delimiter, all frames are payload (e.g. from REQ)
      routing_prefix = Vec::new();
    }

    let peer_info = PeerInfo {
      source_pipe_read_id: source_pipe_id,
      target_endpoint_uri: target_endpoint_uri_for_reply,
      routing_prefix,
    };

    {
      let mut current_state_guard = self.state.lock().await;
      *current_state_guard = RepState::ReceivedRequest(peer_info);
    }

    Ok(payload_frames)
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

  // <<< REVERTED: handle_pipe_event pushes individual ZMTP frames to its own FairQueue >>>
  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { mut msg, .. } => {
        // Add SourcePipeReadId metadata so recv_multipart knows the origin pipe
        msg.metadata_mut().insert_typed(SourcePipeReadId(pipe_read_id)).await;

        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_read_id,
          msg_size = msg.size(),
          more_flag = msg.is_more(),
          "REP handle_pipe_event: Received frame, pushing to FairQueue."
        );
        if let Err(e) = self.incoming_request_queue.push_message(msg).await {
          tracing::error!(
            handle = self.core.handle,
            pipe_id = pipe_read_id,
            "REP: Error pushing frame to FairQueue (incoming_request_queue): {}",
            e
          );
          return Err(e);
        }
      }
      _ => {}
    }
    Ok(())
  }

  async fn pipe_attached(&self, pipe_read_id: usize, _pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    let endpoint_uri_option = self
      .core_state_read()
      .pipe_read_id_to_endpoint_uri
      .get(&pipe_read_id)
      .cloned();

    if let Some(endpoint_uri) = endpoint_uri_option {
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "REP attaching pipe");
      self
        .pipe_read_id_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri);
      self.incoming_request_queue.pipe_attached(pipe_read_id); // Notify FairQueue
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "REP pipe_attached: Endpoint URI not found for pipe. Map not updated."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(handle = self.core.handle, socket_type = "REP", pipe_read_id, ?identity, "update_peer_identity called, but REP socket does not use peer identities beyond routing. Ignoring for main state.");
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(handle = self.core.handle, pipe_read_id, "REP detaching pipe");
    self.pipe_read_id_to_endpoint_uri.write().remove(&pipe_read_id);

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
    self.incoming_request_queue.pipe_detached(pipe_read_id); // Notify FairQueue
  }
}
