// core/src/socket/rep_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::EndpointInfo;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::IncomingMessageOrchestrator;
use crate::socket::{ISocket, SourcePipeReadId};

use async_trait::async_trait;
use parking_lot::{Mutex as ParkingLotMutex, RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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
  incoming_orchestrator: IncomingMessageOrchestrator<(PeerInfo, Vec<Msg>)>,
  state: ParkingLotMutex<RepState>,
  pipe_read_id_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl RepSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let queue_capacity = core.core_state.read().options.rcvhwm.max(1);
    let orchestrator = IncomingMessageOrchestrator::new(core.handle, queue_capacity);
    Self {
      core,
      incoming_orchestrator: orchestrator,
      state: ParkingLotMutex::new(RepState::ReadyToReceive),
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

    if msg.is_more() {
      msg.set_flags(msg.flags() & !MsgFlags::MORE);
    }
    self.incoming_orchestrator.reset_recv_message_buffer().await;
    self.send_multipart(vec![msg]).await
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    {
      let current_state_guard = self.state.lock();
      if !matches!(*current_state_guard, RepState::ReadyToReceive) {
        return Err(ZmqError::InvalidState(
          "REP socket must call send() before receiving again",
        ));
      }
    }

    let rcvtimeo_opt: Option<Duration> = self.core_state_read().options.rcvtimeo;

    // The transform closure will be called by the orchestrator after it pops an item.
    // It is synchronous and returns the payload frames to the orchestrator's buffer.
    let transform_fn = |(peer_info, payload_frames): (PeerInfo, Vec<Msg>)| {
      // Synchronously update state. This is safe because the async part is done.
      *self.state.lock() = RepState::ReceivedRequest(peer_info);
      payload_frames
    };

    self
      .incoming_orchestrator
      .recv_message(rcvtimeo_opt, transform_fn)
      .await
  }

  async fn send_multipart(&self, payload_frames: Vec<Msg>) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    self.incoming_orchestrator.reset_recv_message_buffer().await;

    let mut user_payload_frames = payload_frames;
    if user_payload_frames.is_empty() {
      user_payload_frames.push(Msg::new());
    }

    let peer_to_reply_to = {
      let mut current_state_guard = self.state.lock();
      match std::mem::replace(&mut *current_state_guard, RepState::ReadyToReceive) {
        RepState::ReceivedRequest(info) => info,
        RepState::ReadyToReceive => {
          *current_state_guard = RepState::ReadyToReceive;
          return Err(ZmqError::InvalidState(
            "REP socket must recv() a request before sending a reply",
          ));
        }
      }
    };

    let conn_iface: Arc<dyn ISocketConnection> = {
      let core_s_read = self.core_state_read();
      match core_s_read
        .endpoints
        .get(&peer_to_reply_to.target_endpoint_uri)
      {
        Some(ep_info) => ep_info.connection_iface.clone(),
        None => {
          tracing::error!(handle = self.core.handle, uri = %peer_to_reply_to.target_endpoint_uri, "REP send_multipart: ISocketConnection not found. Peer likely disconnected.");
          return Err(ZmqError::HostUnreachable(
            "Peer disconnected before reply could be sent".into(),
          ));
        }
      }
    };

    let mut zmtp_wire_frames: Vec<Msg> =
      Vec::with_capacity(peer_to_reply_to.routing_prefix.len() + user_payload_frames.len());
    zmtp_wire_frames.extend(peer_to_reply_to.routing_prefix);
    zmtp_wire_frames.extend(user_payload_frames);

    let num_total_frames = zmtp_wire_frames.len();
    if num_total_frames == 0 {
      return Ok(());
    }

    for (i, frame) in zmtp_wire_frames.iter_mut().enumerate() {
      if i < num_total_frames - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
    }

    match conn_iface.send_multipart(zmtp_wire_frames).await {
      Ok(()) => {
        tracing::trace!(handle = self.core.handle, uri = %peer_to_reply_to.target_endpoint_uri, "REP send_multipart: Sent complete reply.");
        Ok(())
      }
      Err(e) => {
        tracing::warn!(handle = self.core.handle, uri = %peer_to_reply_to.target_endpoint_uri, error = %e, "REP send_multipart: Send failed.");
        Err(e)
      }
    }
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    {
      let current_state_guard = self.state.lock();
      if !matches!(*current_state_guard, RepState::ReadyToReceive) {
        return Err(ZmqError::InvalidState(
          "REP socket must call send() before receiving again",
        ));
      }
    }

    let rcvtimeo_opt: Option<Duration> = self.core_state_read().options.rcvtimeo;

    // The transform function is synchronous and just passes the data through after we've
    // updated the state.
    let transform_fn = |(peer_info, payload_frames): (PeerInfo, Vec<Msg>)| {
      *self.state.lock() = RepState::ReceivedRequest(peer_info);
      payload_frames
    };

    self
      .incoming_orchestrator
      .recv_logical_message(rcvtimeo_opt, transform_fn)
      .await
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn process_command(&self, command: Command) -> Result<bool, ZmqError> {

    match command {
      Command::Stop => {
        self.incoming_orchestrator.close().await;
      }
      _ => return Ok(false),
    }
    
    Ok(true)
  }

  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        if let Some(raw_zmtp_message) = self
          .incoming_orchestrator
          .accumulate_pipe_frame(pipe_read_id, msg)?
        {
          let target_endpoint_uri_for_reply = {
            let core_s_read = self.core_state_read();
            core_s_read
              .pipe_read_id_to_endpoint_uri
              .get(&pipe_read_id)
              .cloned()
              .ok_or_else(|| {
                ZmqError::Internal("REP: Endpoint URI lookup failed for request".into())
              })?
          };

          let mut routing_prefix: Vec<Msg> = Vec::new();
          let mut payload_frames: Vec<Msg> = Vec::new();
          let mut delimiter_found = false;

          for frame in raw_zmtp_message {
            if !delimiter_found {
              // Keep adding frames to the routing prefix until we find the empty delimiter.
              let is_delimiter = frame.size() == 0;
              routing_prefix.push(frame);
              if is_delimiter {
                delimiter_found = true;
              }
            } else {
              // Once the delimiter is found, all subsequent frames are part of the payload.
              payload_frames.push(frame);
            }
          }

          // In the case of a REQ socket, there is no delimiter. The spec says the
          // entire message is the payload and the routing prefix is empty.
          if !delimiter_found {
            payload_frames = routing_prefix;
            routing_prefix = Vec::new();
          }

          let peer_info = PeerInfo {
            source_pipe_read_id: pipe_read_id,
            target_endpoint_uri: target_endpoint_uri_for_reply,
            routing_prefix,
          };

          self
            .incoming_orchestrator
            .queue_item(pipe_read_id, (peer_info, payload_frames))
            .await?;
        }
      }
      _ => {}
    }
    Ok(())
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize,
    _peer_identity: Option<&[u8]>,
  ) {
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
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id,
      "REP detaching pipe"
    );
    self
      .pipe_read_id_to_endpoint_uri
      .write()
      .remove(&pipe_read_id);

    {
      let mut current_state_guard = self.state.lock();
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
    self
      .incoming_orchestrator
      .clear_pipe_state(pipe_read_id)
      .await;
  }
}
