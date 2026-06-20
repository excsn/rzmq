use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{Mutex as ParkingLotMutex, RwLock, RwLockReadGuard};

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, FrameBatch, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::AddressedIngressEngine;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;

#[derive(Debug, Clone)]
struct PeerInfo {
  source_pipe_read_id: usize,
  target_endpoint_uri: String,
  routing_prefix: FrameBatch,
}

#[derive(Debug, Clone)]
enum RepState {
  ReadyToReceive,
  ReceivedRequest(PeerInfo),
}

#[derive(Debug)]
pub(crate) struct RepSocket {
  core: Arc<SocketCore>,
  ingress_engine: AddressedIngressEngine,
  pending_pipe_senders: ParkingLotMutex<HashMap<usize, PipeMessageSender>>,
  state: ParkingLotMutex<RepState>,
  pipe_read_id_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl RepSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let max_conn = core.core_state.read().options.max_connections.unwrap_or(1024);
    Self {
      core,
      ingress_engine: AddressedIngressEngine::new(max_conn),
      pending_pipe_senders: ParkingLotMutex::new(HashMap::new()),
      state: ParkingLotMutex::new(RepState::ReadyToReceive),
      pipe_read_id_to_endpoint_uri: RwLock::new(HashMap::new()),
    }
  }

  fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  fn extract_routing_prefix(raw_batch: FrameBatch) -> (FrameBatch, FrameBatch) {
    let mut routing_prefix = FrameBatch::new();
    let mut payload_frames = FrameBatch::new();
    let mut delimiter_found = false;
    for frame in raw_batch {
      if !delimiter_found {
        let is_delimiter = frame.size() == 0;
        routing_prefix.push(frame);
        if is_delimiter {
          delimiter_found = true;
        }
      } else {
        payload_frames.push(frame);
      }
    }
    if !delimiter_found {
      // No delimiter: treat entire message as payload (peer without routing prefix).
      payload_frames = routing_prefix;
      routing_prefix = FrameBatch::new();
    }
    (routing_prefix, payload_frames)
  }

  async fn recv_complete_request(
    &self,
    rcvtimeo_opt: Option<Duration>,
  ) -> Result<(PeerInfo, FrameBatch), ZmqError> {
    let (pipe_read_id, raw_batch) =
      self.ingress_engine.recv_logical_message(rcvtimeo_opt).await?;

    let target_endpoint_uri = self
      .core_state_read()
      .pipe_read_id_to_endpoint_uri
      .get(&pipe_read_id)
      .cloned()
      .ok_or_else(|| ZmqError::Internal("REP: Endpoint URI lookup failed for request".into()))?;

    let (routing_prefix, payload_frames) = Self::extract_routing_prefix(raw_batch);
    let peer_info = PeerInfo { source_pipe_read_id: pipe_read_id, target_endpoint_uri, routing_prefix };
    Ok((peer_info, payload_frames))
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
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    if msg.is_more() {
      msg.set_flags(msg.flags() & !MsgFlags::MORE);
    }
    let mut fb = FrameBatch::new();
    fb.push(msg);
    self.send_multipart(fb).await
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    {
      let guard = self.state.lock();
      if !matches!(*guard, RepState::ReadyToReceive) {
        return Err(ZmqError::InvalidState("REP socket must call send() before receiving again"));
      }
    }

    let rcvtimeo_opt = self.core_state_read().options.rcvtimeo;
    let (peer_info, mut payload_frames) = self.recv_complete_request(rcvtimeo_opt).await?;
    *self.state.lock() = RepState::ReceivedRequest(peer_info);

    if payload_frames.is_empty() {
      Ok(Msg::new())
    } else {
      Ok(payload_frames.remove(0))
    }
  }

  async fn send_multipart(&self, payload_frames: FrameBatch) -> Result<(), ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let mut user_payload_frames = payload_frames;
    if user_payload_frames.is_empty() {
      user_payload_frames.push(Msg::new());
    }

    let peer_to_reply_to = {
      let mut guard = self.state.lock();
      match std::mem::replace(&mut *guard, RepState::ReadyToReceive) {
        RepState::ReceivedRequest(info) => info,
        RepState::ReadyToReceive => {
          *guard = RepState::ReadyToReceive;
          return Err(ZmqError::InvalidState("REP socket must recv() a request before sending a reply"));
        }
      }
    };

    let conn_iface: Arc<dyn ISocketConnection> = {
      let core_s_read = self.core_state_read();
      match core_s_read.endpoints.get(&peer_to_reply_to.target_endpoint_uri) {
        Some(ep_info) => ep_info.connection_iface.clone(),
        None => {
          tracing::error!(
            handle = self.core.handle, uri = %peer_to_reply_to.target_endpoint_uri,
            "REP send_multipart: ISocketConnection not found. Peer likely disconnected."
          );
          return Err(ZmqError::HostUnreachable("Peer disconnected before reply could be sent".into()));
        }
      }
    };

    let mut zmtp_wire_frames = FrameBatch::with_capacity(
      peer_to_reply_to.routing_prefix.len() + user_payload_frames.len(),
    );
    zmtp_wire_frames.extend(peer_to_reply_to.routing_prefix);
    zmtp_wire_frames.extend(user_payload_frames);

    let n = zmtp_wire_frames.len();
    if n == 0 {
      return Ok(());
    }
    for (i, frame) in zmtp_wire_frames.iter_mut().enumerate() {
      if i < n - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
    }

    match conn_iface.send_multipart(zmtp_wire_frames).await {
      Ok(()) => {
        tracing::trace!(handle = self.core.handle, uri = %peer_to_reply_to.target_endpoint_uri, "REP sent complete reply.");
        Ok(())
      }
      Err(e) => {
        tracing::warn!(handle = self.core.handle, uri = %peer_to_reply_to.target_endpoint_uri, error = %e, "REP send failed.");
        Err(e)
      }
    }
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    {
      let guard = self.state.lock();
      if !matches!(*guard, RepState::ReadyToReceive) {
        return Err(ZmqError::InvalidState("REP socket must call send() before receiving again"));
      }
    }

    let rcvtimeo_opt = self.core_state_read().options.rcvtimeo;
    let (peer_info, payload_frames) = self.recv_complete_request(rcvtimeo_opt).await?;
    *self.state.lock() = RepState::ReceivedRequest(peer_info);
    Ok(payload_frames)
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
        self.ingress_engine.close();
      }
      _ => return Ok(false),
    }
    Ok(true)
  }

  async fn handle_pipe_event(&self, _pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { .. } | Command::PipeMessageBatchReceived { .. } => {
        // Data frames are pushed directly by the actor via PipeMessageSender; no action needed.
      }
      _ => {}
    }
    Ok(())
  }

  fn get_incoming_pipe_sender(&self, pipe_read_id: usize) -> Option<PipeMessageSender> {
    self.pending_pipe_senders.lock().remove(&pipe_read_id)
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
      self.pipe_read_id_to_endpoint_uri.write().insert(pipe_read_id, endpoint_uri);

      let (rcvhwm, rcvbatch_count) = {
        let opts = self.core_state_read();
        (opts.options.rcvhwm.max(1), opts.options.rcvbatch_count)
      };
      let sender = self.ingress_engine.register_pipe(pipe_read_id, rcvhwm, rcvbatch_count);
      self.pending_pipe_senders.lock().insert(pipe_read_id, sender);
    } else {
      tracing::warn!(
        handle = self.core.handle, pipe_read_id,
        "REP pipe_attached: Endpoint URI not found for pipe."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle, socket_type = "REP", pipe_read_id, ?identity,
      "update_peer_identity called, REP ignores it for main state."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(handle = self.core.handle, pipe_read_id, "REP detaching pipe");
    self.pipe_read_id_to_endpoint_uri.write().remove(&pipe_read_id);

    {
      let mut guard = self.state.lock();
      if let RepState::ReceivedRequest(ref peer_info) = *guard {
        if peer_info.source_pipe_read_id == pipe_read_id {
          tracing::warn!(
            handle = self.core.handle, pipe_id = pipe_read_id,
            "Peer disconnected while REP socket held its request. Resetting REP state."
          );
          *guard = RepState::ReadyToReceive;
        }
      }
    }

    self.ingress_engine.deregister_pipe(pipe_read_id);
    self.pending_pipe_senders.lock().remove(&pipe_read_id);
  }
}
