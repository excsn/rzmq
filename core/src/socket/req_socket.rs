use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::LoadBalancer;
use crate::socket::patterns::AddressedIngressEngine;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::{Blob, delegate_to_core};

use async_trait::async_trait;
use parking_lot::Mutex as ParkingLotMutex;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout as tokio_timeout;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReqState {
  ReadyToSend,
  ExpectingReply { target_endpoint_uri: String },
}

#[derive(Debug)]
pub(crate) struct ReqSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer,
  ingress_engine: AddressedIngressEngine,
  pending_pipe_senders: ParkingLotMutex<HashMap<usize, PipeMessageSender>>,
  state: ParkingLotMutex<ReqState>,
  reply_available_notifier: Arc<Notify>,
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl ReqSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let max_conn = core.core_state.read().options.max_connections.unwrap_or(1024);
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      ingress_engine: AddressedIngressEngine::new(max_conn),
      pending_pipe_senders: ParkingLotMutex::new(HashMap::new()),
      state: ParkingLotMutex::new(ReqState::ReadyToSend),
      reply_available_notifier: Arc::new(Notify::new()),
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
    }
  }

  fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  fn process_incoming_zmtp_message_for_req(
    &self,
    pipe_read_id: usize,
    mut raw_zmtp_message: FrameBatch,
  ) -> Result<FrameBatch, ZmqError> {
    if !raw_zmtp_message.is_empty() && raw_zmtp_message[0].size() == 0 {
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "REQ: Stripped empty ZMTP delimiter from incoming reply."
      );
      raw_zmtp_message.remove(0);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "REQ: Incoming ZMTP reply from pipe did not start with an empty delimiter as expected from REP."
      );
    }
    Ok(raw_zmtp_message)
  }
}

#[async_trait]
impl ISocket for ReqSocket {
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
      tracing::trace!(
        handle = self.core.handle,
        "REQ send: Cleared MORE flag from user-provided message."
      );
    }

    // === LOCK SCOPE 1: Check State ===
    {
      let current_state_guard = self.state.lock();
      if !matches!(*current_state_guard, ReqState::ReadyToSend) {
        return Err(ZmqError::InvalidState(
          "REQ socket must call recv() before sending again",
        ));
      }
    }

    let timeout_opt: Option<Duration> = { self.core.core_state.read().options.sndtimeo };

    // === ASYNC OPERATION: Find a Peer (No Lock Held) ===
    let peer = loop {
      if let Some(p) = self.load_balancer.get_next_connection() {
        // We got the peer from the LoadBalancer. We assume it is active.
        break p;
      } else {
        if self.core.command_sender().is_closed() {
          return Err(ZmqError::InvalidState("Socket terminated".into()));
        }
        match timeout_opt {
          Some(duration) if duration.is_zero() => return Err(ZmqError::ResourceLimitReached),
          None => self.load_balancer.wait_for_connection().await?,
          Some(duration) => {
            match tokio_timeout(duration, self.load_balancer.wait_for_connection()).await {
              Ok(res) => res?,
              Err(_timeout_elapsed) => return Err(ZmqError::Timeout),
            }
          }
        }
      }
    };

    let mut empty_delimiter = Msg::new();
    empty_delimiter.set_flags(MsgFlags::MORE);
    let mut zmtp_frames_to_send = FrameBatch::new();
    zmtp_frames_to_send.push(empty_delimiter);
    zmtp_frames_to_send.push(msg);

    // === ASYNC OPERATION: Send Message (No Lock Held) ===
    match peer.iface.send_multipart(zmtp_frames_to_send).await {
      Ok(()) => {
        // === LOCK SCOPE 2: Update State on Success ===
        {
          let mut current_state_guard = self.state.lock();
          *current_state_guard = ReqState::ExpectingReply {
            target_endpoint_uri: peer.uri.clone(),
          };
        }
        Ok(())
      }
      Err(ZmqError::ConnectionClosed) => {
        self.load_balancer.remove_connection(&peer.uri);
        Err(ZmqError::HostUnreachable(format!(
          "REQ: Connection to {} closed during send",
          peer.uri
        )))
      }
      Err(e) => Err(e),
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let rcvtimeo_opt: Option<Duration> = self.core.core_state.read().options.rcvtimeo;

    {
      let op_state_guard = self.state.lock();
      if !matches!(*op_state_guard, ReqState::ExpectingReply { .. }) {
        return Err(ZmqError::InvalidState(
          "REQ socket must call send() before receiving",
        ));
      }
    }

    let notifier = self.reply_available_notifier.clone();
    let received_msg_result: Result<Msg, ZmqError>;

    tokio::select! {
      biased;
      _ = notifier.notified() => {
        if !self.core.is_running() {
          tracing::debug!("REQ recv: Notifier signaled, core not running.");
          received_msg_result = Err(ZmqError::ConnectionClosed);
        } else {
          match self.ingress_engine.recv_logical_message(Some(Duration::ZERO)).await {
            Ok((_, batch)) => {
              match self.process_incoming_zmtp_message_for_req(0, batch) {
                Ok(mut payload) => {
                  received_msg_result = Ok(if payload.is_empty() { Msg::new() } else { payload.remove(0) });
                }
                Err(e) => received_msg_result = Err(e),
              }
            }
            Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
              let is_ready = matches!(*self.state.lock(), ReqState::ReadyToSend);
              if is_ready {
                tracing::warn!("REQ recv: Notifier signaled, no immediate message. State reset to ReadyToSend.");
                received_msg_result = Err(ZmqError::InvalidState("Socket state changed while waiting for reply".into()));
              } else {
                tracing::error!("REQ recv: Notifier signaled, no immediate message, still ExpectingReply.");
                received_msg_result = Err(ZmqError::Internal("Receive interrupted by notification".into()));
              }
            }
            Err(e) => received_msg_result = Err(e),
          }
        }
      }
      res = self.ingress_engine.recv_logical_message(rcvtimeo_opt) => {
        received_msg_result = match res {
          Ok((_, batch)) => {
            match self.process_incoming_zmtp_message_for_req(0, batch) {
              Ok(mut payload) => Ok(if payload.is_empty() { Msg::new() } else { payload.remove(0) }),
              Err(e) => Err(e),
            }
          }
          Err(e) => Err(e),
        };
      }
    }

    let mut should_notify = false;
    {
      let mut state_guard = self.state.lock();
      if matches!(*state_guard, ReqState::ExpectingReply { .. }) {
        let finished = received_msg_result.as_ref().map_or(true, |m| !m.is_more());
        if finished {
          *state_guard = ReqState::ReadyToSend;
          should_notify = true;
        }
      }
    }
    if should_notify {
      self.reply_available_notifier.notify_waiters();
    }

    received_msg_result
  }

  async fn send_multipart(&self, _frames: FrameBatch) -> Result<(), ZmqError> {
    tracing::warn!(
      handle = self.core.handle,
      "REQ socket: send_multipart() called. REQ sockets should use send() for single-part requests."
    );
    Err(ZmqError::UnsupportedFeature(
      "REQ sockets use send() for single-part requests. Use DEALER for general multipart messaging.".into(),
    ))
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    {
      let state_guard = self.state.lock();
      if !matches!(*state_guard, ReqState::ExpectingReply { .. }) {
        return Err(ZmqError::InvalidState(
          "REQ socket must call send() before receiving reply",
        ));
      }
    }

    let rcvtimeo_opt: Option<Duration> = self.core.core_state.read().options.rcvtimeo;
    let result = self.ingress_engine.recv_logical_message(rcvtimeo_opt).await;

    {
      let mut state_guard = self.state.lock();
      if matches!(*state_guard, ReqState::ExpectingReply { .. }) {
        *state_guard = ReqState::ReadyToSend;
        self.reply_available_notifier.notify_waiters();
      }
    }

    match result {
      Ok((_, batch)) => self.process_incoming_zmtp_message_for_req(0, batch),
      Err(e) => Err(e),
    }
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
        self.reply_available_notifier.notify_waiters();
      }
      _ => return Ok(false),
    }

    Ok(true)
  }

  async fn handle_pipe_event(&self, _pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { .. } | Command::PipeMessageBatchReceived { .. } => {}
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
    let (endpoint_uri_opt, connection_iface_opt) = {
      let core_s_read = self.core.core_state.read();
      let uri = core_s_read
        .pipe_read_id_to_endpoint_uri
        .get(&pipe_read_id)
        .cloned();
      let iface = uri.as_ref().and_then(|u| {
        core_s_read
          .endpoints
          .get(u)
          .map(|ep| ep.connection_iface.clone())
      });
      (uri, iface)
    };

    if let (Some(endpoint_uri), Some(iface)) = (endpoint_uri_opt, connection_iface_opt) {
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "REQ attaching connection");
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());

      self.load_balancer.add_connection(endpoint_uri, iface);

      // Register per-pipe ingress channel (capacity 1: REQ expects one reply at a time).
      let rcvhwm = self.core.core_state.read().options.rcvhwm.max(1);
      let sender = self.ingress_engine.register_pipe(pipe_read_id, rcvhwm);
      self.pending_pipe_senders.lock().insert(pipe_read_id, sender);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "REQ pipe_attached: Endpoint URI or connection interface not found. Maps not updated."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "REQ",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, REQ socket ignores it."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id,
      "REQ detaching pipe"
    );

    let maybe_endpoint_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id);
    let mut should_notify = false;

    if let Some(detached_uri) = &maybe_endpoint_uri {
      self.load_balancer.remove_connection(detached_uri);

      // === LOCK SCOPE: Check and Update State ===
      {
        let mut op_state_guard = self.state.lock();
        if let ReqState::ExpectingReply {
          ref target_endpoint_uri,
        } = *op_state_guard
        {
          if *target_endpoint_uri == *detached_uri {
            tracing::warn!(handle = self.core.handle, pipe_id = pipe_read_id, uri = %detached_uri, "Target REP peer detached while REQ was expecting reply. Resetting state and notifying recv.");
            *op_state_guard = ReqState::ReadyToSend;
            should_notify = true;
          }
        }
      } // Guard is dropped here
    }

    if should_notify {
      self.reply_available_notifier.notify_one();
    }

    self.ingress_engine.deregister_pipe(pipe_read_id);
    self.pending_pipe_senders.lock().remove(&pipe_read_id);
  }
}
