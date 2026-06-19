use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex as ParkingMutex;

use crate::{Blob, delegate_to_core};
use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::patterns::AnonymousIngressEngine;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;

#[derive(Debug)]
pub(crate) struct PullSocket {
  core: Arc<SocketCore>,
  ingress_engine: AnonymousIngressEngine,
  pending_pipe_senders: ParkingMutex<HashMap<usize, PipeMessageSender>>,
}

impl PullSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      ingress_engine: AnonymousIngressEngine::new(),
      pending_pipe_senders: ParkingMutex::new(HashMap::new()),
    }
  }
}

#[async_trait]
impl ISocket for PullSocket {
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
  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  async fn send(&self, _msg: Msg) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("PULL sockets cannot send messages".into()))
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt = self.core.core_state.read().options.rcvtimeo;
    self.ingress_engine.recv(rcvtimeo_opt).await
  }

  async fn send_multipart(&self, _frames: FrameBatch) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("PULL sockets cannot send messages".into()))
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt = self.core.core_state.read().options.rcvtimeo;
    self.ingress_engine.recv_multipart(rcvtimeo_opt).await
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
  }

  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    delegate_to_core!(self, UserGetOpt, option: option)
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
        // Data frames pushed directly by actor via PipeMessageSender; no action here.
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
    tracing::debug!(handle = self.core.handle, pipe_read_id, "PULL attaching pipe");
    let rcvhwm = self.core.core_state.read().options.rcvhwm.max(1);
    let sender = self.ingress_engine.register_pipe(pipe_read_id, rcvhwm);
    self.pending_pipe_senders.lock().insert(pipe_read_id, sender);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(handle = self.core.handle, pipe_read_id, "PULL detaching pipe");
    self.ingress_engine.deregister_pipe(pipe_read_id);
    self.pending_pipe_senders.lock().remove(&pipe_read_id);
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle, socket_type = "PULL", pipe_read_id, ?identity,
      "update_peer_identity called, PULL ignores it."
    );
  }
}
