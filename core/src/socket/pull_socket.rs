use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use parking_lot::Mutex as ParkingMutex;

use crate::{Blob, delegate_to_core};
use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::options::SocketOptions;
use crate::socket::patterns::AnonymousIngressEngine;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;

#[derive(Debug)]
pub(crate) struct PullSocket {
  core: Arc<SocketCore>,
  ingress_engine: AnonymousIngressEngine,
  pending_pipe_senders: ParkingMutex<HashMap<usize, PipeMessageSender>>,
  /// Lock-free snapshot of socket options so the hot `recv`/`recv_multipart` path
  /// reads `rcvtimeo` without taking the `core_state` RwLock per message. Refreshed
  /// on `set_option`. Mirrors `PushSocket::cached_options`.
  cached_options: ArcSwap<SocketOptions>,
}

impl PullSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let (max_conn, rcvbatch_count, options_snapshot) = {
      let opts = &core.core_state.read().options;
      (opts.max_connections.unwrap_or(1024), opts.rcvbatch_count, opts.clone())
    };
    Self {
      core,
      ingress_engine: AnonymousIngressEngine::new(max_conn, rcvbatch_count),
      pending_pipe_senders: ParkingMutex::new(HashMap::new()),
      cached_options: ArcSwap::from(options_snapshot),
    }
  }
}

#[async_trait]
impl ISocket for PullSocket {
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
    let rcvtimeo_opt = self.cached_options.load().rcvtimeo;
    self.ingress_engine.recv(rcvtimeo_opt).await
  }

  async fn send_multipart(&self, _frames: FrameBatch) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("PULL sockets cannot send messages".into()))
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt = self.cached_options.load().rcvtimeo;
    self.ingress_engine.recv_multipart(rcvtimeo_opt).await
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    let result = delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec());
    if result.is_ok() {
      self.cached_options.store(self.core.core_state.read().options.clone());
    }
    result
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
    let (rcvhwm, rcvbatch_count) = {
      let opts = self.core.core_state.read();
      (opts.options.rcvhwm.max(1), opts.options.rcvbatch_count)
    };
    let sender = self.ingress_engine.register_pipe(pipe_read_id, rcvhwm, rcvbatch_count);
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
