use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::options::SocketOptions;
use crate::socket::patterns::IncomingMessageOrchestrator;
use crate::socket::patterns::incoming_orchestrator::AppFrames;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use std::sync::Arc;

use crate::{Blob, delegate_to_core};

#[derive(Debug)]
pub(crate) struct PullSocket {
  core: Arc<SocketCore>,
  incoming_orchestrator: IncomingMessageOrchestrator<AppFrames>,
  cached_options: ArcSwap<SocketOptions>,
}

impl PullSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let (queue_capacity, options_snapshot) = {
      let s = core.core_state.read();
      (s.options.rcvhwm.max(1), s.options.clone())
    };
    let incoming_orchestrator = IncomingMessageOrchestrator::new(core.handle, queue_capacity);
    Self {
      core,
      incoming_orchestrator,
      cached_options: ArcSwap::from(options_snapshot),
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
    Err(ZmqError::InvalidState(
      "PULL sockets cannot send messages".into(),
    ))
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let rcvtimeo_opt = self.cached_options.load().rcvtimeo;
    let transform_fn = |q_item: AppFrames| q_item;
    self
      .incoming_orchestrator
      .recv_message(rcvtimeo_opt, transform_fn)
      .await
  }

  async fn send_multipart(&self, _frames: FrameBatch) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState(
      "PULL sockets cannot send messages".into(),
    ))
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let rcvtimeo_opt = self.cached_options.load().rcvtimeo;

    let transform_fn = |q_item: AppFrames| q_item;
    self
      .incoming_orchestrator
      .recv_logical_message(rcvtimeo_opt, transform_fn)
      .await
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
        self.incoming_orchestrator.close().await;
      }
      _ => return Ok(false),
    }
    Ok(true)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        if !msg.is_more() && !self.incoming_orchestrator.has_partial_message(pipe_id) {
          self.incoming_orchestrator.queue_item(pipe_id, AppFrames::Single(msg)).await?;
        } else if let Some(raw) = self.incoming_orchestrator.accumulate_pipe_frame(pipe_id, msg)? {
          self.incoming_orchestrator.queue_item(pipe_id, AppFrames::Multiple(raw)).await?;
        }
      }
      Command::PipeMessageBatchReceived { msgs, .. } => {
        let mut assembled_batch = Vec::new();
        for msg in msgs {
          if !msg.is_more() && !self.incoming_orchestrator.has_partial_message(pipe_id) {
            assembled_batch.push(AppFrames::Single(msg));
          } else if let Some(raw) = self.incoming_orchestrator.accumulate_pipe_frame(pipe_id, msg)? {
            assembled_batch.push(AppFrames::Multiple(raw));
          }
        }
        if !assembled_batch.is_empty() {
          self.incoming_orchestrator.queue_batch(pipe_id, assembled_batch).await?;
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
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PULL attaching pipe"
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PULL detaching pipe"
    );
    self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "PULL",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, PULL socket ignores it."
    );
  }
}
