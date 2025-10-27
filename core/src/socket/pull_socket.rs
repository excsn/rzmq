use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::IncomingMessageOrchestrator;

use async_trait::async_trait;
use parking_lot::RwLockReadGuard;
use std::sync::Arc;
use std::time::Duration;

use crate::{Blob, delegate_to_core};

#[derive(Debug)]
pub(crate) struct PullSocket {
  core: Arc<SocketCore>,
  incoming_orchestrator: IncomingMessageOrchestrator<Vec<Msg>>,
}

impl PullSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let queue_capacity = { core.core_state.read().options.rcvhwm.max(1) };

    let incoming_orchestrator = IncomingMessageOrchestrator::new(core.handle, queue_capacity);

    Self {
      core,
      incoming_orchestrator,
    }
  }

  fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
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
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo };
    // The transform function is identity, as the queued item is already the payload.
    let transform_fn = |q_item: Vec<Msg>| q_item;
    self
      .incoming_orchestrator
      .recv_message(rcvtimeo_opt, transform_fn)
      .await
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState(
      "PULL sockets cannot send messages".into(),
    ))
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo };

    let transform_fn = |q_item: Vec<Msg>| q_item;
    self
      .incoming_orchestrator
      .recv_logical_message(rcvtimeo_opt, transform_fn)
      .await
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

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        // Let the orchestrator handle frame accumulation.
        if let Some(raw_zmtp_message_vec) = self
          .incoming_orchestrator
          .accumulate_pipe_frame(pipe_id, msg)?
        {
          // A full logical message is ready. The QItem for PULL is the message itself.
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            num_frames = raw_zmtp_message_vec.len(),
            "PULL handle_pipe_event: Complete message assembled, pushing to orchestrator."
          );
          self
            .incoming_orchestrator
            .queue_item(pipe_id, raw_zmtp_message_vec)
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
