#![allow(dead_code)]

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;

use std::time::Duration;

use async_trait::async_trait;
use fibre::TrySendError;
use fibre::mpmc::AsyncSender;
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub(crate) struct ScaConnectionIface {
  sca_stop_mailbox: MailboxSender,
  sca_handle_id: usize,
  pipe_sender: AsyncSender<Vec<Msg>>,
  pipe_write_id_to_sca: usize,
  sndtimeo: Option<Duration>,
}

impl ScaConnectionIface {
  pub(crate) fn new(
    sca_stop_mailbox: MailboxSender,
    sca_handle_id: usize,
    pipe_sender: AsyncSender<Vec<Msg>>,
    pipe_write_id_to_sca: usize,
    sndtimeo: Option<Duration>,
  ) -> Self {
    Self {
      sca_stop_mailbox,
      sca_handle_id,
      pipe_sender,
      pipe_write_id_to_sca,
      sndtimeo,
    }
  }
}

#[async_trait]
impl ISocketConnection for ScaConnectionIface {
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
    tracing::trace!(
      sca_handle = self.sca_handle_id,
      pipe_id = self.pipe_write_id_to_sca,
      "ScaConnectionIface sending single message via data pipe."
    );

    if self.sndtimeo == Some(Duration::ZERO) {
      match self.pipe_sender.try_send(vec![msg]) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err(ZmqError::ResourceLimitReached),
        Err(TrySendError::Closed(_)) => Err(ZmqError::ConnectionClosed),
        Err(TrySendError::Sent(_)) => unreachable!(),
      }
    } else {
      let timeout_duration = self.sndtimeo.unwrap_or(Duration::from_secs(30));
      match timeout(timeout_duration, self.pipe_sender.send(vec![msg])).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ZmqError::ConnectionClosed),
        Err(_) => Err(ZmqError::ResourceLimitReached),
      }
    }
  }

  async fn send_multipart(&self, msgs: Vec<Msg>) -> Result<(), ZmqError> {
    tracing::trace!(
      sca_handle = self.sca_handle_id,
      pipe_id = self.pipe_write_id_to_sca,
      num_msgs = msgs.len(),
      "ScaConnectionIface sending multipart via data pipe."
    );

    if self.sndtimeo == Some(Duration::ZERO) {
      match self.pipe_sender.try_send(msgs) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err(ZmqError::ResourceLimitReached),
        Err(TrySendError::Closed(_)) => Err(ZmqError::ConnectionClosed),
        Err(TrySendError::Sent(_)) => unreachable!(),
      }
    } else {
      let timeout_duration = self.sndtimeo.unwrap_or(Duration::from_secs(30));
      match timeout(timeout_duration, self.pipe_sender.send(msgs)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ZmqError::ConnectionClosed),
        Err(_) => Err(ZmqError::ResourceLimitReached),
      }
    }
  }

  async fn close_connection(&self) -> Result<(), ZmqError> {
    tracing::debug!(
      "ScaConnectionIface: Sending Stop command to SCA {}.",
      self.sca_handle_id
    );
    // Assuming Command::Stop is a general stop that SCA will handle.
    // If Stop needs to be targeted, Command::Stop would need a handle_id field.
    self
      .sca_stop_mailbox
      .send(Command::Stop)
      .await
      .map_err(|e| {
        ZmqError::Internal(format!(
          "Failed to send Stop to SCA {}: {}",
          self.sca_handle_id, e
        ))
      })
  }

  fn get_connection_id(&self) -> usize {
    self.sca_handle_id
  }

  fn as_any(&self) -> &dyn std::any::Any {
    self
  }
}
