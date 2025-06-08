#![allow(dead_code)]

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::SocketCore;
use fibre::TrySendError;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub(crate) struct ScaConnectionIface {
  sca_stop_mailbox: MailboxSender,
  sca_handle_id: usize,
  socket_core_ref: Arc<SocketCore>,
  pipe_write_id_to_sca: usize,
}

impl ScaConnectionIface {
  pub(crate) fn new(
    sca_stop_mailbox: MailboxSender,
    sca_handle_id: usize,
    socket_core_ref: Arc<SocketCore>,
    pipe_write_id_to_sca: usize,
  ) -> Self {
    Self {
      sca_stop_mailbox,
      sca_handle_id,
      socket_core_ref,
      pipe_write_id_to_sca,
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

    let pipe_sender_opt = {
      // Scope for read lock
      let core_s_reader = self.socket_core_ref.core_state.read();
      core_s_reader.pipes_tx.get(&self.pipe_write_id_to_sca).cloned()
    };

    if let Some(pipe_sender) = pipe_sender_opt {
      let send_timeout_opt = self.socket_core_ref.core_state.read().options.sndtimeo;

      if send_timeout_opt == Some(Duration::ZERO) {
        // Non-blocking send attempt
        match pipe_sender.try_send(vec![msg]) {
          Ok(()) => Ok(()),
          Err(TrySendError::Full(_)) => Err(ZmqError::ResourceLimitReached),
          Err(TrySendError::Closed(_)) => Err(ZmqError::ConnectionClosed),
          Err(TrySendError::Sent(_)) => unreachable!(),
        }
      } else {
        // Blocking/timed send
        let timeout_duration = send_timeout_opt.unwrap_or(Duration::from_secs(30)); // Or some other default for None
        match timeout(timeout_duration, pipe_sender.send(vec![msg])).await {
          Ok(Ok(())) => Ok(()),
          Ok(Err(_)) => Err(ZmqError::ConnectionClosed),
          Err(_) => Err(ZmqError::Timeout), // Use Timeout to match RCVTIMEO behavior
        }
      }
    } else {
      tracing::error!(
        "Pipe sender for core_write_id {} not found for SCA {} in ScaConnectionIface.",
        self.pipe_write_id_to_sca,
        self.sca_handle_id
      );
      Err(ZmqError::Internal(format!(
        "SCA Pipe sender missing in iface for pipe_id {}",
        self.pipe_write_id_to_sca
      )))
    }
  }

  async fn send_multipart(&self, msgs: Vec<Msg>) -> Result<(), ZmqError> {
    tracing::trace!(
      sca_handle = self.sca_handle_id,
      pipe_id = self.pipe_write_id_to_sca,
      num_msgs = msgs.len(),
      "ScaConnectionIface sending multipart via data pipe."
    );

    let pipe_sender_opt = {
      // Scope for read lock
      let core_s_reader = self.socket_core_ref.core_state.read();
      core_s_reader.pipes_tx.get(&self.pipe_write_id_to_sca).cloned()
    };

    if let Some(pipe_sender) = pipe_sender_opt {
      let send_timeout = self
        .socket_core_ref
        .core_state
        .read()
        .options
        .sndtimeo
        .unwrap_or(Duration::from_secs(5));

      
      match timeout(send_timeout, pipe_sender.send(msgs)).await {
        Ok(Ok(())) => {},
        Ok(Err(_send_error)) => {
          return Err(ZmqError::ConnectionClosed);
        }
        Err(_timeout_elapsed) => {
          return Err(ZmqError::ResourceLimitReached);
        }
      }

      Ok(())
    } else {
      Err(ZmqError::Internal(format!(
        "SCA Pipe sender missing in iface for pipe_id {}",
        self.pipe_write_id_to_sca
      )))
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
      .map_err(|e| ZmqError::Internal(format!("Failed to send Stop to SCA {}: {}", self.sca_handle_id, e)))
  }

  fn get_connection_id(&self) -> usize {
    self.sca_handle_id
  }

  fn as_any(&self) -> &dyn std::any::Any {
    self
  }
}
