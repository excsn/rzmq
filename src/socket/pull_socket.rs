// src/socket/pull_socket.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::FairQueue; // Use the fair queue helper
use crate::socket::ISocket;
use async_trait::async_trait;
use tokio::time::timeout;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, MutexGuard};

use super::options::SocketOptions;

#[derive(Debug)]
pub(crate) struct PullSocket {
  core: Arc<SocketCore>,
  fair_queue: FairQueue, // Buffers incoming messages
}

impl PullSocket {
  /// Creates a new PullSocket.
  pub fn new(core: Arc<SocketCore>, options: SocketOptions) -> Self {
    let queue_capacity = options.rcvhwm.max(1);
    Self {
      core,
      fair_queue: FairQueue::new(queue_capacity),
    }
  }

  /// Helper to access core state safely.
  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

#[async_trait]
impl ISocket for PullSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }

  fn mailbox(&self) -> &MailboxSender {
    self.core.mailbox_sender()
  }

  // --- Methods mirroring public API ---

  async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserBind {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox error".into()))?;
    reply_rx.await.map_err(|_| ZmqError::Internal("Reply error".into()))?
  }

  async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserConnect {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox error".into()))?;
    reply_rx.await.map_err(|_| ZmqError::Internal("Reply error".into()))?
  }

  async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserDisconnect {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox error".into()))?;
    reply_rx.await.map_err(|_| ZmqError::Internal("Reply error".into()))?
  }

  async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserUnbind {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox error".into()))?;
    reply_rx.await.map_err(|_| ZmqError::Internal("Reply error".into()))?
  }

  async fn send(&self, _msg: Msg) -> Result<(), ZmqError> {
    // PULL sockets cannot send messages
    Err(ZmqError::InvalidState("PULL sockets cannot send messages"))
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    // Pop message from the fair queue (awaits if empty)
    // 1. Get configured timeout from core state
    let rcvtimeo_opt: Option<Duration> = {
      // Limit lock scope
      self.core_state().await.options.rcvtimeo
    };

    // 2. Pop message from the fair queue, applying timeout if set
    let pop_future = self.fair_queue.pop_message();

    let result = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => {
        // Timeout > 0
        tracing::trace!(handle = self.core.handle, ?duration, "Applying RCVTIMEO to recv");
        match timeout(duration, pop_future).await {
          Ok(Ok(Some(msg))) => Ok(msg), // Message received within timeout
          Ok(Ok(None)) => {
            // Queue closed while waiting (unexpected)
            tracing::error!(handle = self.core.handle, "PULL recv failed: FairQueue closed");
            Err(ZmqError::Internal("Receive queue closed".into()))
          }
          Ok(Err(e)) => Err(e), // Internal error from pop_message
          Err(_timeout_elapsed) => {
            tracing::trace!(handle = self.core.handle, "PULL recv timed out");
            Err(ZmqError::Timeout) // Timeout occurred
          }
        }
      }
      _ => {
        // No timeout (None) or zero timeout (immediate check - though pop_message awaits)
        // Await indefinitely (or until queue closes)
        match pop_future.await? {
          // Use ? to propagate internal errors
          Some(msg) => Ok(msg),
          None => {
            tracing::error!(handle = self.core.handle, "PULL recv failed: FairQueue closed");
            Err(ZmqError::Internal("Receive queue closed".into()))
          }
        }
      }
    };
    result
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    // TODO: Handle RCVHWM by recreating/resizing the FairQueue? Complex.
    // For now, delegate all to core.
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserSetOpt {
        option,
        value: value.to_vec(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox error".into()))?;
    reply_rx.await.map_err(|_| ZmqError::Internal("Reply error".into()))?
  }

  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // TODO: Handle RCVHWM by reading FairQueue capacity?
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserGetOpt { option, reply_tx })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox error".into()))?;
    reply_rx.await.map_err(|_| ZmqError::Internal("Reply error".into()))?
  }

  async fn close(&self) -> Result<(), ZmqError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserClose { reply_tx })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox error".into()))?;
    reply_rx.await.map_err(|_| ZmqError::Internal("Reply error".into()))?
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PULL",
      option = option,
      "set_pattern_option called"
    );
    // PULL socket has no specific pattern options
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PULL",
      option = option,
      "get_pattern_option called"
    );
    // PULL socket has no specific readable pattern options
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Methods ---

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    // PULL doesn't have specific commands
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_id,
          msg_size = msg.size(),
          "PULL pushing message to FairQueue"
        );
        // Push message into the internal queue
        self.fair_queue.push_message(msg).await?;
      }
      // Ignore other pipe events for now? Or log them?
      _ => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_id,
          "PULL received unhandled pipe event: {:?}",
          event
        );
      }
    }
    Ok(())
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize, // PULL cares about the read pipe ID
    _pipe_write_id: usize,
    _peer_identity: Option<&[u8]>,
  ) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PULL attaching pipe"
    );
    // Notify FairQueue helper (might be used for tracking later)
    self.fair_queue.pipe_attached(pipe_read_id);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PULL detaching pipe"
    );
    // Notify FairQueue helper
    self.fair_queue.pipe_detached(pipe_read_id);
  }
}
