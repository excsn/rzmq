use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::fair_queue::FairQueue;
use crate::socket::ISocket;

use async_trait::async_trait;
use parking_lot::RwLockReadGuard;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout as tokio_timeout;

use crate::{delegate_to_core, Blob};

#[derive(Debug)]
pub(crate) struct PullSocket {
  core: Arc<SocketCore>,
  incoming_message_queue: FairQueue<Msg>,
}

impl PullSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let queue_capacity = { core.core_state.read().options.rcvhwm.max(1) }; // Scoped read
    Self {
      core,
      incoming_message_queue: FairQueue::new(queue_capacity),
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
    Err(ZmqError::InvalidState("PULL sockets cannot send messages".into()))
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo }; // Scoped read
    let pop_future = self.incoming_message_queue.pop_item(); // FairQueue<Msg>::pop_item

    match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => match tokio_timeout(duration, pop_future).await {
        Ok(Ok(Some(msg))) => Ok(msg),
        Ok(Ok(None)) => Err(ZmqError::Internal("PULL: Incoming message queue closed".into())),
        Ok(Err(e)) => Err(e),
        Err(_timeout_elapsed) => Err(ZmqError::Timeout),
      },
      _ => {
        if rcvtimeo_opt == Some(Duration::ZERO) {
          match self.incoming_message_queue.try_pop_item() {
            Ok(Some(msg)) => Ok(msg),
            Ok(None) => Err(ZmqError::ResourceLimitReached),
            Err(e) => Err(e),
          }
        } else {
          match pop_future.await? {
            Some(msg) => Ok(msg),
            None => Err(ZmqError::Internal(
              "PULL: Incoming message queue closed (inf wait)".into(),
            )),
          }
        }
      }
    }
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("PULL sockets cannot send messages".into()))
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let mut message_parts = Vec::new();
    let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo }; // Scoped read
    let overall_deadline_opt = rcvtimeo_opt.map(|d| tokio::time::Instant::now() + d);

    loop {
      let current_part_rcvtimeo: Option<Duration> = {
        if let Some(deadline) = overall_deadline_opt {
          let now = tokio::time::Instant::now();
          if now >= deadline {
            if message_parts.is_empty() {
              return Err(ZmqError::Timeout);
            } else {
              tracing::warn!("PULL recv_multipart: Timeout receiving subsequent part. Partial message discarded.");
              // Return the parts received so far, application must check MORE flag on last part.
              // Or, more strictly, return error. ZMQ typically errors if full message not received within timeout.
              return Err(ZmqError::ProtocolViolation(
                "Timeout during multi-part recv for PULL, discarding partial.".into(),
              ));
            }
          }
          Some(deadline - now)
        } else {
          None
        }
      };

      // Use self.recv() which pops one frame from the FairQueue<Msg>
      match self.recv_message_internal_for_multipart(current_part_rcvtimeo).await {
        Ok(frame) => {
          let is_last_part = !frame.is_more();
          message_parts.push(frame);
          if is_last_part {
            return Ok(message_parts);
          }
        }
        Err(ZmqError::Timeout) => {
          if message_parts.is_empty() {
            return Err(ZmqError::Timeout);
          } else {
            tracing::warn!("PULL recv_multipart: Timeout waiting for next frame. Partial message discarded.");
            return Err(ZmqError::ProtocolViolation(
              "Timeout during multi-part recv for PULL, discarding partial.".into(),
            ));
          }
        }
        Err(e @ ZmqError::ResourceLimitReached) => {
          // From RCVTIMEO=0 and queue empty
          if message_parts.is_empty() {
            return Err(e);
          } else {
            // Received some parts, but now would block (EAGAIN) for next. Return what we have.
            // App must check MORE flag on last part.
            tracing::trace!("PULL recv_multipart: ResourceLimitReached after some parts. Returning partial.");
            return Ok(message_parts);
          }
        }
        Err(e) => {
          if message_parts.is_empty() {
            return Err(e);
          } else {
            tracing::warn!(
              "PULL recv_multipart: Error ({:?}) on subsequent frame. Partial message discarded.",
              e
            );
            return Err(ZmqError::ProtocolViolation(format!(
              "Error during multi-part recv for PULL: {}, discarding partial.",
              e
            )));
          }
        }
      }
    }
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
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_id,
          msg_size = msg.size(),
          more_flag = msg.is_more(),
          "PULL handle_pipe_event: Received frame, pushing to FairQueue<Msg>."
        );
        if let Err(e) = self.incoming_message_queue.push_item(msg).await {
          tracing::error!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            "PULL: Error pushing frame to FairQueue: {}",
            e
          );
          return Err(e);
        }
      }
      _ => {}
    }
    Ok(())
  }

  async fn pipe_attached(&self, pipe_read_id: usize, _pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PULL attaching pipe"
    );
    self.incoming_message_queue.pipe_attached(pipe_read_id);
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

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PULL detaching pipe"
    );
    self.incoming_message_queue.pipe_detached(pipe_read_id);
  }
}

// Helper for recv_multipart to call recv() logic internally, similar to ReqSocket
impl PullSocket {
  /// Internal helper for recv_multipart to get single frames with timeout.
  async fn recv_message_internal_for_multipart(&self, rcvtimeo_opt: Option<Duration>) -> Result<Msg, ZmqError> {
    // This duplicates the logic of the public recv() method.
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let pop_future = self.incoming_message_queue.pop_item();

    match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => match tokio_timeout(duration, pop_future).await {
        Ok(Ok(Some(msg))) => Ok(msg),
        Ok(Ok(None)) => Err(ZmqError::Internal(
          "PULL (internal): Incoming message queue closed".into(),
        )),
        Ok(Err(e)) => Err(e),
        Err(_timeout_elapsed) => Err(ZmqError::Timeout),
      },
      _ => {
        if rcvtimeo_opt == Some(Duration::ZERO) {
          match self.incoming_message_queue.try_pop_item() {
            Ok(Some(msg)) => Ok(msg),
            Ok(None) => Err(ZmqError::ResourceLimitReached),
            Err(e) => Err(e),
          }
        } else {
          match pop_future.await? {
            Some(msg) => Ok(msg),
            None => Err(ZmqError::Internal(
              "PULL (internal): Incoming message queue closed (inf wait)".into(),
            )),
          }
        }
      }
    }
  }
}
