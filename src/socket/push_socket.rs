// src/socket/push_socket.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore}; // Need access to CoreState/Core
use crate::socket::patterns::LoadBalancer; // Use the load balancer helper
use crate::socket::ISocket; // Implement the trait
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{oneshot, MutexGuard}; // For locking CoreState

#[derive(Debug)]
pub(crate) struct PushSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer, // Distributes outgoing messages
}

impl PushSocket {
  /// Creates a new PushSocket.
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      load_balancer: LoadBalancer::new(), // Create a new load balancer
    }
  }

  /// Helper to access core state safely.
  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

#[async_trait]
impl ISocket for PushSocket {
  fn mailbox(&self) -> &MailboxSender {
    self.core.mailbox_sender()
  }

  // --- Methods mirroring public API ---

  async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
    // Core handles actual binding and transport actor spawning
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserBind {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
    // Core handles actual connecting and transport actor spawning
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserConnect {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
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
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
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
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    // 1. Select a peer using the load balancer
    let maybe_pipe_id = self.load_balancer.get_next_pipe().await;

    let pipe_id = match maybe_pipe_id {
      Some(id) => id,
      None => {
        // ZMQ behavior for PUSH with no connected peers depends on HWM/blocking mode.
        // For now, return an error indicating no peer available.
        // TODO: Implement blocking/HWM behavior if SNDHWM > 0 or timeout set.
        tracing::debug!(
          handle = self.core.handle,
          "PUSH send failed: No connected peers"
        );
        return Err(ZmqError::ResourceLimitReached); // Simulate EAGAIN/HWM
      }
    };

    // 2. Get the sender channel from CoreState
    let pipe_tx = {
      // Limit lock scope
      let state = self.core_state().await;
      match state.pipes_tx.get(&pipe_id) {
        Some(tx) => tx.clone(), // Clone the sender end
        None => {
          tracing::error!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            "PUSH send failed: Pipe sender not found in core state (consistency issue?)"
          );
          // This indicates a state inconsistency, maybe pipe detached but not removed from LB?
          // Remove the stale pipe from LB
          self.load_balancer.remove_pipe(pipe_id).await;
          return Err(ZmqError::Internal("Pipe sender not found".into()));
        }
      }
    };

    // 3. Send the message via the channel
    // Use send().await - this handles backpressure if the channel buffer (SNDHWM) is full.
    if let Err(_e) = pipe_tx.send(msg).await {
      tracing::warn!(
        handle = self.core.handle,
        pipe_id = pipe_id,
        "PUSH send failed: Pipe channel closed by session/engine"
      );
      // Peer likely disconnected. Remove it from the load balancer.
      self.load_balancer.remove_pipe(pipe_id).await;
      // Return error, maybe ResourceLimitReached or ConnectionClosed?
      Err(ZmqError::ConnectionClosed) // Indicate peer is gone
    } else {
      Ok(())
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    // PUSH sockets cannot receive messages
    Err(ZmqError::InvalidState(
      "PUSH sockets cannot receive messages",
    ))
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    // Core handles general options. PUSH has few specific ones (maybe SNDHWM?)
    // Delegate to core first, then handle pattern specific if any.
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
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // Core handles general options.
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserGetOpt { option, reply_tx })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn close(&self) -> Result<(), ZmqError> {
    // Delegate shutdown to core
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserClose { reply_tx })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PUSH",
      option = option,
      "set_pattern_option called"
    );
    // PUSH socket has no specific options like SUBSCRIBE or ROUTING_ID
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PUSH",
      option = option,
      "get_pattern_option called"
    );
    // PUSH socket has no specific readable pattern options
    Err(ZmqError::UnsupportedOption(option))
  }
  
  // --- Internal Methods ---

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    // PUSH doesn't have many specific commands beyond basic user actions
    Ok(false) // Indicate core should handle or ignore
  }

  async fn handle_pipe_event(&self, _pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    // PUSH doesn't expect messages back, so PipeMessageReceived shouldn't happen.
    tracing::warn!(
      handle = self.core.handle,
      pipe_id = _pipe_id,
      "PUSH socket received unexpected pipe event: {:?}",
      event
    );
    Ok(())
  }

  async fn pipe_attached(
    &self,
    _pipe_read_id: usize, // PUSH doesn't use the read pipe
    pipe_write_id: usize, // Core writes TO peer using this ID
    _peer_identity: Option<&[u8]>,
  ) {
    // Add the writing pipe ID to the load balancer
    tracing::debug!(
      handle = self.core.handle,
      pipe_write_id = pipe_write_id,
      "PUSH attaching pipe"
    );
    self.load_balancer.add_pipe(pipe_write_id).await;
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    // PUSH socket uses the write ID to send, but detached event comes with read ID?
    // This highlights a need to store the mapping (read_id -> write_id) or pass both
    // Let's assume for now pipe_detached gives the ID that needs removing from LB
    // TODO: Clarify ID mapping for detachment. Assuming read_id corresponds to the connection.
    // Need to find the associated write_id to remove from LB.
    // This requires CoreState.endpoints to store both IDs.
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PUSH detaching pipe"
    );
    // Placeholder: We need the write_id associated with this connection/read_id!
    // self.load_balancer.remove_pipe(associated_write_id).await;
    tracing::warn!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "TODO: Implement proper removal from LoadBalancer on pipe_detached"
    );
  }
}
// <<< ADDED PushSocket IMPLEMENTATION END >>>
