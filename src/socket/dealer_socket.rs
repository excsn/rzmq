// src/socket/dealer_socket.rs

use crate::delegate_to_core;
// <<< ADDED DealerSocket IMPLEMENTATION >>>
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::{FairQueue, LoadBalancer}; // Use LB (out) and FQ (in)
use crate::socket::ISocket;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, MutexGuard};

#[derive(Debug)]
pub(crate) struct DealerSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer, // For outgoing messages
  incoming_queue: FairQueue,   // For incoming messages
  // Map Read Pipe ID -> Write Pipe ID (needed for pipe_detached)
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
  // TODO: Option to configure if empty delimiter should be added/expected?
  // connect_to_router: bool, // Example config flag
}

impl DealerSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    // TODO: Get HWMs from core options
    let initial_hwm = 1000;
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      incoming_queue: FairQueue::new(initial_hwm),
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
      // connect_to_router: true, // Default? Or detect?
    }
  }

  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

#[async_trait]
impl ISocket for DealerSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }
  fn mailbox(&self) -> &MailboxSender {
    self.core.mailbox_sender()
  }

  // --- API Method Implementations (Delegate to Core) ---
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

  // --- Pattern-Specific Logic ---
  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    // 1. Select peer using LoadBalancer
    let pipe_write_id = match self.load_balancer.get_next_pipe().await {
      Some(id) => id,
      None => return Err(ZmqError::ResourceLimitReached), // No peers (EAGAIN)
    };

    // 2. Get sender channel
    let pipe_tx = {
      let core_state_guard = self.core_state().await;
      match core_state_guard.get_pipe_sender(pipe_write_id) {
        Some(tx) => tx,
        None => {
          /* ... consistency error, remove from LB, return error ... */
          tracing::error!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            "DEALER send failed: Pipe sender not found"
          );
          self.load_balancer.remove_pipe(pipe_write_id).await;
          return Err(ZmqError::Internal("Pipe sender consistency error".into()));
        }
      }
    };

    // 3. Prepend empty delimiter frame if talking to ROUTER (assume yes for now)
    // TODO: Make this conditional
    let mut delimiter = Msg::new(); // Empty message
    delimiter.set_flags(MsgFlags::MORE); // Set MORE flag on delimiter

    // 4. Send delimiter then message part(s)
    // Need to handle multi-part messages from the user correctly.
    // Send delimiter first
    if let Err(_e) = pipe_tx.send(delimiter).await {
      tracing::warn!(
        handle = self.core.handle,
        pipe_id = pipe_write_id,
        "DEALER send (delimiter) failed: Pipe channel closed"
      );
      self.load_balancer.remove_pipe(pipe_write_id).await;
      return Err(ZmqError::ConnectionClosed);
    }

    // Send original message (part)
    // If original msg had MORE flag, subsequent parts must be sent by user immediately.
    if let Err(_e) = pipe_tx.send(msg).await {
      // Send original msg
      tracing::warn!(
        handle = self.core.handle,
        pipe_id = pipe_write_id,
        "DEALER send (payload) failed: Pipe channel closed"
      );
      self.load_balancer.remove_pipe(pipe_write_id).await;
      // Note: Delimiter was already sent. This might break peer state.
      return Err(ZmqError::ConnectionClosed);
    }

    tracing::trace!(
      handle = self.core.handle,
      pipe_id = pipe_write_id,
      "DEALER sent message"
    );
    Ok(())
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    // Pop message from the fair queue (awaits if empty)
    let mut msg = match self.incoming_queue.pop_message().await? {
      Some(m) => m,
      None => return Err(ZmqError::Internal("Receive queue closed".into())),
    };

    // Strip leading empty delimiter if present (assume peer is ROUTER)
    // TODO: Make this conditional
    if msg.size() == 0 {
      tracing::trace!(
        handle = self.core.handle,
        "DEALER stripping empty delimiter"
      );
      // This was the delimiter, the *next* message part is the payload.
      // This implies recv needs to handle multi-part messages internally if
      // the delimiter and payload arrive separately via handle_pipe_event.
      // RETHINK: FairQueue needs to store Vec<Msg> or recv needs to loop.
      // Let's assume FQ stores single Msg and handle_pipe_event adds metadata.

      // --- Assuming single Msg from FQ, check metadata ---
      #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
      struct IsDelimiter;
      if msg.metadata().get::<IsDelimiter>().await.is_some() {
        // This was marked as a delimiter by handle_pipe_event. Get next part.
        msg = match self.incoming_queue.pop_message().await? {
          Some(m) => m,
          None => {
            return Err(ZmqError::ProtocolViolation(
              "Received delimiter but no payload".into(),
            ))
          }
        };
      } else {
        // It's a zero-byte message, but wasn't marked as delimiter? Treat as payload.
      }
    }

    Ok(msg)
  }

  // --- Pattern Specific Options ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { mut msg, .. } => {
        // Optional: Check if first part is empty delimiter (if peer is ROUTER)
        // Add metadata if it is, before pushing.
        // TODO: Make conditional
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        struct IsDelimiter;
        if msg.size() == 0 && !msg.is_more() {
          // Simplistic check for delimiter-only message
          // Don't queue delimiter? Or queue it with metadata? Queue with metadata.
          msg.metadata_mut().insert_typed(IsDelimiter).await;
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            "DEALER queuing delimiter"
          );
          self.incoming_queue.push_message(msg).await?;
        } else {
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            msg_size = msg.size(),
            "DEALER queuing message"
          );
          self.incoming_queue.push_message(msg).await?;
        }
      }
      _ => { /* DEALER ignores other pipe events */ }
    }
    Ok(())
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    pipe_write_id: usize,
    _peer_identity: Option<&[u8]>,
  ) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "DEALER attaching pipe"
    );
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    self.load_balancer.add_pipe(pipe_write_id).await;
    self.incoming_queue.pipe_attached(pipe_read_id);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "DEALER detaching pipe"
    );
    let maybe_write_id = self
      .pipe_read_to_write_id
      .lock()
      .await
      .remove(&pipe_read_id);
    if let Some(write_id) = maybe_write_id {
      self.load_balancer.remove_pipe(write_id).await;
    }
    self.incoming_queue.pipe_detached(pipe_read_id);
  }
} // end impl ISocket for DealerSocket
