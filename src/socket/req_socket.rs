// src/socket/req_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::{FairQueue, LoadBalancer}; // Use LB for outgoing, FQ for incoming reply
use crate::socket::ISocket;
use async_trait::async_trait;
use std::collections::HashMap; // If tracking requests
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard}; // Use Mutex for state

/// Represents the state of the REQ socket's send/recv cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReqState {
  ReadyToSend,    // Socket is ready to send the next request.
  ExpectingReply, // Socket has sent a request and is waiting for a reply.
}

#[derive(Debug)]
pub(crate) struct ReqSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer, // Chooses peer for outgoing requests
  // Although REQ expects only one reply per request, using FairQueue
  // simplifies receiving that reply via the standard handle_pipe_event mechanism.
  // The state machine ensures only one recv() call waits for it.
  incoming_reply_queue: FairQueue,
  // Track the state of the request-reply cycle
  state: Mutex<ReqState>,
  // Map Read Pipe ID -> Write Pipe ID (needed for pipe_detached)
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
}

impl ReqSocket {
  /// Creates a new ReqSocket.
  pub fn new(core: Arc<SocketCore>) -> Self {
    // TODO: Get RCVHWM for FairQueue capacity? Reply queue likely only needs capacity 1?
    let reply_queue_capacity = 1; // Only expecting one reply at a time
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      incoming_reply_queue: FairQueue::new(reply_queue_capacity),
      state: Mutex::new(ReqState::ReadyToSend), // Start ready to send
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
    }
  }

  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

// --- ISocket Implementation ---
#[async_trait]
impl ISocket for ReqSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }
  
  fn mailbox(&self) -> &MailboxSender {
    self.core.mailbox_sender()
  }

  // --- API Method Implementations ---
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
    let mut current_state = self.state.lock().await;
    if *current_state != ReqState::ReadyToSend {
      return Err(ZmqError::InvalidState(
        "REQ socket must call recv() before sending again",
      ));
    }

    // 1. Select a peer using the load balancer
    let pipe_write_id = match self.load_balancer.get_next_pipe().await {
      Some(id) => id,
      None => {
        tracing::debug!(
          handle = self.core.handle,
          "REQ send failed: No connected peers (REP/ROUTER)"
        );
        return Err(ZmqError::ResourceLimitReached); // No peers available (EAGAIN)
      }
    };

    // 2. Get the sender channel from CoreState
    // NOTE: This requires locking CoreState *again*. Consider if LB can store Sender clones?
    // For now, follow previous pattern.
    let pipe_tx = {
      let core_state_guard = self.core_state().await;
      match core_state_guard.get_pipe_sender(pipe_write_id) {
        Some(tx) => tx,
        None => {
          tracing::error!(
            handle = self.core.handle,
            pipe_id = pipe_write_id,
            "REQ send failed: Pipe sender not found"
          );
          self.load_balancer.remove_pipe(pipe_write_id).await; // Cleanup LB
          return Err(ZmqError::Internal("Pipe sender consistency error".into()));
        }
      }
    };

    // 3. Prepend empty delimiter frame if needed?
    // ZMQ REQ adds an empty frame when sending to ROUTER, but not to REP.
    // How do we know? Assume for now we don't prepend (connect directly to REP).
    // TODO: Add logic based on peer type if/when known.

    // Ensure MORE flag is cleared on the final (or only) part
    if !msg.is_more() {
      // This is the final part, clear MORE just in case
      // msg.flags.remove(MsgFlags::MORE); // Need mutable access to flags if using bitflags directly
      let mut flags = msg.flags();
      flags.remove(MsgFlags::MORE);
      msg.set_flags(flags);
    }

    // 4. Send the message via the channel
    if let Err(_e) = pipe_tx.send(msg).await {
      tracing::warn!(
        handle = self.core.handle,
        pipe_id = pipe_write_id,
        "REQ send failed: Pipe channel closed"
      );
      self.load_balancer.remove_pipe(pipe_write_id).await; // Cleanup LB
                                                           // Don't change state back, user needs to handle error and maybe retry send?
                                                           // Or should we allow sending again? ZMQ REQ usually gets stuck here. Let's error.
                                                           // Keep state as ReadyToSend? No, send *failed*. Let's stay ReadyToSend.
      Err(ZmqError::ConnectionClosed) // Indicate peer likely gone
    } else {
      // 5. Transition state
      *current_state = ReqState::ExpectingReply;
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_write_id,
        "REQ sent request, expecting reply"
      );
      Ok(())
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    let mut current_state = self.state.lock().await;
    if *current_state != ReqState::ExpectingReply {
      return Err(ZmqError::InvalidState(
        "REQ socket must call send() before receiving",
      ));
    }

    // Pop message from the fair queue (should only contain the expected reply)
    // This awaits until the message arrives (pushed by handle_pipe_event)
    let result = match self.incoming_reply_queue.pop_message().await? {
      Some(msg) => {
        // TODO: Check for and remove empty delimiter frame if expected?
        Ok(msg)
      }
      None => {
        tracing::error!(
          handle = self.core.handle,
          "REQ recv failed: Reply queue closed unexpectedly"
        );
        Err(ZmqError::Internal("Receive queue closed".into()))
      }
    };

    // Transition state back regardless of intermediate errors?
    // ZMQ REQ usually gets unstuck only on successful recv. If pop_message fails, we stay stuck?
    // Let's transition back only on success.
    if result.is_ok() {
      *current_state = ReqState::ReadyToSend;
      tracing::trace!(
        handle = self.core.handle,
        "REQ received reply, ready to send"
      );
    } else {
      tracing::warn!(
        handle = self.core.handle,
        "REQ recv failed, state remains ExpectingReply"
      );
    }

    result
  }

  // --- Pattern Specific Options ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "REQ",
      option = option,
      "set_pattern_option called"
    );
    Err(ZmqError::UnsupportedOption(option)) // REQ has no specific pattern options
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "REQ",
      option = option,
      "get_pattern_option called"
    );
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        // Any message received must be the reply we are waiting for
        let current_state = self.state.lock().await;
        if *current_state == ReqState::ExpectingReply {
          // Push reply message into the queue for recv() to pick up
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            msg_size = msg.size(),
            "REQ pushing reply to FairQueue"
          );
          // Use try_send because queue capacity is 1. If it fails, something is wrong.
          if let Err(e) = self.incoming_reply_queue.push_message(msg).await {
            tracing::error!(handle=self.core.handle, pipe_id=pipe_id, error=?e, "Failed to push reply to REQ queue (already full or closed?)");
            // This indicates a state mismatch or error. Maybe drop message?
          }
        } else {
          tracing::warn!(handle=self.core.handle, pipe_id=pipe_id, msg_size=msg.size(), state=?*current_state, "REQ received unexpected message from pipe (dropping)");
          // Drop unexpected messages
        }
      }
      _ => { /* REQ ignores other pipe events like ActivateReadCmd */ }
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
      "REQ attaching pipe"
    );
    // Store mapping for detachment
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    // Add write pipe to load balancer
    self.load_balancer.add_pipe(pipe_write_id).await;
    // Notify incoming queue (though only one message expected at a time)
    self.incoming_reply_queue.pipe_attached(pipe_read_id);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "REQ detaching pipe"
    );
    // Remove from map and get write ID
    let maybe_write_id = self
      .pipe_read_to_write_id
      .lock()
      .await
      .remove(&pipe_read_id);
    // Remove write pipe from load balancer if found
    if let Some(write_id) = maybe_write_id {
      self.load_balancer.remove_pipe(write_id).await;
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        "REQ detach: Write ID not found for read ID"
      );
    }
    // Notify incoming queue
    self.incoming_reply_queue.pipe_detached(pipe_read_id);
    // TODO: What happens to state if pipe detached while ExpectingReply?
    // Should recv() unblock with an error? Should state reset?
    // ZMQ REQ usually gets stuck. For now, we don't change state here.
  }
} // end impl ISocket for ReqSocket
