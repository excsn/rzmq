// src/socket/sub_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::protocol::zmtp::command::{ZMTP_CMD_CANCEL_NAME, ZMTP_CMD_SUBSCRIBE_NAME};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::options::{SUBSCRIBE, UNSUBSCRIBE};
use crate::socket::patterns::{FairQueue, SubscriptionTrie}; // Use Trie and FairQueue
use crate::socket::ISocket;
use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use async_trait::async_trait;
use tokio::time::timeout;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, MutexGuard};

use super::options::SocketOptions; // Mutex needed for pipe_tx map

#[derive(Debug)]
pub(crate) struct SubSocket {
  core: Arc<SocketCore>,
  subscriptions: SubscriptionTrie,
  fair_queue: FairQueue, // Queue for matched messages from all pipes
  // Store write pipes to send SUBSCRIBE/CANCEL commands upstream
  // Map Pipe Read ID -> Pipe Write ID to find correct sender in CoreState
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
}

impl SubSocket {
  pub fn new(core: Arc<SocketCore>, options: SocketOptions) -> Self {
    let queue_capacity = options.rcvhwm.max(1);
    Self {
      core,
      subscriptions: SubscriptionTrie::new(),
      fair_queue: FairQueue::new(queue_capacity),
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
    }
  }

  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }

  /// Helper to send a SUBSCRIBE or CANCEL command message upstream.
  async fn send_subscription_command(&self, command_name: &[u8], topic: &[u8]) {
    let mut command_body = Vec::with_capacity(command_name.len() + topic.len());
    command_body.extend_from_slice(command_name); // e.g., ZMTP_CMD_SUBSCRIBE_NAME
    command_body.extend_from_slice(topic);

    let mut msg = Msg::from_vec(command_body);
    // msg.set_flags(...) // No COMMAND flag for SUBSCRIBE/CANCEL

    let mut targets: Vec<(usize, AsyncSender<Msg>)>;

    // --- Acquire Locks and Collect Data ---
    {
      // Scope to limit lock guards
      let state_guard = self.core_state().await; // Lock core state
      let pipe_map_guard = self.pipe_read_to_write_id.lock().await; // Lock pipe map

      targets = Vec::with_capacity(pipe_map_guard.len());
      if pipe_map_guard.is_empty() {
        tracing::trace!("Subscription command: No peers to send to.");
        return; // Exit early if no pipes
      }

      targets.reserve(pipe_map_guard.len()); // Reserve capacity now
                                             // Collect pairs of (pipe_write_id, Sender<Msg>)
      for write_pipe_id in pipe_map_guard.values() {
        if let Some(sender) = state_guard.get_pipe_sender(*write_pipe_id) {
          targets.push((*write_pipe_id, sender)); // Store ID and cloned sender
        } else {
          tracing::warn!(
            pipe_id = write_pipe_id,
            "Sub command: Pipe sender not found in core state (consistency issue?). Skipping."
          );
          // TODO: Should this inconsistency trigger removal from pipe_read_to_write_id map? Needs careful thought.
        }
      }

      // --- Locks Released Here when scope ends ---
    } // state_guard and pipe_map_guard dropped here

    // --- Create Futures After Locks Released ---
    if targets.is_empty() {
      return; // No valid targets found
    }

    let mut send_futures = Vec::new();
    for (pipe_write_id, sender) in targets {
      let msg_clone = msg.clone(); // Clone msg for each task
      send_futures.push(async move {
        if let Err(e) = sender.send(msg_clone).await {
          tracing::warn!(
            pipe_id = pipe_write_id,
            error = ?e, // Use debug format for error
            "Failed to send subscription command"
          );
          // Optionally return the error/pipe_id for cleanup later?
          // For now, just log.
        }
      });
    }

    // Send concurrently
    futures::future::join_all(send_futures).await;
  }
}

#[async_trait]
impl ISocket for SubSocket {
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

  async fn send(&self, _msg: Msg) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("SUB sockets cannot send data messages"))
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    let rcvtimeo_opt: Option<Duration> = { self.core_state().await.options.rcvtimeo };

    let pop_future = self.fair_queue.pop_message(); // Waits on internal queue

    let result = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => {
        tracing::trace!(handle = self.core.handle, ?duration, "Applying RCVTIMEO to SUB recv");
        match timeout(duration, pop_future).await {
          Ok(Ok(Some(msg))) => Ok(msg), // Matched message received within timeout
          Ok(Ok(None)) => Err(ZmqError::Internal("Receive queue closed".into())),
          Ok(Err(e)) => Err(e),
          Err(_timeout_elapsed) => Err(ZmqError::Timeout),
        }
      }
      _ => {
        // No timeout
        match pop_future.await? {
          Some(msg) => Ok(msg),
          None => Err(ZmqError::Internal("Receive queue closed".into())),
        }
      }
    };

    result
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    match option {
      SUBSCRIBE => self.set_pattern_option(option, value).await,
      UNSUBSCRIBE => self.set_pattern_option(option, value).await,
      _ => {
        // Delegate other options to core
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
    }
  }

  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // SUB has no specific readable pattern options
    // Delegate all to core
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
    delegate_to_core!(self, UserClose,)
  }

  // --- Pattern Specific Options ---
  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    match option {
      SUBSCRIBE => {
        tracing::debug!(handle=self.core.handle, topic=?String::from_utf8_lossy(value), "Subscribing");
        self.subscriptions.subscribe(value).await;
        // Send SUBSCRIBE command message upstream to all connected peers
        self.send_subscription_command(ZMTP_CMD_SUBSCRIBE_NAME, value).await;
        Ok(())
      }
      UNSUBSCRIBE => {
        tracing::debug!(handle=self.core.handle, topic=?String::from_utf8_lossy(value), "Unsubscribing");
        if self.subscriptions.unsubscribe(value).await {
          // Send CANCEL command message upstream to all connected peers
          self.send_subscription_command(ZMTP_CMD_CANCEL_NAME, value).await;
        }
        Ok(())
      }
      _ => Err(ZmqError::UnsupportedOption(option)),
    }
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option)) // No readable SUB options
  }

  // --- Internal Hooks ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        // Check message against subscriptions before queuing
        let topic = msg.data().unwrap_or(&[]); // Use whole message data as topic? ZMQ SUB usually uses first frame.
                                               // TODO: Refine topic extraction for multi-part messages if needed.
        if self.subscriptions.matches(topic).await {
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            msg_size = msg.size(),
            "SUB pushing matched message to FairQueue"
          );
          self.fair_queue.push_message(msg).await?;
        } else {
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            msg_size = msg.size(),
            "SUB dropping unmatched message"
          );
          // Drop messages that don't match any subscription
        }
      }
      _ => { /* SUB ignores other pipe events */ }
    }
    Ok(())
  }

  async fn pipe_attached(&self, pipe_read_id: usize, pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "SUB attaching pipe"
    );
    // Store mapping for sending subscriptions
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    // Notify FairQueue helper
    self.fair_queue.pipe_attached(pipe_read_id);
    // TODO: Send all current subscriptions to the newly attached peer
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "SUB detaching pipe"
    );
    // Remove from map
    self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);
    // Notify FairQueue helper
    self.fair_queue.pipe_detached(pipe_read_id);
  }
}
