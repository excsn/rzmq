// src/socket/sub_socket.rs

use crate::error::ZmqError;
use crate::message::Msg; // For recv method and subscription commands.
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore}; // Core components.
use crate::socket::options::{SocketOptions, SUBSCRIBE, UNSUBSCRIBE}; // Option constants.
use crate::socket::patterns::SubscriptionTrie; // SUB uses SubscriptionTrie and FairQueue.
use crate::socket::ISocket;
use crate::{delegate_to_core, Blob}; // Macro for delegating API calls to SocketCore. // The trait this struct implements.
use super::patterns::IncomingMessageOrchestrator; 

use async_channel::Sender as AsyncSender; // For sending commands over pipes.
use async_trait::async_trait;
use parking_lot::RwLockReadGuard;
use std::collections::HashMap; // For pipe_read_to_write_id map.
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Implements the SUB (Subscribe) socket pattern.
/// SUB sockets receive messages published by PUB (Publish) sockets.
/// They must subscribe to specific topics (or all topics using an empty prefix)
/// to receive messages. Message filtering based on subscriptions happens on the SUB side.
/// SUB sockets do not send application data messages, but they do send
/// SUBSCRIBE/CANCEL command messages upstream to their connected PUB peers.
#[derive(Debug)]
pub(crate) struct SubSocket {
  /// Arc to the shared `SocketCore` actor that manages common socket state and transport.
  core: Arc<SocketCore>,
  /// `SubscriptionTrie` to manage topic subscriptions and efficiently match incoming messages.
  subscriptions: SubscriptionTrie,
  incoming_orchestrator: IncomingMessageOrchestrator,
  /// Maps a pipe's read ID (from SocketCore's perspective) to its corresponding write ID.
  /// This is needed to send SUBSCRIBE/CANCEL command messages upstream to the correct PUB peer.
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
}

impl SubSocket {
  /// Creates a new `SubSocket`.
  ///
  /// # Arguments
  /// * `core` - An `Arc` to the `SocketCore` managing this socket.
  /// * `options` - Initial socket options, used here to determine queue capacity (RCVHWM).
  pub fn new(core: Arc<SocketCore>) -> Self {
    let orchestrator = IncomingMessageOrchestrator::new(
      &core,
    );

    Self {
      core,
      subscriptions: SubscriptionTrie::new(),
      incoming_orchestrator: orchestrator,
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
    }
  }

  /// Helper to get a locked guard for the `CoreState` within `SocketCore`.
  fn core_state(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  /// Constructs a ZMTP SUBSCRIBE or CANCEL message.
  /// Note: These are sent as regular data messages, not ZMTP Command frames.
  /// For libZMQ compatibility:
  /// - Subscribe: 0x01 followed by topic
  /// - Cancel:    0x00 followed by topic
  fn construct_subscription_message(is_subscribe: bool, topic: &[u8]) -> Msg {
    let mut msg_body = Vec::with_capacity(1 + topic.len());
    if is_subscribe {
      msg_body.push(0x01); // Subscribe prefix byte
    } else {
      msg_body.push(0x00); // Cancel prefix byte
    }
    msg_body.extend_from_slice(topic);
    Msg::from_vec(msg_body) // No special flags needed
  }

  /// Helper function to send a subscription command message to a *specific* pipe.
  async fn send_command_to_pipe(
    &self,
    pipe_write_id: usize,
    sender: AsyncSender<Msg>, // Borrow sender
    msg: &Msg,                // Borrow message
  ) {
    // Sub/Cancel commands are best-effort, don't use SNDTIMEO from options?
    // Or use a short, fixed timeout? Let's use try_send for simplicity (non-blocking).
    match sender.try_send(msg.clone()) {
      Ok(()) => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "Sent subscription command via pipe"
        );
      }
      Err(async_channel::TrySendError::Full(_)) => {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "Failed to send subscription command (pipe full, message dropped)."
        );
      }
      Err(async_channel::TrySendError::Closed(_)) => {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "Failed to send subscription command (pipe closed)."
        );
        // Optionally: We could try removing this pipe from pipe_read_to_write_id here,
        // but it might race with pipe_detached. Let pipe_detached handle removal.
      }
    }
  }

  /// Helper function to send a SUBSCRIBE or CANCEL command message upstream to ALL connected PUB peers.
  async fn send_subscription_command_to_all(&self, is_subscribe: bool, topic: &[u8]) {
    let msg = Self::construct_subscription_message(is_subscribe, topic);

    // Collect all target pipe senders to send the command to.
    let mut target_pipe_senders: Vec<(usize, AsyncSender<Msg>)> = Vec::new();
    {
      let pipe_map_guard = self.pipe_read_to_write_id.lock().await;

      if pipe_map_guard.is_empty() {
        tracing::trace!(
          handle = self.core.handle,
          "Subscription command (all): No peer pipes to send to."
        );
        return;
      }
      target_pipe_senders.reserve(pipe_map_guard.len());

      let state_guard = self.core_state();
      for pipe_write_id in pipe_map_guard.values() {
        if let Some(sender) = state_guard.get_pipe_sender(*pipe_write_id) {
          target_pipe_senders.push((*pipe_write_id, sender));
        } else {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = *pipe_write_id,
            "SUB command (all): Pipe sender not found in core state. Skipping."
          );
        }
      }
    } // Locks released

    if target_pipe_senders.is_empty() {
      tracing::trace!(
        handle = self.core.handle,
        "Subscription command (all): No valid pipe senders found."
      );
      return;
    }

    // Send concurrently to all targets.
    let mut send_futures = Vec::new();
    for (pipe_write_id, sender) in target_pipe_senders {
      // Use the single-pipe helper
      send_futures.push(self.send_command_to_pipe(pipe_write_id, sender, &msg));
    }

    let num_peers = send_futures.len();
    futures::future::join_all(send_futures).await;

    tracing::debug!(
        handle = self.core.handle,
        command = if is_subscribe { "SUB" } else { "CANCEL" },
        topic = ?String::from_utf8_lossy(topic),
        num_peers = num_peers,
        "Sent subscription command to all peers."
    );
  }
}

#[async_trait]
impl ISocket for SubSocket {
  /// Returns a reference to the `SocketCore`.
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }

  /// Returns a clone of the `SocketCore`'s command mailbox sender.
  fn mailbox(&self) -> MailboxSender {
    self.core.command_sender()
  }

  // --- API Method Implementations (mostly delegated to SocketCore) ---
  async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
    // SUB sockets typically connect, but binding is allowed (though less common).
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

  /// SUB sockets cannot send application data messages. This method will always return an error.
  async fn send(&self, _msg: Msg) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("SUB sockets cannot send data messages"))
  }

  /// Receives a message using the SUB pattern.
  /// Only messages matching active subscriptions are delivered.
  /// This call will block (asynchronously) if no matching messages are available,
  /// unless a `RCVTIMEO` is set.
  async fn recv(&self) -> Result<Msg, ZmqError> {
    // Get RCVTIMEO from options.
    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };

    return self.incoming_orchestrator.recv_message(rcvtimeo_opt).await;
  }

  /// Sets a socket option.
  /// For SUB sockets, `SUBSCRIBE` and `UNSUBSCRIBE` are handled as pattern-specific options.
  /// Other options are delegated to `SocketCore`.
  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    match option {
      SUBSCRIBE | UNSUBSCRIBE => self.set_pattern_option(option, value).await,
      _ => {
        // Delegate other options to SocketCore.
        delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
      }
    }
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("SUB sockets cannot send data messages"))
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };
    return self.incoming_orchestrator.recv_logical_message(rcvtimeo_opt).await;
  }

  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // SUB sockets do not have specific readable pattern options beyond what SocketCore provides.
    delegate_to_core!(self, UserGetOpt, option: option)
  }

  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  // --- Pattern-Specific Option Handling (SUBSCRIBE, UNSUBSCRIBE) ---
  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    match option {
      SUBSCRIBE => {
        tracing::debug!(handle=self.core.handle, topic=?String::from_utf8_lossy(value), "Subscribing to topic");
        self.subscriptions.subscribe(value).await; // Add to local trie.
                                                   // Send SUBSCRIBE command message upstream to all connected PUB peers.
        self.send_subscription_command_to_all(true, value).await;
        Ok(())
      }
      UNSUBSCRIBE => {
        tracing::debug!(handle=self.core.handle, topic=?String::from_utf8_lossy(value), "Unsubscribing from topic");
        // `unsubscribe` returns true if the topic existed and its count reached zero.
        if self.subscriptions.unsubscribe(value).await {
          // Send CANCEL command message upstream to all connected PUB peers.
          self.send_subscription_command_to_all(false, value).await;
        }
        Ok(())
      }
      _ => {
        // This path should ideally not be reached if `set_option` routes correctly.
        Err(ZmqError::UnsupportedOption(option))
      }
    }
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // SUB sockets do not have readable pattern-specific options (e.g., listing subscriptions).
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks called by SocketCore ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    // SUB sockets do not typically handle special commands beyond user API calls.
    Ok(false) // Indicate command was not handled here.
  }

  /// Handles messages received from a pipe by the `SocketCore`.
  /// For SUB sockets, this means a message part has arrived from a PUB peer.
  /// The message is checked against active subscriptions before being queued.
  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        // 1. Accumulate frame.
        if let Some(raw_zmtp_message_vec) = self.incoming_orchestrator.accumulate_pipe_frame(pipe_read_id, msg)? {
          // accumulate is sync

          // 2. Perform Subscription Filtering (on the first frame of the raw ZMTP message)
          let topic_data = raw_zmtp_message_vec
            .get(0)
            .and_then(|frame| frame.data())
            .unwrap_or_default(); // Default to empty topic if no data/frame

          if self.subscriptions.matches(topic_data).await {
            tracing::trace!(
                handle = self.core.handle, pipe_id = pipe_read_id,
                topic = ?String::from_utf8_lossy(topic_data),
                "SUB: Message matched subscription. Queueing via orchestrator."
            );
            // 3. If matched, the raw_zmtp_message_vec IS the application logical message.
            //    No further transformation is needed by a static "processing function".
            self
              .incoming_orchestrator
              .queue_application_message_frames(pipe_read_id, raw_zmtp_message_vec)
              .await?;
          } else {
            tracing::trace!(
                handle = self.core.handle, pipe_id = pipe_read_id,
                topic = ?String::from_utf8_lossy(topic_data),
                "SUB: Message dropped (no subscription match)."
            );
            // Message is dropped, not queued.
          }
        }
      }
      _ => { /* SUB sockets typically ignore other direct pipe events from SocketCore. */ }
    }
    Ok(())
  }

  /// Called by `SocketCore` when a new pipe (connection to a PUB peer) is attached.
  async fn pipe_attached(
    &self,
    pipe_read_id: usize,           // ID SocketCore uses to read from this PUB peer.
    pipe_write_id: usize,          // ID SocketCore uses to write (subscribe/cancel commands) to this PUB peer.
    _peer_identity: Option<&[u8]>, // Peer identity is not typically used by SUB.
  ) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "SUB attaching pipe"
    );

    // Store the mapping from read ID to write ID for sending subscription commands.
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);

    // --- FIX: Send existing subscriptions to the newly attached peer ---
    let current_topics = self.subscriptions.get_all_topics().await;
    if !current_topics.is_empty() {
      tracing::debug!(
        handle = self.core.handle,
        pipe_write_id = pipe_write_id,
        num_topics = current_topics.len(),
        "Sending existing subscriptions to newly attached peer."
      );

      // Get the sender for *this specific* pipe
      let sender_option = {
        let core_state_guard = self.core_state();
        core_state_guard.get_pipe_sender(pipe_write_id)
        // core_state lock released here
      };

      if let Some(sender) = sender_option {
        for topic in current_topics {
          let msg = Self::construct_subscription_message(true, &topic);
          // Send to the specific pipe using the helper
          self.send_command_to_pipe(pipe_write_id, sender.clone(), &msg).await;
        }
      } else {
        // This might happen if the pipe detached extremely quickly after attaching.
        tracing::warn!(
          handle = self.core.handle,
          pipe_write_id = pipe_write_id,
          "Could not find sender for newly attached pipe to send subscriptions (likely detached)."
        );
      }
    } else {
      tracing::trace!(
        handle = self.core.handle,
        pipe_write_id = pipe_write_id,
        "No existing subscriptions to send to newly attached peer."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "THEIR_SOCKET_TYPE", // e.g., "DEALER"
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but this socket type does not use peer identities. Ignoring."
    );
  }

  /// Called by `SocketCore` when a pipe is detached (PUB peer disconnected or socket closing).
  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "SUB detaching pipe"
    );
    // Remove the read ID -> write ID mapping.
    self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);

    // ADD: Clear any state the orchestrator might hold for this pipe.
    self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
  }
}
