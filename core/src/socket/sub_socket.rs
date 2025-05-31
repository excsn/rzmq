// core/src/socket/sub_socket.rs

use super::patterns::IncomingMessageOrchestrator;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::EndpointInfo;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::options::{SUBSCRIBE, UNSUBSCRIBE};
use crate::socket::patterns::SubscriptionTrie;
use crate::socket::ISocket;
use crate::{delegate_to_core, Blob};

// Removed: use async_channel::Sender as AsyncSender;
use async_trait::async_trait;
use parking_lot::{RwLock, RwLockReadGuard}; // Using parking_lot::RwLock
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration; // For RCVTIMEO

#[derive(Debug)]
pub(crate) struct SubSocket {
  core: Arc<SocketCore>,
  subscriptions: SubscriptionTrie,
  incoming_orchestrator: IncomingMessageOrchestrator,
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl SubSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let orchestrator = IncomingMessageOrchestrator::new(&core);
    Self {
      core,
      subscriptions: SubscriptionTrie::new(),
      incoming_orchestrator: orchestrator,
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
    }
  }

  fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  fn construct_subscription_message(is_subscribe: bool, topic: &[u8]) -> Msg {
    let mut msg_body = Vec::with_capacity(1 + topic.len());
    if is_subscribe {
      msg_body.push(0x01);
    } else {
      msg_body.push(0x00);
    }
    msg_body.extend_from_slice(topic);
    Msg::from_vec(msg_body)
  }

  fn get_endpoint(&self, endpoint_uri: &str) -> Option<Arc<dyn ISocketConnection>> {
    // Short scope for lock
    self
      .core_state_read()
      .endpoints
      .get(endpoint_uri)
      .map(|ep_info| ep_info.connection_iface.clone())
  }

  async fn send_subscription_command_to_uri(&self, endpoint_uri: String, msg: &Msg) {
    let conn_iface_opt = self.get_endpoint(&endpoint_uri);
    if let Some(conn_iface) = conn_iface_opt {
      // Subscription commands are best-effort. ISocketConnection.send_message()
      // will use its internal SNDTIMEO. If it's None (block), these could block.
      // For SUB/UNSUB commands, typically we don't want to block the SubSocket logic indefinitely.
      // However, adhering to SNDTIMEO from options is the current design.
      match conn_iface.send_message(msg.clone()).await {
        Ok(()) => {
          tracing::trace!(handle = self.core.handle, uri = %endpoint_uri, "Sent subscription command to URI");
        }
        Err(e) => {
          tracing::warn!(
              handle = self.core.handle, uri = %endpoint_uri, error = %e,
              "Failed to send subscription command to URI (will be cleaned up if fatal)"
          );
          // If ConnectionClosed, pipe_detached will eventually remove it.
        }
      }
    } else {
      tracing::warn!(
          handle = self.core.handle, uri = %endpoint_uri,
          "SUB: No ISocketConnection found for URI during subscription command send. Peer might have detached."
      );
      // The URI might be stale; pipe_detached should clean up pipe_read_to_endpoint_uri.
    }
  }

  async fn send_subscription_command_to_all(&self, is_subscribe: bool, topic: &[u8]) {
    let msg = Self::construct_subscription_message(is_subscribe, topic);

    let peer_uris: Vec<String> = {
      // Short scope for lock
      self.pipe_read_to_endpoint_uri.read().values().cloned().collect()
    };

    if peer_uris.is_empty() {
      tracing::trace!(
        handle = self.core.handle,
        "Subscription command (all): No peer URIs to send to."
      );
      return;
    }

    let mut send_futures = Vec::new();
    for uri in peer_uris {
      send_futures.push(self.send_subscription_command_to_uri(uri.clone(), &msg));
    }

    let num_peers = send_futures.len();
    futures::future::join_all(send_futures).await; // Await all sends

    tracing::debug!(
        handle = self.core.handle,
        command = if is_subscribe { "SUB" } else { "CANCEL" },
        topic = ?String::from_utf8_lossy(topic),
        num_peers, "Sent subscription command to all known peer URIs."
    );
  }
}

#[async_trait]
impl ISocket for SubSocket {
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

  async fn send(&self, _msg: Msg) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("SUB sockets cannot send data messages"))
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = self.core_state_read().options.rcvtimeo; // Guard dropped
    self.incoming_orchestrator.recv_message(rcvtimeo_opt).await
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    match option {
      SUBSCRIBE | UNSUBSCRIBE => self.set_pattern_option(option, value).await,
      _ => delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec()),
    }
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("SUB sockets cannot send data messages"))
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = self.core_state_read().options.rcvtimeo; // Guard dropped
    self.incoming_orchestrator.recv_logical_message(rcvtimeo_opt).await
  }

  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    delegate_to_core!(self, UserGetOpt, option: option)
  }
  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    match option {
      SUBSCRIBE => {
        tracing::debug!(handle=self.core.handle, topic=?String::from_utf8_lossy(value), "Subscribing to topic");
        self.subscriptions.subscribe(value).await;
        self.send_subscription_command_to_all(true, value).await;
        Ok(())
      }
      UNSUBSCRIBE => {
        tracing::debug!(handle=self.core.handle, topic=?String::from_utf8_lossy(value), "Unsubscribing from topic");
        if self.subscriptions.unsubscribe(value).await {
          self.send_subscription_command_to_all(false, value).await;
        }
        Ok(())
      }
      _ => Err(ZmqError::UnsupportedOption(option)),
    }
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        if let Some(raw_zmtp_message_vec) = self.incoming_orchestrator.accumulate_pipe_frame(pipe_read_id, msg)? {
          let topic_data = raw_zmtp_message_vec
            .get(0)
            .and_then(|frame| frame.data())
            .unwrap_or_default();
          if self.subscriptions.matches(topic_data).await {
            self
              .incoming_orchestrator
              .queue_application_message_frames(pipe_read_id, raw_zmtp_message_vec)
              .await?;
          } else {
            tracing::trace!(handle = self.core.handle, pipe_id = pipe_read_id, topic = ?String::from_utf8_lossy(topic_data), "SUB: Message dropped (no subscription match).");
          }
        }
      }
      _ => {}
    }
    Ok(())
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize, // No longer directly used by SubSocket
    _peer_identity: Option<&[u8]>,
  ) {
    let endpoint_uri_option = self
      .core_state_read() // Guard dropped
      .pipe_read_id_to_endpoint_uri
      .get(&pipe_read_id)
      .cloned();

    if let Some(endpoint_uri) = endpoint_uri_option {
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "SUB attaching connection");
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone()); // Guard dropped

      // Send all current subscriptions to this newly attached peer
      let current_topics = self.subscriptions.get_all_topics().await;
      if !current_topics.is_empty() {
        tracing::debug!(handle = self.core.handle, uri = %endpoint_uri, num_topics = current_topics.len(), "Sending existing subscriptions to newly attached peer.");
        for topic in current_topics {
          let sub_msg = Self::construct_subscription_message(true, &topic);
          self.send_subscription_command_to_uri(endpoint_uri.clone(), &sub_msg).await;
        }
      }
    } else {
      tracing::warn!(
        handle = self.core.handle, pipe_read_id,
        "SUB pipe_attached: Could not find endpoint_uri for pipe_read_id. Cannot update internal map or send initial subscriptions."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "SUB",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but SUB socket does not use peer identities. Ignoring."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(handle = self.core.handle, pipe_read_id, "SUB detaching pipe");

    let removed_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id); // Guard dropped
    if removed_uri.is_some() {
      tracing::trace!(handle = self.core.handle, pipe_read_id, uri = %removed_uri.unwrap(), "SUB removed endpoint_uri mapping for detached pipe");
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "SUB detach: Endpoint URI not found for read ID in local map."
      );
    }
    self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
  }
}
