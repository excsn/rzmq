use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{Mutex as ParkingMutex, RwLock, RwLockReadGuard};

use crate::socket::patterns::AnonymousIngressEngine;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::options::{SUBSCRIBE, UNSUBSCRIBE};
use crate::socket::patterns::SubscriptionTrie;
use crate::{Blob, delegate_to_core};

#[derive(Debug)]
pub(crate) struct SubSocket {
  core: Arc<SocketCore>,
  subscriptions: Arc<SubscriptionTrie>,
  ingress_engine: AnonymousIngressEngine,
  pending_pipe_senders: ParkingMutex<HashMap<usize, PipeMessageSender>>,
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl SubSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let max_conn = core.core_state.read().options.max_connections.unwrap_or(1024);
    Self {
      core,
      subscriptions: Arc::new(SubscriptionTrie::new()),
      ingress_engine: AnonymousIngressEngine::new(max_conn),
      pending_pipe_senders: ParkingMutex::new(HashMap::new()),
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
    }
  }

  fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  fn construct_subscription_message(is_subscribe: bool, topic: &[u8]) -> Msg {
    let mut msg_body = Vec::with_capacity(1 + topic.len());
    msg_body.push(if is_subscribe { 0x01 } else { 0x00 });
    msg_body.extend_from_slice(topic);
    Msg::from_vec(msg_body)
  }

  fn get_endpoint(&self, endpoint_uri: &str) -> Option<Arc<dyn ISocketConnection>> {
    self
      .core_state_read()
      .endpoints
      .get(endpoint_uri)
      .map(|ep_info| ep_info.connection_iface.clone())
  }

  async fn send_subscription_command_via_iface(
    &self,
    conn_iface: &Arc<dyn ISocketConnection>,
    endpoint_uri: &str,
    msg: Msg,
  ) -> Result<(), ZmqError> {
    match conn_iface.send_message(msg).await {
      Ok(()) => {
        tracing::trace!(handle = self.core.handle, uri = %endpoint_uri, "Sent subscription command to URI");
        Ok(())
      }
      Err(e) => {
        tracing::warn!(
          handle = self.core.handle, uri = %endpoint_uri, error = %e,
          "Failed to send subscription command to URI"
        );
        Err(e)
      }
    }
  }

  async fn send_subscription_command_to_uri(&self, endpoint_uri: String, msg: &Msg) {
    let conn_iface_opt = self.get_endpoint(&endpoint_uri);
    if let Some(conn_iface) = conn_iface_opt {
      let _ = self
        .send_subscription_command_via_iface(&conn_iface, &endpoint_uri, msg.clone())
        .await;
    } else {
      tracing::warn!(
        handle = self.core.handle, uri = %endpoint_uri,
        "SUB: No ISocketConnection found for URI during subscription command send."
      );
    }
  }

  async fn send_subscription_command_to_all(&self, is_subscribe: bool, topic: &[u8]) {
    let msg = Self::construct_subscription_message(is_subscribe, topic);
    let peer_uris: Vec<String> = self
      .pipe_read_to_endpoint_uri
      .read()
      .values()
      .cloned()
      .collect();

    if peer_uris.is_empty() {
      return;
    }

    let num_peers = peer_uris.len();
    let mut futures = Vec::with_capacity(num_peers);
    for uri in peer_uris {
      futures.push(self.send_subscription_command_to_uri(uri, &msg));
    }
    futures::future::join_all(futures).await;

    tracing::debug!(
      handle = self.core.handle,
      command = if is_subscribe { "SUB" } else { "CANCEL" },
      topic = ?String::from_utf8_lossy(topic),
      num_peers,
      "Sent subscription command to all known peer URIs."
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
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = self.core_state_read().options.rcvtimeo;
    self.ingress_engine.recv(rcvtimeo_opt).await
  }

  async fn send_multipart(&self, _frames: FrameBatch) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("SUB sockets cannot send data messages"))
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = self.core_state_read().options.rcvtimeo;
    self.ingress_engine.recv_multipart(rcvtimeo_opt).await
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    match option {
      SUBSCRIBE | UNSUBSCRIBE => self.set_pattern_option(option, value).await,
      _ => delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec()),
    }
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
        tracing::debug!(handle = self.core.handle, topic = ?String::from_utf8_lossy(value), "Subscribing to topic");
        self.subscriptions.subscribe(value);
        self.send_subscription_command_to_all(true, value).await;
        Ok(())
      }
      UNSUBSCRIBE => {
        tracing::debug!(handle = self.core.handle, topic = ?String::from_utf8_lossy(value), "Unsubscribing from topic");
        if self.subscriptions.unsubscribe(value) {
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

  async fn process_command(&self, command: Command) -> Result<bool, ZmqError> {
    match command {
      Command::Stop => {
        self.ingress_engine.close();
      }
      _ => return Ok(false),
    }
    Ok(true)
  }

  async fn handle_pipe_event(&self, _pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      // Data frames are pushed directly by the actor via PipeMessageSender; no action needed.
      Command::PipeMessageReceived { .. } | Command::PipeMessageBatchReceived { .. } => {}
      _ => {}
    }
    Ok(())
  }

  fn get_incoming_pipe_sender(&self, pipe_read_id: usize) -> Option<PipeMessageSender> {
    self.pending_pipe_senders.lock().remove(&pipe_read_id)
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize,
    _peer_identity: Option<&[u8]>,
  ) {
    let endpoint_uri_option = self
      .core_state_read()
      .pipe_read_id_to_endpoint_uri
      .get(&pipe_read_id)
      .cloned();

    if let Some(endpoint_uri) = endpoint_uri_option {
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "SUB attaching connection");
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());

      // Register per-pipe ingress channel and create a filtered sender.
      let (rcvhwm, rcvbatch_count) = {
        let opts = self.core_state_read();
        (opts.options.rcvhwm.max(1), opts.options.rcvbatch_count)
      };
      let sender = self.ingress_engine.register_pipe_filtered(pipe_read_id, rcvhwm, Arc::clone(&self.subscriptions), rcvbatch_count);
      self.pending_pipe_senders.lock().insert(pipe_read_id, sender);

      // Send any existing subscriptions to the new peer.
      let current_topics = self.subscriptions.get_all_topics();
      if !current_topics.is_empty() {
        tracing::debug!(
          handle = self.core.handle, uri = %endpoint_uri,
          num_topics = current_topics.len(),
          "Sending existing subscriptions to newly attached peer."
        );
        if let Some(conn_iface) = self.get_endpoint(&endpoint_uri) {
          for topic in current_topics {
            let sub_msg = Self::construct_subscription_message(true, &topic);
            if self
              .send_subscription_command_via_iface(&conn_iface, &endpoint_uri, sub_msg)
              .await
              .is_err()
            {
              tracing::warn!(
                handle = self.core.handle, uri = %endpoint_uri,
                "Aborting subscription sync due to send error."
              );
              break;
            }
          }
        }
      }
    } else {
      tracing::warn!(
        handle = self.core.handle, pipe_read_id,
        "SUB pipe_attached: Could not find endpoint_uri for pipe_read_id."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle, socket_type = "SUB", pipe_read_id, ?identity,
      "update_peer_identity called, SUB ignores peer identities."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(handle = self.core.handle, pipe_read_id, "SUB detaching pipe");

    let removed_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id);
    if let Some(uri) = removed_uri {
      tracing::trace!(handle = self.core.handle, pipe_read_id, uri = %uri, "SUB removed endpoint_uri mapping");
    }

    self.ingress_engine.deregister_pipe(pipe_read_id);
    self.pending_pipe_senders.lock().remove(&pipe_read_id);
  }
}
