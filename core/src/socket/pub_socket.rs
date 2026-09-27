use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::SocketCore;
use crate::socket::patterns::{Distributor, PartialMessage, PipeMessageSender, SubscriptionMatcher};
use crate::socket::ISocket;
use crate::{delegate_to_core, Blob, MsgFlags};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

/// Implements the PUB (Publish) socket pattern.
///
/// Filtering happens on the publisher (like libzmq's XPUB): each subscriber's
/// `\x01`/`\x00` subscription frames are consumed on the session thread into a
/// shared [`SubscriptionMatcher`] (via a [`PipeMessageSender::SubscriptionSink`]),
/// and each outgoing message is matched against it so it is enqueued only to the
/// interested peers.
#[derive(Debug)]
pub(crate) struct PubSocket {
  core: Arc<SocketCore>,
  distributor: Distributor,
  /// Publisher-side subscription database (topic prefix -> subscribed peers).
  matcher: Arc<SubscriptionMatcher>,
  /// Ingress senders created in `pipe_attached`, handed to the session actor via
  /// `get_incoming_pipe_sender`.
  pending_pipe_senders: Mutex<HashMap<usize, PipeMessageSender>>,
  partial: PartialMessage,
}

impl PubSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      distributor: Distributor::new(),
      matcher: Arc::new(SubscriptionMatcher::new()),
      pending_pipe_senders: Mutex::new(HashMap::new()),
      partial: PartialMessage::default(),
    }
  }

  /// Matches one logical message against the subscription database and fans it
  /// out to interested peers, cleaning up any peers that errored during send.
  async fn dispatch(&self, frames: FrameBatch) -> Result<(), ZmqError> {
    match self
      .distributor
      .send_matched_multipart(frames, &self.matcher, self.core.handle)
      .await
    {
      Ok(()) => Ok(()),
      Err(failed_peers) => {
        for (peer_idx, error_detail) in failed_peers {
          tracing::debug!(
            handle = self.core.handle,
            peer_idx,
            error = %error_detail,
            "PUB removing disconnected/errored peer found during send"
          );
          self.distributor.remove_peer_by_idx(peer_idx);
          self.matcher.remove_peer(peer_idx);
        }
        Ok(()) // Still Ok(()) to the user, as per ZMQ PUB behavior.
      }
    }
  }
}

#[async_trait]
impl ISocket for PubSocket {
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
  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
  }
  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    delegate_to_core!(self, UserGetOpt, option: option)
  }
  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    tracing::trace!(
      handle = self.core.handle,
      msg_size = msg.size(),
      "PubSocket::send distributing message"
    );

    if !msg.is_more() && !self.partial.is_open() {
      let mut frames = FrameBatch::new();
      frames.push(msg);
      return self.dispatch(frames).await;
    }
    let Some(frames) = self.partial.push(msg) else {
      return Ok(());
    };
    self.dispatch(frames).await
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    Err(ZmqError::InvalidState("PUB sockets cannot receive messages"))
  }

  async fn send_multipart(&self, mut frames: FrameBatch) -> Result<(), ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    if frames.is_empty() {
      tracing::warn!(
        handle = self.core.handle,
        "PUB send_multipart called with empty frames vector. Doing nothing."
      );
      return Ok(());
    }

    // Adjust MORE flags for the logical ZMQ message parts.
    let num_frames = frames.len();
    for (i, frame) in frames.iter_mut().enumerate() {
      if i < num_frames - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
    }

    self.dispatch(frames).await
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    Err(ZmqError::UnsupportedFeature("PUB sockets cannot receive messages"))
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

  async fn handle_pipe_event(&self, _pipe_id: usize, _event: Command) -> Result<(), ZmqError> {
    // Inbound subscription frames are consumed by the SubscriptionSink ingress
    // sender on the session thread, not via pipe events.
    Ok(())
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize,
    _peer_identity: Option<&[u8]>,
  ) {
    let (endpoint_uri_opt, connection_iface_opt) = {
      let core_s = self.core.core_state.read();
      let uri = core_s
        .pipe_read_id_to_endpoint_uri
        .get(&pipe_read_id)
        .cloned();
      let iface = uri.as_ref().and_then(|u| {
        core_s
          .endpoints
          .get(u)
          .map(|ep| ep.connection_iface.clone())
      });
      (uri, iface)
    };

    if let (Some(endpoint_uri), Some(iface)) = (endpoint_uri_opt, connection_iface_opt) {
      let peer_idx = self
        .distributor
        .add_peer(pipe_read_id, endpoint_uri.clone(), iface);
      tracing::debug!(handle = self.core.handle, pipe_read_id, peer_idx, uri = %endpoint_uri, "PUB attaching connection");

      // Ingress sender that folds this peer's subscription frames into the matcher.
      let sender = PipeMessageSender::SubscriptionSink {
        peer_idx,
        matcher: Arc::clone(&self.matcher),
      };
      self.pending_pipe_senders.lock().insert(pipe_read_id, sender);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "PUB pipe_attached: Could not find endpoint_uri or connection_iface for pipe_read_id. Distributor not updated."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "PUB",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but PUB socket does not use peer identities. Ignoring."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(handle = self.core.handle, pipe_read_id, "PUB detaching connection");

    self.pending_pipe_senders.lock().remove(&pipe_read_id);

    if let Some(peer_idx) = self.distributor.remove_peer_by_pipe(pipe_read_id) {
      // Purge the peer's subscriptions before its index can be recycled.
      self.matcher.remove_peer(peer_idx);
      tracing::trace!(handle = self.core.handle, pipe_read_id, peer_idx, "PUB removed detached connection");
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "PUB detach: peer not found for read ID. Distributor may not be updated."
      );
    }
  }

  fn get_incoming_pipe_sender(&self, pipe_read_id: usize) -> Option<PipeMessageSender> {
    self.pending_pipe_senders.lock().remove(&pipe_read_id)
  }
}
