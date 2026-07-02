use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::SocketCore;
use crate::socket::patterns::Distributor;
use crate::socket::ISocket;
use crate::{delegate_to_core, Blob, MsgFlags};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

/// Implements the PUB (Publish) socket pattern.
#[derive(Debug)]
pub(crate) struct PubSocket {
  core: Arc<SocketCore>,
  distributor: Distributor,
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl PubSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      distributor: Distributor::new(),
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
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

    match self.distributor.send_to_all(&msg, self.core.handle).await {
      Ok(()) => Ok(()),
      Err(failed_uris_with_errors) => {
        for (uri, error_detail) in failed_uris_with_errors {
          tracing::debug!(
            handle = self.core.handle,
            uri = %uri,
            error = %error_detail,
            "PUB removing disconnected/errored peer URI found during send"
          );
          // Remove the URI directly from the distributor.
          self.distributor.remove_peer_uri(&uri);
        }
        Ok(()) // Still return Ok(()) to the user, as per ZMQ PUB behavior.
      }
    }
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

    // Adjust MORE flags for the logical ZMQ message parts
    let num_frames = frames.len();
    for (i, frame) in frames.iter_mut().enumerate() {
      if i < num_frames - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
    }
    // `frames` now correctly represents the ZMTP frames of one logical ZMQ message.

    match self
      .distributor
      .send_to_all_multipart(frames, self.core.handle)
      .await
    {
      Ok(()) => Ok(()),
      Err(failed_uris_with_errors) => {
        for (uri, error_detail) in failed_uris_with_errors {
          tracing::debug!(
            handle = self.core.handle,
            uri = %uri,
            error = %error_detail,
            "PUB send_multipart: Removing disconnected/errored peer URI from distributor."
          );
          self.distributor.remove_peer_uri(&uri);
        }
        Ok(())
      }
    }
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
    Ok(())
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize, // No longer directly used by PubSocket for Distributor
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
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "PUB attaching connection");
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone()); // Guard dropped
      self.distributor.add_peer(endpoint_uri, iface);
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

    let maybe_endpoint_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id); // Guard dropped

    if let Some(endpoint_uri) = maybe_endpoint_uri {
      self.distributor.remove_peer_uri(&endpoint_uri); // Uses its own internal lock
      tracing::trace!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "PUB removed detached connection from distributor");
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "PUB detach: Endpoint URI not found for read ID in local map. Distributor may not be updated."
      );
    }
  }
}
