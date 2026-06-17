use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::options::SocketOptions;
use crate::socket::patterns::OutgoingMessageOrchestrator;

use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::time::timeout as tokio_timeout;

use crate::{Blob, delegate_to_core};

#[derive(Debug)]
pub(crate) struct PushSocket {
  core: Arc<SocketCore>,
  outgoing_orchestrator: OutgoingMessageOrchestrator,
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
  cached_options: ArcSwap<SocketOptions>,
}

impl PushSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let options_snapshot = core.core_state.read().options.clone();
    Self {
      core,
      outgoing_orchestrator: OutgoingMessageOrchestrator::new(),
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
      cached_options: ArcSwap::from(options_snapshot),
    }
  }
}

#[async_trait]
impl ISocket for PushSocket {
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

  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::ResourceLimitReached);
    }
    let sndtimeo = self.cached_options.load().sndtimeo;
    let wait_for_peer = !matches!(sndtimeo, Some(d) if d.is_zero());

    let mut fb = FrameBatch::new();
    fb.push(msg);
    self.send_with_timeout(fb, wait_for_peer, sndtimeo).await
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    Err(ZmqError::UnsupportedFeature(
      "PUSH sockets cannot receive messages",
    ))
  }

  async fn send_multipart(&self, mut frames: FrameBatch) -> Result<(), ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::ResourceLimitReached);
    }
    if frames.is_empty() {
      return Ok(());
    }

    let num_frames = frames.len();
    for (i, frame) in frames.iter_mut().enumerate() {
      if i < num_frames - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
    }

    let sndtimeo = self.cached_options.load().sndtimeo;
    let wait_for_peer = !matches!(sndtimeo, Some(d) if d.is_zero());
    self
      .send_with_timeout(frames, wait_for_peer, sndtimeo)
      .await
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    Err(ZmqError::UnsupportedFeature(
      "PUSH sockets cannot receive messages",
    ))
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    let result = delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec());
    if result.is_ok() {
      self
        .cached_options
        .store(self.core.core_state.read().options.clone());
    }
    result
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

  async fn process_command(&self, command: Command) -> Result<bool, ZmqError> {
    match command {
      Command::Stop => {
        tracing::debug!(
          handle = self.core.handle,
          "PushSocket received Stop. Deactivating outgoing orchestrator."
        );
        self.outgoing_orchestrator.deactivate();
      }
      _ => return Ok(false),
    }
    Ok(true)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    tracing::warn!(
      handle = self.core.handle,
      pipe_id = pipe_id,
      "PUSH socket received unexpected pipe event: {:?}",
      event.variant_name()
    );
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
      tracing::debug!(
        handle = self.core.handle,
        pipe_read_id,
        uri = %endpoint_uri,
        "PUSH attaching connection"
      );
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());
      self
        .outgoing_orchestrator
        .add_connection(endpoint_uri, iface);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "PUSH pipe_attached: Could not find endpoint_uri or connection_iface. Cannot add to orchestrator."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "PUSH",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, PUSH ignores peer identities."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id,
      "PUSH detaching connection"
    );
    if let Some(endpoint_uri) = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id) {
      self.outgoing_orchestrator.remove_connection(&endpoint_uri);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "PUSH detach: Endpoint URI not found for pipe_read_id."
      );
    }
  }
}

impl PushSocket {
  async fn send_with_timeout(
    &self,
    fb: FrameBatch,
    wait_for_peer: bool,
    sndtimeo: Option<Duration>,
  ) -> Result<(), ZmqError> {
    match sndtimeo {
      Some(d) if !d.is_zero() => {
        match tokio_timeout(
          d,
          self.outgoing_orchestrator.route_message(fb, wait_for_peer),
        )
        .await
        {
          Ok(Ok(())) => Ok(()),
          Ok(Err((_, e))) => Err(e),
          Err(_) => Err(ZmqError::Timeout),
        }
      }
      _ => match self
        .outgoing_orchestrator
        .route_message(fb, wait_for_peer)
        .await
      {
        Ok(()) => Ok(()),
        Err((_, e)) => Err(e),
      },
    }
  }
}
