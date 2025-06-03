use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::SocketCore;
use crate::socket::patterns::LoadBalancer;
use crate::socket::ISocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::time::timeout as tokio_timeout;

use crate::{delegate_to_core, Blob, MsgFlags};

#[derive(Debug)]
pub(crate) struct PushSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer, // Stores endpoint_uri
  /// Maps a pipe's read ID (from SocketCore's perspective) to the endpoint_uri of the connection.
  /// Needed during `pipe_detached` to tell LoadBalancer which URI to remove.
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl PushSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
    }
  }

  // Removed core_state(), direct access via self.core.core_state
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

  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      // Consistent with libzmq, PUSH with SNDTIMEO=0 would return EAGAIN if no peers.
      // If SNDTIMEO > 0, it would block/timeout.
      // Returning ResourceLimitReached implies "cannot send now".
      return Err(ZmqError::ResourceLimitReached);
    }

    let sndtimeo_opt: Option<Duration> = self.core.core_state.read().options.sndtimeo;

    // 1. Select a peer connection URI using the load balancer.
    let endpoint_uri_to_send_to = loop {
      if let Some(uri) = self.load_balancer.get_next_connection_uri() {
        // Check if this URI still corresponds to an active endpoint in SocketCore
        if self.core.core_state.read().endpoints.contains_key(&uri) {
          break uri;
        } else {
          // Stale URI in load balancer, remove it and try again
          tracing::warn!(handle = self.core.handle, uri = %uri, "PUSH send: Stale URI found in LoadBalancer. Removing.");
          self.load_balancer.remove_connection(&uri); // remove_connection takes &str
                                                      // No need to notify waiters here, as we are in a loop that will re-check or wait.
        }
      } else {
        // No URI available from load balancer
        if self.core.command_sender().is_closed() {
          // Check if socket is shutting down
          return Err(ZmqError::InvalidState("Socket terminated".into()));
        }
        match sndtimeo_opt {
          Some(duration) if duration.is_zero() => return Err(ZmqError::ResourceLimitReached),
          None => {
            self.load_balancer.wait_for_connection().await;
            // continue loop implicitly
          }
          Some(duration) => {
            match tokio_timeout(duration, self.load_balancer.wait_for_connection()).await {
              Ok(()) => { /* continue loop implicitly */ }
              Err(_timeout_elapsed) => return Err(ZmqError::Timeout),
            }
          }
        }
      }
    };

    // 2. Get the ISocketConnection interface for this URI.
    let connection_iface_arc = {
      let core_s = self.core.core_state.read(); // Read lock
                                                // We re-check contains_key here in case of races, though LB should be fairly up to date.
      core_s
        .endpoints
        .get(&endpoint_uri_to_send_to)
        .map(|ep_info| ep_info.connection_iface.clone())
        .ok_or_else(|| {
          tracing::warn!(
            handle = self.core.handle,
            uri = %endpoint_uri_to_send_to,
            "PUSH send: EndpointInfo disappeared for URI from LoadBalancer *after* selection. Removing from LB."
          );
          self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
          // This indicates the peer likely disconnected right as we were selecting it.
          ZmqError::HostUnreachable("Peer connection disappeared just before send".into())
        })?
    }; // core_s (read lock) is dropped here.

    // 3. Send the message using the connection interface.
    // The ISocketConnection::send_message implementation now handles SNDTIMEO for HWM.
    match connection_iface_arc.send_message(msg).await {
      Ok(()) => Ok(()),
      Err(ZmqError::ConnectionClosed) => {
        tracing::warn!(
          handle = self.core.handle,
          uri = %endpoint_uri_to_send_to,
          "PUSH send: Connection closed by peer or transport for URI. Removing from LB."
        );
        self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
        Err(ZmqError::HostUnreachable("Peer connection closed during send".into()))
      }
      Err(e @ ZmqError::ResourceLimitReached) | Err(e @ ZmqError::Timeout) => {
        // These are expected errors if the peer's HWM is hit or SNDTIMEO expires.
        // PUSH sockets typically drop messages or block based on SNDTIMEO.
        // If SNDTIMEO=0, ResourceLimitReached (EAGAIN) means drop.
        // If SNDTIMEO>0, Timeout means drop/fail.
        // If SNDTIMEO=-1, send_message in ISocketConnection would block, so this path shouldn't be hit unless pipe closes.
        tracing::trace!(
          handle = self.core.handle,
          uri = %endpoint_uri_to_send_to,
          error = %e,
          "PUSH send: HWM reached or timeout on specific connection. Propagating error."
        );
        Err(e)
      }
      Err(e) => {
        // Other, more severe errors (e.g., internal library errors)
        tracing::error!(
            handle = self.core.handle,
            uri = %endpoint_uri_to_send_to,
            error = %e,
            "PUSH send: Unexpected error on connection_iface.send_message. Removing from LB."
        );
        self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
        Err(e)
      }
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    Err(ZmqError::UnsupportedFeature("PUSH sockets cannot receive messages"))
  }

  async fn send_multipart(&self, mut frames: Vec<Msg>) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::ResourceLimitReached);
    }
    if frames.is_empty() {
      return Ok(()); // Sending an empty multipart is a no-op for PUSH
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

    let sndtimeo_opt: Option<Duration> = self.core.core_state.read().options.sndtimeo;

    let endpoint_uri_to_send_to = loop {
      if let Some(uri) = self.load_balancer.get_next_connection_uri() {
        if self.core.core_state.read().endpoints.contains_key(&uri) {
          break uri;
        } else {
          self.load_balancer.remove_connection(&uri);
        }
      } else {
        if !self.core.is_running().await {
          return Err(ZmqError::InvalidState(
            "Socket terminated while waiting for peer".into(),
          ));
        }
        match sndtimeo_opt {
          Some(duration) if duration.is_zero() => return Err(ZmqError::ResourceLimitReached),
          None => {
            self.load_balancer.wait_for_connection().await;
            continue;
          }
          Some(duration) => match tokio_timeout(duration, self.load_balancer.wait_for_connection()).await {
            Ok(()) => continue,
            Err(_) => return Err(ZmqError::Timeout),
          },
        }
      }
    };

    let connection_iface_arc = {
      let core_s = self.core.core_state.read();
      core_s
        .endpoints
        .get(&endpoint_uri_to_send_to)
        .map(|ep_info| ep_info.connection_iface.clone())
        .ok_or_else(|| {
          self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
          ZmqError::HostUnreachable("PUSH: Peer for multipart send disappeared after selection".into())
        })?
    };

    match connection_iface_arc.send_multipart(frames).await {
      Ok(()) => Ok(()),
      Err(ZmqError::ConnectionClosed) => {
        self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
        Err(ZmqError::HostUnreachable(
          "PUSH: Peer connection closed during multipart send".into(),
        ))
      }
      Err(e) => Err(e),
    }
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    Err(ZmqError::UnsupportedFeature("PUSH sockets cannot receive messages"))
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

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
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
    _pipe_write_id: usize, // Not directly used by PUSH LoadBalancer if it stores URIs
    _peer_identity: Option<&[u8]>,
  ) {
    // To add to LoadBalancer (which stores endpoint_uri), we need the endpoint_uri for this pipe.
    // SocketCore maintains pipe_read_id_to_endpoint_uri.
    let endpoint_uri_option = self
      .core
      .core_state
      .read()
      .pipe_read_id_to_endpoint_uri
      .get(&pipe_read_id)
      .cloned();

    if let Some(endpoint_uri) = endpoint_uri_option {
      tracing::debug!(
        handle = self.core.handle,
        pipe_read_id,
        // pipe_write_id, // Not logged as primary key for LB
        uri = %endpoint_uri,
        "PUSH attaching connection"
      );
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());
      self.load_balancer.add_connection(endpoint_uri);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "PUSH pipe_attached: Could not find endpoint_uri for pipe_read_id. Cannot add to LoadBalancer."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "PUSH",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but PUSH socket does not use peer identities. Ignoring."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id,
      "PUSH detaching connection identified by pipe_read_id"
    );

    let maybe_endpoint_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id);

    if let Some(endpoint_uri) = maybe_endpoint_uri {
      self.load_balancer.remove_connection(&endpoint_uri);
      tracing::trace!(
        handle = self.core.handle,
        pipe_read_id,
        uri = %endpoint_uri,
        "PUSH removed detached connection from load balancer"
      );
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "PUSH detach: Endpoint URI not found for pipe_read_id. LoadBalancer may not be updated."
      );
    }
  }
}
