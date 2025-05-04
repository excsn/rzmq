// src/socket/pub_socket.rs

use crate::delegate_to_core;
// <<< ADDED PubSocket IMPLEMENTATION >>>
use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::SocketCore;
use crate::socket::patterns::Distributor; // Use Distributor
use crate::socket::ISocket;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(Debug)]
pub(crate) struct PubSocket {
  core: Arc<SocketCore>,
  distributor: Distributor,
}

impl PubSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      distributor: Distributor::new(),
    }
  }
}

#[async_trait]
impl ISocket for PubSocket {
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
    delegate_to_core!(self, UserClose)
  }

  // --- Pattern-Specific Logic ---
  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    // Use the distributor to send to all connected peers
    match self
      .distributor
      .send_to_all(&msg, &self.core.core_state)
      .await
    {
      Ok(()) => Ok(()),
      Err(errors) => {
        // Decide how to handle partial failures. ZMQ PUB usually just drops messages for disconnected peers.
        // Log errors but return Ok(()) to the user? Or return first error?
        // Let's log and return Ok, consistent with PUB's fire-and-forget nature.
        for (pipe_id, error) in errors {
          tracing::debug!(handle=self.core.handle, pipe_id=pipe_id, %error, "PUB send failed for one peer");
          // TODO: Should potentially trigger pipe removal from distributor?
          self.distributor.remove_pipe(pipe_id).await; // Be proactive
        }
        Ok(())
      }
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    Err(ZmqError::InvalidState(
      "PUB sockets cannot receive messages",
    ))
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    // PUB sockets ignore SUBSCRIBE/UNSUBSCRIBE etc.
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PUB",
      option = option,
      "Ignoring pattern option"
    );
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }
  async fn handle_pipe_event(&self, _pipe_id: usize, _event: Command) -> Result<(), ZmqError> {
    Ok(())
  } // PUB ignores incoming

  async fn pipe_attached(
    &self,
    _pipe_read_id: usize,
    pipe_write_id: usize, // PUB uses the Write ID
    _peer_identity: Option<&[u8]>,
  ) {
    self.distributor.add_pipe(pipe_write_id).await;
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    // TODO: Need mapping from read_id -> write_id to remove from distributor
    tracing::warn!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "TODO: Implement PUB pipe_detached correctly"
    );
    // let write_id = self.core.get_write_id(pipe_read_id).await?;
    // self.distributor.remove_pipe(write_id).await;
    let _ = pipe_read_id; // Mark used
  }
}