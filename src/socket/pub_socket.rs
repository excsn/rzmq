// src/socket/pub_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::SocketCore;
use crate::socket::patterns::Distributor;
use crate::socket::ISocket;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

#[derive(Debug)]
pub(crate) struct PubSocket {
  core: Arc<SocketCore>,
  distributor: Distributor,
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
}

impl PubSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      distributor: Distributor::new(),
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
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
    match self.distributor.send_to_all(&msg, &self.core.core_state).await {
      // send_to_all handles logging drops/errors internally for PUB
      // It returns Vec of errors for peers that need cleanup.
      Ok(()) => Ok(()), // Succeeded for all reachable peers (or dropped)
      Err(errors) => {
        // Some peers disconnected or had errors during send.
        // PUB sockets don't report send errors to the user,
        // but we should clean up the disconnected peers.
        for (pipe_id, _error) in errors {
          tracing::debug!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            "PUB removing disconnected peer found during send"
          );
          // We only have the write ID here. Need to call pipe_detached on Core,
          // which requires the read ID. This mapping problem persists.
          // TODO: Fix pipe ID mapping for cleanup after send failure.
          self.distributor.remove_pipe(pipe_id).await; // Remove directly from distributor for now
        }
        Ok(()) // Still return Ok(()) to the user for PUB
      }
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    Err(ZmqError::InvalidState("PUB sockets cannot receive messages"))
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
    pipe_read_id: usize,
    pipe_write_id: usize, // PUB uses the Write ID
    _peer_identity: Option<&[u8]>,
  ) {
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    self.distributor.add_pipe(pipe_write_id).await;
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(handle=self.core.handle, pipe_read_id=pipe_read_id, "PUB detaching pipe");

    let maybe_write_id = self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);
    if let Some(write_id) = maybe_write_id {
      self.distributor.remove_pipe(write_id).await;
      tracing::trace!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        pipe_write_id = write_id,
        "PUB removed detached pipe from distributor"
      );
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        "PUB detach: Write ID not found for read ID"
      );
    }
  }
}
