// src/socket/push_socket.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, CoreState, SocketCore};
use crate::socket::patterns::LoadBalancer;
use crate::socket::ISocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{oneshot, Mutex, MutexGuard};
use tokio::time::timeout as tokio_timeout;

#[derive(Debug)]
pub(crate) struct PushSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer, // Distributes outgoing messages
  // Map Read Pipe ID -> Write Pipe ID (needed for pipe_detached cleanup)
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
}

impl PushSocket {
  /// Creates a new PushSocket.
  pub fn new(core: Arc<SocketCore>) -> Self {
    Self {
      core,
      load_balancer: LoadBalancer::new(), // Create a new load balancer
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
    }
  }

  /// Helper to access core state safely.
  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

#[async_trait]
impl ISocket for PushSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }

  fn mailbox(&self) -> &MailboxSender {
    self.core.mailbox_sender()
  }

  // --- Methods mirroring public API ---

  async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
    // Core handles actual binding and transport actor spawning
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserBind {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
    // Core handles actual connecting and transport actor spawning
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserConnect {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserDisconnect {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserUnbind {
        endpoint: endpoint.to_string(),
        reply_tx,
      })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    // 1. Get SNDTIMEO setting *before* potentially blocking on peer selection
    let timeout_opt: Option<Duration> = { self.core_state().await.options.sndtimeo };

    // 2. Try to select a peer using the load balancer
    // --- Loop to get a peer, potentially waiting ---
    let pipe_write_id = loop {
      if let Some(id) = self.load_balancer.get_next_pipe().await {
        break id; // Found a peer immediately
      }

      // --- ADD CHECK: If no peer, is the core task likely dead? ---
      // Try a non-blocking send to the core mailbox. If it fails (closed),
      // the core actor is gone, and we shouldn't wait for peers.
      let is_core_mailbox_closed = {
        // Create a dummy oneshot channel (the receiver is immediately dropped)
        let (test_tx, _) = oneshot::channel::<Result<Vec<u8>, ZmqError>>();
        // Use a lightweight command like UserGetOpt with a dummy option ID
        let test_cmd = Command::UserGetOpt {
          option: -999,
          reply_tx: test_tx,
        }; // Dummy option
           // try_send is non-blocking and returns Err if channel is closed or full.
           // After termination, closed is the expected reason for Err.
        self.core.mailbox_sender().try_send(test_cmd).is_err()
      };

      if is_core_mailbox_closed {
        tracing::warn!(
          handle = self.core.handle,
          "PUSH send failed: Core mailbox closed (socket likely terminated)."
        );
        // Return an error indicating the socket is closed/terminated.
        // Using InvalidState seems appropriate here, as the socket handle is being used after termination.
        return Err(ZmqError::InvalidState("Socket terminated".into()));
      }
      // --- END CHECK ---

      // No peer found, but core mailbox seems open. Behavior depends on timeout.
      match timeout_opt {
        Some(duration) if duration.is_zero() => {
          tracing::trace!(
            handle = self.core.handle,
            "PUSH send failed (non-blocking): No connected peers"
          );
          return Err(ZmqError::ResourceLimitReached); // EAGAIN
        }
        None => {
          tracing::trace!(
            handle = self.core.handle,
            "PUSH send blocking: Waiting for available peer..."
          );
          self.load_balancer.wait_for_pipe().await;
          continue; // Loop back after waiting
        }
        Some(duration) => {
          tracing::trace!(
            handle = self.core.handle,
            ?duration,
            "PUSH send timed wait: Waiting for available peer..."
          );
          match tokio_timeout(duration, self.load_balancer.wait_for_pipe()).await {
            Ok(()) => {
              continue; // Wait succeeded, loop back
            }
            Err(_elapsed) => {
              tracing::debug!(
                handle = self.core.handle,
                ?duration,
                "PUSH send timed out waiting for peer"
              );
              return Err(ZmqError::Timeout);
            }
          }
        }
      } // end match timeout_opt
    }; // end loop getting pipe_write_id

    // 3. Peer selected (pipe_write_id is valid), get the sender channel
    let pipe_tx = {
      let core_state_guard = self.core_state().await;
      // If the pipe disappeared between selection and getting sender (unlikely but possible)
      core_state_guard.get_pipe_sender(pipe_write_id).ok_or_else(|| {
        tracing::error!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "PUSH send failed: Pipe sender disappeared after selection."
        );
        // Should we remove this apparently stale pipe_write_id from LoadBalancer here?
        // Core cleanup logic should eventually handle this via pipe_detached.
        ZmqError::Internal("Pipe sender consistency error".into())
      })?
    };

    // 4. Send using the helper function (which respects timeout_opt for HWM)
    match send_msg_with_timeout(&pipe_tx, msg, timeout_opt, self.core.handle, pipe_write_id).await {
      Ok(()) => Ok(()),
      Err(ZmqError::ConnectionClosed) => {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "PUSH send failed: Pipe channel closed"
        );
        self.load_balancer.remove_pipe(pipe_write_id).await; // Cleanup LB
                                                             // PUSH doesn't typically *return* this error unless maybe SNDTIMEO=0?
                                                             // ZMQ often silently drops or waits again depending on config.
                                                             // Let's return the error for now, matches helper behavior.
        Err(ZmqError::ConnectionClosed)
      }
      Err(ZmqError::ResourceLimitReached) => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "PUSH send failed due to HWM (EAGAIN/Timeout)"
        );
        // This is the expected error for SNDTIMEO=0 or SNDTIMEO>0 timeout on HWM.
        Err(ZmqError::ResourceLimitReached)
      }
      Err(ZmqError::Timeout) => {
        tracing::debug!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "PUSH send timed out on HWM"
        );
        // This is the expected error for SNDTIMEO>0 timeout on HWM.
        Err(ZmqError::Timeout)
      }
      Err(e) => Err(e), // Propagate other unexpected errors
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    // PUSH sockets cannot receive messages
    Err(ZmqError::InvalidState("PUSH sockets cannot receive messages"))
  }

  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    // Core handles general options. PUSH has few specific ones (maybe SNDHWM?)
    // Delegate to core first, then handle pattern specific if any.
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
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    // Core handles general options.
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserGetOpt { option, reply_tx })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn close(&self) -> Result<(), ZmqError> {
    // Delegate shutdown to core
    let (reply_tx, reply_rx) = oneshot::channel();
    self
      .core
      .mailbox_sender()
      .send(Command::UserClose { reply_tx })
      .await
      .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
    reply_rx
      .await
      .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PUSH",
      option = option,
      "set_pattern_option called"
    );
    // PUSH socket has no specific options like SUBSCRIBE or ROUTING_ID
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "PUSH",
      option = option,
      "get_pattern_option called"
    );
    // PUSH socket has no specific readable pattern options
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Methods ---

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    // PUSH doesn't have many specific commands beyond basic user actions
    Ok(false) // Indicate core should handle or ignore
  }

  async fn handle_pipe_event(&self, _pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    // PUSH doesn't expect messages back, so PipeMessageReceived shouldn't happen.
    tracing::warn!(
      handle = self.core.handle,
      pipe_id = _pipe_id,
      "PUSH socket received unexpected pipe event: {:?}",
      event
    );
    Ok(())
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,  // PUSH doesn't use the read pipe
    pipe_write_id: usize, // Core writes TO peer using this ID
    _peer_identity: Option<&[u8]>,
  ) {
    // Add the writing pipe ID to the load balancer
    tracing::debug!(
      handle = self.core.handle,
      pipe_write_id = pipe_write_id,
      "PUSH attaching pipe"
    );

    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);

    self.load_balancer.add_pipe(pipe_write_id).await;
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "PUSH detaching pipe"
    );

    let maybe_write_id = self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);
    if let Some(write_id) = maybe_write_id {
      self.load_balancer.remove_pipe(write_id).await;
      tracing::trace!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        pipe_write_id = write_id,
        "PUSH removed detached pipe from load balancer"
      );
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id = pipe_read_id,
        "PUSH detach: Write ID not found for read ID"
      );
    }
  }
}
