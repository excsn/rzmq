// src/socket/dealer_socket.rs

use crate::delegate_to_core;
// <<< ADDED DealerSocket IMPLEMENTATION >>>
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::{FairQueue, LoadBalancer}; // Use LB (out) and FQ (in)
use crate::socket::ISocket;

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, MutexGuard};
use tokio::time::{timeout, Instant};

use super::core::send_msg_with_timeout;
use super::options::SocketOptions;

#[derive(Debug)]
pub(crate) struct DealerSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer, // For outgoing messages
  incoming_queue: FairQueue,   // For incoming messages
  // Map Read Pipe ID -> Write Pipe ID (needed for pipe_detached)
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
  partial_incoming: Mutex<HashMap<usize, Vec<Msg>>>,
  // TODO: Option to configure if empty delimiter should be added/expected?
  // connect_to_router: bool, // Example config flag
}

impl DealerSocket {
  pub fn new(core: Arc<SocketCore>, options: SocketOptions) -> Self {
    let queue_capacity = options.rcvhwm.max(1);
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      incoming_queue: FairQueue::new(queue_capacity),
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
      partial_incoming: Mutex::new(HashMap::new()), // Initialize new state
                                                    // connect_to_router: true, // Default? Or detect?
    }
  }

  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

#[async_trait]
impl ISocket for DealerSocket {
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
    delegate_to_core!(self, UserClose,)
  }

  // --- Pattern-Specific Logic ---
  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    // 1. Get SNDTIMEO setting *before* potentially blocking on peer selection
    let timeout_opt: Option<Duration> = { self.core_state().await.options.sndtimeo };

    // 2. Try to select peer using LoadBalancer
    let pipe_write_id = loop {
      if let Some(id) = self.load_balancer.get_next_pipe().await {
        break id; // Found peer
      }
      // No peer found, check timeout
      match timeout_opt {
        Some(duration) if duration.is_zero() => {
          tracing::trace!(
            handle = self.core.handle,
            "DEALER send failed (non-blocking): No connected peers"
          );
          return Err(ZmqError::ResourceLimitReached);
        }
        None => {
          tracing::trace!(
            handle = self.core.handle,
            "DEALER send blocking: Waiting for available peer..."
          );
          self.load_balancer.wait_for_pipe().await;
          continue; // Loop back after waiting
        }
        Some(duration) => {
          tracing::trace!(
            handle = self.core.handle,
            ?duration,
            "DEALER send timed wait: Waiting for available peer..."
          );
          match timeout(duration, self.load_balancer.wait_for_pipe()).await {
            Ok(()) => continue, // Wait succeeded, loop back
            Err(_elapsed) => {
              tracing::debug!(
                handle = self.core.handle,
                ?duration,
                "DEALER send timed out waiting for peer"
              );
              return Err(ZmqError::Timeout);
            }
          }
        }
      } // end match timeout_opt
    };

    // 3. Peer selected (pipe_write_id is valid), get the sender channel
    let pipe_tx = {
      let core_state_guard = self.core_state().await;
      // Handle potential consistency error
      core_state_guard.get_pipe_sender(pipe_write_id).ok_or_else(|| {
        tracing::error!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "DEALER send failed: Pipe sender disappeared after selection."
        );
        // TODO: Remove stale pipe_write_id from LoadBalancer?
        ZmqError::Internal("Pipe sender consistency error".into())
      })?
    }; // Release lock

    // 4. Prepend empty delimiter frame (conditionally? Assume always for now)
    // TODO: Make conditional based on peer type detection or option?
    let mut delimiter = Msg::new();
    delimiter.set_flags(MsgFlags::MORE);

    // 5. Send delimiter (respect timeout/hwm for the *first* part)
    // Assume for now DEALER always talks to ROUTER and needs delimiter.
    // Could be made optional later via config.
    tracing::trace!(
      handle = self.core.handle,
      pipe_id = pipe_write_id,
      "DEALER prepending empty delimiter"
    );
    let mut delimiter = Msg::new(); // Empty payload
    delimiter.set_flags(MsgFlags::MORE); // Must have MORE flag

    // Send delimiter first
    match send_msg_with_timeout(&pipe_tx, delimiter, timeout_opt, self.core.handle, pipe_write_id).await {
      Ok(()) => {} // Proceed
      Err(e) => {
        /* handle fatal errors (remove pipe), return transient errors */
        // Ensure pipe is removed on fatal error before returning
        if matches!(e, ZmqError::ConnectionClosed | ZmqError::Internal(_)) {
          self.load_balancer.remove_pipe(pipe_write_id).await;
        }
        return Err(e);
      }
    }

    // 6. Send original message part(s)
    // Ensure MORE flag is correct on the final payload part
    let is_last_user_part = !msg.is_more(); // Check flag BEFORE potentially modifying it
    if is_last_user_part {
      msg.set_flags(msg.flags() & !MsgFlags::MORE); // Ensure last part has MORE unset
    } else {
      msg.set_flags(msg.flags() | MsgFlags::MORE); // Ensure intermediate parts have MORE set
    }

    // Send payload using the same timeout logic
    match send_msg_with_timeout(&pipe_tx, msg, timeout_opt, self.core.handle, pipe_write_id).await {
      Ok(()) => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "DEALER sent message payload"
        );
        Ok(()) // Success
      }
      Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::Internal(_)) => {
        // Fatal error sending payload, remove pipe
        tracing::warn!(handle=self.core.handle, pipe_id=pipe_write_id, error=%e, "DEALER send (payload) failed");
        self.load_balancer.remove_pipe(pipe_write_id).await; // Cleanup LB
        Err(e) // Return fatal error
      }
      Err(e @ ZmqError::ResourceLimitReached) | Err(e @ ZmqError::Timeout) => {
        // Failed due to HWM/timeout sending payload
        tracing::trace!(handle=self.core.handle, pipe_id=pipe_write_id, error=%e, "DEALER send (payload) failed HWM/Timeout");
        // Note: Delimiter was already sent! Peer might get partial message. ZMQ doesn't guarantee atomicity here.
        Err(e) // Return EAGAIN or Timeout
      }
      Err(e) => Err(e), // Propagate other unexpected errors
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    let rcvtimeo_opt: Option<Duration> = { self.core_state().await.options.rcvtimeo };

    // Pop message from the fair queue, applying timeout
    let pop_future = self.incoming_queue.pop_message();

    let result = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => {
        tracing::trace!(handle = self.core.handle, ?duration, "Applying RCVTIMEO to DEALER recv");
        match timeout(duration, pop_future).await {
          Ok(Ok(Some(msg))) => Ok(msg), // Got a frame
          Ok(Ok(None)) => Err(ZmqError::Internal("Receive queue closed".into())),
          Ok(Err(e)) => Err(e),
          Err(_timeout_elapsed) => Err(ZmqError::Timeout),
        }
      }
      _ => {
        // No timeout
        match pop_future.await? {
          Some(msg) => Ok(msg),
          None => Err(ZmqError::Internal("Receive queue closed".into())),
        }
      }
    };

    // Post-process: Strip delimiter? Depends on FQ/handle_pipe_event storing parts/metadata
    // Assuming recv just returns the next frame for now. User handles delimiter/identity.

    result
  }

  // --- Pattern Specific Options ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { mut msg, .. } => {
        let mut partial_guard = self.partial_incoming.lock().await;
        let is_last_part = !msg.is_more();
        let msg_size = msg.size(); // Get size for logging

        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_read_id,
          size = msg_size,
          more = msg.is_more(),
          expecting_payload = partial_guard.contains_key(&pipe_read_id),
          "DEALER handle_pipe_event received frame"
        );

        if partial_guard.contains_key(&pipe_read_id) {
          // --- Payload Frame ---
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_read_id,
            size = msg_size,
            "DEALER queuing payload frame"
          );
          // Add metadata indicating source pipe?
          #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
          struct SourcePipeReadId(usize);
          let mut current_msg = msg;
          current_msg
            .metadata_mut()
            .insert_typed(SourcePipeReadId(pipe_read_id))
            .await;

          self.incoming_queue.push_message(current_msg).await?;

          if is_last_part {
            tracing::trace!(
              handle = self.core.handle,
              pipe_id = pipe_read_id,
              "DEALER removing pipe state (last payload part)"
            );
            partial_guard.remove(&pipe_read_id);
          } else {
            tracing::trace!(
              handle = self.core.handle,
              pipe_id = pipe_read_id,
              "DEALER keeping pipe state (more payload parts expected)"
            );
          }
        } else {
          // --- Identity Frame ---
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_read_id,
            size = msg_size,
            "DEALER received/discarded identity frame"
          );

          if is_last_part {
            tracing::warn!(
              handle = self.core.handle,
              pipe_id = pipe_read_id,
              "DEALER received only identity frame from peer (no payload)"
            );
            // No entry was created, nothing to remove.
          } else {
            tracing::trace!(
              handle = self.core.handle,
              pipe_id = pipe_read_id,
              "DEALER inserting pipe state (payload expected)"
            );
            // Insert an empty Vec just to mark that we are waiting.
            partial_guard.insert(pipe_read_id, Vec::new());
          }
        }
      }
      _ => { /* DEALER ignores other pipe events */ }
    }
    Ok(())
  }

  async fn pipe_attached(&self, pipe_read_id: usize, pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "DEALER attaching pipe"
    );
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    self.load_balancer.add_pipe(pipe_write_id).await;
    self.incoming_queue.pipe_attached(pipe_read_id);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "DEALER detaching pipe"
    );
    let maybe_write_id = self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);
    if let Some(write_id) = maybe_write_id {
      self.load_balancer.remove_pipe(write_id).await;
    }
    self.incoming_queue.pipe_detached(pipe_read_id);

    self.partial_incoming.lock().await.remove(&pipe_read_id);
  }
} // end impl ISocket for DealerSocket
