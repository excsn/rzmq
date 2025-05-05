// src/socket/req_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::{FairQueue, LoadBalancer}; // Use LB for outgoing, FQ for incoming reply
use crate::socket::ISocket;
use async_trait::async_trait;
use std::collections::HashMap; // If tracking requests
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::timeout;

use super::core::send_msg_with_timeout;
use super::options::SocketOptions; // Use Mutex for state

/// Represents the state of the REQ socket's send/recv cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReqState {
  ReadyToSend,                                    // Socket is ready to send the next request.
  ExpectingReply { target_pipe_write_id: usize }, // Store target pipe
}

#[derive(Debug)]
pub(crate) struct ReqSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer, // Chooses peer for outgoing requests
  // Although REQ expects only one reply per request, using FairQueue
  // simplifies receiving that reply via the standard handle_pipe_event mechanism.
  // The state machine ensures only one recv() call waits for it.
  incoming_reply_queue: FairQueue,
  // Track the state of the request-reply cycle
  state: Mutex<ReqState>,
  // Map Read Pipe ID -> Write Pipe ID (needed for pipe_detached)
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
}

impl ReqSocket {
  /// Creates a new ReqSocket.
  pub fn new(core: Arc<SocketCore>, _options: SocketOptions) -> Self {
    let reply_queue_capacity = 1;
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      incoming_reply_queue: FairQueue::new(reply_queue_capacity),
      state: Mutex::new(ReqState::ReadyToSend), // Start ready to send
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
    }
  }

  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

// --- ISocket Implementation ---
#[async_trait]
impl ISocket for ReqSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }

  fn mailbox(&self) -> &MailboxSender {
    self.core.mailbox_sender()
  }

  // --- API Method Implementations ---
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
    if msg.is_more() {
      tracing::warn!(
        handle = self.core.handle,
        "REQ send: Attempting to send multi-part message; only the first part will be sent."
      );
      // Optionally return Err(ZmqError::InvalidMessage(...)) ?
      // ZMQ REQ silently sends only the first part if MORE is set. Let's mimic that.
    }

    // Lock state only for the initial check
    {
      // Scope for the state lock guard
      let current_state_guard = self.state.lock().await;
      if !matches!(*current_state_guard, ReqState::ReadyToSend) {
        // Use matches! for clarity
        return Err(ZmqError::InvalidState(
          "REQ socket must call recv() before sending again",
        ));
      }
      // Lock released when guard goes out of scope here
    }

    // 1. Get SNDTIMEO setting
    let timeout_opt: Option<Duration> = { self.core_state().await.options.sndtimeo };

    // 2. Try to select peer using LoadBalancer (no state lock needed)
    let pipe_write_id = loop {
      if let Some(id) = self.load_balancer.get_next_pipe().await {
        break id; // Found peer
      }
      // No peer found, check timeout
      match timeout_opt {
        Some(duration) if duration.is_zero() => {
          // SNDTIMEO = 0 (Non-blocking) -> Return error immediately
          tracing::trace!(
            handle = self.core.handle,
            "REQ send failed (non-blocking): No connected peers"
          );
          return Err(ZmqError::ResourceLimitReached); // EAGAIN
        }
        None => {
          // SNDTIMEO = -1 (Block Indefinitely) -> Wait for notification
          tracing::trace!(
            handle = self.core.handle,
            "REQ send blocking: Waiting for available peer..."
          );
          self.load_balancer.wait_for_pipe().await; // Wait for Notify signal
                                                    // After waiting, loop back to try get_next_pipe() again
          continue;
        }
        Some(duration) => {
          // SNDTIMEO > 0 (Timed Block) -> Wait with timeout
          tracing::trace!(
            handle = self.core.handle,
            ?duration,
            "REQ send timed wait: Waiting for available peer..."
          );
          match timeout(duration, self.load_balancer.wait_for_pipe()).await {
            Ok(()) => {
              // Wait succeeded within timeout, loop back to try get_next_pipe()
              continue;
            }
            Err(_elapsed) => {
              // Timeout elapsed while waiting for a peer
              tracing::debug!(
                handle = self.core.handle,
                ?duration,
                "REQ send timed out waiting for peer"
              );
              return Err(ZmqError::Timeout); // Return Timeout error
            }
          }
        }
      } // end match timeout_opt
    };

    // 3. Peer selected, get the sender channel (no state lock needed)
    let pipe_tx = {
      let core_state_guard = self.core_state().await;
      // If the pipe disappeared between selection and getting sender (unlikely but possible)
      core_state_guard.get_pipe_sender(pipe_write_id).ok_or_else(|| {
        tracing::error!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "REQ send failed: Pipe sender disappeared after selection."
        );
        // Should we remove this apparently stale pipe_write_id from LoadBalancer here?
        // LoadBalancer::remove_pipe requires async, can't call here easily.
        // Core cleanup logic should eventually handle this via pipe_detached.
        ZmqError::Internal("Pipe sender consistency error".into())
      })?
    }; // Release core_state lock

    // 4. Prepare message (clear MORE flag) (no state lock needed)
    let flags = msg.flags() & !MsgFlags::MORE; // Remove MORE
    msg.set_flags(flags);
    // Note: REQ -> REP doesn't typically use empty delimiters like DEALER -> ROUTER

    // 5. Send using helper (no state lock needed)
    match send_msg_with_timeout(&pipe_tx, msg, timeout_opt, self.core.handle, pipe_write_id).await {
      Ok(()) => {
        // Send successful, NOW acquire lock to transition state
        let mut current_state_guard = self.state.lock().await;
        match *current_state_guard {
          ReqState::ReadyToSend => {
            // <<< MODIFIED: Store target pipe ID >>>
            *current_state_guard = ReqState::ExpectingReply {
              target_pipe_write_id: pipe_write_id,
            };
          }
          // <<< MODIFIED: Return error on unexpected state >>>
          _ => {
            tracing::error!(handle = self.core.handle, state=?*current_state_guard, "REQ state was not ReadyToSend after successful send!");
            // Don't force state, return error
            return Err(ZmqError::Internal("State mismatch after successful send".into()));
          }
        }
        drop(current_state_guard); // Release lock

        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "REQ sent request, expecting reply"
        );
        Ok(()) // Return success
      }
      Err(e @ ZmqError::ConnectionClosed) => {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "REQ send failed: Pipe channel closed"
        );
        self.load_balancer.remove_pipe(pipe_write_id).await; // Cleanup LB
                                                             // State remains ReadyToSend (didn't transition)
        Err(e)
      }
      Err(e @ ZmqError::ResourceLimitReached) => {
        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "REQ send failed due to HWM (EAGAIN/Timeout)"
        );
        // State remains ReadyToSend
        Err(e)
      }
      Err(e @ ZmqError::Timeout) => {
        tracing::debug!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "REQ send timed out on HWM"
        );
        // State remains ReadyToSend
        Err(e)
      }
      Err(e) => {
        // Other internal error
        // State remains ReadyToSend
        Err(e)
      }
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    {
      let expected_target_pipe: Option<usize>; // Store expected target if needed
                                               // Scope for initial state check lock
      let current_state_guard = self.state.lock().await;
      match *current_state_guard {
        ReqState::ExpectingReply { target_pipe_write_id } => {
          expected_target_pipe = Some(target_pipe_write_id); // Store for potential future checks
        }
        _ => return Err(ZmqError::InvalidState("REQ socket must call send() before receiving")),
      }
      // Release lock here
    }

    // Drop guard early before await if possible? No, need it later.
    // 1. Get configured timeout
    let rcvtimeo_opt: Option<Duration> = { self.core_state().await.options.rcvtimeo };

    // 2. Pop message from the reply queue with potential timeout
    let pop_future = self.incoming_reply_queue.pop_message();
    let pop_result = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => {
        tracing::trace!(handle = self.core.handle, ?duration, "Applying RCVTIMEO to recv");
        match timeout(duration, pop_future).await {
          Ok(inner_result) => inner_result, // Result<Option<Msg>, ZmqError>
          Err(_timeout_elapsed) => Err(ZmqError::Timeout),
        }
      }
      _ => pop_future.await, // Await indefinitely
    };

    // 3. Process result and update state
    let final_result = match pop_result {
      Ok(Some(mut msg)) => {
        // Check for MORE flag on reply (invalid)
        if msg.is_more() {
          tracing::warn!(
            handle = self.core.handle,
            "REQ recv: Received reply with MORE flag set (invalid for REP reply). Flag ignored."
          );
          // Clear the flag on the received message? Or return error?
          // Let's clear it to be lenient.
          msg.set_flags(msg.flags() & !MsgFlags::MORE);
        }

        let mut current_state_guard = self.state.lock().await;
        // Verify state is still ExpectingReply before updating
        // (Could potentially have been reset by a concurrent pipe_detached)
        if matches!(*current_state_guard, ReqState::ExpectingReply { .. }) {
          *current_state_guard = ReqState::ReadyToSend;
        } else {
          // State was reset (e.g., by pipe_detached) while we were popping the message.
          // This reply is now potentially stale or unexpected.
          tracing::warn!(handle = self.core.handle, state=?*current_state_guard, "REQ state changed while receiving reply. Discarding reply and returning InvalidState.");
          // Return an error indicating the state changed mid-receive
          return Err(ZmqError::InvalidState("Socket state changed during receive operation"));
        }
        drop(current_state_guard); // Release lock

        tracing::trace!(handle = self.core.handle, "REQ received reply, ready to send");
        Ok(msg)
      }
      Ok(None) => {
        // Queue closed while waiting (internal error)
        tracing::error!(
          handle = self.core.handle,
          "REQ recv failed: Reply queue closed unexpectedly"
        );
        // State remains ExpectingReply
        Err(ZmqError::Internal("Receive queue closed".into()))
      }
      Err(ZmqError::Timeout) => {
        tracing::trace!(handle = self.core.handle, "REQ recv timed out");
        // State remains ExpectingReply
        Err(ZmqError::Timeout)
      }
      Err(e) => {
        // Other internal error from pop_message
        // State remains ExpectingReply
        Err(e)
      }
    };

    final_result
  }

  // --- Pattern Specific Options ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "REQ",
      option = option,
      "set_pattern_option called"
    );
    Err(ZmqError::UnsupportedOption(option)) // REQ has no specific pattern options
  }

  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "REQ",
      option = option,
      "get_pattern_option called"
    );
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        // Any message received must be the reply we are waiting for
        let current_state = self.state.lock().await;
        if matches!(*current_state, ReqState::ExpectingReply { .. }) {
          // Push reply message into the queue for recv() to pick up
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            msg_size = msg.size(),
            "REQ pushing reply to FairQueue"
          );
          // Use try_send because queue capacity is 1. If it fails, something is wrong.
          if let Err(e) = self.incoming_reply_queue.push_message(msg).await {
            tracing::error!(handle=self.core.handle, pipe_id=pipe_id, error=?e, "Failed to push reply to REQ queue (already full or closed?)");
            // This indicates a state mismatch or error. Maybe drop message?
          }
        } else {
          tracing::warn!(handle=self.core.handle, pipe_id=pipe_id, msg_size=msg.size(), state=?*current_state, "REQ received unexpected message from pipe (dropping)");
          // Drop unexpected messages
        }
      }
      _ => { /* REQ ignores other pipe events like ActivateReadCmd */ }
    }
    Ok(())
  }

  async fn pipe_attached(&self, pipe_read_id: usize, pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "REQ attaching pipe"
    );
    // Store mapping for detachment
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    // Add write pipe to load balancer
    self.load_balancer.add_pipe(pipe_write_id).await;
    // Notify incoming queue (though only one message expected at a time)
    self.incoming_reply_queue.pipe_attached(pipe_read_id);
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "REQ detaching pipe"
    );
    // Remove from map and get write ID
    let maybe_write_id = self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);

    if let Some(write_id) = maybe_write_id {
      self.load_balancer.remove_pipe(write_id).await;

      // Check state against detached pipe's write ID
      let mut current_state_guard = self.state.lock().await;
      if let ReqState::ExpectingReply { target_pipe_write_id } = *current_state_guard {
        // Only reset state if the detached pipe was the one we sent the request to
        if target_pipe_write_id == write_id {
          tracing::warn!(
            handle = self.core.handle,
            pipe_read_id = pipe_read_id,
            pipe_write_id = write_id,
            "Target REP peer detached while REQ was expecting reply. Resetting state."
          );
          *current_state_guard = ReqState::ReadyToSend;
        } else {
          // A different pipe detached, keep waiting for reply from target
          tracing::debug!(
            handle = self.core.handle,
            detached_pipe_read_id = pipe_read_id,
            target_pipe_write_id = target_pipe_write_id,
            "Non-target pipe detached while REQ expecting reply. State unchanged."
          );
        }
      }
    }
    // Notify incoming queue
    self.incoming_reply_queue.pipe_detached(pipe_read_id);
  }
} // end impl ISocket for ReqSocket
