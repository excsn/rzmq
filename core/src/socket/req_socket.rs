// src/socket/req_socket.rs

use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags}; // For send/recv and managing MORE flag.
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, CoreState, SocketCore}; // Core components and send helper.
use crate::socket::options::SocketOptions; // For initial socket options (e.g., RCVHWM for queue).
use crate::socket::patterns::{FairQueue, LoadBalancer}; // REQ uses LoadBalancer (out) and FairQueue (in for reply).
use crate::socket::ISocket;
use crate::{delegate_to_core, Blob}; // Macro for delegating API calls to SocketCore. // The trait this struct implements.

use async_trait::async_trait;
use std::collections::HashMap; // For pipe_read_to_write_id map.
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, MutexGuard}; // Mutex for state, oneshot for API replies.
use tokio::time::timeout; // For send/recv timeouts.

/// Represents the state of the REQ socket's send/receive cycle.
/// REQ sockets must follow a strict send-then-receive pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReqState {
  /// Socket is ready to send the next request.
  ReadyToSend,
  /// Socket has sent a request to a specific peer (identified by `target_pipe_write_id`)
  /// and is now expecting a reply from that same peer.
  ExpectingReply { target_pipe_write_id: usize },
}

/// Implements the REQ (Request) socket pattern.
/// REQ sockets send requests and receive replies in a strict alternating sequence.
/// A REQ socket must `send()` a message, then `recv()` a reply, before it can `send()` again.
/// Outgoing requests are load-balanced across available connected REP (or ROUTER) peers.
#[derive(Debug)]
pub(crate) struct ReqSocket {
  /// Arc to the shared `SocketCore` actor that manages common socket state and transport.
  core: Arc<SocketCore>,
  /// `LoadBalancer` to choose which connected peer to send the next request to.
  load_balancer: LoadBalancer,
  /// `FairQueue` to buffer the incoming reply. Although REQ expects only one reply
  /// per request, using `FairQueue` (with capacity 1) simplifies receiving that reply
  /// via the standard `handle_pipe_event` mechanism.
  incoming_reply_queue: FairQueue,
  /// Tracks the current state of the request-reply cycle (ReadyToSend or ExpectingReply).
  /// Protected by a `Mutex` to ensure atomic state transitions.
  state: Mutex<ReqState>,
  /// Maps a pipe's read ID (from SocketCore's perspective) to its corresponding write ID.
  /// This is needed during `pipe_detached` to correctly update the state if the target
  /// peer (to whom a request was sent) disconnects.
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
}

impl ReqSocket {
  /// Creates a new `ReqSocket`.
  ///
  /// # Arguments
  /// * `core` - An `Arc` to the `SocketCore` managing this socket.
  /// * `_options` - Initial socket options. `RCVHWM` is implicitly used for the reply queue capacity (set to 1).
  pub fn new(core: Arc<SocketCore>, _options: SocketOptions) -> Self {
    // The reply queue for REQ only ever needs to hold one logical reply (which might be multi-part).
    // A capacity of 1 for the FairQueue is sufficient.
    let reply_queue_capacity = 1;
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      incoming_reply_queue: FairQueue::new(reply_queue_capacity),
      state: Mutex::new(ReqState::ReadyToSend), // Start in a state ready to send.
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
    }
  }

  /// Helper to get a locked guard for the `CoreState` within `SocketCore`.
  async fn core_state(&self) -> MutexGuard<'_, CoreState> {
    self.core.core_state.lock().await
  }
}

#[async_trait]
impl ISocket for ReqSocket {
  /// Returns a reference to the `SocketCore`.
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }

  /// Returns a clone of the `SocketCore`'s command mailbox sender.
  fn mailbox(&self) -> MailboxSender {
    self.core.command_sender()
  }

  // --- API Method Implementations (mostly delegated to SocketCore) ---
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

  // --- Pattern-Specific Logic for send() and recv() ---

  /// Sends a request message using the REQ pattern.
  /// The socket must be in the `ReadyToSend` state. After a successful send,
  /// it transitions to `ExpectingReply`.
  /// Outgoing messages are load-balanced to one available peer.
  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    // ZMQ REQ sockets typically send only the first part of a multi-part message
    // if the MORE flag is set on it, and ignore subsequent parts for that send operation.
    // We'll mimic this by clearing the MORE flag on the message being sent.
    if msg.is_more() {
      tracing::warn!(
        handle = self.core.handle,
        "REQ send: Message has MORE flag set; REQ sends single-part requests. Clearing MORE flag."
      );
      // Clear the MORE flag to ensure only this part is considered the request.
      msg.set_flags(msg.flags() & !MsgFlags::MORE);
    }

    // Lock the state to check and transition.
    // This lock is held briefly for the state check.
    {
      let current_state_guard = self.state.lock().await;
      if !matches!(*current_state_guard, ReqState::ReadyToSend) {
        return Err(ZmqError::InvalidState(
          "REQ socket must call recv() before sending again",
        ));
      }
      // Lock released when current_state_guard goes out of scope here.
    }

    // Get SNDTIMEO from options before potentially blocking on peer selection.
    let timeout_opt: Option<Duration> = { self.core_state().await.options.sndtimeo };

    // Select a peer pipe using the load balancer, waiting if necessary based on SNDTIMEO.
    let pipe_write_id = loop {
      if let Some(id) = self.load_balancer.get_next_pipe().await {
        break id; // Found an available peer pipe.
      }
      // No peer currently available, behavior depends on timeout settings.
      match timeout_opt {
        Some(duration) if duration.is_zero() => {
          tracing::trace!(
            handle = self.core.handle,
            "REQ send failed (non-blocking): No connected peers"
          );
          return Err(ZmqError::ResourceLimitReached); // EAGAIN
        }
        None => {
          tracing::trace!(
            handle = self.core.handle,
            "REQ send blocking: Waiting for available peer..."
          );
          self.load_balancer.wait_for_pipe().await;
          continue; // Loop back to try getting a pipe.
        }
        Some(duration) => {
          tracing::trace!(
            handle = self.core.handle,
            ?duration,
            "REQ send timed wait: Waiting for available peer..."
          );
          match timeout(duration, self.load_balancer.wait_for_pipe()).await {
            Ok(()) => continue, // Wait succeeded.
            Err(_timeout_elapsed) => {
              tracing::debug!(
                handle = self.core.handle,
                ?duration,
                "REQ send timed out waiting for peer"
              );
              return Err(ZmqError::Timeout); // Timeout elapsed.
            }
          }
        }
      }
    };

    // Get the sender channel for the selected pipe from CoreState.
    let pipe_tx = {
      let core_state_guard = self.core_state().await;
      core_state_guard.get_pipe_sender(pipe_write_id).ok_or_else(|| {
        tracing::error!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "REQ send failed: Pipe sender disappeared after selection."
        );
        ZmqError::Internal("Pipe sender consistency error".into())
      })?
    }; // CoreState lock released.

    // REQ sockets do not typically prepend an empty delimiter like DEALER.
    // The message is sent as is (after ensuring no MORE flag).

    // Send the request message, respecting SNDTIMEO for HWM.
    match send_msg_with_timeout(&pipe_tx, msg, timeout_opt, self.core.handle, pipe_write_id).await {
      Ok(()) => {
        // Send was successful. Now, transition state to ExpectingReply.
        // This lock must be acquired *after* the send to avoid holding it during await.
        let mut current_state_guard = self.state.lock().await;
        // It's possible the state changed due to a concurrent pipe_detached event. Re-check.
        match *current_state_guard {
          ReqState::ReadyToSend => {
            *current_state_guard = ReqState::ExpectingReply {
              target_pipe_write_id: pipe_write_id,
            };
          }
          // If state is already ExpectingReply (e.g. due to a race or prior error leaving it so),
          // this send was technically out of order according to the state machine.
          // However, since the actual send succeeded, we might be in an inconsistent situation.
          // For robustness, we could update target_pipe_write_id, but it implies an issue.
          ReqState::ExpectingReply { .. } => {
            tracing::error!(handle = self.core.handle, state=?*current_state_guard, "REQ state was not ReadyToSend after successful send, but send proceeded. Protocol violation likely.");
            // This scenario should ideally be prevented by the initial state check.
            // If it happens, it suggests a race condition or logic error.
            // Overwrite the state with the new target.
            *current_state_guard = ReqState::ExpectingReply {
              target_pipe_write_id: pipe_write_id,
            };
            // Consider returning an error or logging more severely.
          }
        }
        drop(current_state_guard); // Release lock.

        tracing::trace!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "REQ sent request, now expecting reply from this pipe."
        );
        Ok(()) // Return success for the send operation.
      }
      Err(e @ ZmqError::ConnectionClosed) => {
        // Connection closed during send attempt.
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_write_id,
          "REQ send failed: Pipe channel closed"
        );
        self.load_balancer.remove_pipe(pipe_write_id).await; // Clean up load balancer.
                                                             // State remains ReadyToSend as the transition to ExpectingReply didn't happen.
        Err(e)
      }
      // Other errors (ResourceLimitReached, Timeout, Internal) are propagated.
      // State remains ReadyToSend in these cases.
      Err(e) => Err(e),
    }
  }

  /// Receives a reply message using the REQ pattern.
  /// The socket must be in the `ExpectingReply` state. After a successful receive,
  /// it transitions back to `ReadyToSend`.
  async fn recv(&self) -> Result<Msg, ZmqError> {
    // Check current state. Must be ExpectingReply.
    // The target_pipe_write_id stored in state isn't strictly used for filtering incoming replies here,
    // as REQ only expects one reply from the peer it sent to. The FairQueue (capacity 1) ensures this.
    // However, it's useful for `pipe_detached` to know if the *target* peer disconnected.
    {
      // Brief scope for state lock.
      let current_state_guard = self.state.lock().await;
      if !matches!(*current_state_guard, ReqState::ExpectingReply { .. }) {
        return Err(ZmqError::InvalidState("REQ socket must call send() before receiving"));
      }
      // Lock released.
    }

    // Get RCVTIMEO from options.
    let rcvtimeo_opt: Option<Duration> = { self.core_state().await.options.rcvtimeo };

    // Pop the reply message from the (capacity 1) incoming fair queue.
    let pop_future = self.incoming_reply_queue.pop_message();
    let pop_result = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => {
        tracing::trace!(handle = self.core.handle, ?duration, "Applying RCVTIMEO to REQ recv");
        match timeout(duration, pop_future).await {
          Ok(inner_result) => inner_result, // Propagate Result<Option<Msg>, ZmqError>
          Err(_timeout_elapsed) => Err(ZmqError::Timeout), // Timeout occurred.
        }
      }
      _ => pop_future.await, // Await indefinitely or until queue closure.
    };

    // Process the result of popping from the queue.
    match pop_result {
      Ok(Some(mut msg)) => {
        // Successfully received a reply.
        // ZMQ REP sockets send single-part replies. If MORE is set, it's a protocol violation by peer.
        if msg.is_more() {
          tracing::warn!(
            handle = self.core.handle,
            "REQ recv: Received reply with MORE flag set (invalid for REP reply). Clearing flag."
          );
          msg.set_flags(msg.flags() & !MsgFlags::MORE); // Be lenient, clear flag.
        }

        // Transition state back to ReadyToSend.
        // This lock needs to be acquired after the await on pop_future.
        let mut current_state_guard = self.state.lock().await;
        // Re-check state: it's possible a concurrent pipe_detached reset the state.
        if matches!(*current_state_guard, ReqState::ExpectingReply { .. }) {
          *current_state_guard = ReqState::ReadyToSend;
        } else {
          // State changed while we were waiting for the message (e.g., target peer detached).
          // The received message might be stale or from an unexpected source if logic allows.
          // Since reply queue has capacity 1, this should be the reply we were waiting for,
          // but the state reset means we shouldn't have been waiting. This is an edge case.
          tracing::warn!(handle = self.core.handle, state=?*current_state_guard, "REQ state changed to ReadyToSend while receiving reply. The reply is likely valid but state was reset by peer detach.");
          // Proceed with returning the message, but the state is already ReadyToSend.
        }
        drop(current_state_guard); // Release lock.

        tracing::trace!(
          handle = self.core.handle,
          "REQ received reply, now ready to send next request."
        );
        Ok(msg)
      }
      Ok(None) => {
        // Reply queue was closed while waiting. This is an internal error.
        tracing::error!(
          handle = self.core.handle,
          "REQ recv failed: Reply queue closed unexpectedly."
        );
        // State remains ExpectingReply, but further operations will likely fail.
        Err(ZmqError::Internal("Receive queue closed".into()))
      }
      Err(ZmqError::Timeout) => {
        tracing::trace!(handle = self.core.handle, "REQ recv timed out waiting for reply.");
        // State remains ExpectingReply.
        Err(ZmqError::Timeout)
      }
      Err(e) => {
        // Other internal error from pop_message.
        // State remains ExpectingReply.
        Err(e)
      }
    }
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    unimplemented!("Not Implemented yet")
  }

  // --- Pattern-Specific Option Handling ---
  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "REQ",
      option = option,
      "set_pattern_option called"
    );
    // REQ sockets do not have specific pattern options like SUBSCRIBE.
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(
      handle = self.core.handle,
      socket_type = "REQ",
      option = option,
      "get_pattern_option called"
    );
    // REQ sockets do not have readable pattern-specific options.
    Err(ZmqError::UnsupportedOption(option))
  }

  // --- Internal Hooks called by SocketCore ---
  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    // REQ sockets do not typically handle special commands beyond user API calls.
    Ok(false) // Indicate command was not handled here.
  }

  /// Handles messages received from a pipe by the `SocketCore`.
  /// For REQ, any message received on any pipe must be the reply it's waiting for.
  /// It's pushed into the (capacity 1) `incoming_reply_queue`.
  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        // Check current state. Only queue if expecting a reply.
        let current_state_val = *self.state.lock().await; // Clone state for check.
        if matches!(current_state_val, ReqState::ExpectingReply { .. }) {
          tracing::trace!(
            handle = self.core.handle,
            pipe_id = pipe_id,
            msg_size = msg.size(),
            "REQ pushing reply to FairQueue"
          );
          // Attempt to push. FairQueue (capacity 1) might return error if already full,
          // which would be a protocol violation by peer (multiple replies) or internal issue.
          if let Err(e) = self.incoming_reply_queue.push_message(msg).await {
            tracing::error!(handle=self.core.handle, pipe_id=pipe_id, error=?e, "Failed to push reply to REQ queue (already full or closed?). This may indicate a protocol violation by the peer or an internal state issue.");
            // If push fails, the REQ socket might get stuck. Consider resetting state or erroring.
            // For now, just log. The `recv()` call might then timeout or find the queue closed.
          }
        } else {
          // Received a message when not expecting a reply (e.g., in ReadyToSend state).
          // This is a protocol violation by the peer. Drop the message.
          tracing::warn!(handle=self.core.handle, pipe_id=pipe_id, msg_size=msg.size(), state=?current_state_val, "REQ received unexpected message from pipe (dropping)");
        }
      }
      _ => { /* REQ sockets typically ignore other direct pipe events from SocketCore. */ }
    }
    Ok(())
  }

  /// Called by `SocketCore` when a new pipe (connection to a REP/ROUTER peer) is attached.
  async fn pipe_attached(&self, pipe_read_id: usize, pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      pipe_write_id = pipe_write_id,
      "REQ attaching pipe"
    );
    // Store the mapping from read ID to write ID for state management on detachment.
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    // Add the pipe's write ID to the load balancer for sending requests.
    self.load_balancer.add_pipe(pipe_write_id).await;
    // Notify the incoming reply queue (FairQueue) that a pipe is attached.
    self.incoming_reply_queue.pipe_attached(pipe_read_id);
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "THEIR_SOCKET_TYPE", // e.g., "DEALER"
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but this socket type does not use peer identities. Ignoring."
    );
  }

  /// Called by `SocketCore` when a pipe is detached (peer disconnected or socket closing).
  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id = pipe_read_id,
      "REQ detaching pipe"
    );
    // Remove the read ID -> write ID mapping and get the write ID.
    let maybe_write_id = self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);

    if let Some(write_id) = maybe_write_id {
      // Remove the detached pipe's write ID from the load balancer.
      self.load_balancer.remove_pipe(write_id).await;

      // Critical: If the detached pipe was the one we were expecting a reply from,
      // reset the state machine to ReadyToSend to allow sending a new request.
      let mut current_state_guard = self.state.lock().await;
      if let ReqState::ExpectingReply { target_pipe_write_id } = *current_state_guard {
        if target_pipe_write_id == write_id {
          tracing::warn!(
            handle = self.core.handle,
            pipe_read_id = pipe_read_id,
            pipe_write_id = write_id,
            "Target REP peer (pipe_write_id={}) detached while REQ was expecting reply from it. Resetting REQ state to ReadyToSend.",
            target_pipe_write_id
          );
          *current_state_guard = ReqState::ReadyToSend;
          // Any pending recv() call on the user side will now likely timeout or fail
          // if it was waiting on a message from this specific detached pipe.
          // The FairQueue (capacity 1) might still hold a message if it arrived just before detach,
          // but a subsequent recv() will try to pop it. If state is ReadyToSend, that recv will fail the state check.
          // If the queue is now empty, recv() will block until a new request is sent and reply received, or timeout.
        } else {
          // A different pipe (not the one we sent the current request to) detached.
          // Keep state as ExpectingReply, waiting for the reply from the original target.
          tracing::debug!(
            handle = self.core.handle,
            detached_pipe_read_id = pipe_read_id,
            detached_pipe_write_id = write_id,
            target_pipe_write_id = target_pipe_write_id,
            "A non-target pipe detached while REQ expecting reply from target. REQ State unchanged."
          );
        }
      }
      // Lock released.
    }
    // Notify the incoming reply queue that the pipe is detached.
    self.incoming_reply_queue.pipe_detached(pipe_read_id);
  }
}
