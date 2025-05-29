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
use futures::future::Either;
use parking_lot::RwLockReadGuard;
use std::collections::HashMap; // For pipe_read_to_write_id map.
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, MutexGuard, Notify}; // Mutex for state, oneshot for API replies.
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

#[derive(Debug)]
struct ReqRecvState {
  state: ReqState,
  // Notifier for when the specific outstanding request is resolved (either by reply or error)
  reply_or_error_notifier: Arc<Notify>,
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
  state: Mutex<ReqRecvState>,
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
  pub fn new(core: Arc<SocketCore>) -> Self {
    // The reply queue for REQ only ever needs to hold one logical reply (which might be multi-part).
    // A capacity of 1 for the FairQueue is sufficient.
    let reply_queue_capacity = 1;
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      incoming_reply_queue: FairQueue::new(reply_queue_capacity),
      state: Mutex::new(ReqRecvState {
        state: ReqState::ReadyToSend,
        reply_or_error_notifier: Arc::new(Notify::new()),
      }),
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
    }
  }

  /// Helper to get a locked guard for the `CoreState` within `SocketCore`.
  fn core_state(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
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
      if !matches!(current_state_guard.state, ReqState::ReadyToSend) {
        return Err(ZmqError::InvalidState(
          "REQ socket must call recv() before sending again",
        ));
      }
      // Lock released when current_state_guard goes out of scope here.
    }

    // Get SNDTIMEO from options before potentially blocking on peer selection.
    let timeout_opt: Option<Duration> = { self.core_state().options.sndtimeo };

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
      let core_state_guard = self.core_state();
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
        match current_state_guard.state {
          ReqState::ReadyToSend => {
            current_state_guard.state = ReqState::ExpectingReply {
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
            current_state_guard.state = ReqState::ExpectingReply {
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

    // Get RCVTIMEO from options.
    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };

    let (current_target_pipe_id, notifier_clone) = {
      let op_state_guard = self.state.lock().await;
      match op_state_guard.state {
        ReqState::ExpectingReply { target_pipe_write_id } => (
          Some(target_pipe_write_id),
          op_state_guard.reply_or_error_notifier.clone(),
        ),
        ReqState::ReadyToSend => {
          return Err(ZmqError::InvalidState("REQ socket must call send() before receiving"));
        }
      }
    };

    loop {
      // Loop to handle spurious wakeups or state changes
      let pop_future = self.incoming_reply_queue.pop_message(); // Try to get immediately if available
      let notified_future = notifier_clone.notified();

      let selected_future = match rcvtimeo_opt {
        Some(duration) if !duration.is_zero() => {
          tokio::select! {
              biased;
              pop_res = pop_future => Either::Left(pop_res),
              _ = tokio::time::sleep(Duration::from_micros(10)) => { // Brief yield to allow pop_future to resolve if data is immediately there
                  tokio::select! {
                     _ = notified_future => Either::Right(Ok(())), // Notified means state might have changed or error
                     _ = tokio::time::timeout(duration, futures::future::pending::<()>()) => Either::Right(Err(ZmqError::Timeout)),
                  }
              }
          }
        }
        _ => {
          // RCVTIMEO is None (infinite) or zero (check queue once then wait on notifier)
          tokio::select! {
              biased;
              pop_res = pop_future => Either::Left(pop_res),
               _ = tokio::time::sleep(Duration::from_micros(10)) => { // Brief yield
                  tokio::select!{
                      _ = notified_future => Either::Right(Ok(())),
                      // For infinite timeout, if pop_future was None initially, we just wait on notifier
                  }
               }
          }
        }
      };

      match selected_future {
        Either::Left(Ok(Some(mut msg))) => {
          // Got a message from queue
          if msg.is_more() {
            msg.set_flags(msg.flags() & !MsgFlags::MORE);
          }
          let mut op_state_guard = self.state.lock().await;
          op_state_guard.state = ReqState::ReadyToSend;
          // Notify any other tasks that might have been waiting on this old notifier,
          // though for REQ, only one recv should be active.
          op_state_guard.reply_or_error_notifier.notify_waiters();
          return Ok(msg);
        }
        Either::Left(Ok(None)) => {
          // Queue empty on try_pop, continue to wait on notifier or timeout
          if rcvtimeo_opt == Some(Duration::ZERO) {
            return Err(ZmqError::ResourceLimitReached);
          }
          // Fall through to await notifier in the select if not timeout=0
        }
        Either::Left(Err(e)) => {
          // Error from queue (e.g. closed)
          let mut op_state_guard = self.state.lock().await;
          op_state_guard.state = ReqState::ReadyToSend; // Reset state
          op_state_guard.reply_or_error_notifier.notify_waiters();
          return Err(e);
        }
        Either::Right(Ok(())) => {
          // Notified by reply_or_error_notifier
          // First, try to pop any message that might have arrived.
          // This handles the case where handle_pipe_event (reply received) and
          // pipe_detached (peer error) both signal the notifier nearly simultaneously.
          // The reply should take precedence if available.
          if let Ok(Some(mut msg)) = self.incoming_reply_queue.try_pop_message() {
            if msg.is_more() {
              msg.set_flags(msg.flags() & !MsgFlags::MORE);
            }
            let mut op_state_guard = self.state.lock().await;
            op_state_guard.state = ReqState::ReadyToSend;
            op_state_guard.reply_or_error_notifier.notify_waiters();
            return Ok(msg);
          }

          // If no message was popped, then the notification was likely due to detachment/error.
          // Now check the state.
          let op_state_guard = self.state.lock().await;
          if matches!(op_state_guard.state, ReqState::ReadyToSend) {
            tracing::warn!(
              handle = self.core.handle,
              "REQ recv: Notified, queue empty, and state is ReadyToSend (peer likely detached)."
            );
            return Err(ZmqError::Internal("Request cancelled due to peer detachment".into()));
          }
          // Else, still ExpectingReply and queue was empty on this check, loop to wait again (or for RCVTIMEO).
          tracing::trace!(
            handle = self.core.handle,
            "REQ recv: Notified, but queue empty and still ExpectingReply. Looping for RCVTIMEO or next event."
          );
        }
        Either::Right(Err(timeout_err @ ZmqError::Timeout)) => {
          // This was a timeout on waiting for the notifier
          return Err(timeout_err);
        }
        Either::Right(Err(other_err)) => {
          // Should not happen from pending()
          return Err(other_err);
        }
      }
    }
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    unimplemented!("Not Implemented yet")
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    unimplemented!("Not implemented yet")
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
        let op_state_guard = self.state.lock().await;
        match op_state_guard.state {
          ReqState::ExpectingReply { target_pipe_write_id } => {
            // Check if this message is from the correct pipe (optional strictness)
            let expected_read_pipe = self.pipe_read_to_write_id.lock().await.iter().find_map(|(r_id, w_id)| {
              if *w_id == target_pipe_write_id {
                Some(*r_id)
              } else {
                None
              }
            });

            if expected_read_pipe.is_none() || expected_read_pipe != Some(pipe_id) {
              tracing::warn!(
                  handle = self.core.handle,
                  received_from_pipe = pipe_id,
                  expected_from_pipe_via_target_write_id = ?expected_read_pipe,
                  target_write_id_in_state = target_pipe_write_id,
                  "REQ received reply from unexpected pipe. Dropping."
              );
              // Drop message, do not notify, do not change state.
              // The outstanding recv() will eventually timeout or be resolved by target peer detaching.
              return Ok(());
            }
            // Message is from the correct peer. Drop guard before await.
            drop(op_state_guard);

            tracing::trace!(
              handle = self.core.handle,
              pipe_id = pipe_id,
              msg_size = msg.size(),
              "REQ pushing reply to FairQueue"
            );
            if self.incoming_reply_queue.push_message(msg).await.is_err() {
              tracing::error!(
                handle = self.core.handle,
                pipe_id = pipe_id,
                "REQ: Failed to push reply to queue (full/closed)."
              );
              // This is an issue, the queue (capacity 1) should not be full if state is ExpectingReply.
              // Reset state to allow new send if queue is broken.
              let mut op_state_guard_err = self.state.lock().await;
              op_state_guard_err.state = ReqState::ReadyToSend;
              op_state_guard_err.reply_or_error_notifier.notify_waiters(); // Wake up any pending recv
            } else {
              // Successfully queued, notify the pending recv.
              self.state.lock().await.reply_or_error_notifier.notify_one();
            }
          }
          ReqState::ReadyToSend => {
            drop(op_state_guard);
            tracing::warn!(
              handle = self.core.handle,
              pipe_id = pipe_id,
              msg_size = msg.size(),
              "REQ received unexpected message from pipe (state ReadyToSend), dropping."
            );
          }
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
      let mut op_state_guard = self.state.lock().await;
      if let ReqState::ExpectingReply { target_pipe_write_id } = op_state_guard.state {
        if target_pipe_write_id == write_id {
          tracing::warn!(
            handle = self.core.handle,
            pipe_read_id,
            pipe_write_id = write_id,
            "Target REP peer detached while REQ was expecting reply. Resetting state and notifying recv."
          );
          op_state_guard.state = ReqState::ReadyToSend;
          op_state_guard.reply_or_error_notifier.notify_one(); // Notify the pending recv call
        }
      }
      // Lock released.
    }
    // Notify the incoming reply queue that the pipe is detached.
    self.incoming_reply_queue.pipe_detached(pipe_read_id);
  }
}
