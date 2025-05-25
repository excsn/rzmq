// core/src/socket/dealer_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::{send_msg_with_timeout, CoreState, SocketCore};
use crate::socket::options::SocketOptions;
use crate::socket::patterns::{FairQueue, LoadBalancer};
use crate::socket::{ISocket, SourcePipeReadId};

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLockReadGuard;
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore}; // Added Semaphore
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;

// Maximum number of parts that can be buffered by send()
const MAX_DEALER_SEND_BUFFER_PARTS: usize = 64; // Example

#[derive(Debug)]
enum DealerSendTransaction {
  Idle,
  // Buffering parts for a multi-part message.
  // Notify is used to signal other tasks (like send_multipart)
  // when this transaction completes and transitions back to Idle.
  Buffering {
    parts: Vec<Msg>,
    completion_notifier: Arc<Notify>, // Tasks can wait on this
  },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IncomingPipeState {
  Idle,
  StrippedIdentityExpectDelimiterOrPayload,
  StrippedDelimiterExpectPayload,
  ExpectingMorePayload,
}

#[derive(Debug)]
struct DealerSocketOutgoingProcessor {
  core_handle: usize,
  pending_queue: Arc<Mutex<VecDeque<Vec<Msg>>>>,
  load_balancer: Arc<LoadBalancer>,
  core_accessor: Arc<SocketCore>,          // Renamed for clarity
  queue_activity_notifier: Arc<Notify>,    // Notified when queue gets a message or a peer gets a message
  peer_availability_notifier: Arc<Notify>, // Notified when a peer connects/disconnects
  stop_signal: Arc<Notify>,                // To stop this processor task
}

impl DealerSocketOutgoingProcessor {
  async fn run(self) {
    tracing::debug!(
      "[DealerProc {}] Outgoing queue processor task started.",
      self.core_handle
    );
    loop {
      let mut popped_message_parts: Option<Vec<Msg>> = None;

      // Wait for:
      // 1. Stop signal
      // 2. Queue activity (message added) AND a peer is available
      // 3. Peer availability (a peer connected) AND queue has messages
      tokio::select! {
          biased; // Prioritize stop_signal if ready at the same time as others

          _ = self.stop_signal.notified() => {
              tracing::debug!("[DealerProc {}] Stop signal received. Exiting processor loop.", self.core_handle);
              break; // Exit the main loop directly
          }

          // Wait for either a message to be added to the queue (and peers might be available)
                // OR for a peer to become available (and messages might be in the queue)
                // This select acts as a gate: once one notifier fires, we proceed to check actual state.
                _ = async {
                    tokio::select! {
                        _ = self.queue_activity_notifier.notified() => {
                            tracing::trace!("[DealerProc {}] Woke on queue_activity_notifier.", self.core_handle);
                        },
                        _ = self.peer_availability_notifier.notified() => {
                            tracing::trace!("[DealerProc {}] Woke on peer_availability_notifier.", self.core_handle);
                        },
                    }
          } => {
              // Activity occurred. Now, check conditions and try to pop.
              let mut queue_guard = self.pending_queue.lock().await;
              if !queue_guard.is_empty() && self.load_balancer.has_pipes().await {
                  // Conditions met: queue has messages AND a peer is available.
                  popped_message_parts = queue_guard.pop_front();
                  // queue_guard is dropped here
              }
              // If conditions not met (e.g., notified but queue now empty, or peer detached just now),
              // popped_message_parts remains None, and we loop back to select!.
          }
      } // end of outer select!

      // If popped_message_parts is Some, we got a message to send.
      // If it's None, it means the stop_signal branch was taken, and we should exit.
      if let Some(current_message_parts) = popped_message_parts {
        tracing::trace!(
          "[DealerProc {}] Processing message from outgoing queue ({} parts).",
          self.core_handle,
          current_message_parts.len()
        );

        // Attempt to get a peer. get_next_pipe is quick if peers exist.
        if let Some(pipe_write_id) = self.load_balancer.get_next_pipe().await {
          let timeout_opt: Option<Duration>;
          let pipe_tx_opt;
          {
            let core_s = self.core_accessor.core_state.read();
            timeout_opt = core_s.options.sndtimeo;
            pipe_tx_opt = core_s.get_pipe_sender(pipe_write_id);
          }

          if let Some(pipe_tx) = pipe_tx_opt {
            let mut re_queue_message = false;
            for (idx, msg_part) in current_message_parts.iter().cloned().enumerate() {
              match send_msg_with_timeout(&pipe_tx, msg_part, timeout_opt, self.core_handle, pipe_write_id).await {
                Ok(()) => { /* part sent successfully */ }
                Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
                  tracing::warn!(
                    "[DealerProc {}] HWM/Timeout sending part {} of queued msg to pipe {}. Re-queuing.",
                    self.core_handle,
                    idx,
                    pipe_write_id
                  );
                  re_queue_message = true;
                  break; // Stop sending parts of this message
                }
                Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::Internal(_)) => {
                  tracing::warn!(
                    "[DealerProc {}] Pipe {} closed/error for queued msg part {}: {}. Removing pipe, re-queuing.",
                    self.core_handle,
                    pipe_write_id,
                    idx,
                    e
                  );
                  self.load_balancer.remove_pipe(pipe_write_id).await;
                  self.peer_availability_notifier.notify_waiters(); // Signal peer status change
                  re_queue_message = true;
                  break; // Stop sending parts of this message
                }
                Err(e) => {
                  // Other unexpected errors
                  tracing::error!(
                    "[DealerProc {}] Unexpected error sending queued msg part {} to pipe {}: {}. Re-queuing.",
                    self.core_handle,
                    idx,
                    pipe_write_id,
                    e
                  );
                  re_queue_message = true;
                  break; // Stop sending parts of this message
                }
              }
            }
            if re_queue_message {
              let mut queue_guard = self.pending_queue.lock().await;
              queue_guard.push_front(current_message_parts); // Put entire original message back
              drop(queue_guard);
              // We might want to notify queue_activity_notifier here if we expect other tasks
              // to be waiting on its HWM, but this processor will loop and retry.
              // If it was re-queued because a peer was *removed*, peer_availability_notifier
              // was already signaled.
            } else {
              tracing::trace!(
                "[DealerProc {}] Successfully sent all parts of a queued message to pipe {}.",
                self.core_handle,
                pipe_write_id
              );
            }
          } else {
            // Pipe sender gone for selected pipe_write_id
            tracing::warn!(
              "[DealerProc {}] Pipe sender for {} disappeared. Re-queuing message.",
              self.core_handle,
              pipe_write_id
            );
            self.load_balancer.remove_pipe(pipe_write_id).await; // Ensure it's removed
            self.peer_availability_notifier.notify_waiters();
            let mut queue_guard = self.pending_queue.lock().await;
            queue_guard.push_front(current_message_parts);
            drop(queue_guard);
          }
        } else {
          // No peer available from load_balancer even after notification
          tracing::trace!(
            "[DealerProc {}] No peer available from LB for queued message. Re-queuing.",
            self.core_handle
          );
          let mut queue_guard = self.pending_queue.lock().await;
          queue_guard.push_front(current_message_parts);
          drop(queue_guard);
          // The message is back in the queue. The loop will wait for peer_availability or queue_activity again.
        }
      } else {
        tracing::trace!(
          "[DealerProc {}] No message popped (or conditions not met after activity). Continuing to wait.",
          self.core_handle
        );
      }
    } // end main loop
    tracing::debug!(
      "[DealerProc {}] Outgoing queue processor task finished execution.",
      self.core_handle
    );
  }
}

#[derive(Debug)]
pub(crate) struct DealerSocket {
  core: Arc<SocketCore>,
  load_balancer: Arc<LoadBalancer>, // Arc for sharing with processor
  incoming_queue: FairQueue,
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
  pipe_state: Mutex<HashMap<usize, IncomingPipeState>>,
  pending_outgoing_queue: Arc<Mutex<VecDeque<Vec<Msg>>>>, // Arc for sharing
  outgoing_queue_activity_notifier: Arc<Notify>,          // Notifies processor of new msgs or available pipes
  peer_availability_notifier: Arc<Notify>,                // Notifies senders/processor of peer changes
  // Handle for the outgoing processor task
  processor_task_handle: Mutex<Option<JoinHandle<()>>>,
  processor_stop_signal: Arc<Notify>,
  current_send_transaction: Mutex<DealerSendTransaction>,
}

impl DealerSocket {
  pub fn new(core: Arc<SocketCore>, options: SocketOptions) -> Self {
    let queue_capacity = options.rcvhwm.max(1);
    let pending_queue_arc = Arc::new(Mutex::new(VecDeque::new()));
    let lb_arc = Arc::new(LoadBalancer::new());
    let queue_notifier_arc = Arc::new(Notify::new());
    let peer_notifier_arc = Arc::new(Notify::new());
    let stop_signal_arc = Arc::new(Notify::new());

    let processor = DealerSocketOutgoingProcessor {
      core_handle: core.handle,
      pending_queue: pending_queue_arc.clone(),
      load_balancer: lb_arc.clone(),
      core_accessor: core.clone(),
      queue_activity_notifier: queue_notifier_arc.clone(),
      peer_availability_notifier: peer_notifier_arc.clone(),
      stop_signal: stop_signal_arc.clone(),
    };

    let processor_jh = tokio::spawn(processor.run());

    Self {
      core,
      load_balancer: lb_arc,
      incoming_queue: FairQueue::new(queue_capacity),
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
      pipe_state: Mutex::new(HashMap::new()),
      pending_outgoing_queue: pending_queue_arc,
      outgoing_queue_activity_notifier: queue_notifier_arc,
      peer_availability_notifier: peer_notifier_arc,
      processor_task_handle: Mutex::new(Some(processor_jh)),
      processor_stop_signal: stop_signal_arc,
      current_send_transaction: Mutex::new(DealerSendTransaction::Idle),
    }
  }

  fn core_state(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  fn prepare_full_send_sequence(&self, user_msg: Msg) -> Vec<Msg> {
    let mut delimiter = Msg::new();
    delimiter.set_flags(MsgFlags::MORE);
    let mut final_user_msg = user_msg;
    final_user_msg.set_flags(final_user_msg.flags() & !MsgFlags::MORE);
    vec![delimiter, final_user_msg]
  }

  fn prepare_full_multipart_send_sequence(&self, user_frames: Vec<Msg>) -> Vec<Msg> {
    if user_frames.is_empty() {
      let mut delimiter = Msg::new();
      delimiter.set_flags(MsgFlags::MORE);
      return vec![delimiter, Msg::new()];
    }
    let mut frames_to_send = Vec::with_capacity(user_frames.len() + 1);
    let mut delimiter = Msg::new();
    delimiter.set_flags(MsgFlags::MORE);
    frames_to_send.push(delimiter);
    let num_user_frames = user_frames.len();
    for (i, mut frame) in user_frames.into_iter().enumerate() {
      if i < num_user_frames - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
      frames_to_send.push(frame);
    }
    frames_to_send
  }
}

#[async_trait]
impl ISocket for DealerSocket {
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
    let res = delegate_to_core!(self, UserConnect, endpoint: endpoint.to_string());
    if res.is_ok() {
      self.peer_availability_notifier.notify_one();
    } // Potential new peer
    res
  }
  async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
    let res = delegate_to_core!(self, UserDisconnect, endpoint: endpoint.to_string());
    self.peer_availability_notifier.notify_waiters(); // A peer might have been removed
    res
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
    self.processor_stop_signal.notify_one(); // Signal processor to stop
    if let Some(handle) = self.processor_task_handle.lock().await.take() {
      // Optionally wait for the processor task to finish, with a timeout
      if let Err(e) = tokio_timeout(Duration::from_millis(100), handle).await {
        tracing::warn!(
          "[Dealer {}] Timeout or error waiting for processor task on close: {:?}",
          self.core.handle,
          e
        );
      }
    }
    let res = delegate_to_core!(self, UserClose,);
    // Notifying these again after close command ensures any senders blocked on queue HWM
    // or peer availability are woken up to observe the closing state.
    self.outgoing_queue_activity_notifier.notify_waiters();
    self.peer_availability_notifier.notify_waiters();
    res
  }

  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let mut transaction_guard = self.current_send_transaction.lock().await;

    if msg.is_more() {
      match &mut *transaction_guard {
        DealerSendTransaction::Idle => {
          let notifier = Arc::new(Notify::new());
          *transaction_guard = DealerSendTransaction::Buffering {
            parts: vec![msg],
            completion_notifier: notifier,
          };
          tracing::trace!(
            "[Dealer {}] Started send transaction, buffered 1 part.",
            self.core.handle
          );
        }
        DealerSendTransaction::Buffering {
          ref mut parts,
          completion_notifier: _,
        } => {
          // Renamed notifier to avoid conflict
          if parts.len() >= MAX_DEALER_SEND_BUFFER_PARTS {
            let notifier_to_signal = match std::mem::replace(&mut *transaction_guard, DealerSendTransaction::Idle) {
              DealerSendTransaction::Buffering {
                completion_notifier: cn,
                ..
              } => Some(cn),
              _ => None,
            };
            drop(transaction_guard);
            if let Some(notifier_arc) = notifier_to_signal {
              // Use notifier_arc
              notifier_arc.notify_waiters();
            }
            return Err(ZmqError::ResourceLimitReached);
          }
          parts.push(msg);
          tracing::trace!(
            "[Dealer {}] Added to send transaction, buffered {} parts.",
            self.core.handle,
            parts.len()
          );
        }
      }
      Ok(())
    } else {
      // Final part or single-part message
      let (parts_to_send, notifier_opt) = match std::mem::replace(&mut *transaction_guard, DealerSendTransaction::Idle)
      {
        DealerSendTransaction::Idle => (vec![msg], None),
        DealerSendTransaction::Buffering {
          mut parts,
          completion_notifier,
        } => {
          parts.push(msg);
          (parts, Some(completion_notifier))
        }
      };

      drop(transaction_guard); // Release lock before await

      let result = {
        let full_message_parts_for_wire = self.prepare_full_multipart_send_sequence(parts_to_send);
        self.send_logical_message(full_message_parts_for_wire).await
      };

      if let Some(notifier) = notifier_opt {
        notifier.notify_waiters();
      }

      result
    }
  }

  async fn send_multipart(&self, user_frames: Vec<Msg>) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let sndtimeo_opt: Option<Duration> = { self.core_state().options.sndtimeo };

    loop {
      let mut transaction_guard = self.current_send_transaction.lock().await;
      match &*transaction_guard {
        DealerSendTransaction::Idle => {
          drop(transaction_guard);
          let full_message_parts_for_wire = self.prepare_full_multipart_send_sequence(user_frames);
          return self.send_logical_message(full_message_parts_for_wire).await;
        }
        DealerSendTransaction::Buffering {
          completion_notifier, ..
        } => {
          let notifier_clone = completion_notifier.clone();
          drop(transaction_guard);

          tracing::debug!(
            "[Dealer {}] send_multipart waiting for active send() transaction (SNDTIMEO: {:?}).",
            self.core.handle,
            sndtimeo_opt
          );

          // Add select against a socket closing signal if available for more robustness
          let closing_signal_future = async {
            // Placeholder for actual closing signal
            if !self.core.is_running().await {
              // Re-check if socket is still running
              futures::future::pending::<()>().await; // If not running, wait forever conceptually to be preempted
            } else {
              futures::future::pending::<()>().await; // Otherwise, this branch of select doesn't complete
            }
          };

          match sndtimeo_opt {
            Some(duration) if duration.is_zero() => {
              return Err(ZmqError::ResourceLimitReached);
            }
            Some(duration) => {
              tokio::select! {
                  biased; // Prioritize checking if socket is closing.
                   _ = closing_signal_future => {
                        tracing::warn!("[Dealer {}] send_multipart aborted: Socket is closing while waiting for send() transaction.", self.core.handle);
                        return Err(ZmqError::InvalidState("Socket is closing"));
                   }
                  res = tokio::time::timeout(duration, notifier_clone.notified()) => {
                    if res.is_err() { // Timeout elapsed
                        tracing::debug!("[Dealer {}] send_multipart timed out waiting for send() transaction.", self.core.handle);
                        return Err(ZmqError::Timeout);
                    }
                    // Notified, loop again
                  }
              }
            }
            None => {
              // Infinite SNDTIMEO
              tokio::select! {
                  biased;
                  _ = closing_signal_future => {
                        tracing::warn!("[Dealer {}] send_multipart aborted: Socket is closing while waiting for send() transaction (infinite wait).", self.core.handle);
                        return Err(ZmqError::InvalidState("Socket is closing"));
                  }
                  _ = notifier_clone.notified() => {
                      // Notified, loop again
                  }
              }
            }
          }
          tracing::debug!(
            "[Dealer {}] send_multipart woken after send() transaction. Re-checking.",
            self.core.handle
          );
          // continue loop implicitly by falling through
        }
      }
    }
  }

  // recv, set_pattern_option, get_pattern_option, process_command, handle_pipe_event, pipe_attached, update_peer_identity, pipe_detached
  // remain largely the same as your previous correct DealerSocket version,
  // but pipe_attached and pipe_detached need to notify peer_availability_notifier.

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };
    let pop_future = self.incoming_queue.pop_message();
    let received_msg_frame = match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => match tokio_timeout(duration, pop_future).await {
        Ok(Ok(Some(msg))) => msg,
        Ok(Ok(None)) => return Err(ZmqError::Internal("Receive queue closed".into())),
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(ZmqError::Timeout),
      },
      _ => match pop_future.await? {
        Some(msg) => msg,
        None => return Err(ZmqError::Internal("Receive queue closed".into())),
      },
    };
    Ok(received_msg_frame)
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

  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        let mut state_map_guard = self.pipe_state.lock().await;
        let current_pipe_state = state_map_guard.entry(pipe_read_id).or_insert(IncomingPipeState::Idle);
        let is_last_part_of_transport_msg = !msg.is_more();

        tracing::trace!(
            handle = self.core.handle, pipe_id = pipe_read_id, state = ?*current_pipe_state,
            size = msg.size(), more = msg.is_more(), "DEALER handle_pipe_event received ZMTP frame"
        );
        match *current_pipe_state {
          IncomingPipeState::Idle => {
            if msg.size() > 0 && msg.is_more() {
              tracing::trace!(
                "[Dealer {}] Stripped identity-like frame from pipe {}.",
                self.core.handle,
                pipe_read_id
              );
              *current_pipe_state = IncomingPipeState::StrippedIdentityExpectDelimiterOrPayload;
            } else if msg.size() == 0 && msg.is_more() {
              tracing::trace!(
                "[Dealer {}] Stripped initial empty delimiter from pipe {}.",
                self.core.handle,
                pipe_read_id
              );
              *current_pipe_state = IncomingPipeState::StrippedDelimiterExpectPayload;
            } else {
              let mut payload_msg = msg;
              payload_msg
                .metadata_mut()
                .insert_typed(SourcePipeReadId(pipe_read_id))
                .await;
              self.incoming_queue.push_message(payload_msg).await?;
              if is_last_part_of_transport_msg {
                *current_pipe_state = IncomingPipeState::Idle;
              } else {
                *current_pipe_state = IncomingPipeState::ExpectingMorePayload;
              }
            }
          }
          IncomingPipeState::StrippedIdentityExpectDelimiterOrPayload => {
            if msg.size() == 0 && msg.is_more() {
              tracing::trace!(
                "[Dealer {}] Stripped empty delimiter after identity from pipe {}.",
                self.core.handle,
                pipe_read_id
              );
              *current_pipe_state = IncomingPipeState::ExpectingMorePayload;
            } else {
              let mut payload_msg = msg;
              payload_msg
                .metadata_mut()
                .insert_typed(SourcePipeReadId(pipe_read_id))
                .await;
              self.incoming_queue.push_message(payload_msg).await?;
              if is_last_part_of_transport_msg {
                *current_pipe_state = IncomingPipeState::Idle;
              } else {
                *current_pipe_state = IncomingPipeState::ExpectingMorePayload;
              }
            }
          }
          IncomingPipeState::StrippedDelimiterExpectPayload | IncomingPipeState::ExpectingMorePayload => {
            let mut payload_msg = msg;
            payload_msg
              .metadata_mut()
              .insert_typed(SourcePipeReadId(pipe_read_id))
              .await;
            self.incoming_queue.push_message(payload_msg).await?;
            if is_last_part_of_transport_msg {
              *current_pipe_state = IncomingPipeState::Idle;
            } else {
              *current_pipe_state = IncomingPipeState::ExpectingMorePayload;
            }
          }
        }
      }
      _ => {}
    }
    Ok(())
  }

  async fn pipe_attached(&self, pipe_read_id: usize, pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    tracing::debug!(
      "[Dealer {}] Attaching pipe: read_id={}, write_id={}",
      self.core.handle,
      pipe_read_id,
      pipe_write_id
    );
    self
      .pipe_read_to_write_id
      .lock()
      .await
      .insert(pipe_read_id, pipe_write_id);
    self.load_balancer.add_pipe(pipe_write_id).await;
    self.incoming_queue.pipe_attached(pipe_read_id);
    self
      .pipe_state
      .lock()
      .await
      .insert(pipe_read_id, IncomingPipeState::Idle);

    self.peer_availability_notifier.notify_one(); // A peer became available
    self.outgoing_queue_activity_notifier.notify_one(); // Might be messages to send
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "DEALER",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but DEALER does not use peer identities for routing. Ignoring."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!("[Dealer {}] Detaching pipe_read_id: {}", self.core.handle, pipe_read_id);
    let maybe_write_id = self.pipe_read_to_write_id.lock().await.remove(&pipe_read_id);
    if let Some(write_id) = maybe_write_id {
      self.load_balancer.remove_pipe(write_id).await;
    }
    self.incoming_queue.pipe_detached(pipe_read_id);
    self.pipe_state.lock().await.remove(&pipe_read_id);
    self.peer_availability_notifier.notify_waiters(); // A peer was removed
  }
}

impl DealerSocket {
  async fn send_logical_message(&self, full_message_parts: Vec<Msg>) -> Result<(), ZmqError> {
    let core_opts = self.core_state().options.clone();
    let global_sndtimeo: Option<Duration> = core_opts.sndtimeo;
    let global_sndhwm: usize = core_opts.sndhwm.max(1);

    // Try to get a peer. This is non-blocking if LoadBalancer has peers.
    // It might briefly lock if other sends are also trying to pick a peer.
    let mut tried_direct_send = false;
    if let Some(pipe_write_id) = self.load_balancer.get_next_pipe().await {
      tried_direct_send = true;
      let pipe_tx_opt = { self.core_state().get_pipe_sender(pipe_write_id) };

      if let Some(pipe_tx) = pipe_tx_opt {
        let mut pipe_send_failed_critically = false;
        for (idx, msg_part) in full_message_parts.iter().cloned().enumerate() {
          match send_msg_with_timeout(&pipe_tx, msg_part, global_sndtimeo, self.core.handle, pipe_write_id).await {
            Ok(()) => {
              continue;
            }
            Err(e @ ZmqError::ResourceLimitReached) | Err(e @ ZmqError::Timeout) => {
              tracing::debug!(
                "[Dealer {}] Direct send to pipe {} (part {}) failed (HWM/Timeout): {}. Will queue.",
                self.core.handle,
                pipe_write_id,
                idx,
                e
              );
              // Fall through to global queuing for HWM/Timeout on direct send
              pipe_send_failed_critically = true; // Treat as needing queue
              break;
            }
            Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::Internal(_)) => {
              tracing::warn!(
                "[Dealer {}] Direct send to pipe {} (part {}) failed fatally: {}. Removing pipe, will queue.",
                self.core.handle,
                pipe_write_id,
                idx,
                e
              );
              self.load_balancer.remove_pipe(pipe_write_id).await;
              self.peer_availability_notifier.notify_waiters();
              pipe_send_failed_critically = true; // Treat as needing queue
              break;
            }
            Err(e) => return Err(e), // Other unexpected errors are propagated
          }
        }
        if !pipe_send_failed_critically {
          return Ok(());
        } // Successfully sent all parts directly
      } else {
        // Pipe sender gone
        tracing::warn!(
          "[Dealer {}] Pipe {} sender gone. Will queue.",
          self.core.handle,
          pipe_write_id
        );
        self.load_balancer.remove_pipe(pipe_write_id).await;
        self.peer_availability_notifier.notify_waiters();
        // Fall through to global queuing
      }
    }

    println!("sending queuing");
    // No peer available from LB, or direct send to chosen peer failed non-fatally (HWM/Timeout/Closed).
    // Proceed to queue the message.
    if !tried_direct_send {
      tracing::trace!(
        "[Dealer {}] No peer immediately available from LB. Attempting to queue message ({} parts).",
        self.core.handle,
        full_message_parts.len()
      );
    }

    loop {
      // Loop for SNDTIMEO if global queue is full
      let mut queue_guard = self.pending_outgoing_queue.lock().await;
      if queue_guard.len() < global_sndhwm {
        tracing::debug!(
          "[Dealer {}] Queuing message ({} parts). Current queue_len: {}. HWM: {}",
          self.core.handle,
          full_message_parts.len(),
          queue_guard.len(),
          global_sndhwm
        );
        queue_guard.push_back(full_message_parts);
        drop(queue_guard);
        self.outgoing_queue_activity_notifier.notify_one(); // Notify processor
        return Ok(());
      }
      drop(queue_guard);

      match global_sndtimeo {
        Some(duration) if duration.is_zero() => {
          tracing::debug!(
            "[Dealer {}] Global queue full, SNDTIMEO=0. Send failed (ResourceLimitReached).",
            self.core.handle
          );
          return Err(ZmqError::ResourceLimitReached);
        }
        Some(duration) => {
          tracing::debug!(
            "[Dealer {}] Global queue full, SNDTIMEO={:?}. Waiting for space.",
            self.core.handle,
            duration
          );
          match tokio_timeout(duration, self.outgoing_queue_activity_notifier.notified()).await {
            Ok(()) => { /* Notified, loop to re-check queue */ }
            Err(_) => {
              tracing::debug!(
                "[Dealer {}] Timeout waiting for space in global queue.",
                self.core.handle
              );
              return Err(ZmqError::Timeout);
            }
          }
        }
        None => {
          tracing::debug!(
            "[Dealer {}] Global queue full, SNDTIMEO=infinite. Waiting for space.",
            self.core.handle
          );
          self.outgoing_queue_activity_notifier.notified().await;
          // Loop to re-check queue
        }
      }
    }
  }
}

// This Drop impl is crucial for cleaning up the processor task.
impl Drop for DealerSocket {
  fn drop(&mut self) {
    self.processor_stop_signal.notify_one();
    // Note: We can't easily .await the join handle in a synchronous Drop.
    // The task will be signalled to stop. If it needs to be joined explicitly,
    // the `close()` method is the place for that.
    // If `processor_task_handle` is `Some`, it implies it was spawned.
    if let Some(handle) = self
      .processor_task_handle
      .try_lock()
      .ok()
      .and_then(|mut guard| guard.take())
    {
      tracing::debug!(
        "[Dealer {}] Signalled processor task to stop. Aborting task in Drop as a fallback.",
        self.core.handle
      );
      handle.abort(); // Abort as a fallback if it doesn't exit cleanly/quickly
    }

    // Clear any pending send transaction and notify waiters to prevent hangs
    if let Ok(mut transaction_guard) = self.current_send_transaction.try_lock() {
      if let DealerSendTransaction::Buffering {
        completion_notifier,
        parts,
        ..
      } = std::mem::replace(&mut *transaction_guard, DealerSendTransaction::Idle)
      {
        tracing::debug!(
          "[Dealer {}] Clearing {} buffered send parts due to Drop.",
          self.core.handle,
          parts.len()
        );
        completion_notifier.notify_waiters();
      }
    } else {
      // Could not acquire lock, might be held by a task that's being aborted.
      // This is a tricky situation in Drop.
      tracing::warn!("[Dealer {}] Could not acquire transaction lock in Drop to clear buffer. Potential for waiters to hang if not also closing.", self.core.handle);
    }
  }
}
