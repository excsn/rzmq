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
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit};
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;

use super::patterns::{IncomingMessageOrchestrator, WritePipeCoordinator};

// Maximum number of parts that can be buffered by send()
const MAX_DEALER_SEND_BUFFER_PARTS: usize = 128;

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

#[derive(Debug)]
struct DealerSocketOutgoingProcessor {
  core_handle: usize,
  pending_queue: Arc<Mutex<VecDeque<Vec<Msg>>>>,
  load_balancer: Arc<LoadBalancer>,
  core_accessor: Arc<SocketCore>,          // Renamed for clarity
  queue_activity_notifier: Arc<Notify>,    // Notified when queue gets a message or a peer gets a message
  peer_availability_notifier: Arc<Notify>, // Notified when a peer connects/disconnects
  stop_signal: Arc<Notify>,                // To stop this processor task
  pipe_send_coordinator: Arc<WritePipeCoordinator>,
}

impl DealerSocketOutgoingProcessor {
  pub async fn run(self) {
    // Consumes self to run the loop
    tracing::debug!(
      "[DealerProc {}] Outgoing queue processor task started.",
      self.core_handle
    );

    loop {
      let mut popped_message_parts_to_send: Option<Vec<Msg>> = None;

      // Phase 1: Wait for work or stop signal
      tokio::select! {
        biased; // Prioritize stop_signal

        _ = self.stop_signal.notified() => {
          tracing::debug!("[DealerProc {}] Stop signal received. Exiting processor loop.", self.core_handle);
          break; // Exit the main loop directly
        }

        _ = async { // This inner async block creates a future for the select arm
          tokio::select! {
            _ = self.queue_activity_notifier.notified() => {
              tracing::trace!("[DealerProc {}] Woke on queue_activity_notifier.", self.core_handle);
            },
            _ = self.peer_availability_notifier.notified() => {
              tracing::trace!("[DealerProc {}] Woke on peer_availability_notifier.", self.core_handle);
            },
          }
        } => {
          let mut queue_guard = self.pending_queue.lock().await;
          if !queue_guard.is_empty() && self.load_balancer.has_pipes().await {
            popped_message_parts_to_send = queue_guard.pop_front();
          }
        }
      } // end of outer select!

      // Phase 2: Process the popped message if any
      if let Some(current_message_to_send) = popped_message_parts_to_send {
        tracing::trace!(
          "[DealerProc {}] Processing message from outgoing queue ({} parts).",
          self.core_handle,
          current_message_to_send.len()
        );

        if let Some(pipe_write_id) = self.load_balancer.get_next_pipe().await {
          let snd_timeout_opt: Option<Duration>;
          let pipe_tx_opt;
          {
            let core_s = self.core_accessor.core_state.read();
            snd_timeout_opt = core_s.options.sndtimeo;
            pipe_tx_opt = core_s.get_pipe_sender(pipe_write_id);
          }

          if let Some(pipe_tx) = pipe_tx_opt {
            let send_permit_result = self
              .pipe_send_coordinator
              .acquire_send_permit(pipe_write_id, snd_timeout_opt)
              .await;

            let _send_permit: OwnedSemaphorePermit;

            match send_permit_result {
              Ok(permit) => {
                _send_permit = permit;
              }
              Err(e) => {
                tracing::warn!(
                  "[DealerProc {}] Failed to acquire send permit for pipe {}: {}. Re-queuing message.",
                  self.core_handle,
                  pipe_write_id,
                  e
                );
                if matches!(e, ZmqError::HostUnreachable(_)) {
                  self.load_balancer.remove_pipe(pipe_write_id).await;
                  self.peer_availability_notifier.notify_waiters();
                }
                let mut qg = self.pending_queue.lock().await;
                qg.push_front(current_message_to_send);
                drop(qg);
                self.queue_activity_notifier.notify_one();
                continue;
              }
            }
            // --- Permit is now held by _send_permit ---

            let mut re_queue_this_message = false;
            for (idx, msg_part) in current_message_to_send.iter().cloned().enumerate() {
              match send_msg_with_timeout(&pipe_tx, msg_part, snd_timeout_opt, self.core_handle, pipe_write_id).await {
                Ok(()) => { /* part sent successfully */ }
                Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
                  tracing::warn!(
                    "[DealerProc {}] HWM/Timeout sending part {} of queued msg to pipe {}. Re-queuing entire message.",
                    self.core_handle,
                    idx,
                    pipe_write_id
                  );
                  re_queue_this_message = true;
                  break;
                }
                Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::Internal(_)) => {
                  tracing::warn!(
                    "[DealerProc {}] Pipe {} closed/error for queued msg part {}: {}. Removing pipe, re-queuing entire message.",
                    self.core_handle, pipe_write_id, idx, e
                  );
                  self.load_balancer.remove_pipe(pipe_write_id).await;
                  if let Some(sem) = self.pipe_send_coordinator.remove_pipe(pipe_write_id).await {
                    sem.close();
                  }
                  self.peer_availability_notifier.notify_waiters();
                  re_queue_this_message = true;
                  break;
                }
                Err(e) => {
                  tracing::error!(
                    "[DealerProc {}] Unexpected error sending queued msg part {} to pipe {}: {}. Re-queuing entire message.",
                    self.core_handle, idx, pipe_write_id, e
                  );
                  re_queue_this_message = true;
                  break;
                }
              }
            } // End for loop sending parts

            if re_queue_this_message {
              let mut queue_guard = self.pending_queue.lock().await;
              queue_guard.push_front(current_message_to_send);
              drop(queue_guard);
              self.queue_activity_notifier.notify_one();
            } else {
              tracing::trace!(
                "[DealerProc {}] Successfully sent all parts of a queued message to pipe {}.",
                self.core_handle,
                pipe_write_id
              );
            }
          } else {
            tracing::warn!(
              "[DealerProc {}] Pipe sender for {} disappeared before sending. Removing from LB, re-queuing message.",
              self.core_handle,
              pipe_write_id
            );
            self.load_balancer.remove_pipe(pipe_write_id).await;
            if let Some(sem) = self.pipe_send_coordinator.remove_pipe(pipe_write_id).await {
              sem.close();
            }
            self.peer_availability_notifier.notify_waiters();

            let mut queue_guard = self.pending_queue.lock().await;
            queue_guard.push_front(current_message_to_send);
            drop(queue_guard);
            self.queue_activity_notifier.notify_one();
          }
        } else {
          tracing::trace!(
            "[DealerProc {}] No peer available from LB for queued message. Re-queuing message.",
            self.core_handle
          );
          let mut queue_guard = self.pending_queue.lock().await;
          queue_guard.push_front(current_message_to_send);
          drop(queue_guard);
        }
      } else {
        tracing::trace!(
          "[DealerProc {}] No message popped from queue (or conditions not met after activity). Continuing to wait.",
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
  incoming_orchestrator: IncomingMessageOrchestrator,
  pipe_read_to_write_id: Mutex<HashMap<usize, usize>>,
  pending_outgoing_queue: Arc<Mutex<VecDeque<Vec<Msg>>>>, // Arc for sharing
  outgoing_queue_activity_notifier: Arc<Notify>,          // Notifies processor of new msgs or available pipes
  peer_availability_notifier: Arc<Notify>,                // Notifies senders/processor of peer changes
  // Handle for the outgoing processor task
  processor_task_handle: Mutex<Option<JoinHandle<()>>>,
  processor_stop_signal: Arc<Notify>,
  current_send_transaction: Mutex<DealerSendTransaction>,
  pipe_send_coordinator: Arc<WritePipeCoordinator>,
}

impl DealerSocket {
  pub fn new(core: Arc<SocketCore>, options: SocketOptions) -> Self {
    let queue_capacity = options.rcvhwm.max(1);
    let pending_queue_arc = Arc::new(Mutex::new(VecDeque::new()));
    let lb_arc = Arc::new(LoadBalancer::new());
    let queue_notifier_arc = Arc::new(Notify::new());
    let peer_notifier_arc = Arc::new(Notify::new());
    let stop_signal_arc = Arc::new(Notify::new());

    let pipe_send_coordinator = Arc::new(WritePipeCoordinator::new());
    let processor = DealerSocketOutgoingProcessor {
      core_handle: core.handle,
      pending_queue: pending_queue_arc.clone(),
      load_balancer: lb_arc.clone(),
      core_accessor: core.clone(),
      queue_activity_notifier: queue_notifier_arc.clone(),
      peer_availability_notifier: peer_notifier_arc.clone(),
      stop_signal: stop_signal_arc.clone(),
      pipe_send_coordinator: pipe_send_coordinator.clone(),
    };

    let processor_jh = tokio::spawn(processor.run());

    let orchestrator = IncomingMessageOrchestrator::new(core.handle, options.rcvhwm);

    Self {
      core,
      load_balancer: lb_arc,
      incoming_orchestrator: orchestrator,
      pipe_read_to_write_id: Mutex::new(HashMap::new()),
      pending_outgoing_queue: pending_queue_arc,
      outgoing_queue_activity_notifier: queue_notifier_arc,
      peer_availability_notifier: peer_notifier_arc,
      processor_task_handle: Mutex::new(Some(processor_jh)),
      processor_stop_signal: stop_signal_arc,
      current_send_transaction: Mutex::new(DealerSendTransaction::Idle),
      pipe_send_coordinator,
    }
  }

  fn core_state(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  // Static processing function for DEALER messages
  // This function is "without self" and takes any necessary context.
  // For DEALER stripping a ROUTER envelope, it doesn't need extra context beyond the message itself.
  fn process_raw_message_for_dealer(
    _pipe_read_id: usize,           // May be unused for pure envelope stripping
    mut raw_zmtp_message: Vec<Msg>, // e.g., [ID_MORE, (opt_IDs_MORE)?, EMPTY_DELIM_MORE, PAYLOAD_FRAMES...]
    _ctx: (),                       // Context parameter (unused for now for dealer stripping)
  ) -> Result<Vec<Msg>, ZmqError> {
    // Find the first empty delimiter. This marks the end of the routing envelope.
    if let Some(delimiter_pos) = raw_zmtp_message.iter().position(|frame| frame.size() == 0) {
      // The application payload consists of all frames *after* this empty delimiter.
      // Drain consumes the original vector.
      let app_payload: Vec<Msg> = raw_zmtp_message.drain(delimiter_pos + 1..).collect();

      // As per Step 3, orchestrator.queue_application_message_frames will handle
      // the case where app_payload is empty by queueing vec![Msg::new()].
      // So, this function can just return an empty Vec if stripping leads to no payload.
      Ok(app_payload)
    } else {
      // No empty delimiter was found. This could happen if:
      // 1. The DEALER is connected to another DEALER (not a ROUTER).
      // 2. The message from the ROUTER is malformed (missing delimiter).
      // Standard ZMQ DEALER behavior when talking to non-ROUTER or receiving non-enveloped messages
      // is to treat the entire message as payload.
      tracing::warn!(
        // handle = ??? (cannot access self.core.handle in static fn easily, needs context)
        // pipe_id = _pipe_read_id, // Available
        "Dealer processing: Raw ZMTP message received without an empty delimiter. Passing all frames as application payload."
      );
      Ok(raw_zmtp_message)
    }
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
      let transaction_guard = self.current_send_transaction.lock().await;
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

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };
    return self.incoming_orchestrator.recv_message(rcvtimeo_opt).await;
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    let rcvtimeo_opt: Option<Duration> = { self.core_state().options.rcvtimeo };

    return self.incoming_orchestrator.recv_logical_message(rcvtimeo_opt).await;
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
        // 1. Accumulate frame. If a full ZMTP message is ready, orchestrator returns it.
        match self
          .incoming_orchestrator
          .accumulate_pipe_frame(pipe_read_id, msg)
        {
          // Now calling sync version
          Ok(Some(raw_zmtp_message_vec)) => {
            // A full ZMTP message is assembled
            let dealer_ctx = (); // Create context (empty for dealer)

            // 2. Process the raw ZMTP message using DEALER's specific logic
            match Self::process_raw_message_for_dealer(pipe_read_id, raw_zmtp_message_vec, dealer_ctx) {
              Ok(app_logical_message) => {
                // 3. Queue the processed application-level message frames
                self
                  .incoming_orchestrator
                  .queue_application_message_frames(pipe_read_id, app_logical_message)
                  .await?;
              }
              Err(e) => {
                tracing::error!(
                  handle = self.core.handle,
                  pipe_id = pipe_read_id,
                  "DEALER: Error processing raw ZMTP message: {}. Message dropped.",
                  e
                );
                // Optionally, close the problematic pipe or take other error actions.
              }
            }
          }
          Ok(None) => { /* More frames needed for this pipe */ }
          Err(e) => {
            tracing::error!(
              handle = self.core.handle,
              pipe_id = pipe_read_id,
              "DEALER: Error accumulating pipe frame: {}",
              e
            );
            return Err(e);
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
    self.pipe_send_coordinator.add_pipe(pipe_write_id).await;

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

      if let Some(semaphore) = self.pipe_send_coordinator.remove_pipe(write_id).await {
        semaphore.close();
      }
    }
    self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
    self.peer_availability_notifier.notify_waiters(); // A peer was removed
  }
}

impl DealerSocket {
  async fn send_logical_message(&self, full_message_parts: Vec<Msg>) -> Result<(), ZmqError> {
    let core_opts = self.core_state().options.clone();
    let global_sndtimeo: Option<Duration> = core_opts.sndtimeo;
    let global_sndhwm: usize = core_opts.sndhwm.max(1);

    if let Some(pipe_write_id) = self.load_balancer.get_next_pipe().await {
      // Acquire permit for this pipe BEFORE getting pipe_tx and sending
      let _send_permit = match self
        .pipe_send_coordinator
        .acquire_send_permit(pipe_write_id, global_sndtimeo)
        .await
      {
        Ok(permit) => permit,
        Err(e) => {
          // Could be ZmqError::Timeout if permit not acquired, or Internal if pipe gone
          tracing::debug!(
            "[Dealer {}] Failed to acquire send permit for pipe {}: {}. Queuing message.",
            self.core.handle,
            pipe_write_id,
            e
          );
          // Fall through to queuing logic, do NOT try to send directly.
          // If acquire_send_permit itself returns HostUnreachable, it means the pipe was removed from coordinator.
          if matches!(e, ZmqError::HostUnreachable(_)) {
            self.load_balancer.remove_pipe(pipe_write_id).await; // Ensure LB is also updated
            self.peer_availability_notifier.notify_waiters();
          }
          // Force queuing by simulating direct send failure
          return self
            .queue_message_after_send_failure(full_message_parts, global_sndhwm, global_sndtimeo)
            .await;
        }
      };
      // Permit is now held, _send_permit will release it on drop.

      let pipe_tx_opt = { self.core_state().get_pipe_sender(pipe_write_id) };

      if let Some(pipe_tx) = pipe_tx_opt {
        let mut pipe_send_failed_critically = false;
        for (idx, msg_part) in full_message_parts.iter().cloned().enumerate() {
          match send_msg_with_timeout(&pipe_tx, msg_part, global_sndtimeo, self.core.handle, pipe_write_id).await {
            Ok(()) => continue,
            Err(e @ ZmqError::ResourceLimitReached) | Err(e @ ZmqError::Timeout) => {
              pipe_send_failed_critically = true;
              break;
            }
            Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::Internal(_)) => {
              self.load_balancer.remove_pipe(pipe_write_id).await;
              self.peer_availability_notifier.notify_waiters();
              // Also remove from coordinator as pipe is dead
              if let Some(sem) = self.pipe_send_coordinator.remove_pipe(pipe_write_id).await {
                sem.close();
              }
              pipe_send_failed_critically = true;
              break;
            }
            Err(e) => return Err(e),
          }
        }
        if !pipe_send_failed_critically {
          return Ok(());
        }
        // If critically failed, fall through to queue
      } else {
        self.load_balancer.remove_pipe(pipe_write_id).await;
        self.peer_availability_notifier.notify_waiters();
        if let Some(sem) = self.pipe_send_coordinator.remove_pipe(pipe_write_id).await {
          sem.close();
        }
        // Fall through to queue
      }
    }
    // If no peer, or direct send failed critically, queue it:
    self
      .queue_message_after_send_failure(full_message_parts, global_sndhwm, global_sndtimeo)
      .await
  }

  // Helper for the queuing part of send_logical_message
  async fn queue_message_after_send_failure(
    &self,
    full_message_parts: Vec<Msg>,
    global_sndhwm: usize,
    global_sndtimeo: Option<Duration>,
  ) -> Result<(), ZmqError> {
    tracing::trace!(
      "[Dealer {}] No peer immediately available or direct send failed. Attempting to queue message ({} parts).",
      self.core.handle,
      full_message_parts.len()
    );
    loop {
      let mut queue_guard = self.pending_outgoing_queue.lock().await;
      if queue_guard.len() < global_sndhwm {
        queue_guard.push_back(full_message_parts);
        drop(queue_guard);
        self.outgoing_queue_activity_notifier.notify_one();
        return Ok(());
      }
      drop(queue_guard);
      // ... (timeout logic for waiting on queue_activity_notifier as before) ...
      match global_sndtimeo {
        Some(duration) if duration.is_zero() => return Err(ZmqError::ResourceLimitReached),
        Some(duration) => {
          match tokio_timeout(duration, self.outgoing_queue_activity_notifier.notified()).await {
            Ok(()) => { /* Notified, loop to re-check queue */ }
            Err(_) => return Err(ZmqError::Timeout),
          }
        }
        None => {
          self.outgoing_queue_activity_notifier.notified().await;
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
