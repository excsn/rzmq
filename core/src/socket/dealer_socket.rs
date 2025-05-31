// core/src/socket/dealer_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::EndpointInfo;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::LoadBalancer;
use crate::socket::ISocket;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock as ParkingLotRwLock;
use tokio::sync::{Mutex as TokioMutex, Notify, OwnedSemaphorePermit}; // TokioRwLock not used here anymore
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;

use super::patterns::{IncomingMessageOrchestrator, WritePipeCoordinator};

const MAX_DEALER_SEND_BUFFER_PARTS: usize = 128;

#[derive(Debug)]
enum DealerSendTransaction {
  Idle,
  Buffering {
    parts: Vec<Msg>,
    completion_notifier: Arc<Notify>,
  },
}

#[derive(Debug)]
struct DealerSocketOutgoingProcessor {
  core_handle: usize,
  pending_queue: Arc<TokioMutex<VecDeque<Vec<Msg>>>>,
  load_balancer: Arc<LoadBalancer>,
  core_accessor: Arc<SocketCore>,
  queue_activity_notifier: Arc<Notify>,
  peer_availability_notifier: Arc<Notify>,
  stop_signal: Arc<Notify>,
  pipe_send_coordinator: Arc<WritePipeCoordinator>,
}

impl DealerSocketOutgoingProcessor {
  pub async fn run(self) {
    tracing::debug!(
      "[DealerProc {}] Outgoing queue processor task started.",
      self.core_handle
    );

    loop {
      let mut current_message_to_send_option: Option<Vec<Msg>> = None;

      tokio::select! {
        biased;
        _ = self.stop_signal.notified() => {
          tracing::debug!("[DealerProc {}] Stop signal received. Exiting processor loop.", self.core_handle);
          break;
        }
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
          let mut queue_guard = self.pending_queue.lock().await;
          if !queue_guard.is_empty() && self.load_balancer.has_connections() {
            current_message_to_send_option = queue_guard.pop_front();
          }
          // queue_guard dropped
        }
      }

      if let Some(current_message_parts) = current_message_to_send_option {
        tracing::trace!(
          "[DealerProc {}] Processing message from outgoing queue ({} parts).",
          self.core_handle,
          current_message_parts.len()
        );

        let options_arc = self.core_accessor.core_state.read().options.clone();
        let snd_timeout_opt: Option<Duration> = options_arc.sndtimeo;

        let mut message_successfully_sent_to_a_peer = false;
        let mut attempts_this_message_cycle = 0;
        let max_attempts = self.load_balancer.connection_count().await.max(1);

        'peer_send_loop: while !message_successfully_sent_to_a_peer && attempts_this_message_cycle < max_attempts {
          if !self.core_accessor.is_running().await {
            tracing::warn!(
              "[DealerProc {}] Socket closing, stopping send attempts for current message from queue.",
              self.core_handle
            );
            // Re-queue the message if the socket is closing to ensure it's not lost if the processor restarts (though it won't here)
            // Or, just let it be dropped with the processor. For now, re-queue is safer if the pattern logic were different.
            self
              .pending_queue
              .lock()
              .await
              .push_front(current_message_parts.clone()); // Clone as original is consumed by loop if not sent
            self.queue_activity_notifier.notify_one();
            break 'peer_send_loop; // Exit attempt loop for this message
          }
          attempts_this_message_cycle += 1;

          if let Some(endpoint_uri) = self.load_balancer.get_next_connection_uri() {
            let (connection_iface_opt, connection_id_for_coord_opt) = {
              let core_s = self.core_accessor.core_state.read();
              core_s.endpoints.get(&endpoint_uri).map_or((None, None), |ep_info| {
                (Some(ep_info.connection_iface.clone()), Some(ep_info.handle_id))
              })
            };

            if let (Some(conn_iface), Some(conn_id_for_coord)) = (connection_iface_opt, connection_id_for_coord_opt) {
              match self
                .pipe_send_coordinator
                .acquire_send_permit(conn_id_for_coord, snd_timeout_opt)
                .await
              {
                Ok(_send_permit) => {
                  let mut part_send_failed_for_this_peer = false;
                  for (idx, msg_part) in current_message_parts.iter().cloned().enumerate() {
                    if !self.core_accessor.is_running().await {
                      tracing::warn!(
                        "[DealerProc {}] Socket closing mid-part-send to conn_id {}. Abandoning current message parts.",
                        self.core_handle,
                        conn_id_for_coord
                      );
                      // _send_permit is dropped, permit released. Message is NOT re-queued here as it's mid-send.
                      // The outer loop will break due to stop_signal if that's what caused is_running to be false.
                      part_send_failed_for_this_peer = true; // To prevent marking as successfully sent
                      message_successfully_sent_to_a_peer = true; // But stop trying for *this* message overall.
                      break; // from parts loop
                    }
                    match conn_iface.send_message(msg_part).await {
                      Ok(()) => { /* part sent successfully */ }
                      Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
                        tracing::warn!(
                          "[DealerProc {}] HWM/Timeout on part {} to conn_id {}. Trying next peer.",
                          self.core_handle,
                          idx,
                          conn_id_for_coord
                        );
                        part_send_failed_for_this_peer = true;
                        break;
                      }
                      Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::HostUnreachable(_)) => {
                        tracing::warn!(
                          "[DealerProc {}] Conn {} (URI {}) closed/unreachable for part {}: {}. Removing.",
                          self.core_handle,
                          conn_id_for_coord,
                          endpoint_uri,
                          idx,
                          e
                        );
                        self.load_balancer.remove_connection(&endpoint_uri);
                        if let Some(sem) = self.pipe_send_coordinator.remove_pipe(conn_id_for_coord).await {
                          sem.close();
                        }
                        self.peer_availability_notifier.notify_waiters();
                        part_send_failed_for_this_peer = true;
                        break;
                      }
                      Err(e) => {
                        tracing::error!(
                          "[DealerProc {}] Unexpected error on part {} to conn_id {}: {}. Re-queuing and stopping this message cycle.",
                          self.core_handle, idx, conn_id_for_coord, e
                        );
                        part_send_failed_for_this_peer = true;
                        message_successfully_sent_to_a_peer = false; // Signal to re-queue
                        attempts_this_message_cycle = max_attempts;
                        break;
                      }
                    }
                  }

                  if !part_send_failed_for_this_peer {
                    message_successfully_sent_to_a_peer = true;
                  }
                }
                Err(e) => {
                  tracing::warn!(
                    "[DealerProc {}] Failed to acquire send permit for conn_id {} (URI {}): {}. Trying next peer.",
                    self.core_handle,
                    conn_id_for_coord,
                    endpoint_uri,
                    e
                  );
                  if matches!(e, ZmqError::HostUnreachable(_)) {
                    self.load_balancer.remove_connection(&endpoint_uri);
                    // If remove_pipe was also called by coordinator, it's fine.
                    self.peer_availability_notifier.notify_waiters();
                  }
                }
              }
            } else {
              tracing::warn!(
                "[DealerProc {}] URI {} from LB was stale (no EndpointInfo). Removing and trying next peer.",
                self.core_handle,
                endpoint_uri
              );
              self.load_balancer.remove_connection(&endpoint_uri);
              self.peer_availability_notifier.notify_waiters();
            }
          } else {
            tracing::trace!(
              "[DealerProc {}] No (more) peers available from LB for this message cycle.",
              self.core_handle
            );
            break 'peer_send_loop;
          }
        } // End 'peer_send_loop

        if !message_successfully_sent_to_a_peer {
          tracing::debug!(
            "[DealerProc {}] Message could not be sent in this cycle after {} attempts. Re-queuing.",
            self.core_handle,
            attempts_this_message_cycle
          );
          // Lock is acquired, used, and guard dropped.
          self.pending_queue.lock().await.push_front(current_message_parts);
          self.queue_activity_notifier.notify_one(); // Notify because we put it back
        }
      } else {
        tracing::trace!(
          "[DealerProc {}] No message popped from queue (or conditions not met). Continuing to wait.",
          self.core_handle
        );
      }
    }
    tracing::debug!(
      "[DealerProc {}] Outgoing queue processor task finished execution.",
      self.core_handle
    );
  }
}

#[derive(Debug)]
pub(crate) struct DealerSocket {
  core: Arc<SocketCore>,
  load_balancer: Arc<LoadBalancer>,
  incoming_orchestrator: IncomingMessageOrchestrator,
  pipe_read_to_endpoint_uri: ParkingLotRwLock<HashMap<usize, String>>,
  pending_outgoing_queue: Arc<TokioMutex<VecDeque<Vec<Msg>>>>,
  outgoing_queue_activity_notifier: Arc<Notify>,
  peer_availability_notifier: Arc<Notify>,
  processor_task_handle: TokioMutex<Option<JoinHandle<()>>>,
  processor_stop_signal: Arc<Notify>,
  current_send_transaction: TokioMutex<DealerSendTransaction>,
  pipe_send_coordinator: Arc<WritePipeCoordinator>,
}

impl DealerSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let pending_queue_arc = Arc::new(TokioMutex::new(VecDeque::new()));
    let lb_arc = Arc::new(LoadBalancer::new());
    let queue_notifier_arc = Arc::new(Notify::new());
    let peer_notifier_arc = Arc::new(Notify::new());
    let stop_signal_arc = Arc::new(Notify::new());
    let pipe_send_coordinator_arc = Arc::new(WritePipeCoordinator::new());

    let processor = DealerSocketOutgoingProcessor {
      core_handle: core.handle,
      pending_queue: pending_queue_arc.clone(),
      load_balancer: lb_arc.clone(),
      core_accessor: core.clone(),
      queue_activity_notifier: queue_notifier_arc.clone(),
      peer_availability_notifier: peer_notifier_arc.clone(),
      stop_signal: stop_signal_arc.clone(),
      pipe_send_coordinator: pipe_send_coordinator_arc.clone(),
    };

    let processor_jh = tokio::spawn(processor.run());
    let orchestrator = IncomingMessageOrchestrator::new(&core);

    Self {
      core,
      load_balancer: lb_arc,
      incoming_orchestrator: orchestrator,
      pipe_read_to_endpoint_uri: ParkingLotRwLock::new(HashMap::new()),
      pending_outgoing_queue: pending_queue_arc,
      outgoing_queue_activity_notifier: queue_notifier_arc,
      peer_availability_notifier: peer_notifier_arc,
      processor_task_handle: TokioMutex::new(Some(processor_jh)),
      processor_stop_signal: stop_signal_arc,
      current_send_transaction: TokioMutex::new(DealerSendTransaction::Idle),
      pipe_send_coordinator: pipe_send_coordinator_arc,
    }
  }

  fn process_raw_message_for_dealer(
    _pipe_read_id: usize,
    mut raw_zmtp_message: Vec<Msg>,
    _ctx: (),
  ) -> Result<Vec<Msg>, ZmqError> {
    if let Some(delimiter_pos) = raw_zmtp_message.iter().position(|frame| frame.size() == 0) {
      let app_payload: Vec<Msg> = raw_zmtp_message.drain(delimiter_pos + 1..).collect();
      Ok(app_payload)
    } else {
      tracing::warn!(
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
    }
    res
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
    self.processor_stop_signal.notify_one();
    // Lock is acquired and guard is dropped within the same synchronous scope if Some.
    if let Some(handle) = self.processor_task_handle.lock().await.take() {
      if let Err(e) = tokio_timeout(Duration::from_millis(100), handle).await {
        tracing::warn!(
          "[Dealer {}] Timeout or error waiting for processor task on close: {:?}",
          self.core.handle,
          e
        );
      }
    }
    let res = delegate_to_core!(self, UserClose,);
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
        }
        DealerSendTransaction::Buffering { parts, .. } => {
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
              notifier_arc.notify_waiters();
            }
            return Err(ZmqError::ResourceLimitReached);
          }
          parts.push(msg);
        }
      }
      Ok(())
    } else {
      let (parts_to_send_app_level, notifier_opt) =
        match std::mem::replace(&mut *transaction_guard, DealerSendTransaction::Idle) {
          DealerSendTransaction::Idle => (vec![msg], None),
          DealerSendTransaction::Buffering {
            mut parts,
            completion_notifier,
          } => {
            parts.push(msg);
            (parts, Some(completion_notifier))
          }
        };
      drop(transaction_guard);

      let full_message_for_wire = self.prepare_full_multipart_send_sequence(parts_to_send_app_level);
      let result = self.send_logical_message(full_message_for_wire).await;

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
    let sndtimeo_opt: Option<Duration> = self.core.core_state.read().options.sndtimeo;

    loop {
      // Lock is acquired and released in each iteration.
      let mut transaction_guard = self.current_send_transaction.lock().await;
      match &*transaction_guard {
        DealerSendTransaction::Idle => {
          drop(transaction_guard);
          let full_message_for_wire = self.prepare_full_multipart_send_sequence(user_frames);
          return self.send_logical_message(full_message_for_wire).await;
        }
        DealerSendTransaction::Buffering {
          completion_notifier, ..
        } => {
          let notifier_clone = completion_notifier.clone();
          drop(transaction_guard);

          let closing_signal_future = async {
            if !self.core.is_running().await {
              futures::future::pending::<()>().await;
            } else {
              futures::future::pending::<()>().await;
            }
          };

          match sndtimeo_opt {
            Some(duration) if duration.is_zero() => return Err(ZmqError::ResourceLimitReached),
            Some(duration) => {
              tokio::select! {
                biased; _ = closing_signal_future => return Err(ZmqError::InvalidState("Socket is closing")),
                res = tokio_timeout(duration, notifier_clone.notified()) => {
                  if res.is_err() { return Err(ZmqError::Timeout); }
                }
              }
            }
            None => {
              tokio::select! {
                biased; _ = closing_signal_future => return Err(ZmqError::InvalidState("Socket is closing")),
                _ = notifier_clone.notified() => {}
              }
            }
          }
        }
      }
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = { self.core.core_state.read().options.rcvtimeo };
    self.incoming_orchestrator.recv_message(rcvtimeo_opt).await
  }
  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = { self.core.core_state.read().options.rcvtimeo };
    self.incoming_orchestrator.recv_logical_message(rcvtimeo_opt).await
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
        if let Some(raw_zmtp_message_vec) = self.incoming_orchestrator.accumulate_pipe_frame(pipe_read_id, msg)? {
          let dealer_ctx = ();
          match Self::process_raw_message_for_dealer(pipe_read_id, raw_zmtp_message_vec, dealer_ctx) {
            Ok(app_logical_message) => {
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
            }
          }
        }
      }
      _ => {}
    }
    Ok(())
  }

  async fn pipe_attached(&self, pipe_read_id: usize, _pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    // Lock is acquired and released within this scope.
    let (endpoint_uri_opt, connection_id_opt) = {
      let core_s_read = self.core.core_state.read();
      let uri = core_s_read.pipe_read_id_to_endpoint_uri.get(&pipe_read_id).cloned();
      let conn_id = if let Some(ref u) = uri {
        core_s_read.endpoints.get(u).map(|ep_info| ep_info.handle_id)
      } else {
        None
      };
      (uri, conn_id)
    };

    if let Some(endpoint_uri) = endpoint_uri_opt {
      tracing::debug!(
        handle = self.core.handle, pipe_read_id, uri = %endpoint_uri,
        conn_id = ?connection_id_opt, "DEALER attaching connection"
      );
      // Lock is acquired and released for this write.
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());
      self.load_balancer.add_connection(endpoint_uri.clone());

      if let Some(conn_id) = connection_id_opt {
        self.pipe_send_coordinator.add_pipe(conn_id).await;
      } else {
        tracing::warn!(
          handle = self.core.handle, pipe_read_id, uri = %endpoint_uri,
          "DEALER pipe_attached: Could not find connection_id for URI. WritePipeCoordinator not updated."
        );
      }
      self.peer_availability_notifier.notify_one();
      self.outgoing_queue_activity_notifier.notify_one();
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "DEALER pipe_attached: Could not find endpoint_uri for pipe_read_id. Cannot update internal maps."
      );
    }
  }

  async fn update_peer_identity(&self, _pipe_read_id: usize, _identity: Option<Blob>) {
    /* DEALER ignores */
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      "[Dealer {}] Detaching connection via pipe_read_id: {}",
      self.core.handle,
      pipe_read_id
    );

    // Lock is acquired and released for this write.
    let maybe_endpoint_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id);

    if let Some(endpoint_uri) = maybe_endpoint_uri {
      self.load_balancer.remove_connection(&endpoint_uri);

      let connection_id_to_remove_opt = {
        let core_s_read = self.core.core_state.read();
        core_s_read
          .endpoints
          .get(&endpoint_uri)
          .map(|ep_info| ep_info.handle_id)
      };

      if let Some(conn_id) = connection_id_to_remove_opt {
        if let Some(semaphore) = self.pipe_send_coordinator.remove_pipe(conn_id).await {
          semaphore.close();
        }
      } else {
        tracing::warn!(
          handle = self.core.handle, pipe_read_id, uri = %endpoint_uri,
          "DEALER pipe_detached: Could not find connection_id for URI to update WritePipeCoordinator."
        );
      }
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "DEALER detach: Endpoint URI not found for pipe_read_id in local map."
      );
    }
    self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
    self.peer_availability_notifier.notify_waiters();
  }
}

impl DealerSocket {
  async fn send_logical_message(&self, full_message_parts: Vec<Msg>) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing or terminated".into()));
    }
    // Lock is acquired and released for this read.
    let socket_options_arc = self.core.core_state.read().options.clone();
    let global_sndtimeo: Option<Duration> = socket_options_arc.sndtimeo;
    let global_sndhwm: usize = socket_options_arc.sndhwm.max(1);

    let mut attempts_this_message_cycle = 0;
    let max_direct_send_attempts = self.load_balancer.connection_count().await.max(1);

    'direct_send_attempt_loop: loop {
      if attempts_this_message_cycle >= max_direct_send_attempts {
        return self
          .queue_message_or_error(full_message_parts, global_sndhwm, global_sndtimeo)
          .await;
      }
      attempts_this_message_cycle += 1;

      if !self.core.is_running().await {
        return Err(ZmqError::InvalidState("Socket closing mid-send_logical_message".into()));
      }

      let endpoint_uri_to_send_to = match self.load_balancer.get_next_connection_uri() {
        Some(uri) => {
          // Lock is acquired and released for this read.
          if self.core.core_state.read().endpoints.contains_key(&uri) {
            uri
          } else {
            self.load_balancer.remove_connection(&uri);
            self.peer_availability_notifier.notify_waiters();
            continue 'direct_send_attempt_loop;
          }
        }
        None => {
          return self
            .queue_message_or_error(full_message_parts, global_sndhwm, global_sndtimeo)
            .await;
        }
      };

      let (connection_iface_arc, connection_id_for_coord) = {
        let core_s = self.core.core_state.read(); // Lock is acquired and released.
        match core_s.endpoints.get(&endpoint_uri_to_send_to) {
          Some(ep_info) => (ep_info.connection_iface.clone(), ep_info.handle_id),
          None => {
            self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
            self.peer_availability_notifier.notify_waiters();
            continue 'direct_send_attempt_loop;
          }
        }
      };

      match self
        .pipe_send_coordinator
        .acquire_send_permit(connection_id_for_coord, global_sndtimeo)
        .await
      {
        Ok(_send_permit) => {
          let mut direct_send_failed_this_peer = false;
          for (idx, msg_part) in full_message_parts.iter().cloned().enumerate() {
            if !self.core.is_running().await {
              return Err(ZmqError::InvalidState("Socket closing mid-part-send".into()));
            }

            match connection_iface_arc.send_message(msg_part).await {
              Ok(()) => { /* part sent */ }
              Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
                direct_send_failed_this_peer = true;
                break;
              }
              Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::HostUnreachable(_)) => {
                self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
                if let Some(sem) = self.pipe_send_coordinator.remove_pipe(connection_id_for_coord).await {
                  sem.close();
                }
                self.peer_availability_notifier.notify_waiters();
                direct_send_failed_this_peer = true;
                break;
              }
              Err(e) => return Err(e),
            }
          }
          if !direct_send_failed_this_peer {
            return Ok(());
          }
        }
        Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
          return self
            .queue_message_or_error(full_message_parts, global_sndhwm, global_sndtimeo)
            .await;
        }
        Err(ZmqError::HostUnreachable(_)) => {
          self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
          self.peer_availability_notifier.notify_waiters();
          // continue 'direct_send_attempt_loop to try another peer implicitly
        }
        Err(e) => return Err(e),
      }
    }
  }

  async fn queue_message_or_error(
    &self,
    full_message_parts: Vec<Msg>,
    global_sndhwm: usize,
    global_sndtimeo: Option<Duration>,
  ) -> Result<(), ZmqError> {
    tracing::trace!(
      "[Dealer {}] Attempting to queue message ({} parts).",
      self.core.handle,
      full_message_parts.len()
    );

    loop {
      if !self.core.is_running().await {
        return Err(ZmqError::InvalidState("Socket is closing while trying to queue".into()));
      }

      // Lock is acquired for len check, then potentially again for push_back.
      if self.pending_outgoing_queue.lock().await.len() < global_sndhwm {
        self.pending_outgoing_queue.lock().await.push_back(full_message_parts);
        self.outgoing_queue_activity_notifier.notify_one();
        return Ok(());
      }

      match global_sndtimeo {
        Some(duration) if duration.is_zero() => return Err(ZmqError::ResourceLimitReached),
        Some(duration) => {
          let queue_wait_fut = self.outgoing_queue_activity_notifier.notified();
          if tokio_timeout(duration, queue_wait_fut).await.is_err() {
            return Err(ZmqError::Timeout);
          }
        }
        None => {
          tokio::select! {
            biased;
             _ = async { if !self.core.is_running().await { futures::future::pending().await } else { futures::future::pending().await } } => {
                return Err(ZmqError::InvalidState("Socket is closing while waiting for queue space".into()));
            }
            _ = self.outgoing_queue_activity_notifier.notified() => {}
            _ = self.peer_availability_notifier.notified() => {} // Also listen for peer changes
          }
        }
      }
    }
  }
}

impl Drop for DealerSocket {
  fn drop(&mut self) {
    self.processor_stop_signal.notify_one();
    // Lock is acquired and released within this scope.
    if let Ok(mut guard) = self.processor_task_handle.try_lock() {
      if let Some(handle) = guard.take() {
        handle.abort();
      }
    }

    // Lock is acquired and released within this scope.
    if let Ok(mut transaction_guard) = self.current_send_transaction.try_lock() {
      if let DealerSendTransaction::Buffering {
        completion_notifier, ..
      } = std::mem::replace(&mut *transaction_guard, DealerSendTransaction::Idle)
      {
        completion_notifier.notify_waiters();
      }
    }
  }
}
