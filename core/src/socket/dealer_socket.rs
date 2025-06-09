use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::SocketCore;
use crate::socket::patterns::incoming_orchestrator::IncomingMessageOrchestrator;
use crate::socket::patterns::LoadBalancer;
use crate::socket::ISocket;
use super::patterns::WritePipeCoordinator;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock as ParkingLotRwLock;
use tokio::sync::{Mutex as TokioMutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;


const MAX_DEALER_SEND_BUFFER_PARTS: usize = 10240;

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
        }
      }

      if let Some(zmtp_frames_for_logical_message) = current_message_to_send_option {
        tracing::trace!(
          "[DealerProc {}] Processing message from outgoing queue ({} parts).",
          self.core_handle,
          zmtp_frames_for_logical_message.len()
        );

        let snd_timeout_opt: Option<Duration> = { self.core_accessor.core_state.read().options.sndtimeo };

        let mut message_successfully_sent_to_a_peer = false;
        let mut attempts_this_message_cycle = 0;
        let max_attempts = self.load_balancer.connection_count().await.max(1);

        'peer_send_loop: while !message_successfully_sent_to_a_peer && attempts_this_message_cycle < max_attempts {
          if !self.core_accessor.is_running().await {
            tracing::warn!(
              "[DealerProc {}] Socket closing, stopping send attempts for current message from queue.",
              self.core_handle
            );
            self
              .pending_queue
              .lock()
              .await
              .push_front(zmtp_frames_for_logical_message.clone());
            self.queue_activity_notifier.notify_one();
            break 'peer_send_loop;
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
                Ok(_send_permit) => match conn_iface.send_multipart(zmtp_frames_for_logical_message.clone()).await {
                  Ok(()) => {
                    message_successfully_sent_to_a_peer = true;
                  }
                  Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
                    tracing::warn!(
                      "[DealerProc {}] HWM/Timeout on send_multipart to conn_id {}. Trying next peer.",
                      self.core_handle,
                      conn_id_for_coord
                    );
                  }
                  Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::HostUnreachable(_)) => {
                    tracing::warn!(
                      "[DealerProc {}] Conn {} (URI {}) closed/unreachable for send_multipart: {}. Removing.",
                      self.core_handle,
                      conn_id_for_coord,
                      endpoint_uri,
                      e
                    );
                    self.load_balancer.remove_connection(&endpoint_uri);
                    self.pipe_send_coordinator.remove_pipe(conn_id_for_coord).await;
                    self.peer_availability_notifier.notify_waiters();
                  }
                  Err(e) => {
                    tracing::error!(
                        "[DealerProc {}] Unexpected error on send_multipart to conn_id {}: {}. Re-queuing and stopping this message cycle.",
                        self.core_handle, conn_id_for_coord, e
                      );
                    message_successfully_sent_to_a_peer = false;
                    attempts_this_message_cycle = max_attempts;
                  }
                },
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
        }

        if !message_successfully_sent_to_a_peer {
          tracing::debug!(
            "[DealerProc {}] Message could not be sent in this cycle after {} attempts. Re-queuing.",
            self.core_handle,
            attempts_this_message_cycle
          );
          self
            .pending_queue
            .lock()
            .await
            .push_front(zmtp_frames_for_logical_message);
          self.queue_activity_notifier.notify_one();
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
  incoming_orchestrator: IncomingMessageOrchestrator<Vec<Msg>>,
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
    let rcvhwm = { core.core_state.read().options.rcvhwm };
    let orchestrator = IncomingMessageOrchestrator::new(core.handle, rcvhwm);

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

  fn process_incoming_zmtp_message_for_dealer(
    &self,
    pipe_read_id: usize,
    mut frames: Vec<Msg>,
  ) -> Result<Vec<Msg>, ZmqError> {
    tracing::trace!(
      handle = self.core.handle,
      pipe_id = pipe_read_id,
      num_raw_frames = frames.len(),
      "Dealer processing incoming ZMTP message"
    );

    if frames.is_empty() {
      tracing::warn!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "Dealer received empty ZMTP message. Returning as empty payload."
      );
      return Ok(frames);
    }

    // A DEALER socket, when receiving from a ROUTER, expects the ZMTP message to be:
    // [identity_of_this_dealer_M, empty_delimiter_M, app_payload_frames...]
    // The DEALER application should only see the app_payload_frames.
    //
    // If receiving from another DEALER, the ZMTP message is:
    // [empty_delimiter_M, app_payload_frames...]
    // The DEALER application should see app_payload_frames.

    // We need to determine if the first frame is an identity (non-empty) or a delimiter (empty).
    // This is inherently ambiguous for a DEALER as it doesn't know its peer type.
    // However, the ZMQ spec implies that the DEALER socket itself handles stripping
    // its own identity if present, and always strips the first empty delimiter before payload.

    // For rzmq, the 'identity_of_this_dealer' is a ZMTP routing detail.
    // The ISocketConnection would have received it. If it's part of the `frames` here,
    // it means it was part of the ZMTP message on the wire for this specific connection.
    // Let's assume if the first frame is NOT empty, it's an identity from a ROUTER and should be stripped.
    if frames[0].size() != 0 {
      // Assume it's an identity frame sent by a ROUTER, discard it.
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "Dealer: Discarding first non-empty frame (assumed identity from ROUTER)."
      );
      frames.remove(0);
      // After discarding the identity, the next frame *must* be an empty delimiter.
      if frames.is_empty() || frames[0].size() != 0 {
        tracing::warn!(handle = self.core.handle, pipe_id = pipe_read_id, "Dealer: Expected empty delimiter after presumed identity frame, but not found or no frames left. Message might be malformed from peer ROUTER.");
        // Return what's left, which might be an error for the app or an unexpected payload.
        return Ok(frames);
      }
      // Now frames[0] is the empty delimiter, discard it.
      frames.remove(0);
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "Dealer: Stripped empty delimiter after presumed identity."
      );
    }
    // If the first frame *was* empty (or became empty after stripping identity and the check above)
    else if !frames.is_empty() && frames[0].size() == 0 {
      // This handles the case of DEALER-to-DEALER (first frame is delimiter)
      // OR the delimiter that followed an identity from a ROUTER (already handled by the `else if` above if identity was present).
      // So, this specifically catches the DEALER-DEALER case now.
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "Dealer: Stripped leading empty ZMTP delimiter (likely from peer DEALER)."
      );
      frames.remove(0);
    }
    // `frames` now contains only the application payload parts.
    Ok(frames)
  }

  fn prepare_full_multipart_send_sequence(&self, user_frames: Vec<Msg>) -> Vec<Msg> {
    if user_frames.is_empty() {
      let mut delimiter = Msg::new();
      delimiter.set_flags(MsgFlags::MORE);
      let mut last_empty = Msg::new();
      last_empty.set_flags(last_empty.flags() & !MsgFlags::MORE);
      return vec![delimiter, last_empty];
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
    delegate_to_core!(self, UserClose,)
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
    
    let sndtimeo_opt = { self.core.core_state.read().options.sndtimeo };

    loop {
      
      let mut transaction_guard = self.current_send_transaction.lock().await;
      match &*transaction_guard {
        DealerSendTransaction::Idle => {
          drop(transaction_guard);
          
          let zmtp_wire_frames = self.prepare_full_multipart_send_sequence(user_frames);
          let res = self.send_logical_message(zmtp_wire_frames).await;

          
      return res;
        }
        DealerSendTransaction::Buffering {
          completion_notifier, ..
        } => {
          
          let notifier_clone = completion_notifier.clone();
          drop(transaction_guard);

          let closing_signal_future = async {
            loop {
              if !self.core.is_running().await {
                break;
              }
              tokio::time::sleep(Duration::from_millis(50)).await;
            }
          };
          
          match sndtimeo_opt {
            Some(duration) if duration.is_zero() => return Err(ZmqError::ResourceLimitReached),
            Some(duration) => {
              tokio::select! {
                biased;
                _ = closing_signal_future => return Err(ZmqError::InvalidState("Socket is closing while waiting for prior send tx".into())),
                res = tokio_timeout(duration, notifier_clone.notified()) => {
                  
                  if res.is_err() { return Err(ZmqError::Timeout); }
                }
              }
            }
            None => {
              tokio::select! {
                biased;
                _ = closing_signal_future => return Err(ZmqError::InvalidState("Socket is closing while waiting for prior send tx".into())),
                _ = notifier_clone.notified() => { }
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
    // For DEALER, QItem is Vec<Msg> (payload parts).
    // The transform_fn tells orchestrator.recv_message how to convert QItem into app_frames.
    // For DEALER, the QItem (payload Vec<Msg>) IS the app_frames for the orchestrator's buffer.
    let transform_fn = |q_item: Vec<Msg>| q_item;
    self
      .incoming_orchestrator
      .recv_message(rcvtimeo_opt, transform_fn)
      .await
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let rcvtimeo_opt: Option<Duration> = { self.core.core_state.read().options.rcvtimeo };
    // For DEALER, QItem is Vec<Msg> (the payload parts). Transform is identity.
    let transform_fn = |q_item: Vec<Msg>| q_item;
    self
      .incoming_orchestrator
      .recv_logical_message(rcvtimeo_opt, transform_fn)
      .await
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn process_command(&self, command: Command) -> Result<bool, ZmqError> {

    match command {
      Command::Stop => {
        self.incoming_orchestrator.close().await;
        self.processor_stop_signal.notify_one();
        if let Some(handle) = self.processor_task_handle.lock().await.take() {
          if let Err(e) = tokio_timeout(Duration::from_millis(100), handle).await {
            tracing::warn!(
              "[Dealer {}] Timeout or error waiting for processor task on close: {:?}",
              self.core.handle,
              e
            );
          }
        }
        self.outgoing_queue_activity_notifier.notify_waiters();
        self.peer_availability_notifier.notify_waiters();
      }
      _ => return Ok(false),
    }
    
    Ok(true)
  }

  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        if let Some(raw_zmtp_message_vec) = self.incoming_orchestrator.accumulate_pipe_frame(pipe_read_id, msg)? {
          match self.process_incoming_zmtp_message_for_dealer(pipe_read_id, raw_zmtp_message_vec) {
            Ok(payload_parts_vec) => {
              self
                .incoming_orchestrator
                .queue_item(pipe_read_id, payload_parts_vec)
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
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, conn_id = ?connection_id_opt, "DEALER attaching connection");
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());
      self.load_balancer.add_connection(endpoint_uri.clone());

      if let Some(conn_id) = connection_id_opt {
        self.pipe_send_coordinator.add_pipe(conn_id).await;
      } else {
        tracing::warn!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "DEALER pipe_attached: Could not find connection_id for URI. WritePipeCoordinator not updated.");
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
    /* DEALER ignores ZMTP identity from peer for its core logic */
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      "[Dealer {}] Detaching connection via pipe_read_id: {}",
      self.core.handle,
      pipe_read_id
    );
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
        self.pipe_send_coordinator.remove_pipe(conn_id).await;
      }
    }
    self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
    self.peer_availability_notifier.notify_waiters();
  }
}

impl DealerSocket {
  async fn send_logical_message(&self, zmtp_wire_frames: Vec<Msg>) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing or terminated".into()));
    }
    let (global_sndtimeo, global_sndhwm) = {
      let core_s_read = self.core.core_state.read();
      (core_s_read.options.sndtimeo, core_s_read.options.sndhwm.max(1))
    };

    let mut attempts_this_message_cycle = 0;
    let max_direct_send_attempts = self.load_balancer.connection_count().await.max(1);

    'direct_send_attempt_loop: loop {
      if attempts_this_message_cycle >= max_direct_send_attempts {
        return self
          .queue_message_or_error(zmtp_wire_frames, global_sndhwm, global_sndtimeo)
          .await;
      }
      attempts_this_message_cycle += 1;

      if !self.core.is_running().await {
        return Err(ZmqError::InvalidState("Socket closing mid-send_logical_message".into()));
      }

      let endpoint_uri_to_send_to = match self.load_balancer.get_next_connection_uri() {
        Some(uri) => {
          let uri_exists = { self.core.core_state.read().endpoints.contains_key(&uri) };
          if uri_exists {
            uri
          } else {
            self.load_balancer.remove_connection(&uri);
            self.peer_availability_notifier.notify_waiters();
            continue 'direct_send_attempt_loop;
          }
        }
        None => {
          return self
            .queue_message_or_error(zmtp_wire_frames, global_sndhwm, global_sndtimeo)
            .await;
        }
      };

      let (connection_iface_arc, connection_id_for_coord) = {
        let core_s = self.core.core_state.read();
        match core_s.endpoints.get(&endpoint_uri_to_send_to) {
          Some(ep_info) => (ep_info.connection_iface.clone(), ep_info.handle_id),
          None => {
            drop(core_s);
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
        Ok(_send_permit) => match connection_iface_arc.send_multipart(zmtp_wire_frames.clone()).await {
          Ok(()) => {
            return Ok(());
          }
          Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
            tracing::warn!(
              "[Dealer {}] HWM/Timeout on send_multipart to conn_id {}. Trying next peer.",
              self.core.handle,
              connection_id_for_coord
            );
            continue 'direct_send_attempt_loop;
          }
          Err(e @ ZmqError::ConnectionClosed) | Err(e @ ZmqError::HostUnreachable(_)) => {
            self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
            self.pipe_send_coordinator.remove_pipe(connection_id_for_coord).await;
            self.peer_availability_notifier.notify_waiters();
            tracing::warn!(
              "[Dealer {}] Conn {} (URI {}) closed/unreachable for send_multipart: {}. Removed.",
              self.core.handle,
              connection_id_for_coord,
              endpoint_uri_to_send_to,
              e
            );
            continue 'direct_send_attempt_loop;
          }
          Err(e) => {
            return Err(e);
          }
        },
        Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
          return self
            .queue_message_or_error(zmtp_wire_frames, global_sndhwm, global_sndtimeo)
            .await;
        }
        Err(ZmqError::HostUnreachable(_)) => {
          self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
          self.peer_availability_notifier.notify_waiters();
          continue 'direct_send_attempt_loop;
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
            _ = self.peer_availability_notifier.notified() => {}
          }
        }
      }
    }
  }
}

impl Drop for DealerSocket {
  fn drop(&mut self) {
    self.processor_stop_signal.notify_one();
    if let Ok(mut guard) = self.processor_task_handle.try_lock() {
      if let Some(handle) = guard.take() {
        handle.abort();
      }
    }
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
