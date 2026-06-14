use super::patterns::OutgoingMessageOrchestrator;
use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, FrameBatch, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::options::AUTO_DELIMITER;
use crate::socket::patterns::incoming_orchestrator::IncomingMessageOrchestrator;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::socket::patterns::{FramingLatch, dealer_auto_decode, dealer_auto_encode};

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{Mutex as ParkingMutex, RwLock as ParkingLotRwLock};
use tokio::sync::{Mutex as TokioMutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;

use super::parse_bool_option;

const MAX_DEALER_SEND_BUFFER_PARTS: usize = 10240;

#[derive(Debug)]
enum DealerSendTransaction {
  Idle,
  Buffering {
    parts: FrameBatch,
    completion_notifier: Arc<Notify>,
  },
}

#[derive(Debug)]
struct DealerSocketOutgoingProcessor {
  core_handle: usize,
  pending_queue: Arc<TokioMutex<VecDeque<FrameBatch>>>,
  outgoing_orchestrator: Arc<OutgoingMessageOrchestrator>,
  queue_activity_notifier: Arc<Notify>,
  peer_availability_notifier: Arc<Notify>,
  stop_signal: Arc<Notify>,
}

impl DealerSocketOutgoingProcessor {
  pub async fn run(self) {
    tracing::debug!(
      "[DealerProc {}] Outgoing queue processor task started.",
      self.core_handle
    );

    loop {
      let mut current_message_to_send_option: Option<FrameBatch> = None;

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
          if !queue_guard.is_empty() && self.outgoing_orchestrator.has_connections() {
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

        match self.outgoing_orchestrator.route_message(zmtp_frames_for_logical_message, false).await {
          Ok(()) => {}
          Err((returned, _)) => {
            tracing::debug!(
              "[DealerProc {}] route_message failed (all peers full or no peers). Re-queuing.",
              self.core_handle
            );
            self.pending_queue.lock().await.push_front(returned);
            self.queue_activity_notifier.notify_one();
          }
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
  outgoing_orchestrator: Arc<OutgoingMessageOrchestrator>,
  incoming_orchestrator: IncomingMessageOrchestrator,
  pending_pipe_senders: ParkingMutex<HashMap<usize, PipeMessageSender>>,
  frame_recv_buffer: ParkingMutex<Option<VecDeque<Msg>>>,
  pipe_read_to_endpoint_uri: ParkingLotRwLock<HashMap<usize, String>>,
  pending_outgoing_queue: Arc<TokioMutex<VecDeque<FrameBatch>>>,
  outgoing_queue_activity_notifier: Arc<Notify>,
  peer_availability_notifier: Arc<Notify>,
  processor_task_handle: TokioMutex<Option<JoinHandle<()>>>,
  processor_stop_signal: Arc<Notify>,
  current_send_transaction: TokioMutex<DealerSendTransaction>,
  framing: FramingLatch,
}

impl DealerSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let pending_queue_arc = Arc::new(TokioMutex::new(VecDeque::new()));
    let orchestrator_arc = Arc::new(OutgoingMessageOrchestrator::new());
    let queue_notifier_arc = Arc::new(Notify::new());
    let peer_notifier_arc = Arc::new(Notify::new());
    let stop_signal_arc = Arc::new(Notify::new());

    let processor = DealerSocketOutgoingProcessor {
      core_handle: core.handle,
      pending_queue: pending_queue_arc.clone(),
      outgoing_orchestrator: orchestrator_arc.clone(),
      queue_activity_notifier: queue_notifier_arc.clone(),
      peer_availability_notifier: peer_notifier_arc.clone(),
      stop_signal: stop_signal_arc.clone(),
    };

    let processor_jh = tokio::spawn(processor.run());
    let incoming_orchestrator = IncomingMessageOrchestrator::new(core.handle);

    Self {
      core,
      outgoing_orchestrator: orchestrator_arc,
      incoming_orchestrator,
      pending_pipe_senders: ParkingMutex::new(HashMap::new()),
      frame_recv_buffer: ParkingMutex::new(None),
      pipe_read_to_endpoint_uri: ParkingLotRwLock::new(HashMap::new()),
      pending_outgoing_queue: pending_queue_arc,
      outgoing_queue_activity_notifier: queue_notifier_arc,
      peer_availability_notifier: peer_notifier_arc,
      processor_task_handle: TokioMutex::new(Some(processor_jh)),
      processor_stop_signal: stop_signal_arc,
      current_send_transaction: TokioMutex::new(DealerSendTransaction::Idle),
      framing: FramingLatch::new(dealer_auto_encode, dealer_auto_decode),
    }
  }

  fn process_incoming_zmtp_message_for_dealer(
    &self,
    pipe_read_id: usize,
    mut frames: FrameBatch,
  ) -> Result<FrameBatch, ZmqError> {
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

    if self.framing.is_manual() {
      return Ok(frames);
    }

    if frames[0].size() != 0 {
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "Dealer: Discarding first non-empty frame (assumed identity from ROUTER)."
      );
      frames.remove(0);
      if frames.is_empty() || frames[0].size() != 0 {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_read_id,
          "Dealer: Expected empty delimiter after presumed identity frame, but not found."
        );
        return Ok(frames);
      }
      frames.remove(0);
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "Dealer: Stripped empty delimiter after presumed identity."
      );
    } else if !frames.is_empty() && frames[0].size() == 0 {
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "Dealer: Stripped leading empty ZMTP delimiter (likely from peer DEALER)."
      );
      frames.remove(0);
    }
    Ok(frames)
  }

  fn prepare_full_multipart_send_sequence(&self, mut frames: FrameBatch) -> FrameBatch {
    if self.framing.is_manual() {
      let len = frames.len();
      for (i, frame) in frames.iter_mut().enumerate() {
        if i < len - 1 {
          frame.set_flags(frame.flags() | MsgFlags::MORE);
        } else {
          frame.set_flags(frame.flags() & !MsgFlags::MORE);
        }
      }
      return frames;
    }

    if frames.is_empty() {
      let mut delimiter = Msg::new();
      delimiter.set_flags(MsgFlags::MORE);
      let mut last_empty = Msg::new();
      last_empty.set_flags(last_empty.flags() & !MsgFlags::MORE);
      let mut fb = FrameBatch::new();
      fb.push(delimiter);
      fb.push(last_empty);
      return fb;
    }

    self.framing.encode(&mut frames);

    let len = frames.len();
    for (i, frame) in frames.iter_mut().enumerate() {
      if i < len - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
    }
    frames
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
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let mut transaction_guard = self.current_send_transaction.lock().await;

    if msg.is_more() {
      match &mut *transaction_guard {
        DealerSendTransaction::Idle => {
          let notifier = Arc::new(Notify::new());
          let mut fb = FrameBatch::new();
          fb.push(msg);
          *transaction_guard = DealerSendTransaction::Buffering {
            parts: fb,
            completion_notifier: notifier,
          };
        }
        DealerSendTransaction::Buffering { parts, .. } => {
          if parts.len() >= MAX_DEALER_SEND_BUFFER_PARTS {
            let notifier_to_signal =
              match std::mem::replace(&mut *transaction_guard, DealerSendTransaction::Idle) {
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
          DealerSendTransaction::Idle => {
            let mut fb = FrameBatch::new();
            fb.push(msg);
            (fb, None)
          }
          DealerSendTransaction::Buffering {
            mut parts,
            completion_notifier,
          } => {
            parts.push(msg);
            (parts, Some(completion_notifier))
          }
        };
      drop(transaction_guard);

      let full_message_for_wire =
        self.prepare_full_multipart_send_sequence(parts_to_send_app_level);
      let result = self.send_logical_message(full_message_for_wire).await;

      if let Some(notifier) = notifier_opt {
        notifier.notify_waiters();
      }
      result
    }
  }

  async fn send_multipart(&self, user_frames: FrameBatch) -> Result<(), ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let sndtimeo_opt = { self.core.core_state.read().options.sndtimeo };

    loop {
      let transaction_guard = self.current_send_transaction.lock().await;
      match &*transaction_guard {
        DealerSendTransaction::Idle => {
          drop(transaction_guard);
          let zmtp_wire_frames = self.prepare_full_multipart_send_sequence(user_frames);
          return self.send_logical_message(zmtp_wire_frames).await;
        }
        DealerSendTransaction::Buffering {
          completion_notifier,
          ..
        } => {
          let notifier_clone = completion_notifier.clone();
          drop(transaction_guard);

          let closing_signal_future = async {
            loop {
              if !self.core.is_running() {
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
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    {
      let mut guard = self.frame_recv_buffer.lock();
      if let Some(ref mut frames) = *guard {
        if let Some(frame) = frames.pop_front() {
          if frames.is_empty() {
            *guard = None;
          }
          return Ok(frame);
        }
        *guard = None;
      }
    }
    let rcvtimeo_opt: Option<Duration> = self.core.core_state.read().options.rcvtimeo;
    let (_, batch) = self.incoming_orchestrator.recv_logical_message(rcvtimeo_opt).await?;
    let mut stripped = self.process_incoming_zmtp_message_for_dealer(0, batch)?;
    if stripped.is_empty() {
      return Ok(Msg::new());
    }
    if stripped.len() == 1 {
      return Ok(stripped.remove(0));
    }
    let mut deque: VecDeque<Msg> = stripped.into_iter().collect();
    let first = deque.pop_front().unwrap();
    *self.frame_recv_buffer.lock() = Some(deque);
    Ok(first)
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = self.core.core_state.read().options.rcvtimeo;
    let (_, batch) = self.incoming_orchestrator.recv_logical_message(rcvtimeo_opt).await?;
    self.process_incoming_zmtp_message_for_dealer(0, batch)
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    if option == AUTO_DELIMITER {
      if !parse_bool_option(_value)? {
        self.framing.set_manual();
      }
      Ok(())
    } else {
      Err(ZmqError::UnsupportedOption(option))
    }
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    if option == AUTO_DELIMITER {
      Ok((!self.framing.is_manual() as i32).to_ne_bytes().to_vec())
    } else {
      Err(ZmqError::UnsupportedOption(option))
    }
  }

  async fn process_command(&self, command: Command) -> Result<bool, ZmqError> {
    match command {
      Command::Stop => {
        self.incoming_orchestrator.close().await;
        self.outgoing_orchestrator.deactivate();
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

  async fn handle_pipe_event(&self, _pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { .. } | Command::PipeMessageBatchReceived { .. } => {}
      _ => {}
    }
    Ok(())
  }

  fn get_incoming_pipe_sender(&self, pipe_read_id: usize) -> Option<PipeMessageSender> {
    self.pending_pipe_senders.lock().remove(&pipe_read_id)
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize,
    _peer_identity: Option<&[u8]>,
  ) {
    let (endpoint_uri_opt, connection_iface_opt) = {
      let core_s = self.core.core_state.read();
      let uri = core_s.pipe_read_id_to_endpoint_uri.get(&pipe_read_id).cloned();
      let iface = uri.as_ref().and_then(|u| {
        core_s.endpoints.get(u).map(|ep| ep.connection_iface.clone())
      });
      (uri, iface)
    };

    if let (Some(endpoint_uri), Some(iface)) = (endpoint_uri_opt, connection_iface_opt) {
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "DEALER attaching connection");
      self.pipe_read_to_endpoint_uri.write().insert(pipe_read_id, endpoint_uri.clone());
      self.outgoing_orchestrator.add_connection(endpoint_uri.clone(), iface);

      let rcvhwm = self.core.core_state.read().options.rcvhwm.max(1);
      let raw_sender = self.incoming_orchestrator.register_connection_pipe(pipe_read_id, rcvhwm);
      self.pending_pipe_senders.lock().insert(pipe_read_id, PipeMessageSender::Direct(raw_sender));

      self.peer_availability_notifier.notify_one();
      self.outgoing_queue_activity_notifier.notify_one();
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "DEALER pipe_attached: Could not find endpoint_uri or connection_iface."
      );
    }
  }

  async fn update_peer_identity(&self, _pipe_read_id: usize, _identity: Option<Blob>) {
    /* DEALER ignores ZMTP identity from peer */
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      "[Dealer {}] Detaching connection via pipe_read_id: {}",
      self.core.handle,
      pipe_read_id
    );
    if let Some(endpoint_uri) = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id) {
      self.outgoing_orchestrator.remove_connection(&endpoint_uri);
    }

    self.incoming_orchestrator.deregister_connection_pipe(pipe_read_id);
    self.pending_pipe_senders.lock().remove(&pipe_read_id);
    self.peer_availability_notifier.notify_waiters();
  }
}

impl DealerSocket {
  async fn send_logical_message(&self, zmtp_wire_frames: FrameBatch) -> Result<(), ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState(
        "Socket is closing or terminated".into(),
      ));
    }
    let (global_sndtimeo, global_sndhwm) = {
      let core_s_read = self.core.core_state.read();
      (
        core_s_read.options.sndtimeo,
        core_s_read.options.sndhwm.max(1),
      )
    };

    match self.outgoing_orchestrator.route_message(zmtp_wire_frames, false).await {
      Ok(()) => Ok(()),
      Err((returned, _)) => {
        self.queue_message_or_error(returned, global_sndhwm, global_sndtimeo).await
      }
    }
  }

  async fn queue_message_or_error(
    &self,
    full_message_parts: FrameBatch,
    global_sndhwm: usize,
    global_sndtimeo: Option<Duration>,
  ) -> Result<(), ZmqError> {
    tracing::trace!(
      "[Dealer {}] Attempting to queue message ({} parts).",
      self.core.handle,
      full_message_parts.len()
    );
    loop {
      if !self.core.is_running() {
        return Err(ZmqError::InvalidState(
          "Socket is closing while trying to queue".into(),
        ));
      }
      {
        let mut queue_guard = self.pending_outgoing_queue.lock().await;
        if queue_guard.len() < global_sndhwm {
          queue_guard.push_back(full_message_parts);
          self.outgoing_queue_activity_notifier.notify_one();
          return Ok(());
        }
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
            _ = async { if !self.core.is_running() { futures::future::pending().await } else { futures::future::pending().await } } => {
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
        completion_notifier,
        ..
      } = std::mem::replace(&mut *transaction_guard, DealerSendTransaction::Idle)
      {
        completion_notifier.notify_waiters();
      }
    }
  }
}
