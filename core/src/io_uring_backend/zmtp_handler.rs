#![cfg(feature = "io-uring")]

use std::any::Any;
use std::collections::VecDeque;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, error, info, trace, warn};

use crate::ZmqError;
use crate::io_uring_backend::ZC_SEND_THRESHOLD;
use crate::io_uring_backend::{
  buffer_manager::BufferRingManager,
  connection_handler::{
    HandlerIoOps, HandlerSqeBlueprint, UringConnectionHandler, UringWorkerInterface, WorkerIoConfig,
  },
  ops::{HANDLER_INTERNAL_SEND_OP_UD, UserData, WAKEUP_STATE_SIGNALED, WAKEUP_STATE_SLEEPING},
  worker::{InternalOpTracker, MultishotReader},
};
use crate::message::FrameBatch;
use crate::protocol::zmtp::{
  actions::{AppAction, EngineOutput, NetAction},
  engine::{ZmtpEngine, ZmtpPhase},
};
use crate::runtime::Command;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;

// Low values - The worker becomes less willing to wait. The moment it accumulates 4 or 8 messages, it stops spinning, exits the loop, and commits the write.
// High values - The worker becomes highly aggressive about building large batches. It will continue to execute micro-spins until it reaches the larger target size.
const MICRO_COALESCE_THRESHOLD: usize = 16;

pub(crate) struct ZmtpSmartConnection {
  fd: RawFd,
  egress_tx: fibre::mpsc::BoundedAsyncSender<FrameBatch>,
  event_fd: eventfd::EventFD,
  worker_asleep: Arc<AtomicU8>,
  sndtimeo: Option<Duration>,
}

impl std::fmt::Debug for ZmtpSmartConnection {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ZmtpSmartConnection")
      .field("fd", &self.fd)
      .field("egress_tx_is_closed", &self.egress_tx.is_closed())
      .finish()
  }
}

impl ZmtpSmartConnection {
  pub(crate) fn new(
    fd: RawFd,
    egress_tx: fibre::mpsc::BoundedAsyncSender<FrameBatch>,
    event_fd: eventfd::EventFD,
    worker_asleep: Arc<AtomicU8>,
    sndtimeo: Option<Duration>,
  ) -> Self {
    Self {
      fd,
      egress_tx,
      event_fd,
      worker_asleep,
      sndtimeo,
    }
  }

  fn signal_worker(&self) {
    if self.worker_asleep.load(Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
      if self
        .worker_asleep
        .compare_exchange(
          WAKEUP_STATE_SLEEPING,
          WAKEUP_STATE_SIGNALED,
          Ordering::AcqRel,
          Ordering::Acquire,
        )
        .is_ok()
      {
        if let Err(e) = self.event_fd.write(1) {
          error!(
            "ZmtpSmartConnection fd={}: failed to signal worker eventfd: {}",
            self.fd, e
          );
        }
      }
    }
  }
}

#[async_trait]
impl ISocketConnection for ZmtpSmartConnection {
  async fn send_multipart(&self, msgs: FrameBatch) -> Result<(), ZmqError> {
    match self.egress_tx.try_send(msgs) {
      Ok(()) => {
        self.signal_worker();
        Ok(())
      }
      Err(fibre::TrySendError::Closed(_)) => Err(ZmqError::ConnectionClosed),
      Err(fibre::TrySendError::Full(_)) if self.sndtimeo == Some(Duration::ZERO) => {
        Err(ZmqError::ResourceLimitReached)
      }
      Err(fibre::TrySendError::Full(returned_msgs)) => {
        let timeout_duration = self.sndtimeo.unwrap_or(Duration::from_secs(30));
        match tokio::time::timeout(timeout_duration, self.egress_tx.send(returned_msgs)).await {
          Ok(Ok(())) => {
            self.signal_worker();
            Ok(())
          }
          Ok(Err(_)) => Err(ZmqError::ConnectionClosed),
          Err(_) => Err(ZmqError::ResourceLimitReached),
        }
      }
      _ => unreachable!(),
    }
  }

  async fn send_multipart_owned(&self, msgs: FrameBatch) -> Result<(), (FrameBatch, ZmqError)> {
    match self.egress_tx.try_send(msgs) {
      Ok(()) => {
        self.signal_worker();
        Ok(())
      }
      Err(fibre::TrySendError::Closed(returned)) => Err((returned, ZmqError::ConnectionClosed)),
      Err(fibre::TrySendError::Full(returned)) if self.sndtimeo == Some(Duration::ZERO) => {
        Err((returned, ZmqError::ResourceLimitReached))
      }
      Err(fibre::TrySendError::Full(returned)) => {
        let timeout_duration = self.sndtimeo.unwrap_or(Duration::from_secs(30));
        match tokio::time::timeout(timeout_duration, self.egress_tx.send(returned)).await {
          Ok(Ok(())) => {
            self.signal_worker();
            Ok(())
          }
          Ok(Err(_)) => Err((FrameBatch::new(), ZmqError::ConnectionClosed)),
          Err(_) => Err((FrameBatch::new(), ZmqError::Timeout)),
        }
      }
      _ => unreachable!(),
    }
  }

  async fn close_connection(&self) -> Result<(), ZmqError> {
    Ok(())
  }

  fn as_any(&self) -> &dyn Any {
    self
  }
}

/// Protocol-aware io_uring connection handler.
///
/// Owns a `ZmtpEngine` and drives all handshake and data-phase framing directly
/// on the `UringWorker` OS thread. No `UringStream` or `SessionConnectionActorX`
/// is involved.
pub(crate) struct ZmtpUringHandler {
  fd: RawFd,
  worker_io_config: Arc<WorkerIoConfig>,
  engine: ZmtpEngine,
  /// Egress channel receiver: SocketCore → handler (via ZmtpSmartConnection → egress_tx).
  egress_rx: Arc<fibre::mpsc::BoundedReceiver<FrameBatch>>,
  /// Direct ingress sender: delivers decoded messages straight to the socket's ReadyPipeQueue.
  /// Set by `attach_ingress` after SocketCore completes pipe registration.
  ingress_sender: Option<PipeMessageSender>,
  /// FIFO spillover queue to stash complete ZMTP message batches when the main queue is congested.
  /// Prevents silent message drops.
  spillover: VecDeque<FrameBatch>,
  /// True when the ingress pipe queue is full (rcvhwm). Reads are throttled until
  /// SocketCore sends `ResumeConnection`.
  is_throttled: AtomicBool,
  multishot_reader: Option<MultishotReader>,
  is_closing: bool,
  /// Non-blocking delayed close (replaces `thread::sleep`).
  /// Armed when `NetAction::ScheduleClose(Some(delay))` fires; `prepare_sqes` polls it.
  close_deadline: Option<Instant>,
  use_send_zerocopy: bool,
  use_recv_multishot: bool,
  send_buffer_slot_size: usize,
  /// Reusable scratch space for coalescing egress batches — avoids per-cycle heap allocation.
  coalesce_scratch: Vec<FrameBatch>,
  write_in_flight: u32,
  event_fd: eventfd::EventFD,
  worker_asleep: Arc<AtomicU8>,
}

impl ZmtpUringHandler {
  pub(crate) fn new(
    fd: RawFd,
    worker_io_config: Arc<WorkerIoConfig>,
    engine: ZmtpEngine,
    egress_rx: Arc<fibre::mpsc::BoundedReceiver<FrameBatch>>,
    use_send_zerocopy: bool,
    use_recv_multishot: bool,
    send_buffer_slot_size: usize,
    event_fd: eventfd::EventFD,
    worker_asleep: Arc<AtomicU8>,
  ) -> Self {
    Self {
      fd,
      worker_io_config,
      engine,
      egress_rx,
      ingress_sender: None,
      spillover: VecDeque::new(),
      is_throttled: AtomicBool::new(false),
      multishot_reader: None,
      is_closing: false,
      close_deadline: None,
      use_send_zerocopy,
      use_recv_multishot,
      send_buffer_slot_size,
      coalesce_scratch: Vec::with_capacity(32),
      write_in_flight: 0,
      event_fd,
      worker_asleep,
    }
  }

  /// Translate an `EngineOutput` into `HandlerIoOps`, with side-effects:
  /// - `AppAction::HandshakeComplete` → send `UringConnectionEstablished` to SocketCore
  /// - `AppAction::DeliverMessage` → push batch to `inbound_data_tx`
  /// - `AppAction::PeerError` → mark closing
  fn apply_engine_output(&mut self, output: EngineOutput) -> HandlerIoOps {
    let mut ops = HandlerIoOps::new();

    for net in output.net_actions {
      match net {
        NetAction::Send { data, zc_eligible } => {
          if zc_eligible && self.use_send_zerocopy && data.len() >= ZC_SEND_THRESHOLD {
            ops
              .sqe_blueprints
              .push(HandlerSqeBlueprint::RequestSendZeroCopy {
                data_to_send: data,
                send_op_flags: 0,
                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                batch_count: 1,
              });
          } else {
            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend {
              data,
              send_op_flags: 0,
              originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
              batch_count: 1,
            });
          }
        }
        NetAction::SetCork(enable) => {
          ops
            .sqe_blueprints
            .push(HandlerSqeBlueprint::RequestSetCork(enable));
        }
        NetAction::ScheduleClose(delay) => {
          if let Some(d) = delay {
            // Non-blocking: uncork to flush TCP, then arm deadline.
            ops
              .sqe_blueprints
              .push(HandlerSqeBlueprint::RequestSetCork(false));
            self.close_deadline = Some(Instant::now() + d);
            self.is_closing = true;
          } else {
            ops.initiate_close_due_to_error = true;
          }
        }
      }
    }

    for app in output.app_actions {
      match app {
        AppAction::HandshakeComplete {
          peer_identity,
          peer_socket_type,
        } => {
          let cmd = Command::UringConnectionEstablished {
            endpoint_uri: self.worker_io_config.endpoint_uri.clone(),
            target_endpoint_uri: self.worker_io_config.target_endpoint_uri.clone(),
            connection_iface: self.worker_io_config.connection_iface.clone(),
            peer_identity,
            fd: self.fd,
          };
          if let Err(e) = self.worker_io_config.socket_mailbox.try_send(cmd) {
            warn!(
              fd = self.fd,
              "ZmtpUringHandler: failed to send UringConnectionEstablished to SocketCore: {:?}", e
            );
          }
        }
        AppAction::DeliverMessage(batch) => {
          if let Some(ref sender) = self.ingress_sender {
            // FIFO Guard: if we already have stashed batches, we must append to spillover
            // first to preserve strict FIFO delivery order.
            if !self.spillover.is_empty() {
              self.spillover.push_back(batch);
              self.is_throttled.store(true, Ordering::Release);
            } else {
              match sender.try_send_sync(batch) {
                Ok(()) => {
                  self.is_throttled.store(false, Ordering::Release);
                }
                Err(fibre::TrySendError::Full(returned_batch)) => {
                  // Stash the un-sent batch instead of dropping it on the floor!
                  self.spillover.push_back(returned_batch);
                  self.is_throttled.store(true, Ordering::Release);
                  trace!(
                    fd = self.fd,
                    "ZmtpUringHandler: ingress queue full, stashed batch to spillover"
                  );
                }
                Err(fibre::TrySendError::Closed(_)) => {
                  warn!(
                    fd = self.fd,
                    "ZmtpUringHandler: ingress sender closed, closing connection"
                  );
                  ops.initiate_close_due_to_error = true;
                }
                Err(fibre::TrySendError::Sent(_)) => unreachable!(),
              }
            }
          } else {
            // Stash the message in the spillover queue instead of dropping it.
            // When SocketCore attaches the pipe, the worker will flush this queue.
            self.spillover.push_back(batch);
            self.is_throttled.store(true, Ordering::Release);
            trace!(
              fd = self.fd,
              "ZmtpUringHandler: stashed early message to spillover — waiting for ingress sender"
            );
          }
        }
        AppAction::PeerError(e) => {
          warn!(fd = self.fd, err = %e, "ZmtpUringHandler: peer error");
          self.is_closing = true;
          let _ = self
            .worker_io_config
            .socket_mailbox
            .try_send(Command::UringFdError {
              endpoint_uri: self.worker_io_config.endpoint_uri.clone(),
              error: e,
            });
          ops.initiate_close_due_to_error = true;
        }
      }
    }

    ops
  }

  fn prepare_multishot_cancel(&mut self) -> Option<HandlerSqeBlueprint> {
    if let Some(ref mut reader) = self.multishot_reader {
      reader.prepare_cancel_intent()
    } else {
      None
    }
  }

  /// Attempt to drain the stashed spillover batches into the socket's ReadyPipeQueue.
  /// If the queue becomes congested again, it puts the batch back and maintains the throttle.
  fn try_drain_spillover(&mut self) {
    if self.spillover.is_empty() {
      return;
    }
    if let Some(ref sender) = self.ingress_sender {
      while let Some(batch) = self.spillover.pop_front() {
        match sender.try_send_sync(batch) {
          Ok(()) => {}
          Err(fibre::TrySendError::Full(returned_batch)) => {
            // Main queue is full again: put the batch back at the front and keep throttled
            self.spillover.push_front(returned_batch);
            self.is_throttled.store(true, Ordering::Release);
            return;
          }
          Err(_) => {
            // Receiver dropped; discard remainder.
            self.spillover.clear();
            return;
          }
        }
      }
      // Spillover fully flushed: we can safely clear the throttle flag
      self.is_throttled.store(false, Ordering::Release);
      trace!(fd = self.fd, "ZmtpUringHandler: spillover fully drained");
    }
  }
}

impl UringConnectionHandler for ZmtpUringHandler {
  fn fd(&self) -> RawFd {
    self.fd
  }

  fn io_config(&self) -> &Arc<WorkerIoConfig> {
    &self.worker_io_config
  }

  fn is_closing_or_closed(&self) -> bool {
    self.is_closing
  }

  fn connection_ready(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
    debug!(fd = self.fd, "ZmtpUringHandler: connection_ready");

    if self.use_send_zerocopy && self.send_buffer_slot_size == 0 {
      warn!(
        fd = self.fd,
        "ZmtpUringHandler: zero-copy send enabled but send buffer pool slot size is zero; \
                 sends will fall back to heap copies"
      );
    }

    if self.use_recv_multishot {
      if let Some(bgid) = interface.default_buffer_group_id() {
        self.multishot_reader = Some(MultishotReader::new(self.fd, bgid));
      } else {
        warn!(
          fd = self.fd,
          "ZmtpUringHandler: no default buffer group — multishot reads disabled"
        );
      }
    }

    let out = self.engine.start();
    self.apply_engine_output(out)
  }

  fn process_ring_read_bytes(
    &mut self,
    bytes: Bytes,
    _interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps {
    trace!(
      fd = self.fd,
      len = bytes.len(),
      "ZmtpUringHandler: process_ring_read_bytes"
    );

    if bytes.is_empty() {
      info!(fd = self.fd, "ZmtpUringHandler: EOF from peer");
      self.is_closing = true;
      let _ = self
        .worker_io_config
        .socket_mailbox
        .try_send(Command::UringFdError {
          endpoint_uri: self.worker_io_config.endpoint_uri.clone(),
          error: ZmqError::ConnectionClosed,
        });
      return HandlerIoOps::new().add_blueprint(HandlerSqeBlueprint::RequestClose);
    }

    if self.is_closing {
      return HandlerIoOps::new();
    }

    let out = self.engine.on_network_bytes(bytes);
    self.apply_engine_output(out)
  }

  fn handle_internal_sqe_completion(
    &mut self,
    _sqe_user_data: UserData,
    cqe_result: i32,
    _cqe_flags: u32,
    interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps {
    if interface.is_write_completion {
      // Unlock the local serialization gate upon write completion
      self.write_in_flight = self.write_in_flight.saturating_sub(1);
      // println!("write in flight completed {:?}", self.write_in_flight);
    }

    if cqe_result < 0 {
      error!(
        fd = self.fd,
        errno = -cqe_result,
        "ZmtpUringHandler: send SQE failed"
      );
      self.is_closing = true;
      let mut ops = HandlerIoOps::new();
      ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
      return ops;
    }
    HandlerIoOps::new()
  }

  fn prepare_sqes(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
    if self.is_closing && self.close_deadline.is_none() {
      return HandlerIoOps::new();
    }

    // (a) Try to flush stashed batches back into the queue first
    self.try_drain_spillover();

    let mut ops = HandlerIoOps::new();

    // (a) Re-arm multishot read.
    if !self.is_closing && !self.should_throttle_reads() {
      if let Some(reader) = &mut self.multishot_reader {
        if let Some(blueprint) = reader.prepare_recv_multi_intent() {
          ops.sqe_blueprints.push(blueprint);
        }
      }
    }

    // (b) Coalesce/Batch egress messages into a single, high-throughput write SQE.
    // Lock the queue draining if we currently have an unacknowledged write in-flight.
    if !self.is_closing
      && self.engine.phase == crate::protocol::zmtp::engine::ZmtpPhase::Data
      && self.write_in_flight < 1
      && interface.pending_egress_count < MICRO_COALESCE_THRESHOLD
    {
      self.coalesce_scratch.clear();
      let limit = self.engine.config().sndbatch_count.max(1);

      // BULK DRAIN: Acquire channel synchronization lock exactly once.
      // First fast non-blocking bulk drain
      let mut drained = self
        .egress_rx
        .try_recv_batch_mut(&mut self.coalesce_scratch, limit)
        .unwrap_or(0);

      // OPPORTUNISTIC DOUBLE-DRAIN:
      // If we pulled an underfilled batch, execute a micro-pause (one instruction)
      // to let the active producer task's concurrent write settle in the ring buffer,
      // then perform one final bulk drain to top off our batch.
      if drained > 0 && drained < limit {
        std::hint::spin_loop(); // PAUSE instruction (low-power CPU pipeline pause)

        let additional = self
          .egress_rx
          .try_recv_batch_mut(&mut self.coalesce_scratch, limit - drained)
          .unwrap_or(0);

        drained += additional;
      }

      if drained > 0 {
        self.write_in_flight += 1; // Lock the pipeline gate for vectored writes
        let batch_count = drained as u32;

        // Generate one highly-coalesced scatter-gather writev SQE
        match self.engine.frame_batch_vectored(&self.coalesce_scratch) {
          Ok(bufs) => {
            ops
              .sqe_blueprints
              .push(HandlerSqeBlueprint::RequestSendRawVectored {
                bufs,
                send_op_flags: 0,
                batch_count,
              });
          }
          Err(e) => {
            let _ = self
              .worker_io_config
              .socket_mailbox
              .try_send(Command::UringFdError {
                endpoint_uri: self.worker_io_config.endpoint_uri.clone(),
                error: e,
              });
            ops.initiate_close_due_to_error = true;
          }
        }
      }
    }

    // (c) Non-blocking delayed close check.
    if let Some(deadline) = self.close_deadline {
      if Instant::now() >= deadline {
        self.close_deadline = None;
        ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
      }
    }

    ops
  }

  fn handle_outgoing_app_data(
    &mut self,
    _data: crate::io_uring_backend::connection_handler::OutgoingMessage,
    _interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps {
    // Egress is delivered via egress_rx in prepare_sqes; this path is not used.
    HandlerIoOps::new()
  }

  fn close_initiated(&mut self, _interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
    info!(fd = self.fd, "ZmtpUringHandler: close_initiated");
    if self.is_closing {
      return HandlerIoOps::new();
    }
    self.is_closing = true;
    self.close_deadline = None;
    let mut ops = HandlerIoOps::new();
    if let Some(ref mut reader) = self.multishot_reader {
      if let Some(cancel_bp) = reader.prepare_cancel_intent() {
        ops.sqe_blueprints.push(cancel_bp);
      }
    }
    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
    ops
  }

  fn fd_has_been_closed(&mut self) {
    info!(fd = self.fd, "ZmtpUringHandler: fd_has_been_closed");
    self.is_closing = true;
    self.close_deadline = None;
  }

  fn delegate_cqe_to_multishot_reader(
    &mut self,
    cqe: &io_uring::cqueue::Entry,
    buffer_manager: &BufferRingManager,
    worker_interface: &UringWorkerInterface<'_>,
    internal_op_tracker: &mut InternalOpTracker,
  ) -> Option<Result<(HandlerIoOps, bool), ZmqError>> {
    let cqe_user_data = cqe.user_data();
    let reader_might_handle = self
      .multishot_reader
      .as_ref()
      .map_or(false, |r| r.matches_cqe_user_data(cqe_user_data));

    if reader_might_handle {
      if let Some(mut reader) = self.multishot_reader.take() {
        let result = reader.process_cqe(
          cqe,
          buffer_manager,
          self,
          worker_interface,
          internal_op_tracker,
        );
        self.multishot_reader = Some(reader);
        return Some(result);
      }
    }
    None
  }

  fn inform_multishot_reader_op_submitted(
    &mut self,
    op_user_data: UserData,
    is_cancel_op: bool,
    target_op_data_if_cancel: Option<UserData>,
  ) {
    if let Some(reader) = &mut self.multishot_reader {
      if is_cancel_op {
        if let Some(target_ud) = target_op_data_if_cancel {
          reader.mark_cancellation_submitted(op_user_data, target_ud);
        }
      } else {
        reader.mark_operation_submitted(op_user_data);
      }
    }
  }

  fn prefers_multishot_read(&self) -> bool {
    self.multishot_reader.is_some()
  }

  fn should_throttle_reads(&self) -> bool {
    if self.is_closing {
      return true;
    }
    
    // PUSH and PUB sockets never receive payload data in the Data phase.
    // This drops the EAGAIN CQE storm to exactly 0.
    let socket_type = self.engine.config().socket_type_name.as_str();
    if (socket_type == "PUSH" || socket_type == "PUB")
      && self.engine.phase == crate::protocol::zmtp::engine::ZmtpPhase::Data
    {
      return true;
    }


    // We must never read more from the kernel while we still have stashed batches to deliver
    if !self.spillover.is_empty() {
      return true;
    }
    let throttled = self.is_throttled.load(Ordering::Acquire);
    if throttled {
      if let Some(ref sender) = self.ingress_sender {
        if sender.is_drained() {
          self.is_throttled.store(false, Ordering::Release);
          return false;
        }
      }
    }
    throttled
  }

  fn attach_ingress(&mut self, mut sender: PipeMessageSender) {
    debug!(fd = self.fd, "ZmtpUringHandler: ingress sender attached");

    sender.bind_uring_wakeup(crate::socket::patterns::ready_pipe_queue::UringWakeup {
      event_fd: self.event_fd.clone(),
      worker_asleep: Arc::clone(&self.worker_asleep),
    });

    self.ingress_sender = Some(sender);
    // Clear any throttle that may have been set before the sender arrived.
    self.is_throttled.store(false, Ordering::Release);
  }

  fn resume_ingress(&mut self) {
    trace!(
      fd = self.fd,
      "ZmtpUringHandler: resume_ingress — clearing throttle"
    );
    self.is_throttled.store(false, Ordering::Release);
  }

  /// Returns true when there is pending egress data or a close deadline is armed,
  /// or when we have stashed ingress data that can now flow.
  fn has_drainable_spillover(&self) -> bool {
    let has_ingress_spillover = !self.spillover.is_empty()
      && self
        .ingress_sender
        .as_ref()
        .map_or(false, |s| !s.is_congested());

    !self.egress_rx.is_empty() || self.close_deadline.is_some() || has_ingress_spillover
  }

  fn on_buffer_ring_exhausted(&mut self) {
    trace!(
      fd = self.fd,
      "ZmtpUringHandler: buffer ring exhausted (ENOBUFS) — transient"
    );
  }
}
