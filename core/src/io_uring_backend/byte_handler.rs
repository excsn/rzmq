#![cfg(feature = "io-uring")]

use std::any::Any;
use std::collections::VecDeque;
use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, error, info, trace, warn};

/// Zero-copy lease over a kernel buffer ring slot.
///
/// Produced by the worker after a multishot recv CQE; forwarded to the Tokio-side `UringStream`
/// via `raw_inbound_tx`. The lease's `Drop` sends `RecycleRecvBuffer` to re-register the slot
/// with the kernel ring once the Tokio side has finished reading.
pub(crate) struct UringInboundLease {
  /// Buffer ring slot ID (index into `IoUringBufRing`).
  pub id: u16,
  /// Stable pointer to kernel-filled data. Valid until `reprovide_buffer` is called.
  pub ptr: *const u8,
  /// Number of bytes written by the kernel into this slot.
  pub len: usize,
}

// SAFETY: The pointer is stable (ring buffers are pinned at init) and exclusively
// owned by this lease between `mem::forget(BorrowedBuffer)` and `reprovide_buffer`.
unsafe impl Send for UringInboundLease {}
unsafe impl Sync for UringInboundLease {}

impl Drop for UringInboundLease {
  fn drop(&mut self) {
    if self.len == 0 {
      return; // EOF sentinel — no slot was consumed, nothing to recycle
    }
    if let Ok(op_tx) = crate::uring::global_state::get_global_uring_worker_op_tx() {
      let _ = op_tx.try_send(UringOpRequest::RecycleRecvBuffer { id: self.id });
    }
  }
}

/// Channel message type for the Tokio → UringWorker egress pipe.
///
/// Lives in `byte_handler` (not `uring_stream`) so `ops.rs` and `uring_stream.rs` can both
/// import it without creating a circular dependency between `transport` and `io_uring_backend`.
#[derive(Debug)]
pub(crate) enum EgressChunk {
  /// Single contiguous byte slice (from `poll_write`).
  Contiguous(Bytes),
  /// Scatter-gather slices (from `poll_write_vectored`) — issued as one `writev` SQE.
  Vectored(Vec<Bytes>),
  /// Asynchronous TCP_CORK toggle — issued as a `setsockopt` SQE on the worker thread,
  /// keeping the Tokio side 100% non-blocking.
  SetCork(bool),
  /// Pre-registered buffer slot already written by the Tokio side; worker issues `SEND_ZC`
  /// directly from the registered index — no copy needed on the worker thread.
  LeasedZeroCopy { lease: SendBufferLease, actual_len: usize },
}

use crate::io_uring_backend::{
  buffer_manager::BufferRingManager,
  connection_handler::{
    HandlerIoOps, HandlerSqeBlueprint, OutgoingMessage, UringConnectionHandler, UringWorkerInterface,
    WorkerIoConfig,
  },
  ops::{UringOpRequest, UserData, HANDLER_INTERNAL_SEND_OP_UD},
  send_buffer_pool::{RegisteredSendBufferId, SendBufferLease, SendBufferPool},
  worker::{InternalOpTracker, MultishotReader},
};
use crate::message::Msg;
use crate::runtime::MailboxSyncSender;
use crate::socket::connection_iface::ISocketConnection;
use crate::ZmqError;

/// Minimum payload size (bytes) above which we attempt a zero-copy send.
/// Below this threshold the `ioctl`/registration overhead outweighs the copy cost.
const ZC_SEND_THRESHOLD: usize = 1024;

#[derive(Debug)]
struct DummySocketConnection;

#[async_trait]
impl ISocketConnection for DummySocketConnection {
  async fn send_multipart(&self, _msgs: Vec<Msg>) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState("UringByteHandler: route via raw channel".into()))
  }
  async fn close_connection(&self) -> Result<(), ZmqError> {
    Ok(())
  }
  fn as_any(&self) -> &dyn Any {
    self
  }
}

pub(crate) struct UringByteHandler {
  fd: RawFd,
  worker_io_config: Arc<WorkerIoConfig>,
  raw_inbound_tx: fibre::mpsc::BoundedSender<UringInboundLease>,
  raw_egress_rx: fibre::mpsc::BoundedReceiver<EgressChunk>,
  multishot_reader: Option<MultishotReader>,
  is_closing: bool,
  spillover: VecDeque<UringInboundLease>,
  mailbox_full_throttled: bool,
  /// When true and payload exceeds `ZC_SEND_THRESHOLD`, emit `RequestSendZeroCopy`.
  use_send_zerocopy: bool,
}

impl UringByteHandler {
  pub(crate) fn new(
    fd: RawFd,
    socket_mailbox: MailboxSyncSender,
    endpoint_uri: String,
    target_endpoint_uri: String,
    raw_inbound_tx: fibre::mpsc::BoundedSender<UringInboundLease>,
    raw_egress_rx: fibre::mpsc::BoundedReceiver<EgressChunk>,
    use_send_zerocopy: bool,
  ) -> Self {
    let (dummy_tx, _dummy_rx) = fibre::mpsc::bounded::<Vec<Msg>>(1);
    let worker_io_config = Arc::new(WorkerIoConfig {
      socket_mailbox,
      inbound_data_tx: dummy_tx,
      endpoint_uri,
      target_endpoint_uri,
      connection_iface: Arc::new(DummySocketConnection),
    });
    Self {
      fd,
      worker_io_config,
      raw_inbound_tx,
      raw_egress_rx,
      multishot_reader: None,
      is_closing: false,
      spillover: VecDeque::new(),
      mailbox_full_throttled: false,
      use_send_zerocopy,
    }
  }

  fn try_drain_spillover(&mut self) -> bool {
    while let Some(bytes) = self.spillover.pop_front() {
      match self.raw_inbound_tx.try_send(bytes) {
        Ok(()) => {}
        Err(fibre::TrySendError::Full(returned)) => {
          self.spillover.push_front(returned);
          return false;
        }
        Err(_) => {} // receiver dropped; discard
      }
    }
    true
  }

  /// Prepare a multishot cancel blueprint when we need to stop kernel-side reads.
  fn prepare_multishot_cancel(&mut self) -> Option<HandlerSqeBlueprint> {
    if let Some(ref mut reader) = self.multishot_reader {
      reader.prepare_cancel_intent()
    } else {
      None
    }
  }
}

impl UringConnectionHandler for UringByteHandler {
  fn fd(&self) -> RawFd {
    self.fd
  }

  fn io_config(&self) -> &WorkerIoConfig {
    &self.worker_io_config
  }

  fn is_closing_or_closed(&self) -> bool {
    self.is_closing
  }

  fn connection_ready(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
    debug!(fd = self.fd, "UringByteHandler: connection_ready");
    if let Some(bgid) = interface.default_buffer_group_id() {
      self.multishot_reader = Some(MultishotReader::new(self.fd, bgid));
    } else {
      warn!(
        fd = self.fd,
        "UringByteHandler: no default buffer group id — multishot reads disabled"
      );
    }
    HandlerIoOps::default()
  }

  fn process_ring_read_data(
    &mut self,
    lease: UringInboundLease,
    _interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps {
    trace!(fd = self.fd, len = lease.len, "UringByteHandler: process_ring_read_data");
    if lease.len == 0 {
      // EOF sentinel — Drop is a no-op (len == 0), no slot to recycle.
      info!(fd = self.fd, "UringByteHandler: EOF received from peer");
      return HandlerIoOps::default();
    }
    match self.raw_inbound_tx.try_send(lease) {
      Ok(()) => {}
      Err(fibre::TrySendError::Full(returned)) => {
        // Channel full: stash lease and cancel the active multishot read so the kernel
        // stops delivering data until we drain the spillover queue.
        self.spillover.push_back(returned);
        self.mailbox_full_throttled = true;
        let mut ops = HandlerIoOps::default();
        if let Some(cancel_bp) = self.prepare_multishot_cancel() {
          ops.sqe_blueprints.push(cancel_bp);
        }
        return ops;
      }
      Err(_) => {
        // Receiver dropped — lease's Drop fires RecycleRecvBuffer automatically.
      }
    }
    HandlerIoOps::default()
  }

  fn handle_internal_sqe_completion(
    &mut self,
    _sqe_user_data: UserData,
    cqe_result: i32,
    _cqe_flags: u32,
    _interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps {
    if cqe_result < 0 {
      error!(fd = self.fd, errno = -cqe_result, "UringByteHandler: send SQE failed");
      self.is_closing = true;
      let mut ops = HandlerIoOps::default();
      ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
      return ops;
    }
    HandlerIoOps::default()
  }

  fn prepare_sqes(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
    if self.is_closing {
      return HandlerIoOps::default();
    }
    let mut ops = HandlerIoOps::default();

    // (a) Drain spillover back into the inbound channel; re-arm reads when clear.
    if !self.spillover.is_empty() && self.try_drain_spillover() {
      self.mailbox_full_throttled = false;
    }

    // (b) Re-arm multishot read if not throttled.
    if !self.should_throttle_reads() {
      if let Some(reader) = &mut self.multishot_reader {
        if let Some(blueprint) = reader.prepare_recv_multi_intent() {
          ops.sqe_blueprints.push(blueprint);
        }
      }
    }

    // (c) Drain egress channel and submit sends/control ops.
    // Apply backpressure: stop draining if there are already enough pending blueprints in the work_map
    if interface.pending_egress_count < 16 {
      let mut chunks_pulled = 0;
      let pull_limit = 16 - interface.pending_egress_count;
      while chunks_pulled < pull_limit {
        let chunk = match self.raw_egress_rx.try_recv() {
          Ok(c) => c,
          Err(_) => break,
        };
        chunks_pulled += 1;
        match chunk {
          EgressChunk::Contiguous(bytes) => {
            if bytes.is_empty() {
              continue;
            }
            let bp = if self.use_send_zerocopy && bytes.len() > ZC_SEND_THRESHOLD {
              HandlerSqeBlueprint::RequestSendZeroCopy {
                data_to_send: bytes,
                send_op_flags: 0,
                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
              }
            } else {
              HandlerSqeBlueprint::RequestSend {
                data: bytes,
                send_op_flags: 0,
                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
              }
            };
            ops.sqe_blueprints.push(bp);
          }
          EgressChunk::Vectored(bufs) => {
            let non_empty: Vec<Bytes> = bufs.into_iter().filter(|b| !b.is_empty()).collect();
            if !non_empty.is_empty() {
              ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSendRawVectored {
                bufs: non_empty,
                send_op_flags: 0,
              });
            }
          }
          EgressChunk::SetCork(enable) => {
            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSetCork(enable));
          }
          EgressChunk::LeasedZeroCopy { lease, actual_len } => {
            lease.released_to_worker.store(true, Ordering::Release);
            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSendZeroCopyLeased {
              id: lease.id,
              ptr: lease.ptr as *const u8,
              len: actual_len as u32,
            });
          }
        }
      }
    }

    ops
  }

  fn handle_outgoing_app_data(
    &mut self,
    _data: OutgoingMessage,
    _interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps {
    HandlerIoOps::default()
  }

  fn close_initiated(&mut self, _interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
    info!(fd = self.fd, "UringByteHandler: close_initiated");
    if self.is_closing {
      return HandlerIoOps::default();
    }
    self.is_closing = true;
    let mut ops = HandlerIoOps::default();
    if let Some(ref mut reader) = self.multishot_reader {
      if let Some(cancel_bp) = reader.prepare_cancel_intent() {
        ops.sqe_blueprints.push(cancel_bp);
      }
    }
    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
    ops
  }

  fn fd_has_been_closed(&mut self) {
    info!(fd = self.fd, "UringByteHandler: fd_has_been_closed");
    self.is_closing = true;
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
        let result = reader.process_cqe(cqe, buffer_manager, self, worker_interface, internal_op_tracker);
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
    self.mailbox_full_throttled || !self.spillover.is_empty()
  }

  fn on_resume_connection(&mut self, _interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
    if self.try_drain_spillover() {
      self.mailbox_full_throttled = false;
    }
    HandlerIoOps::default()
  }

  fn on_buffer_ring_exhausted(&mut self) {
    if self.mailbox_full_throttled {
      return;
    }
    // tracing::warn!(fd = self.fd, "UringByteHandler: buffer ring exhausted (ENOBUFS) — throttling reads until app catches up");
    self.mailbox_full_throttled = true;
    // We DO NOT instantly send ResumeConnection here.
    // The application (Tokio side) will send ResumeConnection when it fully consumes a lease,
    // guaranteeing that the buffer ring has capacity again.
  }
}
