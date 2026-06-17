#![cfg(feature = "io-uring")]

use std::any::Any;
use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, error, info, trace, warn};


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
  ops::{UserData, HANDLER_INTERNAL_SEND_OP_UD},
  send_buffer_pool::{RegisteredSendBufferId, SendBufferLease, SendBufferPool},
  worker::{InternalOpTracker, MultishotReader},
};
use crate::message::{FrameBatch, Msg};
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
  async fn send_multipart(&self, _msgs: FrameBatch) -> Result<(), ZmqError> {
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
  raw_inbound_tx: fibre::mpsc::BoundedSender<Bytes>,
  raw_egress_rx: fibre::mpsc::BoundedReceiver<EgressChunk>,
  multishot_reader: Option<MultishotReader>,
  is_closing: bool,
  /// FIFO overflow stash for inbound chunks when `raw_inbound_tx` is full.
  /// `Vec` (not `VecDeque`) so it can feed `try_send_batch_mut`, which drains
  /// sent items from the front and leaves the unsent tail in place.
  spillover: Vec<Bytes>,
  /// Reusable scratch for batch-draining `raw_egress_rx` in `prepare_sqes`.
  egress_scratch: Vec<EgressChunk>,
  /// When true and payload exceeds `ZC_SEND_THRESHOLD`, emit `RequestSendZeroCopy`.
  use_send_zerocopy: bool,
  /// When true, instantiate `MultishotReader` in `connection_ready`. Mirrors the
  /// socket's `io_uring.recv_multishot` option passed through `RegisterExternalByteFd`.
  use_recv_multishot: bool,
  /// Capacity (bytes) of each pre-registered send buffer slot in the worker's `SendBufferPool`.
  /// Zero means the pool was not initialized. Used in `connection_ready` to validate that
  /// zero-copy sends can succeed without silent fallbacks.
  send_buffer_slot_size: usize,
}

impl UringByteHandler {
  pub(crate) fn new(
    fd: RawFd,
    socket_mailbox: MailboxSyncSender,
    endpoint_uri: String,
    target_endpoint_uri: String,
    raw_inbound_tx: fibre::mpsc::BoundedSender<Bytes>,
    raw_egress_rx: fibre::mpsc::BoundedReceiver<EgressChunk>,
    use_send_zerocopy: bool,
    use_recv_multishot: bool,
    send_buffer_slot_size: usize,
  ) -> Self {
    let worker_io_config = Arc::new(WorkerIoConfig {
      socket_mailbox,
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
      spillover: Vec::new(),
      egress_scratch: Vec::new(),
      use_send_zerocopy,
      use_recv_multishot,
      send_buffer_slot_size,
    }
  }

  fn try_drain_spillover(&mut self) -> bool {
    if self.spillover.is_empty() {
      return true;
    }
    let initial_len = self.spillover.len();
    // One batch pass: sent chunks are drained from the front; on a full channel the
    // unsent tail stays in `spillover`, preserving FIFO order.
    match self.raw_inbound_tx.try_send_batch_mut(&mut self.spillover) {
      Ok(_sent) => {}
      Err(_) => {
        // Receiver dropped; discard buffered chunks.
        self.spillover.clear();
      }
    }
    if self.spillover.is_empty() {
      tracing::trace!(
        fd = self.fd,
        drained_chunks = initial_len,
        "UringByteHandler: spillover fully drained"
      );
      true
    } else {
      false
    }
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

  fn io_config(&self) -> &Arc<WorkerIoConfig> {
    &self.worker_io_config
  }

  fn is_closing_or_closed(&self) -> bool {
    self.is_closing
  }

  fn connection_ready(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
    debug!(fd = self.fd, "UringByteHandler: connection_ready");

    if self.use_send_zerocopy {
      if self.send_buffer_slot_size == 0 {
        warn!(
          fd = self.fd,
          "UringByteHandler: zero-copy send is enabled but the send buffer pool slot size is \
           zero — all sends will fall back to heap-allocated copies. Ensure the io_uring backend \
           was initialized with a non-zero default_send_buffer_size."
        );
      } else {
        debug!(
          fd = self.fd,
          send_buffer_slot_size = self.send_buffer_slot_size,
          "UringByteHandler: zero-copy send armed; writes exceeding {} bytes per call will \
           fall back to heap-allocated copies",
          self.send_buffer_slot_size
        );
      }
    }

    if self.use_recv_multishot {
      if let Some(bgid) = interface.default_buffer_group_id() {
        self.multishot_reader = Some(MultishotReader::new(self.fd, bgid));
      } else {
        warn!(
          fd = self.fd,
          "UringByteHandler: no default buffer group id — multishot reads disabled"
        );
      }
    } else {
      trace!(fd = self.fd, "UringByteHandler: multishot recv disabled by socket option");
    }
    HandlerIoOps::default()
  }

  fn process_ring_read_bytes(
    &mut self,
    bytes: Bytes,
    _interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps {
    trace!(fd = self.fd, len = bytes.len(), "UringByteHandler: process_ring_read_bytes");
    if bytes.is_empty() {
      // EOF sentinel — peer closed the connection. Mark closing and request socket teardown
      // to prevent prepare_sqes from re-arming a read that would immediately EOF again.
      info!(fd = self.fd, "UringByteHandler: EOF received from peer");
      self.is_closing = true;
      let mut ops = HandlerIoOps::default();
      ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
      return ops;
    }
    // FIFO guard: if spillover is non-empty, this chunk must not bypass it.
    if !self.spillover.is_empty() {
      // tracing::warn!(
      //   fd = self.fd,
      //   spillover_len = self.spillover.len(),
      //   chunk_len = bytes.len(),
      //   "UringByteHandler: stashing incoming chunk to spillover queue due to active backpressure"
      // );
      self.spillover.push(bytes);
      return HandlerIoOps::default();
    }

    match self.raw_inbound_tx.try_send(bytes) {
      Ok(()) => {}
      Err(fibre::TrySendError::Full(returned)) => {
        // Channel full: stash bytes and cancel the active multishot read.
        // The worker re-arms reads automatically once spillover drains in prepare_sqes.
        self.spillover.push(returned);
        let mut ops = HandlerIoOps::default();
        if let Some(cancel_bp) = self.prepare_multishot_cancel() {
          ops.sqe_blueprints.push(cancel_bp);
        }
        return ops;
      }
      Err(_) => {} // Receiver dropped; discard.
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
    if !self.spillover.is_empty() {
      self.try_drain_spillover();
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
      let pull_limit = 16 - interface.pending_egress_count;
      // One batch pass over the channel instead of one try_recv per chunk.
      self.egress_scratch.clear();
      let _ = self
        .raw_egress_rx
        .try_recv_batch_mut(&mut self.egress_scratch, pull_limit);
      for chunk in self.egress_scratch.drain(..) {
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
                batch_count: 1,
              }
            } else {
              HandlerSqeBlueprint::RequestSend {
                data: bytes,
                send_op_flags: 0,
                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                batch_count: 1,
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
                batch_count: 1,
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
    !self.spillover.is_empty()
  }

  fn has_drainable_spillover(&self) -> bool {
    !self.spillover.is_empty() && !self.raw_inbound_tx.is_full()
  }

  fn on_buffer_ring_exhausted(&mut self) {
    tracing::trace!(fd = self.fd, "UringByteHandler: buffer ring exhausted (ENOBUFS) — transient, will re-arm next cycle");
  }

}
