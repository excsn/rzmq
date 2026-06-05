#![cfg(feature = "io-uring")]

use crate::message::Msg;
use crate::{Blob, ZmqError};

use std::os::unix::io::RawFd;
use std::sync::Arc;

use bytes::Bytes;
use crate::runtime::MailboxSyncSender;
use crate::socket::connection_iface::ISocketConnection;

pub use crate::io_uring_backend::ops::UserData;

use super::buffer_manager::BufferRingManager;
use super::ops::ProtocolConfig;
use super::worker::InternalOpTracker;

// --- Blueprints for SQEs requested by handlers ---
#[derive(Debug, Clone)]
pub enum HandlerSqeBlueprint {
  /// Request to send data. The UringWorker will build a Send SQE.
  RequestSend {
    data: Bytes,
    send_op_flags: i32,
    originating_app_op_ud: UserData,
  },
  RequestSendZeroCopy {
    data_to_send: Bytes, // Data the handler wants to send via ZC
    send_op_flags: i32,  // For MSG_MORE
    originating_app_op_ud: UserData,
  },
  /// Vectored write: header (≤9 bytes of ZMTP framing) + payload (zero-copy Bytes ref).
  /// Only valid for unencrypted (NullFramer) paths; encrypted framers emit RequestSend instead.
  RequestSendVectored {
    header: Bytes,
    payload: Bytes,
    send_op_flags: i32,
    originating_app_op_ud: UserData,
  },
  /// Request to close the handler's FD. The UringWorker will build a Close SQE.
  RequestClose,
  /// Request a multishot ring-buffered read for the handler's FD.
  /// Signals intent to start a multishot read. Worker will generate UserData.
  RequestNewRingReadMultishot { fd: RawFd, bgid: u16 },
  /// Signals intent to cancel an operation. Worker will generate UserData for the cancel op.
  RequestNewAsyncCancel {
    fd: RawFd,                  // For context, and to find the MultishotReader
    target_user_data: UserData, // UserData of the operation to be cancelled
  },
  /// Request to set the TCP_CORK option on the handler's FD. This is a direct
  /// worker action, not an SQE submission.
  RequestSetCork(bool),
  // Potentially others: RequestPollAdd, RequestTimeout, etc.
}

impl HandlerSqeBlueprint {
  /// Returns true if this blueprint is an ingress (receive path) operation exempt from the
  /// egress write-serialization gate.
  pub fn is_ingress(&self) -> bool {
    matches!(
      self,
      Self::RequestNewRingReadMultishot { .. } | Self::RequestNewAsyncCancel { .. }
    )
  }

  /// Returns `Some(total_bytes)` for write ops; `None` for control ops.
  pub fn write_len(&self) -> Option<usize> {
    match self {
      Self::RequestSend { data, .. } => Some(data.len()),
      Self::RequestSendZeroCopy { data_to_send, .. } => Some(data_to_send.len()),
      Self::RequestSendVectored { header, payload, .. } => Some(header.len() + payload.len()),
      _ => None,
    }
  }
}

/// Output from handler methods, indicating what I/O the worker should perform.
#[derive(Debug, Default)]
pub struct HandlerIoOps {
  /// List of SQE blueprints the worker should try to queue.
  pub sqe_blueprints: Vec<HandlerSqeBlueprint>,
  /// If true, the worker should consider this handler for forceful closure due to an error.
  /// The handler might also queue a `RequestClose` blueprint.
  pub initiate_close_due_to_error: bool,
}

impl HandlerIoOps {
  pub fn new() -> Self {
    Self::default()
  }
  pub fn add_blueprint(mut self, bp: HandlerSqeBlueprint) -> Self {
    self.sqe_blueprints.push(bp);
    self
  }
  pub fn set_error_close(mut self) -> Self {
    self.initiate_close_due_to_error = true;
    self
  }
}

// --- SubmissionQueueWriter Helper ---
pub struct SubmissionQueueWriter<'sq_borrow> {
  sq: &'sq_borrow mut io_uring::squeue::SubmissionQueue<'sq_borrow>,
}
impl<'sq_borrow> SubmissionQueueWriter<'sq_borrow> {
  pub(crate) fn new(sq: &'sq_borrow mut io_uring::squeue::SubmissionQueue<'sq_borrow>) -> Self {
    Self { sq }
  }
  pub fn push(&mut self, entry: &io_uring::squeue::Entry) -> Result<(), String> {
    unsafe { self.sq.push(entry) }
      .map(|_| ())
      .map_err(|e| format!("SQ push error: {:?}", e))
  }
  pub fn is_full(&self) -> bool {
    unsafe { self.sq.is_full() }
  }
}

// --- UringWorkerInterface ---
// 'iface_life: Lifetime of the UringWorkerInterface instance itself.
// 'cfg_life: Lifetime of the borrowed configurations.
pub struct UringWorkerInterface<'cfg_life> {
  pub fd: RawFd,
  pub worker_io_config: &'cfg_life WorkerIoConfig,
  pub buffer_manager: Option<&'cfg_life super::buffer_manager::BufferRingManager>,
  pub default_bgid_for_handler_use: Option<u16>,
  // UserData of the external UringOpRequest (e.g. SendDataViaHandler) that
  // triggered the current handler action (e.g., handle_outgoing_app_data).
  // This is needed by the handler to correctly populate blueprints.
  pub current_external_op_ud: UserData,
}

impl<'cfg_life> UringWorkerInterface<'cfg_life> {
  pub(crate) fn new(
    fd: RawFd,
    worker_io_config: &'cfg_life WorkerIoConfig,
    buffer_manager: Option<&'cfg_life super::buffer_manager::BufferRingManager>,
    default_bgid_for_handler_use: Option<u16>,
    current_external_op_ud: UserData,
  ) -> Self {
    Self {
      fd,
      worker_io_config,
      buffer_manager,
      default_bgid_for_handler_use,
      current_external_op_ud,
    }
  }
  // Methods for handlers to get info, but not to directly queue ops.
  pub fn fd(&self) -> RawFd {
    self.fd
  }
  pub fn default_buffer_group_id(&self) -> Option<u16> {
    self.default_bgid_for_handler_use
  }
}

// --- UringConnectionHandler Trait ---
pub trait UringConnectionHandler: Send {
  fn fd(&self) -> RawFd;

  /// Returns a reference to the connection-specific I/O config (including the socket mailbox).
  fn io_config(&self) -> &WorkerIoConfig;

  /// Checks if the handler is in a terminal (Closing, Closed, Error) state.
  fn is_closing_or_closed(&self) -> bool;
  
  /// Called when the connection is first established and ready.
  /// Handler should return blueprints for initial I/O (e.g., start reading, send greeting).
  fn connection_ready(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps;

  /// Called when data is available from a completed ring-buffered read.
  /// `buffer_slice` contains the data, `buffer_id` identifies the ring buffer slot.
  /// Handler processes data, may produce Msgs for upstream, and returns blueprints for next I/O.
  fn process_ring_read_data(
    &mut self,
    buffer_slice: &[u8],
    buffer_id: u16, // Still useful for the handler to know which buffer this was
    interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps;

  /// Called when a non-ring-read internal SQE (e.g., Send, handler-specific Poll) completes.
  /// `sqe_user_data` is the UserData of the completed internal SQE.
  /// Handler processes the completion and returns blueprints for next I/O.
  fn handle_internal_sqe_completion(
    &mut self,
    sqe_user_data: UserData, // The internal UD of the SQE that completed
    cqe_result: i32,
    cqe_flags: u32,
    interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps;

  /// Called by the worker to allow the handler to prepare new SQEs.
  /// This is the primary way handlers request I/O (reads, writes).
  fn prepare_sqes(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps;

  /// Called when the application (e.g., ZmtpEngineRawUring) wants to send data.
  /// `data` is the application-level message. Handler frames/encrypts it and
  /// returns blueprints, typically `RequestSend`.
  fn handle_outgoing_app_data(
    &mut self,
    data: OutgoingMessage,
    interface: &UringWorkerInterface<'_>,
  ) -> HandlerIoOps;

  /// Called by the worker when it intends to close this handler's connection
  /// (e.g., due to external request or fatal error elsewhere).
  /// Handler should prepare to shut down and can return a `RequestClose` blueprint.
  fn close_initiated(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps;

  /// Final notification that the FD has been closed by the worker.
  /// Handler should perform any final cleanup.
  fn fd_has_been_closed(&mut self);

  /// Called by `cqe_processor` to delegate a CQE that might belong to this handler's
  /// `MultishotReader` (if it has one).
  ///
  /// Returns:
  /// - `Some(Ok((HandlerIoOps, bool)))`: If handled by the multishot reader.
  ///   - `HandlerIoOps`: Operations requested by the handler after processing the data/event.
  ///   - `bool (should_cleanup_active_op_ud)`: True if the UserData associated with the
  ///     *original multishot RECV_MULTISHOT operation* should now be cleaned up from the
  ///     InternalOpTracker (e.g., MORE flag not set, or error).
  ///     For an AsyncCancel CQE, this bool refers to the cleanup of the AsyncCancel's *own* UserData.
  /// - `Some(Err(ZmqError))`: If an error occurred within the multishot reader's processing.
  /// - `None`: If this handler doesn't use multishot for this CQE, or the CQE wasn't for its reader.
  fn delegate_cqe_to_multishot_reader(
    &mut self,
    cqe: &io_uring::cqueue::Entry,
    buffer_manager: &BufferRingManager,
    worker_interface: &UringWorkerInterface<'_>,
    internal_op_tracker: &mut InternalOpTracker,
  ) -> Option<Result<(HandlerIoOps, bool), ZmqError>>;

  /// Called by `cqe_processor` (specifically `process_handler_blueprint`) after it successfully
  /// submits an SQE that was initiated by this handler's `MultishotReader` (either a new
  /// multishot read or a cancellation for one).
  ///
  /// `op_user_data`: The `UserData` assigned by the worker to the submitted SQE.
  /// `is_cancel_op`: True if the submitted SQE was an `AsyncCancel`.
  /// `target_op_data_if_cancel`: If `is_cancel_op` is true, this is the `UserData` of the
  ///                             multishot operation that was targeted for cancellation.
  fn inform_multishot_reader_op_submitted(
    &mut self,
    op_user_data: UserData,
    is_cancel_op: bool,
    target_op_data_if_cancel: Option<UserData>,
  );

  /// Returns true if this handler manages its own multishot read operations.
  /// When true, the global worker will not automatically submit standard single-shot reads for its FD.
  fn prefers_multishot_read(&self) -> bool {
    false
  }

  /// Returns true if the handler's internal receive buffers are full and reads should be throttled.
  fn should_throttle_reads(&self) -> bool {
    false
  }
}

pub trait ProtocolHandlerFactory: Send + Sync + 'static {
  fn id(&self) -> &'static str;

  // This is now the primary method for the generic worker to call
  fn create_handler(
    &self,
    fd: RawFd,
    worker_io_config: Arc<WorkerIoConfig>,
    protocol_config: &ProtocolConfig, // Takes a reference to the enum
    is_server: bool,
  ) -> Result<Box<dyn UringConnectionHandler + Send>, String>;
}

/// A message sent from a socket actor to the io_uring worker over the per-FD mpsc channel.
#[derive(Debug)]
pub enum OutgoingMessage {
  Single(Msg),
  Multipart(Vec<Msg>),
}

#[derive(Clone)]
pub struct WorkerIoConfig {
  /// Synchronous sender to the parent SocketCore mailbox.
  /// Must be the sync variant so the UringWorker OS-thread can wake Tokio tasks correctly.
  pub socket_mailbox: MailboxSyncSender,
  /// Logical endpoint URI for this connection (e.g. "tcp://1.2.3.4:5678").
  pub endpoint_uri: String,
  /// The original target URI requested by the user.
  pub target_endpoint_uri: String,
  /// The ISocketConnection interface for this connection, used in UringConnectionEstablished.
  pub connection_iface: Arc<dyn ISocketConnection>,
}
