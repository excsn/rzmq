// core/src/io_uring_backend/connection_handler.rs

#![cfg(feature = "io-uring")]

use crate::message::Msg;
use crate::ZmqError;
use std::any::Any;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use kanal::Sender as KanalSender;
use bytes::Bytes; // For HandlerSqeBlueprint::RequestSend

// --- Blueprints for SQEs requested by handlers ---
#[derive(Debug, Clone)]
pub enum HandlerSqeBlueprint {
    /// Request a ring-buffered read for the handler's FD.
    /// The UringWorker will use the default_buffer_ring_group_id.
    RequestRingRead,
    /// Request to send data. The UringWorker will build a Send SQE.
    RequestSend { data: Bytes }, // Bytes is cheap to clone
    /// Request to close the handler's FD. The UringWorker will build a Close SQE.
    RequestClose,
    // Potentially others: RequestPollAdd, RequestTimeout, etc.
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
    pub fn new() -> Self { Self::default() }
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
// This is still useful if the UringWorker itself needs to push many SQEs directly.
// Not directly used by handlers in Alternative A.
pub struct SubmissionQueueWriter<'sq_borrow> {
  sq: &'sq_borrow mut io_uring::squeue::SubmissionQueue<'sq_borrow>,
}
impl<'sq_borrow> SubmissionQueueWriter<'sq_borrow> {
  pub(crate) fn new(sq: &'sq_borrow mut io_uring::squeue::SubmissionQueue<'sq_borrow>) -> Self { Self { sq } }
  pub fn push(&mut self, entry: &io_uring::squeue::Entry) -> Result<(), String> {
    unsafe { self.sq.push(entry) }
        .map(|_| ())
        .map_err(|e| format!("SQ push error: {:?}", e))
  }
  pub fn is_full(&self) -> bool { unsafe { self.sq.is_full() } }
}


// --- UringWorkerInterface (Lean Version for Alternative A) ---
// 'iface_life: Lifetime of the UringWorkerInterface instance itself.
// 'cfg_life: Lifetime of the borrowed configurations.
pub struct UringWorkerInterface<'cfg_life> {
    pub fd: RawFd,
    pub worker_io_config: &'cfg_life WorkerIoConfig,
    pub buffer_manager: Option<&'cfg_life super::buffer_manager::BufferRingManager>,
    pub default_bgid_for_handler_use: Option<u16>,
}

impl<'cfg_life> UringWorkerInterface<'cfg_life> {
    pub(crate) fn new(
        fd: RawFd,
        worker_io_config: &'cfg_life WorkerIoConfig,
        buffer_manager: Option<&'cfg_life super::buffer_manager::BufferRingManager>,
        default_bgid_for_handler_use: Option<u16>, 
    ) -> Self {
        Self { fd, worker_io_config, buffer_manager, default_bgid_for_handler_use }
    }
    // Methods for handlers to get info, but not to directly queue ops.
    pub fn fd(&self) -> RawFd { self.fd }
    pub fn default_buffer_group_id(&self) -> Option<u16> {
        self.default_bgid_for_handler_use
    }
}


// --- UringConnectionHandler Trait (Modified for Alternative A) ---
pub trait UringConnectionHandler: Send {
    fn fd(&self) -> RawFd;

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
        data: Arc<dyn Any + Send + Sync>,
        interface: &UringWorkerInterface<'_>
    ) -> HandlerIoOps;

    /// Called by the worker when it intends to close this handler's connection
    /// (e.g., due to external request or fatal error elsewhere).
    /// Handler should prepare to shut down and can return a `RequestClose` blueprint.
    fn close_initiated(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps;

    /// Final notification that the FD has been closed by the worker.
    /// Handler should perform any final cleanup.
    fn fd_has_been_closed(&mut self);
}

pub trait ProtocolHandlerFactory: Send + Sync + 'static {
    fn id(&self) -> &'static str;

    // This is now the primary method for the generic worker to call
    fn create_handler( // Renamed from create_handler_from_enum_config for simplicity
        &self,
        fd: RawFd,
        worker_io_config: Arc<WorkerIoConfig>,
        protocol_config: &ProtocolConfig, // Takes a reference to the enum
        is_server: bool,
    ) -> Result<Box<dyn UringConnectionHandler + Send>, String>;
}

#[derive(Clone)]
pub struct WorkerIoConfig {
   pub parsed_msg_tx_zmtp: KanalSender<(RawFd, Result<Msg, ZmqError>)>,
}

// UserData re-export from ops.rs
pub use crate::io_uring_backend::ops::UserData;

use super::ops::ProtocolConfig;