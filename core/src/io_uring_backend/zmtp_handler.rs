#![cfg(feature = "io-uring")]

use std::any::Any;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, error, info, trace, warn};

use crate::io_uring_backend::{
    buffer_manager::BufferRingManager,
    connection_handler::{
        HandlerIoOps, HandlerSqeBlueprint, UringConnectionHandler, UringWorkerInterface,
        WorkerIoConfig,
    },
    ops::{UserData, HANDLER_INTERNAL_SEND_OP_UD, WAKEUP_STATE_SLEEPING, WAKEUP_STATE_SIGNALED},
    worker::{InternalOpTracker, MultishotReader},
};
use crate::message::FrameBatch;
use crate::protocol::zmtp::{
    actions::{AppAction, EngineOutput, NetAction},
    engine::ZmtpEngine,
};
use crate::runtime::Command;
use crate::socket::connection_iface::ISocketConnection;
use crate::ZmqError;

const ZC_SEND_THRESHOLD: usize = 1024;

/// `ISocketConnection` implementation for the io_uring smart path.
///
/// SocketCore calls `send_multipart(batch)` on this; it pushes the `FrameBatch`
/// into the bounded sync egress channel (non-blocking `try_send`) and signals the
/// worker if it is sleeping. Full channel → `ResourceLimitReached` (HWM enforced).
pub(crate) struct ZmtpSmartConnection {
    fd: RawFd,
    egress_tx: fibre::mpsc::BoundedSender<FrameBatch>,
    event_fd: eventfd::EventFD,
    worker_asleep: Arc<AtomicU8>,
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
        egress_tx: fibre::mpsc::BoundedSender<FrameBatch>,
        event_fd: eventfd::EventFD,
        worker_asleep: Arc<AtomicU8>,
    ) -> Self {
        Self { fd, egress_tx, event_fd, worker_asleep }
    }

    fn signal_worker(&self) {
        if self.worker_asleep.load(Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
            if self.worker_asleep.compare_exchange(
                WAKEUP_STATE_SLEEPING,
                WAKEUP_STATE_SIGNALED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ).is_ok() {
                if let Err(e) = self.event_fd.write(1) {
                    error!("ZmtpSmartConnection fd={}: failed to signal worker eventfd: {}", self.fd, e);
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
            Err(fibre::TrySendError::Full(_)) => Err(ZmqError::ResourceLimitReached),
            Err(_) => Err(ZmqError::ConnectionClosed),
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
    /// Taken once on `HandshakeComplete` and forwarded to SocketCore in `UringConnectionEstablished`.
    inbound_data_rx: Option<fibre::mpsc::BoundedAsyncReceiver<FrameBatch>>,
    multishot_reader: Option<MultishotReader>,
    is_closing: bool,
    /// Non-blocking delayed close (replaces `thread::sleep`).
    /// Armed when `NetAction::ScheduleClose(Some(delay))` fires; `prepare_sqes` polls it.
    close_deadline: Option<Instant>,
    use_send_zerocopy: bool,
    use_recv_multishot: bool,
    send_buffer_slot_size: usize,
}

impl ZmtpUringHandler {
    pub(crate) fn new(
        fd: RawFd,
        worker_io_config: Arc<WorkerIoConfig>,
        engine: ZmtpEngine,
        egress_rx: Arc<fibre::mpsc::BoundedReceiver<FrameBatch>>,
        inbound_data_rx: fibre::mpsc::BoundedAsyncReceiver<FrameBatch>,
        use_send_zerocopy: bool,
        use_recv_multishot: bool,
        send_buffer_slot_size: usize,
    ) -> Self {
        Self {
            fd,
            worker_io_config,
            engine,
            egress_rx,
            inbound_data_rx: Some(inbound_data_rx),
            multishot_reader: None,
            is_closing: false,
            close_deadline: None,
            use_send_zerocopy,
            use_recv_multishot,
            send_buffer_slot_size,
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
                        ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSendZeroCopy {
                            data_to_send: data,
                            send_op_flags: 0,
                            originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                        });
                    } else {
                        ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend {
                            data,
                            send_op_flags: 0,
                            originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                        });
                    }
                }
                NetAction::SetCork(enable) => {
                    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSetCork(enable));
                }
                NetAction::ScheduleClose(delay) => {
                    if let Some(d) = delay {
                        // Non-blocking: uncork to flush TCP, then arm deadline.
                        ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSetCork(false));
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
                AppAction::HandshakeComplete { peer_identity, peer_socket_type } => {
                    if let Some(inbound_rx) = self.inbound_data_rx.take() {
                        let cmd = Command::UringConnectionEstablished {
                            endpoint_uri: self.worker_io_config.endpoint_uri.clone(),
                            target_endpoint_uri: self.worker_io_config.target_endpoint_uri.clone(),
                            connection_iface: self.worker_io_config.connection_iface.clone(),
                            peer_identity,
                            inbound_data_rx: Some(inbound_rx),
                            fd: self.fd,
                        };
                        if let Err(e) = self.worker_io_config.socket_mailbox.try_send(cmd) {
                            warn!(
                                fd = self.fd,
                                "ZmtpUringHandler: failed to send UringConnectionEstablished to SocketCore: {:?}", e
                            );
                        }
                    }
                }
                AppAction::DeliverMessage(batch) => {
                    if let Err(_e) = self.worker_io_config.inbound_data_tx.try_send(batch) {
                        trace!(fd = self.fd, "ZmtpUringHandler: inbound_data_tx full, dropping batch");
                    }
                }
                AppAction::PeerError(e) => {
                    warn!(fd = self.fd, err = %e, "ZmtpUringHandler: peer error");
                    self.is_closing = true;
                    let _ = self.worker_io_config.socket_mailbox.try_send(
                        Command::UringFdError {
                            endpoint_uri: self.worker_io_config.endpoint_uri.clone(),
                            error: e,
                        },
                    );
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
                warn!(fd = self.fd, "ZmtpUringHandler: no default buffer group — multishot reads disabled");
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
        trace!(fd = self.fd, len = bytes.len(), "ZmtpUringHandler: process_ring_read_bytes");

        if bytes.is_empty() {
            info!(fd = self.fd, "ZmtpUringHandler: EOF from peer");
            self.is_closing = true;
            let _ = self.worker_io_config.socket_mailbox.try_send(Command::UringFdError {
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
        _interface: &UringWorkerInterface<'_>,
    ) -> HandlerIoOps {
        if cqe_result < 0 {
            error!(fd = self.fd, errno = -cqe_result, "ZmtpUringHandler: send SQE failed");
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

        let mut ops = HandlerIoOps::new();

        // (a) Re-arm multishot read.
        if !self.is_closing && !self.should_throttle_reads() {
            if let Some(reader) = &mut self.multishot_reader {
                if let Some(blueprint) = reader.prepare_recv_multi_intent() {
                    ops.sqe_blueprints.push(blueprint);
                }
            }
        }

        // (b) Drain egress channel — call engine.on_app_message for each batch.
        if !self.is_closing && interface.pending_egress_count < 16 {
            let pull_limit = 16 - interface.pending_egress_count;
            let mut pulled = 0;
            while pulled < pull_limit {
                match self.egress_rx.try_recv() {
                    Ok(batch) => {
                        let out = self.engine.on_app_message(batch);
                        let more = self.apply_engine_output(out);
                        ops.sqe_blueprints.extend(more.sqe_blueprints);
                        if more.initiate_close_due_to_error {
                            ops.initiate_close_due_to_error = true;
                            break;
                        }
                        pulled += 1;
                    }
                    Err(_) => break,
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
        false
    }

    /// Returns true when there is pending egress data or a close deadline is armed,
    /// so the worker's pre-sleep double-check keeps it awake to process these.
    fn has_drainable_spillover(&self) -> bool {
        !self.egress_rx.is_empty() || self.close_deadline.is_some()
    }

    fn on_buffer_ring_exhausted(&mut self) {
        trace!(fd = self.fd, "ZmtpUringHandler: buffer ring exhausted (ENOBUFS) — transient");
    }
}
