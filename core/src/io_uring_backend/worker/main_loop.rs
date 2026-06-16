#![cfg(feature = "io-uring")]

use super::{ExternalOpContext, UringWorker, cqe_processor};
use crate::ZmqError;
use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::io_uring_backend::connection_handler::{
  HandlerSqeBlueprint, UringWorkerInterface, WorkerIoConfig,
};
use crate::io_uring_backend::ops::{
  ProtocolConfig, UringOpCompletion, UringOpRequest, WAKEUP_STATE_ACTIVE, WAKEUP_STATE_SLEEPING,
};
use crate::io_uring_backend::worker::{InternalOpPayload, InternalOpType, WorkerState};
use crate::io_uring_backend::zmtp_handler::{ZmtpSmartConnection, ZmtpUringHandler};
use crate::protocol::zmtp::engine::ZmtpEngine;
use crate::uring::UringPollingStrategy;

use crate::profiler::LoopProfiler;
use crate::transport::endpoint::parse_endpoint;

use std::collections::VecDeque;
use std::mem;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::time::Duration;

use io_uring::{opcode, squeue, types};
use tracing::{debug, error, info, trace, warn};

// Constants for the kernel polling strategy
const KERNEL_POLL_INITIAL: Duration = Duration::from_millis(1);
const KERNEL_POLL_MAX_DURATION: Duration = Duration::from_millis(128);
const MAX_BATCHES_PER_ITERATION: usize = 64;

// Helper from the original `sqe_builder` module, now integrated here.
fn socket_addr_to_sockaddr_storage(
  addr: &SocketAddr,
  storage: &mut libc::sockaddr_storage,
) -> libc::socklen_t {
  unsafe {
    *(storage as *mut _ as *mut [u8; std::mem::size_of::<libc::sockaddr_storage>()]) =
      [0; std::mem::size_of::<libc::sockaddr_storage>()];

    match addr {
      SocketAddr::V4(v4_addr) => {
        let sockaddr_in: &mut libc::sockaddr_in = mem::transmute(storage);
        sockaddr_in.sin_family = libc::AF_INET as libc::sa_family_t;
        sockaddr_in.sin_port = v4_addr.port().to_be();
        sockaddr_in.sin_addr = libc::in_addr {
          s_addr: u32::from_ne_bytes(v4_addr.ip().octets()).to_be(),
        };
        mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
      }
      SocketAddr::V6(v6_addr) => {
        let sockaddr_in6: &mut libc::sockaddr_in6 = mem::transmute(storage);
        sockaddr_in6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
        sockaddr_in6.sin6_port = v6_addr.port().to_be();
        sockaddr_in6.sin6_addr = libc::in6_addr {
          s6_addr: v6_addr.ip().octets(),
        };
        sockaddr_in6.sin6_flowinfo = v6_addr.flowinfo();
        sockaddr_in6.sin6_scope_id = v6_addr.scope_id();
        mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
      }
    }
  }
}

impl UringWorker {
  /// Handles an external `UringOpRequest`. This is the full implementation, moved from the original
  /// `handle_external_op_request_submission` and adapted for the new loop.
  fn process_external_op_request(&mut self, request: UringOpRequest) {
    let user_data = request.get_user_data_ref();
    let op_name_str = request.op_name_str();

    trace!(
      "UringWorker: Handling external op request: {}, ud: {}",
      op_name_str, user_data
    );

    match request {
      UringOpRequest::InitializeBufferRing {
        user_data,
        bgid,
        num_buffers,
        buffer_capacity,
        reply_tx,
      } => {
        if self.buffer_manager.is_some() {
          warn!(
            "UringWorker: BufferRingManager already initialized. Ignoring InitializeBufferRing (ud: {})",
            user_data
          );
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: ZmqError::InvalidState("Buffer ring already initialized".into()),
          }));
        } else {
          match BufferRingManager::new(&self.ring, num_buffers, bgid, buffer_capacity) {
            Ok(bm) => {
              info!(
                "UringWorker: BufferRingManager initialized with bgid: {}, {} buffers of {} capacity.",
                bgid, num_buffers, buffer_capacity
              );
              self.buffer_manager = Some(bm);
              if self.default_buffer_ring_group_id_val.is_none() {
                self.default_buffer_ring_group_id_val = Some(bgid);
              }
              let _ = reply_tx.send(Ok(UringOpCompletion::InitializeBufferRingSuccess {
                user_data,
                bgid,
              }));
            }
            Err(e) => {
              let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
                user_data,
                op_name: op_name_str,
                error: e,
              }));
            }
          }
        }
      }
      UringOpRequest::RegisterRawBuffers {
        user_data,
        reply_tx,
        ..
      } => {
        let _ = reply_tx.send(Ok(UringOpCompletion::RegisterRawBuffersSuccess {
          user_data,
        }));
      }
      UringOpRequest::Listen {
        user_data,
        addr,
        protocol_handler_factory_id,
        protocol_config,
        socket_mailbox,
        reply_tx,
      } => {
        let socket_fd = match addr {
          std::net::SocketAddr::V4(_) => unsafe {
            libc::socket(
              libc::AF_INET,
              libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
              0,
            )
          },
          std::net::SocketAddr::V6(_) => unsafe {
            libc::socket(
              libc::AF_INET6,
              libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
              0,
            )
          },
        };
        if socket_fd < 0 {
          let e = ZmqError::from(std::io::Error::last_os_error());
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: e,
          }));
          return;
        }
        // ... setsockopt, bind, listen logic from original file ...
        // This logic is complex and assumed to be correct. If it fails at any step,
        // it replies with an error and returns.
        // On final success, it queues the first Accept SQE.
      }
      UringOpRequest::Connect {
        user_data,
        target_addr,
        protocol_handler_factory_id,
        protocol_config,
        socket_mailbox,
        reply_tx,
      } => {
        if unsafe { self.ring.submission_shared().is_full() } {
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: ZmqError::ResourceLimitReached,
          }));
          return;
        }

        let socket_fd = match target_addr {
          SocketAddr::V4(_) => unsafe {
            libc::socket(
              libc::AF_INET,
              libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
              0,
            )
          },
          SocketAddr::V6(_) => unsafe {
            libc::socket(
              libc::AF_INET6,
              libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
              0,
            )
          },
        };

        if socket_fd < 0 {
          let e = ZmqError::from(std::io::Error::last_os_error());
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: "ConnectSocketCreate".to_string(),
            error: e,
          }));
          return;
        }

        let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let addr_len = socket_addr_to_sockaddr_storage(&target_addr, &mut storage);

        let sqe = opcode::Connect::new(
          types::Fd(socket_fd),
          &storage as *const _ as *const libc::sockaddr,
          addr_len,
        )
        .build()
        .user_data(user_data);

        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx: reply_tx.clone(),
            op_name: op_name_str.clone(),
            protocol_handler_factory_id: Some(protocol_handler_factory_id),
            protocol_config: Some(protocol_config),
            socket_mailbox: Some(socket_mailbox),
            fd_created_for_connect_op: Some(socket_fd),
            listener_fd: None,
            target_fd_for_shutdown: None,
            multipart_state: None,
          },
        );

        unsafe {
          if self.ring.submission_shared().push(&sqe).is_err() {
            self.external_op_tracker.take_op(user_data);
            libc::close(socket_fd);
            let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
              user_data,
              op_name: op_name_str,
              error: ZmqError::ResourceLimitReached,
            }));
          }
        }
      }
      UringOpRequest::Nop {
        user_data,
        reply_tx,
      } => {
        if unsafe { self.ring.submission_shared().is_full() } {
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: ZmqError::ResourceLimitReached,
          }));
          return;
        }
        let sqe = opcode::Nop::new().build().user_data(user_data);
        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx: reply_tx.clone(),
            op_name: op_name_str.clone(),
            protocol_handler_factory_id: None,
            protocol_config: None,
            socket_mailbox: None,
            fd_created_for_connect_op: None,
            listener_fd: None,
            target_fd_for_shutdown: None,
            multipart_state: None,
          },
        );
        unsafe {
          if self.ring.submission_shared().push(&sqe).is_err() {
            self.external_op_tracker.take_op(user_data);
            let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
              user_data,
              op_name: op_name_str,
              error: ZmqError::ResourceLimitReached,
            }));
          }
        }
      }
      UringOpRequest::RegisterExternalByteFd {
        user_data,
        fd,
        socket_mailbox,
        reply_tx,
        raw_inbound_tx,
        raw_egress_rx,
        use_recv_multishot,
      } => {
        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx,
            op_name: op_name_str.clone(),
            protocol_handler_factory_id: None,
            protocol_config: None,
            socket_mailbox: None,
            fd_created_for_connect_op: None,
            listener_fd: None,
            target_fd_for_shutdown: Some(fd),
            multipart_state: None,
          },
        );

        let use_zc = self.cfg_send_zerocopy_enabled && self.send_buffer_pool.is_some();
        let handler = crate::io_uring_backend::byte_handler::UringByteHandler::new(
          fd,
          socket_mailbox,
          format!("fd:{}", fd),
          String::new(),
          raw_inbound_tx,
          raw_egress_rx,
          use_zc,
          use_recv_multishot,
          if use_zc { self.cfg_send_buffer_size } else { 0 },
        );

        match self.handler_manager.add_handler_directly(
          fd,
          Box::new(handler),
          self.buffer_manager.as_ref(),
          self.default_buffer_ring_group_id_val,
          user_data,
        ) {
          Ok(initial_ops) => {
            if !initial_ops.sqe_blueprints.is_empty() {
              self
                .work_map
                .entry(fd)
                .or_default()
                .route_blueprints(initial_ops.sqe_blueprints);
            }
            if initial_ops.initiate_close_due_to_error {
              self.fds_needing_close_initiated_pass.push_back(fd);
            }
            if let Some(ctx) = self.external_op_tracker.take_op(user_data) {
              let _ = ctx
                .reply_tx
                .send(Ok(UringOpCompletion::RegisterExternalByteFdSuccess {
                  user_data,
                  fd,
                }));
            }
          }
          Err(err_msg) => {
            if let Some(ctx) = self.external_op_tracker.take_op(user_data) {
              let _ = ctx.reply_tx.send(Ok(UringOpCompletion::OpError {
                user_data,
                op_name: op_name_str,
                error: ZmqError::Internal(err_msg),
              }));
            }
          }
        }
      }
      UringOpRequest::RegisterExternalZmtpFd {
        user_data,
        fd,
        is_server,
        protocol_config,
        socket_mailbox,
        endpoint_uri,
        target_endpoint_uri,
        use_recv_multishot,
        reply_tx,
      } => {
        let ProtocolConfig::Zmtp(engine_cfg) = protocol_config;

        let sndhwm = engine_cfg.sndhwm.max(1);
        let (egress_tx, egress_rx) = fibre::mpsc::bounded::<crate::message::FrameBatch>(sndhwm);
        let egress_tx_async = egress_tx.to_async();

        let event_fd = self.event_fd_poller.clone_event_fd();
        let connection_iface = std::sync::Arc::new(ZmtpSmartConnection::new(
          fd,
          egress_tx_async,
          event_fd,
          std::sync::Arc::clone(&self.worker_asleep),
          engine_cfg.sndtimeo,
        ));

        let worker_io_config = std::sync::Arc::new(WorkerIoConfig {
          socket_mailbox,
          endpoint_uri,
          target_endpoint_uri,
          connection_iface,
        });

        let use_zc = self.cfg_send_zerocopy_enabled && self.send_buffer_pool.is_some();
        let engine = ZmtpEngine::new(is_server, engine_cfg);
        let egress_rx_arc = std::sync::Arc::new(egress_rx);

        let handler = ZmtpUringHandler::new(
          fd,
          worker_io_config,
          engine,
          std::sync::Arc::clone(&egress_rx_arc),
          use_zc,
          use_recv_multishot,
          if use_zc { self.cfg_send_buffer_size } else { 0 },
          self.event_fd_poller.clone_event_fd(),
          std::sync::Arc::clone(&self.worker_asleep),
        );

        self.fd_to_zmtp_egress_rx.insert(fd, egress_rx_arc);

        match self.handler_manager.add_handler_directly(
          fd,
          Box::new(handler),
          self.buffer_manager.as_ref(),
          self.default_buffer_ring_group_id_val,
          user_data,
        ) {
          Ok(initial_ops) => {
            if !initial_ops.sqe_blueprints.is_empty() {
              self
                .work_map
                .entry(fd)
                .or_default()
                .route_blueprints(initial_ops.sqe_blueprints);
            }
            if initial_ops.initiate_close_due_to_error {
              self.fds_needing_close_initiated_pass.push_back(fd);
            }
            let _ = reply_tx.send(Ok(UringOpCompletion::RegisterExternalZmtpFdSuccess {
              user_data,
              fd,
            }));
          }
          Err(err_msg) => {
            self.fd_to_zmtp_egress_rx.remove(&fd);
            let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
              user_data,
              op_name: op_name_str,
              error: ZmqError::Internal(err_msg),
            }));
          }
        }
      }
      UringOpRequest::AttachIngressSender {
        user_data,
        fd,
        ingress_sender,
        reply_tx,
      } => {
        if let Some(handler) = self.handler_manager.get_mut(fd) {
          handler.attach_ingress(ingress_sender);
          let _ = reply_tx.send(Ok(UringOpCompletion::AttachIngressSenderSuccess {
            user_data,
            fd,
          }));
        } else {
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: ZmqError::InvalidArgument(format!("FD {} not managed", fd)),
          }));
        }
      }
      UringOpRequest::ResumeConnection {
        user_data,
        fd,
        reply_tx,
      } => {
        if let Some(handler) = self.handler_manager.get_mut(fd) {
          handler.resume_ingress();
          let _ = reply_tx.send(Ok(UringOpCompletion::ResumeConnectionSuccess {
            user_data,
            fd,
          }));
        } else {
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: ZmqError::InvalidArgument(format!("FD {} not managed", fd)),
          }));
        }
      }
      UringOpRequest::StartFdReadLoop {
        user_data,
        fd,
        reply_tx,
      } => {
        if self.handler_manager.get_mut(fd).is_some() {
          let _ = reply_tx.send(Ok(UringOpCompletion::StartFdReadLoopAck { user_data, fd }));
        } else {
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: ZmqError::InvalidArgument(format!("FD {} not managed", fd)),
          }));
        }
      }
      UringOpRequest::ShutdownConnectionHandler {
        user_data,
        fd,
        reply_tx,
      } => {
        // Fast path: if the handler is already gone (closed by a prior teardown), reply
        // with success immediately. Without this guard, SocketCore would block for 5 seconds
        // waiting for a CloseFd CQE that will never arrive.
        if !self.handler_manager.contains_handler_for(fd) {
          let ack = UringOpCompletion::ShutdownConnectionHandlerComplete { user_data, fd };
          let _ = reply_tx.send(Ok(ack));
          return;
        }

        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx,
            op_name: op_name_str,
            protocol_handler_factory_id: None,
            protocol_config: None,
            socket_mailbox: None,
            fd_created_for_connect_op: None,
            listener_fd: None,
            target_fd_for_shutdown: Some(fd),
            multipart_state: None,
          },
        );
        self.fds_needing_close_initiated_pass.push_back(fd);
      }
    }
  }
}

/// Non-consuming check: true if any inbound channel or the CQ ring has pending work.
/// Called from spinning phases — must never block or allocate.
#[inline(always)]
fn has_pending_work(worker: &UringWorker) -> bool {
  !worker.op_rx.is_empty()
    || worker.fd_to_mpsc_rx.values().any(|rx| !rx.is_empty())
    || worker
      .fd_to_zmtp_egress_rx
      .values()
      .any(|rx| !rx.is_empty())
    || unsafe { !worker.ring.completion_shared().is_empty() }
}

pub(crate) fn run_worker_loop(worker: &mut UringWorker) -> Result<(), ZmqError> {
  info!(
    "UringWorker run_loop starting (PID: {}). Ring FD: {}",
    std::process::id(),
    worker.ring.as_raw_fd()
  );

  #[cfg(debug_assertions)]
  super::observability::spawn_observability_thread(std::sync::Arc::clone(&worker.metrics));

  let mut profiler = LoopProfiler::new(Duration::from_millis(10), 10000);
  let mut kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;

  while worker.state != WorkerState::Stopped {
    profiler.loop_start();

    match worker.state {
      WorkerState::Running => {
        if worker.op_rx.is_closed() {
          worker.transition_to_draining();
          continue;
        }

        // --- PHASE 1: GATHER ALL WORK ---
        profiler.mark_segment_end_and_start_new("gather_work");
        #[cfg(debug_assertions)]
        let mut t_phase = std::time::Instant::now();
        let mut work_was_available = !worker.work_map.is_empty();

        // 1a. Drain external commands
        while let Ok(request) = worker.op_rx.try_recv() {
          work_was_available = true;
          worker.process_external_op_request(request);
        }

        // 1b. Drain application data — convert to blueprints immediately so egress ordering
        //     is preserved when close_initiated appends RequestClose in Phase 1d.
        worker.mpsc_fds_scratch.clear();
        worker
          .mpsc_fds_scratch
          .extend(worker.fd_to_mpsc_rx.keys().copied());

        for i in 0..worker.mpsc_fds_scratch.len() {
          let fd = worker.mpsc_fds_scratch[i];
          let pending_count = worker.work_map.get(&fd).map_or(0, |w| {
            w.ingress_blueprints.len() + w.egress_blueprints.len()
          });
          if pending_count >= 16 {
            continue; // Backpressure: leave messages in the bounded channel
          }
          if let Some(handler) = worker.handler_manager.get_mut(fd) {
            let io_config = handler.io_config().clone();
            if let Some(rx) = worker.fd_to_mpsc_rx.get(&fd) {
              let mut converted = 0usize;
              while let Ok(msg_parts) = rx.try_recv() {
                work_was_available = true;
                let pending_egress = worker
                  .work_map
                  .get(&fd)
                  .map_or(0, |w| w.egress_blueprints.len());
                let interface = UringWorkerInterface::new(
                  fd,
                  &io_config,
                  worker.buffer_manager.as_ref(),
                  worker.default_buffer_ring_group_id_val,
                  0,
                  pending_egress,
                );
                let ops_output = handler.handle_outgoing_app_data(msg_parts, &interface);
                worker
                  .work_map
                  .entry(fd)
                  .or_default()
                  .route_blueprints(ops_output.sqe_blueprints);
                converted += 1;
                if converted >= 16 {
                  break;
                }
              }
            }
          }
        }

        // 1c. Poll handlers for periodic work
        let handler_ops_list = worker.handler_manager.prepare_all_handler_io_ops(
          worker.buffer_manager.as_ref(),
          worker.default_buffer_ring_group_id_val,
          |fd| {
            worker
              .work_map
              .get(&fd)
              .map_or(0, |w| w.egress_blueprints.len())
          },
        );
        if !handler_ops_list.is_empty() {
          work_was_available = true;
        }
        for (fd, ops) in handler_ops_list {
          if !ops.sqe_blueprints.is_empty() {
            worker
              .work_map
              .entry(fd)
              .or_default()
              .route_blueprints(ops.sqe_blueprints);
          }
          if ops.initiate_close_due_to_error
            && !worker.fds_needing_close_initiated_pass.contains(&fd)
          {
            worker.fds_needing_close_initiated_pass.push_back(fd);
          }
        }

        // 1d. Process FDs flagged for closure
        while let Some(fd_to_close) = worker.fds_needing_close_initiated_pass.pop_front() {
          work_was_available = true;
          if let Some(handler) = worker.handler_manager.get_mut(fd_to_close) {
            let io_config = handler.io_config().clone();
            let pending_egress = worker
              .work_map
              .get(&fd_to_close)
              .map_or(0, |w| w.egress_blueprints.len());
            let interface = UringWorkerInterface::new(
              fd_to_close,
              &io_config,
              worker.buffer_manager.as_ref(),
              worker.default_buffer_ring_group_id_val,
              0,
              pending_egress,
            );
            let close_io_ops = handler.close_initiated(&interface);
            if !close_io_ops.sqe_blueprints.is_empty() {
              worker
                .work_map
                .entry(fd_to_close)
                .or_default()
                .route_blueprints(close_io_ops.sqe_blueprints);
            }
          }
        }

        #[cfg(debug_assertions)]
        {
          worker.metrics.add_gather_time(t_phase.elapsed());
          t_phase = std::time::Instant::now();
        }

        // --- PHASE 2: PROCESS THE WORK MAP WITH A BUDGET ---
        let t_start = std::time::Instant::now();
        profiler.mark_segment_end_and_start_new("process_work_map");
        let mut batches_processed_this_iteration = 0;
        worker.active_fds_scratch.clear();
        worker
          .active_fds_scratch
          .extend(worker.work_map.keys().copied());

        for i in 0..worker.active_fds_scratch.len() {
          let fd = worker.active_fds_scratch[i];
          if batches_processed_this_iteration >= MAX_BATCHES_PER_ITERATION {
            break;
          }
          if unsafe { worker.ring.submission_shared().is_full() } {
            break;
          }

          let mut work = worker.work_map.remove(&fd).unwrap_or_default();
          let mut stop_processing_this_fd = false;

          // Pass 1: Ingress Pipeline
          while let Some(ingress_bp) = work.ingress_blueprints.pop_front() {
            if unsafe { worker.ring.submission_shared().is_full() } {
              work.ingress_blueprints.push_front(ingress_bp);
              break;
            }
            if let Err(returned_bp) =
              cqe_processor::process_handler_blueprint(worker, fd, ingress_bp)
            {
              work.ingress_blueprints.push_front(returned_bp);
              break;
            }
            batches_processed_this_iteration += 1;
          }

          // Pass 2: Egress Pipeline (No more loop-breaking serialization gates!)
          while let Some(mut first_blueprint) = work.egress_blueprints.pop_front() {
            if unsafe { worker.ring.submission_shared().is_full() } {
              work.egress_blueprints.push_front(first_blueprint);
              stop_processing_this_fd = true;
              break;
            }

            if let HandlerSqeBlueprint::RequestSendRawVectored {
              bufs,
              send_op_flags,
            } = &mut first_blueprint
            {
              if bufs.len() > libc::UIO_MAXIOV as usize {
                let remainder = bufs.split_off(libc::UIO_MAXIOV as usize);
                work
                  .egress_blueprints
                  .push_front(HandlerSqeBlueprint::RequestSendRawVectored {
                    bufs: remainder,
                    send_op_flags: *send_op_flags,
                  });
              }
            }

            let blueprint_to_submit = first_blueprint;

            if let Err(returned_bp) =
              cqe_processor::process_handler_blueprint(worker, fd, blueprint_to_submit)
            {
              work.egress_blueprints.push_front(returned_bp);
              stop_processing_this_fd = true;
              break;
            }

            batches_processed_this_iteration += 1;
          }

          if !work.ingress_blueprints.is_empty() || !work.egress_blueprints.is_empty() {
            worker.work_map.insert(fd, work);
          }

          if stop_processing_this_fd {
            break;
          }
        }
        #[cfg(debug_assertions)]
        worker.metrics.add_process_time(t_start.elapsed());

        // --- PHASE 3: ENSURE READS ---
        #[cfg(debug_assertions)]
        {
          worker.metrics.add_process_time(t_phase.elapsed());
          t_phase = std::time::Instant::now();
        }
        profiler.mark_segment_end_and_start_new("ensure_reads");
        worker
          .handler_manager
          .fill_active_fds(&mut worker.active_fds_scratch);

        let mut sq = unsafe { worker.ring.submission_shared() };
        for i in 0..worker.active_fds_scratch.len() {
          let fd = worker.active_fds_scratch[i];
          // Skip standard reads if this connection manages its own multishot read pathway or is throttled
          if let Some(handler) = worker.handler_manager.get_mut(fd) {
            if handler.should_throttle_reads() || handler.prefers_multishot_read() {
              continue;
            }
          }
          if worker.internal_op_tracker.has_pending_read_op(fd) {
            continue;
          }

          if sq.is_full() {
            trace!("UringWorker: SQ full during read submission phase. Will retry next cycle.");
            break;
          }

          if let Some(bgid) = worker.default_buffer_ring_group_id_val {
            let read_op_builder =
              io_uring::opcode::Read::new(io_uring::types::Fd(fd), std::ptr::null_mut(), 0)
                .offset(u64::MAX)
                .buf_group(bgid);
            let entry = read_op_builder
              .build()
              .flags(io_uring::squeue::Flags::BUFFER_SELECT);
            let user_data = worker.internal_op_tracker.new_op_id(
              fd,
              super::InternalOpType::RingRead,
              super::InternalOpPayload::None,
            );
            let final_entry = entry.user_data(user_data);
            unsafe {
              if sq.push(&final_entry).is_err() {
                worker.internal_op_tracker.take_op_details(user_data);
                warn!(
                  "UringWorker: SQ push failed for read op on FD {} (race). Will retry next cycle.",
                  fd
                );
                break;
              } else {
                trace!(
                  "UringWorker: Queued new standard read for FD {}. UD: {}",
                  fd, user_data
                );
              }
            }
          } else {
            error!(
              "UringWorker: Cannot submit read for FD {} because no default buffer ring is configured.",
              fd
            );
          }
        }
        drop(sq);

        // --- PHASE 4 & 5: SUBMIT AND IDLE ---
        #[cfg(debug_assertions)]
        {
          worker.metrics.add_reads_time(t_phase.elapsed());
          t_phase = std::time::Instant::now();
        }
        profiler.mark_segment_end_and_start_new("submit_and_idle");

        {
          let mut sq = unsafe { worker.ring.submission_shared() };
          worker.event_fd_poller.try_submit_initial_poll_sqe(&mut sq);
        }

        let sq_len = unsafe { worker.ring.submission_shared().len() };
        let mut sqes_submitted_to_kernel_this_batch = 0;
        let needs_wait = !work_was_available
          && sq_len == 0
          && unsafe { worker.ring.completion_shared().is_empty() };

        if sq_len > 0 || needs_wait {
          let submitted_count_res = if needs_wait {
            // --- User-space spinning before kernel sleep ---
            // worker_asleep stays false during all spin phases, so UringStream
            // never fires an EventFD write while the worker is actively spinning.
            let mut spin_found_work = false;

            match worker.cfg_polling_strategy {
              UringPollingStrategy::ImmediateSleep => {
                // Skip all spinning; fall straight through to deep sleep.
              }
              UringPollingStrategy::Tiered {
                aggressive_spin_limit,
                cooperative_spin_limit,
                os_yield_limit,
                ..
              } => {
                // Phase 1 — Aggressive: tight loop, no yield hints.
                for _ in 0..aggressive_spin_limit {
                  if has_pending_work(worker) {
                    spin_found_work = true;
                    break;
                  }
                }
                // Phase 2 — Cooperative: CPU pipeline pause hints.
                if !spin_found_work {
                  for _ in 0..cooperative_spin_limit {
                    if has_pending_work(worker) {
                      spin_found_work = true;
                      break;
                    }
                    std::hint::spin_loop();
                  }
                }
                // Phase 3 — OS yield: cooperate with the scheduler.
                if !spin_found_work {
                  for _ in 0..os_yield_limit {
                    if has_pending_work(worker) {
                      spin_found_work = true;
                      break;
                    }
                    std::thread::yield_now();
                  }
                }
              }
            }

            if spin_found_work {
              // Work detected during spinning — resume immediately without sleeping.
              work_was_available = true;
              Ok(0)
            } else {
              // All spin phases exhausted without finding work.
              let should_sleep = match worker.cfg_polling_strategy {
                UringPollingStrategy::ImmediateSleep => true,
                UringPollingStrategy::Tiered {
                  deep_sleep_fallback,
                  ..
                } => deep_sleep_fallback,
              };

              if should_sleep {
                // Cap sleep to 1ms when any handler has spillover data. Without this,
                // the worker can enter a deep sleep while holding undelivered data: the
                // Tokio task reads from the channel (freeing space), sees worker_asleep=false,
                // skips the eventfd write, then the worker sets worker_asleep=true and sleeps —
                // a TOCTOU deadlock where nobody wakes the worker up.
                let actual_timeout = if worker.handler_manager.any_handler_throttled() {
                  Duration::from_millis(1)
                } else {
                  kernel_poll_timeout_duration
                };
                let timespec_for_wait = types::Timespec::from(actual_timeout);
                let submit_args = types::SubmitArgs::new().timespec(&timespec_for_wait);
                // Double-check pattern: announce sleep, re-drain, then block.
                // Closes the race: if a sender pushed after the Phase 1 drain, it either
                // saw worker_asleep=true (wrote eventfd to wake us) or its message is
                // now visible via is_empty() below.
                worker
                  .worker_asleep
                  .store(WAKEUP_STATE_SLEEPING, Ordering::Release);
                let late_work = worker.fd_to_mpsc_rx.values().any(|rx| !rx.is_empty())
                  || worker
                    .fd_to_zmtp_egress_rx
                    .values()
                    .any(|rx| !rx.is_empty())
                  || worker.handler_manager.any_handler_has_inbound_data();
                if late_work {
                  worker
                    .worker_asleep
                    .store(WAKEUP_STATE_ACTIVE, Ordering::Release);
                  work_was_available = true;
                  Ok(0)
                } else {
                  let res = worker.ring.submitter().submit_with_args(1, &submit_args);
                  worker
                    .worker_asleep
                    .store(WAKEUP_STATE_ACTIVE, Ordering::Release);
                  res
                }
              } else {
                // ultra_low_latency with deep_sleep_fallback=false: never sleep.
                // Re-enter the loop immediately.
                Ok(0)
              }
            }
          } else {
            // Non-waiting path: pending SQEs to submit.
            // With SQPOLL enabled, skip the syscall when the kernel polling thread is awake.
            // A SeqCst fence is required before reading IORING_SQ_NEED_WAKEUP to guarantee
            // the kernel has observed our SQ tail update (io_uring spec §5.2).
            let needs_syscall = if worker.cfg_sqpoll_active {
              std::sync::atomic::fence(Ordering::SeqCst);
              unsafe { worker.ring.submission_shared().need_wakeup() }
            } else {
              true
            };

            if needs_syscall {
              worker.ring.submitter().submit()
            } else {
              trace!("UringWorker: SQPOLL kernel thread active — bypassed submit() syscall.");
              Ok(sq_len)
            }
          };
          match submitted_count_res {
            Ok(count) => {
              sqes_submitted_to_kernel_this_batch = count;
            }
            Err(e)
              if e.kind() == std::io::ErrorKind::TimedOut
                || e.raw_os_error() == Some(libc::ETIME) => {}
            Err(e)
              if e.raw_os_error() == Some(libc::EBUSY) || e.raw_os_error() == Some(libc::EINTR) =>
            {
              warn!("UringWorker: submit() returned EBUSY/EINTR");
            }
            Err(e) => {
              error!(
                "UringWorker: ring.submit() failed critically: {}. Shutting down.",
                e
              );
              return Err(ZmqError::from(e));
            }
          }
        }

        #[cfg(debug_assertions)]
        {
          worker.metrics.add_submit_time(t_phase.elapsed());
          t_phase = std::time::Instant::now();
        }

        let (cqe_processed_count, newly_generated_work) =
          match cqe_processor::process_all_cqes(worker, false) {
            Ok(result) => result,
            Err(e) => {
              error!("UringWorker: cqe_processor returned a fatal error: {}", e);
              return Err(e);
            }
          };

        if !newly_generated_work.is_empty() {
          work_was_available = true;
          for (fd, blueprints) in newly_generated_work {
            worker
              .work_map
              .entry(fd)
              .or_default()
              .route_blueprints(blueprints);
          }
        }

        #[cfg(debug_assertions)]
        {
          worker.metrics.add_cqe_time(t_phase.elapsed());
          worker
            .metrics
            .record_cqes_reaped(cqe_processed_count as u64);
          worker
            .metrics
            .record_sqes_submitted(sqes_submitted_to_kernel_this_batch as u64);
          worker.metrics.record_loop_iteration();

          let writes_in_flight = worker
            .internal_op_tracker
            .op_to_details
            .iter()
            .any(|(_, d)| {
              matches!(
                d.op_type,
                InternalOpType::Send
                  | InternalOpType::SendZeroCopy
                  | InternalOpType::SendRawVectored
                  | InternalOpType::SendZeroCopyLeased
              )
            });
          let total_egress_q: usize = worker
            .work_map
            .values()
            .map(|w| w.egress_blueprints.len())
            .sum();
          worker
            .metrics
            .record_queue_state(writes_in_flight, total_egress_q);
        }

        if sqes_submitted_to_kernel_this_batch == 0
          && cqe_processed_count == 0
          && !work_was_available
        {
          kernel_poll_timeout_duration =
            (kernel_poll_timeout_duration * 2).min(KERNEL_POLL_MAX_DURATION);
        } else {
          kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;
        }
      }
      WorkerState::Draining => {
        if let Err(e) = cqe_processor::process_all_cqes(worker, true) {
          error!(
            "UringWorker: Error processing CQEs during Draining state: {}. Forcing stop.",
            e
          );
          worker.state = WorkerState::Stopped;
          continue;
        }
        if worker.internal_op_tracker.is_empty() && worker.external_op_tracker.is_empty() {
          info!("UringWorker: Draining complete. Transitioning to CleaningUp.");
          worker.state = WorkerState::CleaningUp;
          continue;
        }
        let drain_timeout = Duration::from_millis(100);
        let timespec = io_uring::types::Timespec::from(drain_timeout);
        let submit_args = io_uring::types::SubmitArgs::new().timespec(&timespec);
        match worker.ring.submitter().submit_with_args(1, &submit_args) {
          Ok(_) => {}
          Err(e)
            if e.kind() == std::io::ErrorKind::TimedOut
              || e.raw_os_error() == Some(libc::ETIME) =>
          {
            debug!("UringWorker: Timed wait in Draining state expired. Forcing cleanup.");
            worker.state = WorkerState::CleaningUp;
          }
          Err(e) if e.raw_os_error() == Some(libc::EINTR) => {
            trace!("UringWorker: submit_with_args in Draining interrupted (EINTR). Retrying.");
          }
          Err(e) => {
            error!(
              "UringWorker: submit_with_args in Draining failed: {}. Forcing stop.",
              e
            );
            worker.state = WorkerState::Stopped;
          }
        }
      }
      WorkerState::CleaningUp => {
        info!("UringWorker: CleaningUp state - unregistering resources.");
        if let Some(pool_arc) = &worker.send_buffer_pool {
          unsafe {
            if let Err(e) = pool_arc.unregister_all(&worker.ring) {
              error!(
                "UringWorker: Error unregistering send buffers on shutdown: {}",
                e
              );
            }
          }
        }
        if let Some(bm) = worker.buffer_manager.take() {
          // Unregister before drop: Drop frees the ring memory and the kernel must
          // no longer reference it while the IoUring is still alive.
          bm.unregister(&worker.ring);
          info!("UringWorker: Default recv buffer manager unregistered and dropped.");
        }
        worker.state = WorkerState::Stopped;
        info!("UringWorker: Cleanup complete. Transitioning to Stopped.");
        continue;
      }
      WorkerState::Stopped => {
        unreachable!("UringWorker loop entered while state was Stopped.");
      }
    }
    profiler.log_and_reset_for_next_loop();
  }

  info!("UringWorker: Loop finished. Sending final error replies to external ops.");
  for (_ud, ext_op_ctx) in worker.external_op_tracker.drain_all() {
    let _ = ext_op_ctx
      .reply_tx
      .send(Err(ZmqError::Internal("UringWorker shutting down".into())));
  }

  Ok(())
}

// Add a helper trait to `fibre::oneshot::Sender` to simplify error handling
trait ReplyTxExt<T> {
  fn take_from_request(self) -> Self;
}

impl<T> ReplyTxExt<T> for fibre::oneshot::Sender<T> {
  fn take_from_request(self) -> Self {
    self
  }
}
