#![cfg(feature = "io-uring")]

use super::{cqe_processor, ExternalOpContext, UringWorker};
use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::io_uring_backend::connection_handler::{UringWorkerInterface};
use crate::io_uring_backend::ops::{UringOpCompletion, UringOpRequest};
use crate::io_uring_backend::worker::{InternalOpPayload, InternalOpType, WorkItem, WorkerState};
use crate::io_uring_backend::UserData;
use crate::profiler::LoopProfiler;
use crate::ZmqError;

use std::os::fd::AsRawFd;
use std::os::unix::io::RawFd;
use std::time::Duration;

use io_uring::{opcode, types};
use tracing::{debug, error, info, trace, warn};

const KERNEL_POLL_INITIAL: Duration = Duration::from_millis(1);
const KERNEL_POLL_MAX_DURATION: Duration = Duration::from_millis(512);

impl UringWorker {
  /// Handles an external UringOpRequest: may build an SQE, or handle the request directly
  /// by changing worker state or interacting with managers.
  ///
  /// # Arguments
  /// * `request`: The UringOpRequest to handle (taken by value).
  ///
  /// # Returns
  /// * `Ok(true)` if an SQE was built and pushed by this function, needing kernel submission.
  /// * `Ok(false)` if op was handled directly, or error replied, or delegated and SQEs queued by helper.
  /// * `Err(UringOpRequest)` if SQ was full when trying to submit an SQE directly for this request,
  ///   and the original request (passed by value) should be re-queued by the caller.
  fn handle_external_op_request_submission(
    &mut self,
    request: UringOpRequest,
  ) -> Result<bool, UringOpRequest> {
    let user_data_from_req = request.get_user_data_ref();
    let op_name_str = request.op_name_str();

    trace!(
      "UringWorker: Handling external op request: {}, ud: {}",
      op_name_str,
      user_data_from_req
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
          match BufferRingManager::new(
            &self.ring,
            num_buffers,
            bgid,
            buffer_capacity,
          ) {
            Ok(bm) => {
              info!(
                "UringWorker: BufferRingManager initialized with bgid: {}, {} buffers of {} capacity.",
                bgid, num_buffers, buffer_capacity
              );
              self.buffer_manager = Some(bm);
              if self.default_buffer_ring_group_id_val.is_none() {
                self.default_buffer_ring_group_id_val = Some(bgid);
                debug!(
                  "UringWorker: Worker's default buffer group ID set to {}",
                  bgid
                );
              } else {
                warn!(
                  "UringWorker: Worker's default buffer group ID was already set. New bgid {} not made default.",
                  bgid
                );
              }
              let _ = reply_tx.send(Ok(UringOpCompletion::InitializeBufferRingSuccess {
                user_data,
                bgid,
              }));
            }
            Err(e) => {
              error!("UringWorker: Failed to initialize BufferRingManager: {}", e);
              let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
                user_data,
                op_name: op_name_str,
                error: e,
              }));
            }
          }
        }
        Ok(false)
      }
      UringOpRequest::RegisterRawBuffers {
        user_data,
        reply_tx,
        ..
      } => {
        warn!("UringWorker: RegisterRawBuffers direct ring call not fully implemented. Sending placeholder ack.");
        let _ = reply_tx.send(Ok(UringOpCompletion::RegisterRawBuffersSuccess {
          user_data,
        }));
        Ok(false)
      }
      UringOpRequest::Listen {
        user_data,
        addr,
        ref protocol_handler_factory_id,
        protocol_config,
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
          error!("Listen(ud:{}): Failed to create socket: {}", user_data, e);
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: e,
          }));
          return Ok(false);
        }

        let optval: libc::c_int = 1;
        if unsafe {
          libc::setsockopt(
            socket_fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
          )
        } < 0
        {
          let e = ZmqError::from(std::io::Error::last_os_error());
          error!(
            "Listen(ud:{}): Failed to set SO_REUSEADDR for fd {}: {}",
            user_data, socket_fd, e
          );
          unsafe {
            libc::close(socket_fd);
          }
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: e,
          }));
          return Ok(false);
        }

        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let addr_len = super::socket_addr_to_sockaddr_storage(&addr, &mut storage);
        if unsafe {
          libc::bind(
            socket_fd,
            &storage as *const _ as *const libc::sockaddr,
            addr_len,
          )
        } < 0
        {
          let e = ZmqError::from(std::io::Error::last_os_error());
          error!(
            "Listen(ud:{}): Failed to bind fd {}: {}",
            user_data, socket_fd, e
          );
          unsafe {
            libc::close(socket_fd);
          }
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: e,
          }));
          return Ok(false);
        }

        let mut actual_addr_storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut actual_addr_len = std::mem::size_of_val(&actual_addr_storage) as libc::socklen_t;
        let actual_bound_addr = if unsafe {
          libc::getsockname(
            socket_fd,
            &mut actual_addr_storage as *mut _ as *mut libc::sockaddr,
            &mut actual_addr_len,
          )
        } == 0
        {
          super::sockaddr_storage_to_socket_addr(&actual_addr_storage, actual_addr_len)
            .unwrap_or(addr)
        } else {
          addr
        };

        if unsafe { libc::listen(socket_fd, 128) } < 0 {
          let e = ZmqError::from(std::io::Error::last_os_error());
          error!(
            "Listen(ud:{}): Failed to listen on fd {}: {}",
            user_data, socket_fd, e
          );
          unsafe {
            libc::close(socket_fd);
          }
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: e,
          }));
          return Ok(false);
        }

        info!(
          "UringWorker: Listener socket {} successfully set up for address {}",
          socket_fd, actual_bound_addr
        );

        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx: reply_tx.clone(),
            op_name: op_name_str.clone(),
            protocol_handler_factory_id: Some(protocol_handler_factory_id.clone()),
            protocol_config: Some(protocol_config.clone()),
            fd_created_for_connect_op: None,
            listener_fd: Some(socket_fd),
            target_fd_for_shutdown: None,
            multipart_state: None,
          },
        );

        self.handler_manager.add_listener_metadata(
          socket_fd,
          protocol_handler_factory_id.clone(),
          protocol_config,
        );

        let accept_ud = self.internal_op_tracker.new_op_id(
          socket_fd,
          InternalOpType::Accept,
          InternalOpPayload::None,
        );
        let mut client_addr_storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut client_addr_len = std::mem::size_of_val(&client_addr_storage) as libc::socklen_t;
        let accept_sqe = opcode::Accept::new(
          types::Fd(socket_fd),
          &mut client_addr_storage as *mut _ as *mut _,
          &mut client_addr_len,
        )
        .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
        .build()
        .user_data(accept_ud);

        let mut sq = unsafe { self.ring.submission_shared() };
        let submitted_accept_sqe_ok = if !sq.is_full() {
          match unsafe { sq.push(&accept_sqe) } {
            Ok(_) => {
              trace!(
                "UringWorker: Queued first Accept SQE (ud:{}) for listener_fd {}",
                accept_ud,
                socket_fd
              );
              true
            }
            Err(push_err) => {
              error!(
                "UringWorker: Failed to push first Accept SQE for listener_fd {}: {:?}. Listener setup failed.",
                socket_fd, push_err
              );
              self.internal_op_tracker.take_op_details(accept_ud);
              self.handler_manager.remove_handler(socket_fd);
              unsafe {
                libc::close(socket_fd);
              }
              if let Some(ctx) = self.external_op_tracker.take_op(user_data) {
                let _ = ctx.reply_tx.send(Ok(UringOpCompletion::OpError {
                  user_data,
                  op_name: op_name_str,
                  error: ZmqError::Internal("Failed to queue first accept".into()),
                }));
              }
              false
            }
          }
        } else {
          error!(
            "UringWorker: SQ FULL when trying to push first Accept SQE for listener_fd {}. Listener setup failed.",
            socket_fd
          );
          self.internal_op_tracker.take_op_details(accept_ud);
          self.handler_manager.remove_handler(socket_fd);
          unsafe {
            libc::close(socket_fd);
          }
          if let Some(ctx) = self.external_op_tracker.take_op(user_data) {
            let _ = ctx.reply_tx.send(Ok(UringOpCompletion::OpError {
              user_data,
              op_name: op_name_str,
              error: ZmqError::Internal("SQ full for first accept".into()),
            }));
          }
          false
        };
        drop(sq);

        if submitted_accept_sqe_ok {
          if let Some(ctx) = self.external_op_tracker.take_op(user_data) {
            let _ = ctx.reply_tx.send(Ok(UringOpCompletion::ListenSuccess {
              user_data,
              listener_fd: socket_fd,
              actual_addr: actual_bound_addr,
            }));
          }
        }
        Ok(submitted_accept_sqe_ok)
      }

      UringOpRequest::Connect {
        user_data,
        target_addr,
        ref protocol_handler_factory_id,
        protocol_config,
        reply_tx,
      } => {
        let mut fd_created_for_connect_op: Option<RawFd> = None;
        let ring_has_ext_arg = self.ring.params().is_feature_ext_arg();

        let temp_connect_req_for_builder = UringOpRequest::Connect {
          user_data,
          target_addr,
          protocol_handler_factory_id: protocol_handler_factory_id.clone(),
          protocol_config: protocol_config.clone(),
          reply_tx: reply_tx.clone(),
        };

        match super::sqe_builder::build_sqe_for_external_request(
          &temp_connect_req_for_builder,
          &mut fd_created_for_connect_op,
          ring_has_ext_arg,
        ) {
          Ok(Some(sqe)) => {
            self.external_op_tracker.add_op(
              user_data,
              ExternalOpContext {
                reply_tx: reply_tx.clone(),
                op_name: op_name_str.clone(),
                protocol_handler_factory_id: Some(protocol_handler_factory_id.clone()),
                protocol_config: Some(protocol_config.clone()),
                fd_created_for_connect_op,
                listener_fd: None,
                target_fd_for_shutdown: None,
                multipart_state: None,
              },
            );

            let mut sq = unsafe { self.ring.submission_shared() };
            if !sq.is_full() {
              match unsafe { sq.push(&sqe) } {
                Ok(_) => Ok(true),
                Err(e) => {
                  error!(
                    "UringWorker: Failed to push SQE for {}: {:?}. Op failed.",
                    op_name_str, e
                  );
                  self.external_op_tracker.take_op(user_data);
                  if let Some(fd_conn) = fd_created_for_connect_op {
                    unsafe {
                      libc::close(fd_conn);
                    }
                  }
                  let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
                    user_data,
                    op_name: op_name_str,
                    error: ZmqError::Internal("SQ push failed".into()),
                  }));
                  Ok(false)
                }
              }
            } else {
              debug!("UringWorker: SQ full for op {}. Re-queuing.", op_name_str);
              self.external_op_tracker.take_op(user_data);
              if let Some(fd_conn) = fd_created_for_connect_op {
                unsafe {
                  libc::close(fd_conn);
                }
              }
              Err(UringOpRequest::Connect {
                user_data,
                target_addr,
                protocol_handler_factory_id: protocol_handler_factory_id.clone(),
                protocol_config,
                reply_tx,
              })
            }
          }
          Ok(None) => {
            warn!(
              "UringWorker: build_sqe_for_external_request returned Ok(None) for op: {}",
              op_name_str
            );
            let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
              user_data,
              op_name: op_name_str.clone(),
              error: ZmqError::Internal(format!(
                "Op {} did not produce SQE via builder as expected",
                op_name_str
              )),
            }));
            Ok(false)
          }
          Err(op_completion_error) => {
            error!(
              "UringWorker: Failed to build SQE for {}: {:?}",
              op_name_str, op_completion_error
            );
            let _ = reply_tx.send(Ok(op_completion_error));
            Ok(false)
          }
        }
      }
      UringOpRequest::Nop {
        user_data,
        reply_tx,
      } => {
        let mut fd_created_dummy: Option<RawFd> = None;
        match super::sqe_builder::build_sqe_for_external_request(
          &UringOpRequest::Nop {
            user_data,
            reply_tx: reply_tx.clone(),
          },
          &mut fd_created_dummy,
          false,
        ) {
          Ok(Some(sqe)) => {
            self.external_op_tracker.add_op(
              user_data,
              ExternalOpContext {
                reply_tx: reply_tx.clone(),
                op_name: op_name_str.clone(),
                protocol_handler_factory_id: None,
                protocol_config: None,
                fd_created_for_connect_op: None,
                listener_fd: None,
                target_fd_for_shutdown: None,
                multipart_state: None,
              },
            );
            let mut sq = unsafe { self.ring.submission_shared() };
            if !sq.is_full() {
              if unsafe { sq.push(&sqe) }.is_ok() {
                Ok(true)
              } else {
                self.external_op_tracker.take_op(user_data);
                let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
                  user_data,
                  op_name: op_name_str,
                  error: ZmqError::Internal("SQ Nop push failed".into()),
                }));
                Ok(false)
              }
            } else {
              self.external_op_tracker.take_op(user_data);
              Err(UringOpRequest::Nop {
                user_data,
                reply_tx,
              })
            }
          }
          Ok(None) => {
            let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
              user_data,
              op_name: op_name_str,
              error: ZmqError::Internal("Nop build returned None".into()),
            }));
            Ok(false)
          }
          Err(e) => {
            let _ = reply_tx.send(Ok(e));
            Ok(false)
          }
        }
      }

      UringOpRequest::RegisterExternalFd {
        user_data,
        fd,
        protocol_handler_factory_id,
        protocol_config,
        is_server_role,
        reply_tx,
        mpsc_rx_for_worker,
      } => {
        info!(
          "UringWorker: Received RegisterExternalFd (ud: {}, fd: {}, factory: {}, server_role: {})",
          user_data, fd, protocol_handler_factory_id, is_server_role
        );
        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx: reply_tx.clone(),
            op_name: op_name_str.clone(),
            protocol_handler_factory_id: Some(protocol_handler_factory_id.clone()),
            protocol_config: Some(protocol_config.clone()),
            fd_created_for_connect_op: None,
            listener_fd: None,
            target_fd_for_shutdown: Some(fd),
            multipart_state: None,
          },
        );

        self.fd_to_mpsc_rx.insert(fd, mpsc_rx_for_worker);
        trace!("UringWorker: Stored MPSC receiver for FD {}", fd);

        match self.handler_manager.create_and_add_handler(
          fd,
          &protocol_handler_factory_id,
          &protocol_config,
          is_server_role,
          self.buffer_manager.as_ref(),
          self.default_buffer_ring_group_id_val,
          user_data,
        ) {
          Ok(initial_ops_from_handler) => {
            debug!(
              "UringWorker: Handler created for registered FD {}. Processing initial ops.",
              fd
            );
            if let Err(remaining_blueprints) = cqe_processor::process_handler_blueprints(
              self,
              fd,
              initial_ops_from_handler.sqe_blueprints,
            ) {
              warn!("UringWorker: SQ full during initial op submission for registered FD {}. Re-queueing {} blueprints.", fd, remaining_blueprints.len());
              for bp in remaining_blueprints {
                self.pending_sqe_retry_queue.push_back((fd, bp));
              }
            }
            if initial_ops_from_handler.initiate_close_due_to_error {
              warn!(
                "UringWorker: Registered FD {} handler requested immediate close.",
                fd
              );
              self.fds_needing_close_initiated_pass.push_back(fd);
            }

            if let Some(op_ctx) = self.external_op_tracker.take_op(user_data) {
              let _ = op_ctx
                .reply_tx
                .send(Ok(UringOpCompletion::RegisterExternalFdSuccess {
                  user_data,
                  fd,
                }));
            } else {
              error!(
                "UringWorker: ExternalOpContext missing for ud {} after successful RegisterExternalFd.",
                user_data
              );
            }
            Ok(true)
          }
          Err(err_msg) => {
            error!(
              "UringWorker: Failed to create handler for registered FD {}: {}",
              fd, err_msg
            );
            self.fd_to_mpsc_rx.remove(&fd);

            if let Some(op_ctx) = self.external_op_tracker.take_op(user_data) {
              let _ = op_ctx.reply_tx.send(Ok(UringOpCompletion::OpError {
                user_data,
                op_name: op_name_str.clone(),
                error: ZmqError::Internal(err_msg),
              }));
            }
            Ok(false)
          }
        }
      }

      UringOpRequest::StartFdReadLoop {
        user_data,
        fd,
        reply_tx,
      } => {
        if self.handler_manager.contains_handler_for(fd) {
          trace!(
            "UringWorker: Received StartFdReadLoop for fd {}. Handler will manage reads via prepare_sqes.",
            fd
          );
          let _ = reply_tx.send(Ok(UringOpCompletion::StartFdReadLoopAck { user_data, fd }));
        } else {
          warn!("UringWorker: StartFdReadLoop for unknown fd {}", fd);
          let _ = reply_tx.send(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: ZmqError::InvalidArgument(format!("FD {} not managed for StartFdReadLoop", fd)),
          }));
        }
        Ok(false)
      }

      UringOpRequest::ShutdownConnectionHandler {
        user_data,
        fd,
        reply_tx,
      } => {
        debug!(
          fd,
          "UringWorker: Processing ShutdownConnectionHandler for FD."
        );
        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx: reply_tx.clone(),
            op_name: op_name_str,
            protocol_handler_factory_id: None,
            protocol_config: None,
            fd_created_for_connect_op: None,
            listener_fd: None,
            target_fd_for_shutdown: Some(fd),
            multipart_state: None,
          },
        );

        if let Some(handler) = self.handler_manager.get_mut(fd) {
          if handler.is_closing_or_closed() {
            debug!(
                    fd,
                    "UringWorker: Received ShutdownConnectionHandler for FD that is already closing. Acknowledging immediately."
                );
            let ack = UringOpCompletion::ShutdownConnectionHandlerComplete { user_data, fd };
            let _ = reply_tx.send(Ok(ack));
            return Ok(false);
          }
          let close_ops = handler.close_initiated(&UringWorkerInterface::new(
            fd,
            &self.worker_io_config,
            self.buffer_manager.as_ref(),
            self.default_buffer_ring_group_id_val,
            user_data,
          ));
          if let Err(remaining) =
            cqe_processor::process_handler_blueprints(self, fd, close_ops.sqe_blueprints)
          {
            warn!(
              "SQ Full during shutdown of FD {}. Requeuing {} blueprints.",
              fd,
              remaining.len()
            );
            for bp in remaining {
              self.pending_sqe_retry_queue.push_back((fd, bp));
            }
          }
        } else {
          warn!(
                fd,
                "UringWorker: Received ShutdownConnectionHandler for FD with no handler. Assuming already closed."
            );
          let ack = UringOpCompletion::ShutdownConnectionHandlerComplete { user_data, fd };
          let _ = reply_tx.send(Ok(ack));
          return Ok(false);
        }

        Ok(true)
      }
    }
  }
}

pub(crate) fn run_worker_loop(worker: &mut UringWorker) -> Result<(), ZmqError> {
  info!(
    "UringWorker run_loop starting (PID: {}). Ring FD: {}",
    std::process::id(),
    worker.ring.as_raw_fd()
  );

  // <<< MODIFIED START >>>
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

        // --- PHASE 1: Gather ALL potential new work into the unified queue ---
        profiler.mark_segment_end_and_start_new("gather_work");
        let mut work_was_available = !worker.work_queue.is_empty();

        // 1a. Prioritize retries
        if !worker.pending_sqe_retry_queue.is_empty() {
          let retries = worker
            .pending_sqe_retry_queue
            .drain(..)
            .map(|(fd, bp)| WorkItem::BlueprintBatch(fd, vec![bp]));
          worker.work_queue.extend(retries);
          work_was_available = true;
        }

        // 1b. Drain external command channel
        while let Ok(request) = worker.op_rx.try_recv() {
          worker.work_queue.push_back(WorkItem::ExternalOp(request));
          work_was_available = true;
        }

        // 1c. Poll handlers for I/O readiness
        let handler_ops_list = worker.handler_manager.prepare_all_handler_io_ops(
          worker.buffer_manager.as_ref(),
          worker.default_buffer_ring_group_id_val,
        );
        for (fd, ops) in handler_ops_list {
          if !ops.sqe_blueprints.is_empty() {
            worker
              .work_queue
              .push_back(WorkItem::BlueprintBatch(fd, ops.sqe_blueprints));
            work_was_available = true;
          }
          if ops.initiate_close_due_to_error
            && !worker.fds_needing_close_initiated_pass.contains(&fd)
          {
            worker.fds_needing_close_initiated_pass.push_back(fd);
          }
        }

        // 1d. Process FDs flagged for close
        while let Some(fd_to_close) = worker.fds_needing_close_initiated_pass.pop_front() {
          worker
            .work_queue
            .push_back(WorkItem::InitiateClose(fd_to_close));
          work_was_available = true;
        }

        // 1e. Drain MPSC data queues from all sockets (one message per socket per loop to ensure fairness)
        let fds_to_drain: Vec<RawFd> = worker.fd_to_mpsc_rx.keys().copied().collect();
        for fd in fds_to_drain {
          if let Some(rx) = worker.fd_to_mpsc_rx.get(&fd) {
            if let Ok(msg_parts) = rx.try_recv() {
              worker
                .work_queue
                .push_back(WorkItem::ApplicationData(fd, msg_parts));
              work_was_available = true;
            }
          }
        }

        // --- PHASE 2: Process the unified work queue sequentially ---
        profiler.mark_segment_end_and_start_new("process_work_queue");
        let mut sq_was_full = false;
        while let Some(item) = worker.work_queue.pop_front() {
          if sq_was_full {
            // If the queue filled up on a previous item, put this one back at the front
            // and stop processing for this cycle.
            worker.work_queue.push_front(item);
            break;
          }

          match item {
            WorkItem::ExternalOp(req) => {
              if let Err(retry_req) = worker.handle_external_op_request_submission(req) {
                worker
                  .work_queue
                  .push_front(WorkItem::ExternalOp(retry_req));
                sq_was_full = true;
              }
            }
            WorkItem::BlueprintBatch(fd, blueprints) => {
              if let Err(remaining_blueprints) =
                cqe_processor::process_handler_blueprints(worker, fd, blueprints)
              {
                // Re-queue the failed batch at the front and stop processing.
                worker
                  .work_queue
                  .push_front(WorkItem::BlueprintBatch(fd, remaining_blueprints));
                sq_was_full = true;
              }
            }
            WorkItem::ApplicationData(fd, msg_parts) => {
              if let Some(handler) = worker.handler_manager.get_mut(fd) {
                const DRAIN_OP_UD: UserData = 0; // Sentinel UD for this context
                let interface =
                  crate::io_uring_backend::connection_handler::UringWorkerInterface::new(
                    fd,
                    &worker.worker_io_config,
                    worker.buffer_manager.as_ref(),
                    worker.default_buffer_ring_group_id_val,
                    DRAIN_OP_UD,
                  );
                let ops_output = handler.handle_outgoing_app_data(msg_parts, &interface);

                if let Err(remaining) =
                  cqe_processor::process_handler_blueprints(worker, fd, ops_output.sqe_blueprints)
                {
                  worker
                    .work_queue
                    .push_front(WorkItem::BlueprintBatch(fd, remaining));
                  sq_was_full = true;
                }
                if ops_output.initiate_close_due_to_error
                  && !worker.fds_needing_close_initiated_pass.contains(&fd)
                {
                  worker.fds_needing_close_initiated_pass.push_back(fd);
                }
              }
            }
            WorkItem::InitiateClose(fd) => {
              if let Some(handler) = worker.handler_manager.get_mut(fd) {
                const INTERNAL_OP_SENTINEL_UD: u64 = 0;
                let interface_for_close =
                  crate::io_uring_backend::connection_handler::UringWorkerInterface::new(
                    fd,
                    &worker.worker_io_config,
                    worker.buffer_manager.as_ref(),
                    worker.default_buffer_ring_group_id_val,
                    INTERNAL_OP_SENTINEL_UD,
                  );
                let close_io_ops = handler.close_initiated(&interface_for_close);
                if let Err(remaining) =
                  cqe_processor::process_handler_blueprints(worker, fd, close_io_ops.sqe_blueprints)
                {
                  worker
                    .work_queue
                    .push_front(WorkItem::BlueprintBatch(fd, remaining));
                  sq_was_full = true;
                }
              }
            }
          }
        }

        // --- PHASE 3: Submit to Kernel and Process Completions ---
        profiler.mark_segment_end_and_start_new("submit_and_process");

        let sq_len = unsafe { worker.ring.submission_shared().len() };
        let mut sqes_submitted_to_kernel_this_batch = 0;

        let needs_wait = !work_was_available
          && sq_len == 0
          && unsafe { worker.ring.completion_shared().is_empty() };

        if sq_len > 0 || needs_wait {
          let submitted_count_res = if needs_wait {
            let timespec_for_wait = types::Timespec::from(kernel_poll_timeout_duration);
            let submit_args = types::SubmitArgs::new().timespec(&timespec_for_wait);
            worker.ring.submitter().submit_with_args(1, &submit_args)
          } else {
            worker.ring.submitter().submit()
          };

          match submitted_count_res {
            Ok(count) => {
              sqes_submitted_to_kernel_this_batch = count;
              if needs_wait {
                kernel_poll_timeout_duration = if count > 0 {
                  KERNEL_POLL_INITIAL
                } else {
                  (kernel_poll_timeout_duration * 2).min(KERNEL_POLL_MAX_DURATION)
                };
              }
            }
            Err(e) => {
              if e.kind() == std::io::ErrorKind::TimedOut || e.raw_os_error() == Some(libc::ETIME) {
                if needs_wait {
                  kernel_poll_timeout_duration =
                    (kernel_poll_timeout_duration * 2).min(KERNEL_POLL_MAX_DURATION);
                }
              } else if e.raw_os_error() == Some(libc::EBUSY)
                || e.raw_os_error() == Some(libc::EINTR)
              {
                warn!("UringWorker: submit/submit_with_args returned EBUSY/EINTR");
              } else {
                error!(
                  "UringWorker: ring.submit() failed critically: {}. Shutting down.",
                  e
                );
                return Err(ZmqError::from(e));
              }
            }
          }
        }

        let cqe_processed_count = if let Ok(count) = cqe_processor::process_all_cqes(worker, false)
        {
          count
        } else {
          break;
        };

        // --- PHASE 4: Idle Strategy ---
        profiler.mark_segment_end_and_start_new("idle_strategy");
        if sqes_submitted_to_kernel_this_batch == 0
          && cqe_processed_count == 0
          && !work_was_available
          && worker.work_queue.is_empty()
        {
          if needs_wait { /* The wait already happened */
          } else {
            kernel_poll_timeout_duration =
              (kernel_poll_timeout_duration * 2).min(KERNEL_POLL_MAX_DURATION);
          }
        } else {
          kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;
        }
      }
      WorkerState::Draining => {
        // First, always process any completions that might be on the ring.
        if let Err(e) = cqe_processor::process_all_cqes(
          worker, true, // is_worker_shutting_down
        ) {
          error!(
            "UringWorker: Error processing CQEs during Draining state: {}. Forcing stop.",
            e
          );
          worker.state = WorkerState::Stopped;
          continue;
        }

        // Now, check if we are done.
        if worker.internal_op_tracker.is_empty() && worker.external_op_tracker.is_empty() {
          info!(
            "UringWorker: Draining complete. All operations finished. Transitioning to CleaningUp."
          );
          worker.state = WorkerState::CleaningUp;
          continue;
        }

        // If not done, use a TIMED wait instead of a blocking one to prevent hangs.
        // This gives the kernel a chance to produce CQEs for cancellations.
        // If it times out, we assume a stall and proceed with cleanup.
        let drain_timeout = Duration::from_millis(100); // A reasonable timeout
        let timespec = io_uring::types::Timespec::from(drain_timeout);
        let submit_args = io_uring::types::SubmitArgs::new().timespec(&timespec);

        // This will wait for 1 CQE or until the timeout.
        match worker.ring.submitter().submit_with_args(1, &submit_args) {
          Ok(_) => { /* CQEs reaped (if any) will be processed in the next iteration */ }
          Err(e)
            if e.kind() == std::io::ErrorKind::TimedOut
              || e.raw_os_error() == Some(libc::ETIME) =>
          {
            // This is the expected outcome if we're stalled.
            debug!("UringWorker: Timed wait in Draining state expired. Trackers not empty. Forcing cleanup. Internal ops: {}, External ops: {}",
                worker.internal_op_tracker.op_to_details.len(),
                worker.external_op_tracker.in_flight.len()
            );
            // Force transition to the next state to prevent hang.
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
            worker.state = WorkerState::Stopped; // Force exit from loop
          }
        }
      }
      WorkerState::CleaningUp => {
        info!("UringWorker: CleaningUp state - unregistering resources.");

        // Unregister send buffer pool
        if let Some(pool_arc) = &worker.send_buffer_pool {
          unsafe {
            if let Err(e) = pool_arc.unregister_all(&worker.ring) {
              error!(
                "UringWorker: Error unregistering send buffers on shutdown: {}",
                e
              );
            } else {
              info!("UringWorker: Send buffer pool unregistered.");
            }
          }
        }

        // Drop the buffer manager to trigger its Drop impl, which unregisters its ring
        if worker.buffer_manager.take().is_some() {
          info!("UringWorker: Default recv buffer manager dropped and unregistered.");
        }

        // After all cleanup, transition to the final state.
        worker.state = WorkerState::Stopped;
        info!("UringWorker: Cleanup complete. Transitioning to Stopped.");
        continue;
      }
      WorkerState::Stopped => {
        // This arm is not strictly needed due to the `while` condition,
        // but it's good practice to handle all enum variants.
        // It should not be reached.
        unreachable!("UringWorker loop entered while state was Stopped.");
      }
    }
    profiler.log_and_reset_for_next_loop();
  } // end main loop

  // This is safe because all kernel interaction is done.
  info!("UringWorker: Loop finished. Sending final error replies to external ops.");
  for (_ud, ext_op_ctx) in worker.external_op_tracker.drain_all() {
    let _ = ext_op_ctx
      .reply_tx
      .send(Err(ZmqError::Internal("UringWorker shutting down".into())));
  }

  Ok(())
}
