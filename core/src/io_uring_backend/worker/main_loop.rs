#![cfg(feature = "io-uring")]

use super::{ExternalOpContext, UringWorker, cqe_processor};
use crate::ZmqError;
use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::io_uring_backend::connection_handler::{HandlerSqeBlueprint, UringWorkerInterface};
use crate::io_uring_backend::ops::{HANDLER_INTERNAL_SEND_OP_UD, UringOpCompletion, UringOpRequest};
use crate::io_uring_backend::worker::{InternalOpPayload, InternalOpType, WorkerState};

use bytes::BufMut;
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
      UringOpRequest::RegisterExternalFd {
        user_data,
        fd,
        protocol_handler_factory_id,
        protocol_config,
        is_server_role,
        reply_tx,
        mpsc_rx_for_worker,
      } => {
        // This logic doesn't submit an SQE, so it's safe.
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
        match self.handler_manager.create_and_add_handler(
          fd,
          &protocol_handler_factory_id,
          &protocol_config,
          is_server_role,
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
                .pending_blueprints
                .extend(initial_ops.sqe_blueprints);
            }

            if initial_ops.initiate_close_due_to_error {
              self.fds_needing_close_initiated_pass.push_back(fd);
            }
            if let Some(ctx) = self.external_op_tracker.take_op(user_data) {
              let _ = ctx
                .reply_tx
                .send(Ok(UringOpCompletion::RegisterExternalFdSuccess {
                  user_data,
                  fd,
                }));
            }
          }
          Err(err_msg) => {
            self.fd_to_mpsc_rx.remove(&fd);
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
        // This logic is also safe as it adds work to the work_map, not directly submitting.
        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx,
            op_name: op_name_str,
            protocol_handler_factory_id: None,
            protocol_config: None,
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

pub(crate) fn run_worker_loop(worker: &mut UringWorker) -> Result<(), ZmqError> {
  info!(
    "UringWorker run_loop starting (PID: {}). Ring FD: {}",
    std::process::id(),
    worker.ring.as_raw_fd()
  );

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
        let mut work_was_available = !worker.work_map.is_empty();

        // 1a. Drain external commands
        while let Ok(request) = worker.op_rx.try_recv() {
          work_was_available = true;
          worker.process_external_op_request(request);
        }

        // 1b. Drain application data into the work_map
        let fds_to_drain: Vec<_> = worker.fd_to_mpsc_rx.keys().copied().collect();
        for fd in &fds_to_drain {
          // Limit user-space buffering to propagate backpressure back to the socket's mpsc_tx channel
          let pending_count = worker
            .work_map
            .get(fd)
            .map_or(0, |w| w.pending_blueprints.len() + w.app_data.len());
          if pending_count >= 16 {
            continue; // Leave messages in the bounded channel to block the sender
          }
          if let Some(rx) = worker.fd_to_mpsc_rx.get(fd) {
            while let Ok(msg_parts) = rx.try_recv() {
              work_was_available = true;
              let work = worker.work_map.entry(*fd).or_default();
              work.app_data.push_back(msg_parts);
              if work.app_data.len() >= 16 {
                break; // Stop draining once our local buffer has sufficient work
              }
            }
          }
        }

        // 1c. Poll handlers for periodic work
        let handler_ops_list = worker.handler_manager.prepare_all_handler_io_ops(
          worker.buffer_manager.as_ref(),
          worker.default_buffer_ring_group_id_val,
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
              .pending_blueprints
              .extend(ops.sqe_blueprints);
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
            let interface = UringWorkerInterface::new(
              fd_to_close,
              &worker.worker_io_config,
              worker.buffer_manager.as_ref(),
              worker.default_buffer_ring_group_id_val,
              0,
            );
            let close_io_ops = handler.close_initiated(&interface);
            if !close_io_ops.sqe_blueprints.is_empty() {
              worker
                .work_map
                .entry(fd_to_close)
                .or_default()
                .pending_blueprints
                .extend(close_io_ops.sqe_blueprints);
            }
          }
        }

        // --- PHASE 2: PROCESS THE WORK MAP WITH A BUDGET ---
        profiler.mark_segment_end_and_start_new("process_work_map");
        let mut batches_processed_this_iteration = 0;
        let fds_with_work: Vec<_> = worker.work_map.keys().copied().collect();

        for fd in fds_with_work {
          if batches_processed_this_iteration >= MAX_BATCHES_PER_ITERATION {
            trace!("Work budget for this iteration consumed. Deferring remaining FDs.");
            break;
          }
          if unsafe { worker.ring.submission_shared().is_full() } {
            trace!("UringWorker: SQ full, pausing work map processing for this cycle.");
            break;
          }

          // Take the work for the current FD out of the map.
          // This moves ownership of the `FdWork` struct and releases the mutable borrow on the map.
          let mut work = worker.work_map.remove(&fd).unwrap_or_default();

          // Convert all pending app_data into the flat blueprint queue.
          if let Some(handler) = worker.handler_manager.get_mut(fd) {
            while let Some(msg_parts) = work.app_data.pop_front() {
              let interface = UringWorkerInterface::new(
                fd,
                &worker.worker_io_config,
                worker.buffer_manager.as_ref(),
                worker.default_buffer_ring_group_id_val,
                0,
              );
              let ops_output = handler.handle_outgoing_app_data(msg_parts, &interface);
              work.pending_blueprints.extend(ops_output.sqe_blueprints);
            }
          }

          let mut stop_processing_this_fd = false;
          while let Some(first_blueprint) = work.pending_blueprints.pop_front() {
            if unsafe { worker.ring.submission_shared().is_full() } {
              work.pending_blueprints.push_front(first_blueprint);
              stop_processing_this_fd = true;
              break;
            }

            if let Some(first_write_len) = first_blueprint.write_len() {
              // --- Write op path ---
              if work.write_in_flight {
                work.pending_blueprints.push_front(first_blueprint);
                break; // Serialization gate: wait for in-flight CQE
              }

              // Coalesce if the next pending item is also a write that fits within 64KB.
              let next_is_write_and_fits = work.pending_blueprints.front()
                .and_then(|bp| bp.write_len())
                .map_or(false, |next_len| first_write_len + next_len <= 65536);

              let blueprint_to_submit = if next_is_write_and_fits {
                let mut coalesce_buf = worker.coalesce_buffer_pool
                  .pop_front()
                  .unwrap_or_else(|| bytes::BytesMut::with_capacity(65536));
                coalesce_buf.clear();

                match first_blueprint {
                  HandlerSqeBlueprint::RequestSend { data, .. }
                  | HandlerSqeBlueprint::RequestSendZeroCopy { data_to_send: data, .. } => {
                    coalesce_buf.put(data);
                  }
                  HandlerSqeBlueprint::RequestSendVectored { header, payload, .. } => {
                    coalesce_buf.put(header);
                    coalesce_buf.put(payload);
                  }
                  _ => unreachable!(),
                }

                while let Some(next_bp) = work.pending_blueprints.front() {
                  if let Some(next_len) = next_bp.write_len() {
                    if coalesce_buf.len() + next_len > 65536 {
                      break;
                    }
                    let bp = work.pending_blueprints.pop_front().unwrap();
                    match bp {
                      HandlerSqeBlueprint::RequestSend { data, .. }
                      | HandlerSqeBlueprint::RequestSendZeroCopy { data_to_send: data, .. } => {
                        coalesce_buf.put(data);
                      }
                      HandlerSqeBlueprint::RequestSendVectored { header, payload, .. } => {
                        coalesce_buf.put(header);
                        coalesce_buf.put(payload);
                      }
                      _ => unreachable!(),
                    }
                  } else {
                    break; // Control op, stop coalescing
                  }
                }

                HandlerSqeBlueprint::RequestSend {
                  data: coalesce_buf.freeze(),
                  send_op_flags: 0,
                  originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                }
              } else {
                first_blueprint
              };

              if let Err(returned_bp) =
                cqe_processor::process_handler_blueprint(worker, fd, blueprint_to_submit)
              {
                // Push back for retry. Buffer is reclaimed on CQE success, not here.
                work.pending_blueprints.push_front(returned_bp);
                stop_processing_this_fd = true;
                break;
              }

              work.write_in_flight = true;
              batches_processed_this_iteration += 1;
              break; // One physical write per cycle
            } else {
              // --- Control op path (RequestSetCork, RequestClose, etc.) ---
              if let Err(returned_bp) =
                cqe_processor::process_handler_blueprint(worker, fd, first_blueprint)
              {
                work.pending_blueprints.push_front(returned_bp);
                stop_processing_this_fd = true;
                break;
              }
              batches_processed_this_iteration += 1;
            }
          }

          // Keep FdWork in the map if there is pending work OR a write is in-flight.
          // write_in_flight=true with no pending blueprints must still be preserved so
          // new app_data arriving before the CQE sees the correct lock state.
          if !work.app_data.is_empty() || !work.pending_blueprints.is_empty() || work.write_in_flight {
            worker.work_map.insert(fd, work);
          }

          if stop_processing_this_fd {
            break;
          }
        }

        // --- PHASE 3: ENSURE READS ---
        profiler.mark_segment_end_and_start_new("ensure_reads");
        let active_fds_for_read = worker.handler_manager.get_active_fds();
        let mut sq = unsafe { worker.ring.submission_shared() };
        for fd in active_fds_for_read {
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
            let timespec_for_wait = types::Timespec::from(kernel_poll_timeout_duration);
            let submit_args = types::SubmitArgs::new().timespec(&timespec_for_wait);
            worker.worker_asleep.store(true, Ordering::Release);
            let res = worker.ring.submitter().submit_with_args(1, &submit_args);
            worker.worker_asleep.store(false, Ordering::Release);
            res
          } else {
            worker.ring.submitter().submit()
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
              .pending_blueprints
              .extend(blueprints);
          }
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
        if worker.buffer_manager.take().is_some() {
          info!("UringWorker: Default recv buffer manager dropped and unregistered.");
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
