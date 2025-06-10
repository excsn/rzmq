// core/src/io_uring_backend/worker/cqe_processor.rs

#![cfg(feature = "io-uring")]

use super::internal_op_tracker::{InternalOpDetails, InternalOpPayload, InternalOpType};
use crate::io_uring_backend::connection_handler::{
  HandlerIoOps, HandlerSqeBlueprint, UringWorkerInterface,
};
use crate::io_uring_backend::ops::{UringOpCompletion, UserData, HANDLER_INTERNAL_SEND_OP_UD};
use crate::io_uring_backend::worker::multishot_reader::IOURING_CQE_F_MORE;
use crate::io_uring_backend::worker::UringWorker;
use crate::{uring, Command, ZmqError};

use io_uring::cqueue::Entry;
use io_uring::{cqueue, opcode, squeue, types};
use std::mem;
use std::os::unix::io::RawFd;
use tracing::{debug, error, info, trace, warn};

#[cfg(feature = "io-uring")]
const CQE_F_NOTIFY_FLAG: u32 = 1 << 3; // (8) for kernels 6.0+

// The signature is changed to take blueprints directly and return a Result for backpressure.
pub(crate) fn process_handler_blueprints(
  worker: &mut UringWorker,
  fd_from_handler_iteration: RawFd,
  mut blueprints: Vec<HandlerSqeBlueprint>,
) -> Result<(), Vec<HandlerSqeBlueprint>> {
  let ring = &mut worker.ring;
  let sq = &mut unsafe { ring.submission_shared() };
  let external_ops = &mut worker.external_op_tracker;
  let internal_ops = &mut worker.internal_op_tracker;
  let handler_manager = &mut worker.handler_manager;
  let send_buffer_pool = &worker.send_buffer_pool;
  let default_bgid_for_read_sqe = worker.default_buffer_ring_group_id_val;

  let worker_cfg_send_zerocopy_enabled = worker.cfg_send_zerocopy_enabled;

  // Loop signature changed to get the index `idx`.
  for (idx, blueprint_ref) in blueprints.iter().enumerate() {
    if sq.is_full() {
      debug!(
        "CQE Processor: Submission queue full. Re-queueing {} remaining blueprints for FD {}.",
        blueprints.len() - idx,
        fd_from_handler_iteration
      );
      // Return the portion of the vec that was not processed.
      let remaining = blueprints.split_off(idx);
      return Err(remaining);
    }

    let mut op_payload_for_tracker = InternalOpPayload::None;

    let sqe_build_result_opt: Option<Result<(squeue::Entry, InternalOpType), String>> =
      match blueprint_ref {
        HandlerSqeBlueprint::RequestSetCork(enable) => {
          let cork_val: i32 = if *enable { 1 } else { 0 };
          let entry = opcode::SetSockOpt::new(
            types::Fd(fd_from_handler_iteration),
            libc::SOL_TCP as _,
            libc::TCP_CORK as _,
            &cork_val as *const _ as *const libc::c_void,
            std::mem::size_of_val(&cork_val) as u32,
          )
          .build();
          Some(Ok((entry, InternalOpType::GenericHandlerOp)))
        }
        HandlerSqeBlueprint::RequestSend {
          data,
          send_op_flags,
          originating_app_op_ud,
        } => {
          let app_op_name_str = external_ops
            .get_op_context_ref(*originating_app_op_ud)
            .map(|ctx| ctx.op_name.clone())
            .unwrap_or_else(|| "UnknownAppOpSend".to_string());
          op_payload_for_tracker = InternalOpPayload::SendBuffer {
            buffer: data.clone(),
            app_op_ud: Some(*originating_app_op_ud),
            app_op_name: Some(app_op_name_str),
          };
          let entry = opcode::Send::new(
            types::Fd(fd_from_handler_iteration),
            data.as_ptr(),
            data.len() as u32,
          )
          .flags(*send_op_flags)
          .build();
          Some(Ok((entry, InternalOpType::Send)))
        }
        HandlerSqeBlueprint::RequestSendZeroCopy {
          data_to_send,
          send_op_flags,
          originating_app_op_ud,
        } => {
          let mut submitted_via_zc_path = false;
          let mut temp_sqe_gen_result: Option<Result<(squeue::Entry, InternalOpType), String>> =
            None;

          if worker_cfg_send_zerocopy_enabled {
            if let Some(pool_arc) = send_buffer_pool {
              if let Some((reg_buf_id, buffer_ptr, len_copied)) =
                pool_arc.acquire_and_prep_buffer(data_to_send)
              {
                let app_op_name_str = external_ops
                  .get_op_context_ref(*originating_app_op_ud)
                  .map(|ctx| ctx.op_name.clone())
                  .unwrap_or_else(|| "UnknownAppOpSendZC".to_string());
                op_payload_for_tracker = InternalOpPayload::SendZeroCopy {
                  send_buf_id: reg_buf_id,
                  app_op_ud: *originating_app_op_ud,
                  app_op_name: app_op_name_str,
                };
                let entry_zc =
                  opcode::SendZc::new(types::Fd(fd_from_handler_iteration), buffer_ptr, len_copied)
                    .flags(*send_op_flags)
                    .build();
                temp_sqe_gen_result = Some(Ok((entry_zc, InternalOpType::SendZeroCopy)));
                submitted_via_zc_path = true;
                trace!(
                  "CQE Processor: Prepared SendZeroCopy SQE for FD {} (payload set for tracker).",
                  fd_from_handler_iteration
                );
              } else {
                debug!(
                "CQE Processor: ZC Send - Failed to acquire/prep registered buffer for FD {}. Falling back.",
                fd_from_handler_iteration
              );
              }
            } else {
              debug!(
                "CQE Processor: ZC Send - SendBufferPool not available for FD {}. Falling back.",
                fd_from_handler_iteration
              );
            }
          }

          if !submitted_via_zc_path {
            debug!(
              "CQE Processor: Falling back to normal send for FD {} (ZC intended for op_ud {}).",
              fd_from_handler_iteration, *originating_app_op_ud
            );
            let app_op_name_str_fb = external_ops
              .get_op_context_ref(*originating_app_op_ud)
              .map(|ctx| ctx.op_name.clone())
              .unwrap_or_else(|| "UnknownAppOpSendFallback".to_string());
            op_payload_for_tracker = InternalOpPayload::SendBuffer {
              buffer: data_to_send.clone(),
              app_op_ud: Some(*originating_app_op_ud),
              app_op_name: Some(app_op_name_str_fb),
            };
            let entry_fb = opcode::Send::new(
              types::Fd(fd_from_handler_iteration),
              data_to_send.as_ptr(),
              data_to_send.len() as u32,
            )
            .flags(*send_op_flags)
            .build();
            temp_sqe_gen_result = Some(Ok((entry_fb, InternalOpType::Send)));
          }
          temp_sqe_gen_result
        }
        HandlerSqeBlueprint::RequestClose => {
          let entry = opcode::Close::new(types::Fd(fd_from_handler_iteration)).build();
          Some(Ok((entry, InternalOpType::CloseFd)))
        }
        HandlerSqeBlueprint::RequestNewRingReadMultishot { fd, bgid } => {
          let fd = *fd;
          let bgid = *bgid;
          if fd != fd_from_handler_iteration {
            error!(
            "CQE Processor: FD mismatch in RequestNewRingReadMultishot! Handler FD {}, Blueprint FD {}. Dropping.",
            fd_from_handler_iteration, fd
          );
            continue;
          }
          let op_user_data = internal_ops.new_op_id(
            fd,
            InternalOpType::RingReadMultishot,
            InternalOpPayload::None,
          );
          let entry = opcode::RecvMulti::new(types::Fd(fd), bgid)
            .build()
            .user_data(op_user_data);

          // Logic for handling push failure is now to return Err.
          unsafe {
            if sq.push(&entry).is_err() {
              debug!(
                "CQE Processor: SQ push failed for FD {} RecvMulti. Re-queueing work.",
                fd
              );
              internal_ops.take_op_details(op_user_data); // Cleanup tracker for failed push
              let remaining = blueprints.split_off(idx);
              return Err(remaining);
            } else {
              trace!(
                "CQE Processor: Queued RecvMulti SQE (ud:{}) for FD {}.",
                op_user_data,
                fd
              );
              if let Some(handler) = handler_manager.get_mut(fd) {
                handler.inform_multishot_reader_op_submitted(op_user_data, false, None);
              } else {
                debug!(
                "CQE Processor: Handler for FD {} not found after submitting RecvMulti. Reader not informed.",
                fd
              );
              }
            }
          }
          None
        }
        HandlerSqeBlueprint::RequestNewAsyncCancel {
          fd,
          target_user_data,
        } => {
          let fd = *fd;
          let target_user_data = *target_user_data;
          if fd != fd_from_handler_iteration {
            error!(
            "CQE Processor: FD mismatch in RequestNewAsyncCancel! Handler FD {}, Blueprint FD {}. Dropping.",
            fd_from_handler_iteration, fd
          );
            continue;
          }
          let cancel_op_payload = InternalOpPayload::CancelTarget { target_user_data };
          let cancel_op_user_data =
            internal_ops.new_op_id(fd, InternalOpType::AsyncCancel, cancel_op_payload);
          let entry = opcode::AsyncCancel::new(target_user_data)
            .build()
            .user_data(cancel_op_user_data);

          // Logic for handling push failure is now to return Err.
          unsafe {
            if sq.push(&entry).is_err() {
              debug!(
                "CQE Processor: SQ push failed for FD {} AsyncCancel (target_ud: {}). Re-queueing work.",
                fd, target_user_data
              );
              internal_ops.take_op_details(cancel_op_user_data);
              let remaining = blueprints.split_off(idx);
              return Err(remaining);
            } else {
              trace!(
                "CQE Processor: Queued AsyncCancel SQE (ud:{}, target_ud:{}) for FD {}.",
                cancel_op_user_data,
                target_user_data,
                fd
              );
              if let Some(handler) = handler_manager.get_mut(fd) {
                handler.inform_multishot_reader_op_submitted(
                  cancel_op_user_data,
                  true,
                  Some(target_user_data),
                );
              } else {
                debug!(
                "CQE Processor: Handler for FD {} not found after submitting AsyncCancel. Reader not informed.",
                fd
              );
              }
            }
          }
          None
        }
      };

    if let Some(sqe_build_result) = sqe_build_result_opt {
      match sqe_build_result {
        Ok((mut entry_to_submit, op_type)) => {
          let user_data =
            internal_ops.new_op_id(fd_from_handler_iteration, op_type, op_payload_for_tracker);
          entry_to_submit = entry_to_submit.user_data(user_data);

          // Logic for handling push failure is now to return Err.
          unsafe {
            let push_result = sq.push(&entry_to_submit);

            if push_result.is_err() {
              debug!("CQE Processor: SQ push failed for FD {} blueprint {:?} (race condition). Re-queueing work.", fd_from_handler_iteration, entry_to_submit);
              let _dropped_details = internal_ops.take_op_details(user_data); // Clean up tracker
              let remaining = blueprints.split_off(idx);
              return Err(remaining);
            } else {
              trace!(
                "CQE Processor: Queued SQE (ud:{}) for FD {} from blueprint: {:?}",
                user_data,
                fd_from_handler_iteration,
                entry_to_submit
              );
            }
          }
        }
        Err(s) => {
          error!(
            "CQE Processor: Failed to build SQE for FD {} from blueprint: {}. Blueprint dropped.",
            fd_from_handler_iteration, s
          );
        }
      }
    }
  }
  // If the loop completes successfully, return Ok.
  Ok(())
}

pub(crate) fn process_all_cqes(
  worker: &mut UringWorker,
  is_worker_shutting_down: bool,
) -> Result<usize, ZmqError> {
  let mut cq: cqueue::CompletionQueue<'_> = unsafe { worker.ring.completion_shared() };
  cq.sync();
  let entries: Vec<Entry> = cq.into_iter().collect();
  let num_processed = entries.len();

  for cqe in entries {
    let cqe_user_data = cqe.user_data();
    let cqe_result = cqe.result();
    let cqe_flags = cqe.flags();

    trace!(
      "[CQE Proc] CQE: ud={}, res={}, flags={:x}",
      cqe_user_data,
      cqe_result,
      cqe_flags
    );

    let event_fd_poller = &mut worker.event_fd_poller;
    if event_fd_poller.handle_cqe_if_matches(
      cqe_user_data,
      cqe_result,
      &mut worker.internal_op_tracker,
      is_worker_shutting_down,
    ) {
      if worker
        .internal_op_tracker
        .take_op_details(cqe_user_data)
        .is_none()
      {
        warn!(
          "[CQE Proc] EventFdPoll CQE (ud:{}) handled, but no InternalOpDetails found to consume.",
          cqe_user_data
        );
      }
      continue;
    }

    if let Some(mut ext_op_ctx) = worker.external_op_tracker.take_op(cqe_user_data) {
      trace!(
        "[CQE Proc] EXTERNAL op '{}' (ud:{}, res:{})",
        ext_op_ctx.op_name,
        cqe_user_data,
        cqe_result
      );
      let completion_to_send: UringOpCompletion = if cqe_result < 0 {
        let zmq_err = ZmqError::from(std::io::Error::from_raw_os_error(-cqe_result));
        error!(
          "CQE for external op '{}' (ud:{}) KERNEL FAILED. Errno: {}, ZmqErr: {}",
          ext_op_ctx.op_name, cqe_user_data, -cqe_result, zmq_err
        );
        if ext_op_ctx.op_name == "Connect" {
          if let Some(fd_to_close) = ext_op_ctx.fd_created_for_connect_op.take() {
            debug!(
              "Closing FD {} from failed Connect CQE (ud:{})",
              fd_to_close, cqe_user_data
            );
            unsafe {
              libc::close(fd_to_close);
            }
          }
        }
        UringOpCompletion::OpError {
          user_data: cqe_user_data,
          op_name: ext_op_ctx.op_name.clone(),
          error: zmq_err,
        }
      } else {
        match ext_op_ctx.op_name.as_str() {
          "Connect" => {
            let connected_fd = ext_op_ctx
              .fd_created_for_connect_op
              .expect("Connect op context missing FD after successful CQE");
            let (peer_addr, local_addr) = crate::io_uring_backend::worker::get_peer_local_addr(
              connected_fd,
            )
            .unwrap_or_else(|e| {
              warn!(
                "CQE ConnectSuccess: Failed to get peer/local addr for connected_fd {}: {}",
                connected_fd, e
              );
              (
                crate::io_uring_backend::worker::dummy_socket_addr(),
                crate::io_uring_backend::worker::dummy_socket_addr(),
              )
            });

            if let Some(ref factory_id) = ext_op_ctx.protocol_handler_factory_id {
              let protocol_config = ext_op_ctx.protocol_config.as_ref().unwrap();

              let recv_buffer_manager = worker.buffer_manager.as_ref();
              let default_bgid_val_from_worker = worker.default_buffer_ring_group_id_val;
              match worker.handler_manager.create_and_add_handler(
                connected_fd,
                factory_id,
                protocol_config,
                false,
                recv_buffer_manager,
                default_bgid_val_from_worker,
                cqe_user_data,
              ) {
                Ok(initial_ops) => {
                  info!(
                    "CQE Processor: Connect successful (ext_ud:{}), new FD:{}. Handler created. Peer: {}, Local: {}",
                    cqe_user_data, connected_fd, peer_addr, local_addr
                  );
                  
                  // Call site must now handle the Result from process_handler_blueprints.
                  if let Err(remaining_blueprints) =
                    process_handler_blueprints(worker, connected_fd, initial_ops.sqe_blueprints)
                  {
                    warn!("CQE Processor: SQ full during initial op submission for new connection FD {}. Re-queueing {} blueprints.", connected_fd, remaining_blueprints.len());
                    for bp in remaining_blueprints {
                      worker.pending_sqe_retry_queue.push_back((connected_fd, bp));
                    }
                  }
                  if initial_ops.initiate_close_due_to_error {
                    warn!(
                      "CQE Processor: New connection handler for FD {} requested immediate close.",
                      connected_fd
                    );
                    worker
                      .fds_needing_close_initiated_pass
                      .push_back(connected_fd);
                  }
                  
                  UringOpCompletion::ConnectSuccess {
                    user_data: cqe_user_data,
                    connected_fd,
                    peer_addr,
                    local_addr,
                  }
                }
                Err(err_msg) => {
                  error!(
                    "CQE Processor: Connect successful (ext_ud:{}), FD:{}, but handler creation failed: {}",
                    cqe_user_data, connected_fd, err_msg
                  );
                  unsafe {
                    libc::close(connected_fd);
                  }
                  UringOpCompletion::OpError {
                    user_data: cqe_user_data,
                    op_name: ext_op_ctx.op_name.clone(),
                    error: ZmqError::Internal(err_msg),
                  }
                }
              }
            } else {
              error!(
                "CQE Processor: Connect successful (ext_ud:{}), but factory_id missing.",
                cqe_user_data
              );
              UringOpCompletion::OpError {
                user_data: cqe_user_data,
                op_name: ext_op_ctx.op_name.clone(),
                error: ZmqError::Internal("Connect factory_id missing".into()),
              }
            }
          }
          "Nop" => UringOpCompletion::NopSuccess {
            user_data: cqe_user_data,
          },
          "RegisterRawBuffers" => UringOpCompletion::RegisterRawBuffersSuccess {
            user_data: cqe_user_data,
          },
          _ => {
            warn!(
              "CQE Processor: Unhandled successful external op CQE: name='{}', ud={}",
              ext_op_ctx.op_name, cqe_user_data
            );
            UringOpCompletion::OpError {
              user_data: cqe_user_data,
              op_name: ext_op_ctx.op_name.clone(),
              error: ZmqError::Internal(format!("Unhandled external op: {}", ext_op_ctx.op_name)),
            }
          }
        }
      };
      if ext_op_ctx.reply_tx.send(Ok(completion_to_send)).is_err() {
        tracing::warn!(
          "CQE Processor: Reply_tx for external op ud {} was already taken.",
          cqe_user_data
        );
      }
      continue;
    }

    let mut op_details_taken_for_final_processing: Option<InternalOpDetails> = None;
    let mut is_zc_initial_pending_notify = false;
    let mut is_multishot_read_pending_more = false;
    let mut was_delegated_to_multishot_handler = false;

    if let Some(peeked_details) = worker.internal_op_tracker.get_op_details(cqe_user_data) {
      let handler_fd_peeked = peeked_details.fd;
      let op_type_peeked = peeked_details.op_type;

      if op_type_peeked == InternalOpType::SendZeroCopy {
        let is_notification_cqe = (cqe_flags & CQE_F_NOTIFY_FLAG) != 0;
        if !is_notification_cqe && cqe_result >= 0 {
          is_zc_initial_pending_notify = true;
          trace!(
            "[CQE Proc] SendZeroCopy INITIAL success (ud:{}, fd:{}, res:{}) - awaiting NOTIFY.",
            cqe_user_data,
            handler_fd_peeked,
            cqe_result
          );
        }
      } else if op_type_peeked == InternalOpType::RingReadMultishot {
        if cqe_result >= 0 && (cqe_flags & IOURING_CQE_F_MORE) != 0 {
          is_multishot_read_pending_more = true;
          trace!(
            "[CQE Proc] RingReadMultishot (ud:{}, fd:{}) has F_MORE. Op still active in kernel.",
            cqe_user_data,
            handler_fd_peeked
          );
        }
      }

      if matches!(op_type_peeked, InternalOpType::RingReadMultishot) {
        if let Some(handler) = worker.handler_manager.get_mut(handler_fd_peeked) {
          let recv_buffer_manager = worker.buffer_manager.as_ref();
          let worker_io_config = &worker.worker_io_config;
          let default_bgid_val_from_worker = worker.default_buffer_ring_group_id_val;
          let fds_to_initiate_close_queue = &mut worker.fds_needing_close_initiated_pass;
          let recv_bm_ref =
            recv_buffer_manager.expect("Recv BufferManager missing for multishot op");
          let interface = UringWorkerInterface::new(
            handler_fd_peeked,
            worker_io_config,
            Some(recv_bm_ref),
            default_bgid_val_from_worker,
            cqe_user_data,
          );
          if let Some(delegation_result) = handler.delegate_cqe_to_multishot_reader(
            &cqe,
            recv_bm_ref,
            &interface,
            &mut worker.internal_op_tracker,
          ) {
            was_delegated_to_multishot_handler = true;
            match delegation_result {
              Ok((handler_ops, should_cleanup_delegated_op_ud)) => {
                if should_cleanup_delegated_op_ud {
                  if worker
                    .internal_op_tracker
                    .take_op_details(cqe_user_data)
                    .is_none()
                  {
                    warn!("[CQE Proc] Multishot delegate requested cleanup for ud:{}, but it was already taken or not found.", cqe_user_data);
                  }
                } else if op_type_peeked == InternalOpType::RingReadMultishot {
                  is_multishot_read_pending_more = true;
                }
                
                if !handler_ops.sqe_blueprints.is_empty() {
                  if let Err(remaining_blueprints) = process_handler_blueprints(
                    worker,
                    handler_fd_peeked,
                    handler_ops.sqe_blueprints,
                  ) {
                    warn!("CQE Processor: SQ full during multishot delegate op submission for FD {}. Re-queueing {} blueprints.", handler_fd_peeked, remaining_blueprints.len());
                    for bp in remaining_blueprints {
                      worker
                        .pending_sqe_retry_queue
                        .push_back((handler_fd_peeked, bp));
                    }
                  }
                }
                if handler_ops.initiate_close_due_to_error {
                  worker
                    .fds_needing_close_initiated_pass
                    .push_back(handler_fd_peeked);
                }
                
              }
              Err(e) => {
                error!(
                  "[CQE Proc] Multishot delegate for FD {} (ud:{}) returned error: {}. Closing handler.",
                  handler_fd_peeked, cqe_user_data, e
                );
                if !fds_to_initiate_close_queue.contains(&handler_fd_peeked) {
                  fds_to_initiate_close_queue.push_back(handler_fd_peeked);
                }
                worker.internal_op_tracker.take_op_details(cqe_user_data);
              }
            }
          }
        } else {
          warn!(
            "[CQE Proc] Handler for FD {} (ud:{}, type:{:?}) not found for multishot/cancel. Orphaned op.",
            handler_fd_peeked, cqe_user_data, op_type_peeked
          );
          worker.internal_op_tracker.take_op_details(cqe_user_data);
        }
      }
    }

    if !is_zc_initial_pending_notify
      && !is_multishot_read_pending_more
      && !was_delegated_to_multishot_handler
    {
      op_details_taken_for_final_processing =
        worker.internal_op_tracker.take_op_details(cqe_user_data);
    }

    if let Some(op_details) = op_details_taken_for_final_processing {
      let handler_fd = op_details.fd;
      let op_type = op_details.op_type;
      trace!(
        "[CQE Proc] INTERNAL op (ud:{}, type:{:?}, fd:{}, res:{}) - Final Processing Path",
        cqe_user_data,
        op_type,
        handler_fd,
        cqe_result
      );

      match op_type {
        InternalOpType::Accept => {
          if cqe_result >= 0 {
            let client_fd = cqe_result as RawFd;
            let listener_fd = handler_fd;
            info!(
              "CQE Processor: Internal Accept CQE (ud:{}) on listener FD {} -> new client FD {}",
              cqe_user_data, listener_fd, client_fd
            );

            let recv_buffer_manager = worker.buffer_manager.as_ref();
            let default_bgid_val_from_worker = worker.default_buffer_ring_group_id_val;

            let factory_id_opt;
            let protocol_config_opt;
            {
              let listener_meta_opt = worker.handler_manager.get_listener_metadata(listener_fd);
              factory_id_opt =
                listener_meta_opt.map(|m| m.factory_id_for_accepted_connections.clone());
              protocol_config_opt =
                listener_meta_opt.map(|m| m.protocol_config_for_accepted.clone());
            }

            if let (Some(factory_id), Some(protocol_config)) = (factory_id_opt, protocol_config_opt)
            {
              const ACCEPTED_CONNECTION_SENTINEL_UD: UserData = 0;
              match worker.handler_manager.create_and_add_handler(
                client_fd,
                &factory_id,
                &protocol_config,
                true,
                recv_buffer_manager,
                default_bgid_val_from_worker,
                ACCEPTED_CONNECTION_SENTINEL_UD,
              ) {
                Ok(initial_ops) => {
                  
                  // Call site must now handle the Result from process_handler_blueprints.
                  if let Err(remaining_blueprints) =
                    process_handler_blueprints(worker, client_fd, initial_ops.sqe_blueprints)
                  {
                    warn!("CQE Processor: SQ full during initial op submission for accepted FD {}. Re-queueing {} blueprints.", client_fd, remaining_blueprints.len());
                    for bp in remaining_blueprints {
                      worker.pending_sqe_retry_queue.push_back((client_fd, bp));
                    }
                  }
                  if initial_ops.initiate_close_due_to_error {
                    warn!(
                      "CQE Processor: New accepted handler for FD {} requested immediate close.",
                      client_fd
                    );
                    worker.fds_needing_close_initiated_pass.push_back(client_fd);
                  }
                  
                }
                Err(e) => {
                  error!(
                    "Worker: Failed to create handler for accepted client_fd {} from listener {}: {}",
                    client_fd, listener_fd, e
                  );
                  unsafe {
                    libc::close(client_fd);
                  }
                }
              }
            } else {
              warn!("Worker: Could not find factory_id/protocol_config for listener fd {} to create handler for client_fd {}", listener_fd, client_fd);
              unsafe {
                libc::close(client_fd);
              }
            }

            if !is_worker_shutting_down {
              let new_accept_internal_ud = worker.internal_op_tracker.new_op_id(
                listener_fd,
                InternalOpType::Accept,
                InternalOpPayload::None,
              );
              let mut client_addr_storage_reaccept: libc::sockaddr_storage =
                unsafe { mem::zeroed() };
              let mut client_addr_len_reaccept =
                mem::size_of_val(&client_addr_storage_reaccept) as libc::socklen_t;
              let accept_sqe_reaccept = opcode::Accept::new(
                types::Fd(listener_fd),
                &mut client_addr_storage_reaccept as *mut _ as *mut _,
                &mut client_addr_len_reaccept,
              )
              .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
              .build()
              .user_data(new_accept_internal_ud);
              let mut sq_for_reaccept_op = unsafe { worker.ring.submission_shared() };
              unsafe {
                if sq_for_reaccept_op.push(&accept_sqe_reaccept).is_err() {
                  error!(
                    "Worker: Failed to re-submit Accept SQE for listener fd {} (SQ FULL?). Listener may stop accepting.",
                    listener_fd
                  );

                  let fds_to_initiate_close_queue = &mut worker.fds_needing_close_initiated_pass;
                  worker
                    .internal_op_tracker
                    .take_op_details(new_accept_internal_ud);
                  if !fds_to_initiate_close_queue.contains(&listener_fd) {
                    fds_to_initiate_close_queue.push_back(listener_fd);
                  }
                } else {
                  trace!(
                    "Worker: Re-submitted Accept SQE (ud:{}) for listener fd {}",
                    new_accept_internal_ud,
                    listener_fd
                  );
                }
              }
            } else {
              info!(
                "Worker: Not re-arming Accept on listener fd {} because worker is shutting down.",
                listener_fd
              );
            }
          } else {
            let accept_err = std::io::Error::from_raw_os_error(-cqe_result);
            error!(
              "CQE Processor: Internal Accept op (ud:{}) for listener FD {} FAILED with errno {}: {}",
              cqe_user_data, handler_fd, -cqe_result, accept_err
            );

            if !is_worker_shutting_down {
              let new_accept_internal_ud_on_err = worker.internal_op_tracker.new_op_id(
                handler_fd,
                InternalOpType::Accept,
                InternalOpPayload::None,
              );
              let mut client_addr_storage_err: libc::sockaddr_storage = unsafe { mem::zeroed() };
              let mut client_addr_len_err =
                mem::size_of_val(&client_addr_storage_err) as libc::socklen_t;
              let accept_sqe_on_err = opcode::Accept::new(
                types::Fd(handler_fd),
                &mut client_addr_storage_err as *mut _ as *mut _,
                &mut client_addr_len_err,
              )
              .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
              .build()
              .user_data(new_accept_internal_ud_on_err);
              let mut sq_for_reaccept_err_op = unsafe { worker.ring.submission_shared() };

              let fds_to_initiate_close_queue = &mut worker.fds_needing_close_initiated_pass;
              unsafe {
                if sq_for_reaccept_err_op.push(&accept_sqe_on_err).is_err() {
                  error!("Worker: Failed to re-submit Accept SQE after error for listener_fd {} (SQ FULL?). Listener may stop accepting.", handler_fd);
                  worker
                    .internal_op_tracker
                    .take_op_details(new_accept_internal_ud_on_err);
                  if !fds_to_initiate_close_queue.contains(&handler_fd) {
                    fds_to_initiate_close_queue.push_back(handler_fd);
                  }
                }
              }
            }
          }
        }
        InternalOpType::CloseFd => {
          if cqe_result >= 0 {
            info!(
              "CQE Processor: Internal CloseFd op (ud:{}) for FD {} successful.",
              cqe_user_data, handler_fd
            );

            if worker.fd_to_mpsc_rx.remove(&handler_fd).is_some() {
              trace!(
                "CQE Processor: Removed MPSC receiver for closed FD {}",
                handler_fd
              );
            }

            loop {
              if let Some((ext_ud, ext_ctx)) = worker
                .external_op_tracker
                .take_op_if_shutdown_for_fd(handler_fd)
              {
                info!(
                        "CQE Processor: Notifying external shutdown requester (ext_ud: {}) for closed FD {}.",
                        ext_ud, handler_fd
                    );
                let ack = UringOpCompletion::ShutdownConnectionHandlerComplete {
                  user_data: ext_ud,
                  fd: handler_fd,
                };
                if ext_ctx.reply_tx.send(Ok(ack)).is_err() {
                  tracing::warn!(
                            "CQE Processor: Reply_tx for external Shutdown op (ext_ud {}) was already taken.",
                            ext_ud
                        );
                }
              } else {
                break;
              }
            }

            if let Some(mut h) = worker.handler_manager.remove_handler(handler_fd) {
              h.fd_has_been_closed();
              info!(
                "CQE Processor: Handler for FD {} removed after successful CloseFd.",
                handler_fd
              );
            } else {
              warn!(
                "CQE Processor: No handler found for FD {} to remove after CloseFd (ud:{}), possibly already removed.",
                handler_fd, cqe_user_data
              );
            }

            let mailbox_map_opt =
              uring::global_state::get_uring_fd_to_socket_core_mailbox_map_oncecell().get();

            if let Some(map_arc) = mailbox_map_opt {
              let mailbox_clone_opt = map_arc.read().get(&handler_fd).cloned();

              if let Some(socket_core_mailbox) = mailbox_clone_opt {
                debug!(
                  "CQE Processor: Notifying SocketCore of successful FD {} close via UringFdError command.",
                  handler_fd
                );
                let notification_cmd = Command::UringFdError {
                  fd: handler_fd,
                  error: ZmqError::ConnectionClosed,
                };
                if socket_core_mailbox.try_send(notification_cmd).is_err() {
                  warn!("CQE Processor: Failed to send final close notification to SocketCore for FD {}. Channel might be full or closed.", handler_fd);
                }
              }
              uring::global_state::unregister_uring_fd_socket_core_mailbox(handler_fd);
            }
          } else {
            let raw_errno = -cqe_result;
            let close_err = std::io::Error::from_raw_os_error(raw_errno);
            error!(
              "CQE Processor: Internal CloseFd op (ud:{}) for FD {} FAILED: {}. Forcing handler removal.",
              cqe_user_data, handler_fd, close_err
            );
            if let Some(mut h) = worker.handler_manager.remove_handler(handler_fd) {
              h.fd_has_been_closed();
            }

            loop {
              if let Some((ext_ud, ext_ctx)) = worker
                .external_op_tracker
                .take_op_if_shutdown_for_fd(handler_fd)
              {
                let err_completion = UringOpCompletion::OpError {
                  user_data: ext_ud,
                  op_name: "ShutdownConnectionHandler (via CloseFd failure)".to_string(),
                  error: ZmqError::from(std::io::Error::from_raw_os_error(raw_errno)),
                };
                let _ = ext_ctx.reply_tx.send(Ok(err_completion));
              } else {
                break;
              }
            }
          }
        }
        InternalOpType::SendZeroCopy => {
          trace!(
            "[CQE Proc] Finalizing SendZeroCopy (ud:{}, fd:{}, res:{}, notify_flag:{})",
            cqe_user_data,
            handler_fd,
            cqe_result,
            (cqe_flags & CQE_F_NOTIFY_FLAG) != 0
          );

          if let InternalOpPayload::SendZeroCopy {
            send_buf_id,
            app_op_ud,
            ref app_op_name,
            ..
          } = op_details.payload
          {
            if let Some(pool_arc) = &worker.send_buffer_pool {
              pool_arc.release_buffer(send_buf_id);
              trace!(
                "[CQE Proc] Released ZC send_buf_id {:?} for ud:{}",
                send_buf_id,
                cqe_user_data
              );
            } else {
              error!(
                "[CQE Proc] SendBufferPool MISSING for SendZeroCopy finalization (ud:{}, buf_id:{:?})!",
                cqe_user_data, send_buf_id
              );
            }

            if app_op_ud != HANDLER_INTERNAL_SEND_OP_UD {
              if let Some(ext_op_ctx_mut) = worker.external_op_tracker.get_op_context_mut(app_op_ud)
              {
                if cqe_result < 0 {
                  let zmq_err = ZmqError::from(std::io::Error::from_raw_os_error(-cqe_result));
                  let completion = Ok(UringOpCompletion::OpError {
                    user_data: app_op_ud,
                    op_name: app_op_name.clone(),
                    error: zmq_err,
                  });
                  if let Some(ctx_to_reply) = worker.external_op_tracker.take_op(app_op_ud) {
                    let _ = ctx_to_reply.reply_tx.send(completion);
                  }
                } else if let Some(multi_state) = &mut ext_op_ctx_mut.multipart_state {
                  multi_state.completed_parts += 1;
                  if multi_state.completed_parts >= multi_state.total_parts {
                    let completion = Ok(UringOpCompletion::SendDataViaHandlerAck {
                      user_data: app_op_ud,
                      fd: handler_fd,
                    });
                    if let Some(ctx_to_reply) = worker.external_op_tracker.take_op(app_op_ud) {
                      let _ = ctx_to_reply.reply_tx.send(completion);
                    }
                  }
                } else {
                  let completion = Ok(UringOpCompletion::SendDataViaHandlerAck {
                    user_data: app_op_ud,
                    fd: handler_fd,
                  });
                  if let Some(ctx_to_reply) = worker.external_op_tracker.take_op(app_op_ud) {
                    let _ = ctx_to_reply.reply_tx.send(completion);
                  }
                }
              }
            } else {
              trace!("[CQE Proc] ZC Final: Send for internal handler op (ud:{}, app_op_ud:{}) completed. No external reply needed.", cqe_user_data, app_op_ud);
            }
          } else {
            error!(
              "[CQE Proc] ZC Final (ud:{}): Mismatched InternalOpPayload type.",
              cqe_user_data
            );
          }
        }
        InternalOpType::Send => {
          trace!(
            "[CQE Proc] Normal Send completion (ud:{}, fd:{}, res:{})",
            cqe_user_data,
            handler_fd,
            cqe_result
          );
          if let InternalOpPayload::SendBuffer {
            app_op_ud: Some(app_op_ud_val),
            app_op_name: Some(ref app_op_name),
            ..
          } = op_details.payload
          {
            if app_op_ud_val != HANDLER_INTERNAL_SEND_OP_UD {
              if let Some(ext_op_ctx_mut) =
                worker.external_op_tracker.get_op_context_mut(app_op_ud_val)
              {
                if cqe_result < 0 {
                  let zmq_err = ZmqError::from(std::io::Error::from_raw_os_error(-cqe_result));
                  let completion = Ok(UringOpCompletion::OpError {
                    user_data: app_op_ud_val,
                    op_name: app_op_name.clone(),
                    error: zmq_err,
                  });
                  if let Some(ctx_to_reply) = worker.external_op_tracker.take_op(app_op_ud_val) {
                    let _ = ctx_to_reply.reply_tx.send(completion);
                  }
                } else if let Some(multi_state) = &mut ext_op_ctx_mut.multipart_state {
                  multi_state.completed_parts += 1;
                  if multi_state.completed_parts >= multi_state.total_parts {
                    let completion = Ok(UringOpCompletion::SendDataViaHandlerAck {
                      user_data: app_op_ud_val,
                      fd: handler_fd,
                    });
                    if let Some(ctx_to_reply) = worker.external_op_tracker.take_op(app_op_ud_val) {
                      let _ = ctx_to_reply.reply_tx.send(completion);
                    }
                  }
                } else {
                  let completion = Ok(UringOpCompletion::SendDataViaHandlerAck {
                    user_data: app_op_ud_val,
                    fd: handler_fd,
                  });
                  if let Some(ctx_to_reply) = worker.external_op_tracker.take_op(app_op_ud_val) {
                    let _ = ctx_to_reply.reply_tx.send(completion);
                  }
                }
              }
            } else {
              trace!("[CQE Proc] Normal Send: Send for internal handler op (ud:{}, app_op_ud:{}) completed.", cqe_user_data, app_op_ud_val);
              
              if let Some(handler) = worker.handler_manager.get_mut(handler_fd) {
                let recv_buffer_manager = worker.buffer_manager.as_ref();
                let worker_io_config = &worker.worker_io_config;
                let default_bgid_val_from_worker = worker.default_buffer_ring_group_id_val;
                let interface = UringWorkerInterface::new(
                  handler_fd,
                  worker_io_config,
                  recv_buffer_manager,
                  default_bgid_val_from_worker,
                  cqe_user_data,
                );
                let handler_output = handler.handle_internal_sqe_completion(
                  cqe_user_data,
                  cqe_result,
                  cqe_flags,
                  &interface,
                );
                if !handler_output.sqe_blueprints.is_empty() {
                  if let Err(remaining_blueprints) =
                    process_handler_blueprints(worker, handler_fd, handler_output.sqe_blueprints)
                  {
                    warn!("CQE Processor: SQ full during internal send op submission for FD {}. Re-queueing {} blueprints.", handler_fd, remaining_blueprints.len());
                    for bp in remaining_blueprints {
                      worker.pending_sqe_retry_queue.push_back((handler_fd, bp));
                    }
                  }
                }
                if handler_output.initiate_close_due_to_error {
                  worker
                    .fds_needing_close_initiated_pass
                    .push_back(handler_fd);
                }
              } else {
                warn!(
                  "[CQE Proc] Handler for FD {} not found for internal send completion (ud:{}).",
                  handler_fd, cqe_user_data
                );
              }
              
            }
          } else {
            if cqe_result < 0 {
              warn!(
                "[CQE Proc] Internal normal send (ud:{}) for FD {} FAILED: {}. Notifying handler.",
                cqe_user_data,
                handler_fd,
                std::io::Error::from_raw_os_error(-cqe_result)
              );
            }

            
            if let Some(handler) = worker.handler_manager.get_mut(handler_fd) {
              let recv_buffer_manager = worker.buffer_manager.as_ref();
              let worker_io_config = &worker.worker_io_config;
              let default_bgid_val_from_worker = worker.default_buffer_ring_group_id_val;

              let interface = UringWorkerInterface::new(
                handler_fd,
                worker_io_config,
                recv_buffer_manager,
                default_bgid_val_from_worker,
                cqe_user_data,
              );
              let handler_output = handler.handle_internal_sqe_completion(
                cqe_user_data,
                cqe_result,
                cqe_flags,
                &interface,
              );
              if !handler_output.sqe_blueprints.is_empty() {
                if let Err(remaining_blueprints) =
                  process_handler_blueprints(worker, handler_fd, handler_output.sqe_blueprints)
                {
                  warn!("CQE Processor: SQ full during internal send (no app_op_ud) op submission for FD {}. Re-queueing {} blueprints.", handler_fd, remaining_blueprints.len());
                  for bp in remaining_blueprints {
                    worker.pending_sqe_retry_queue.push_back((handler_fd, bp));
                  }
                }
              }
              if handler_output.initiate_close_due_to_error {
                worker
                  .fds_needing_close_initiated_pass
                  .push_back(handler_fd);
              }
            } else {
              warn!(
                "[CQE Proc] Handler for FD {} not found for internal send completion (ud:{}).",
                handler_fd, cqe_user_data
              );
            }
            
          }
        }
        InternalOpType::RingRead | InternalOpType::GenericHandlerOp => {
          if let Some(handler) = worker.handler_manager.get_mut(handler_fd) {
            let recv_buffer_manager = worker.buffer_manager.as_ref();
            let worker_io_config = &worker.worker_io_config;
            let default_bgid_val_from_worker = worker.default_buffer_ring_group_id_val;

            let interface = UringWorkerInterface::new(
              handler_fd,
              worker_io_config,
              recv_buffer_manager,
              default_bgid_val_from_worker,
              cqe_user_data,
            );
            let handler_output: HandlerIoOps;
            if op_type == InternalOpType::RingRead && cqueue::buffer_select(cqe_flags).is_some() {
              if cqe_result >= 0 {
                let fds_to_initiate_close_queue = &mut worker.fds_needing_close_initiated_pass;
                if let Some(bid) = cqueue::buffer_select(cqe_flags) {
                  let bytes_read = cqe_result as usize;
                  if let Some(bm) = recv_buffer_manager {
                    match unsafe { bm.borrow_kernel_filled_buffer(bid, bytes_read) } {
                      Ok(borrowed_buf) => {
                        handler_output =
                          handler.process_ring_read_data(&borrowed_buf, bid, &interface);
                      }
                      Err(e) => {
                        error!(
                          "[CQE Proc] RingRead (ud:{}): Error borrowing buffer: {}. Closing FD {}.",
                          cqe_user_data, e, handler_fd
                        );
                        handler_output = HandlerIoOps::default().set_error_close();
                        if !fds_to_initiate_close_queue.contains(&handler_fd) {
                          fds_to_initiate_close_queue.push_back(handler_fd);
                        }
                      }
                    }
                  } else {
                    error!(
                      "[CQE Proc] RingRead (ud:{}): RecvBufferManager is None. Closing FD {}.",
                      cqe_user_data, handler_fd
                    );
                    handler_output = HandlerIoOps::default().set_error_close();
                    if !fds_to_initiate_close_queue.contains(&handler_fd) {
                      fds_to_initiate_close_queue.push_back(handler_fd);
                    }
                  }
                } else {
                  error!("[CQE Proc] RingRead (ud:{}): CQE_F_BUFFER set but buffer_select() is None or invalid. Closing FD {}.", cqe_user_data, handler_fd);
                  handler_output = HandlerIoOps::default().set_error_close();
                  if !fds_to_initiate_close_queue.contains(&handler_fd) {
                    fds_to_initiate_close_queue.push_back(handler_fd);
                  }
                }
              } else {
                error!(
                  "[CQE Proc] RingRead op (ud:{}) for FD {} FAILED: {}. Notifying handler.",
                  cqe_user_data,
                  handler_fd,
                  std::io::Error::from_raw_os_error(-cqe_result)
                );
                handler_output = handler.handle_internal_sqe_completion(
                  cqe_user_data,
                  cqe_result,
                  cqe_flags,
                  &interface,
                );
              }
            } else {
              handler_output = handler.handle_internal_sqe_completion(
                cqe_user_data,
                cqe_result,
                cqe_flags,
                &interface,
              );
              if cqe_result < 0 && op_type != InternalOpType::RingRead {
                warn!(
                  "[CQE Proc] Generic op (ud:{}, type:{:?}) for FD {} FAILED: {}. Handler notified.",
                  cqe_user_data,
                  op_type,
                  handler_fd,
                  std::io::Error::from_raw_os_error(-cqe_result)
                );
              }
            }

            
            if !handler_output.sqe_blueprints.is_empty() {
              if let Err(remaining_blueprints) =
                process_handler_blueprints(worker, handler_fd, handler_output.sqe_blueprints)
              {
                warn!("CQE Processor: SQ full during RingRead/Generic op submission for FD {}. Re-queueing {} blueprints.", handler_fd, remaining_blueprints.len());
                for bp in remaining_blueprints {
                  worker.pending_sqe_retry_queue.push_back((handler_fd, bp));
                }
              }
            }
            if handler_output.initiate_close_due_to_error {
              worker
                .fds_needing_close_initiated_pass
                .push_back(handler_fd);
            }
          
          } else {
            warn!(
              "[CQE Proc] Handler for FD {} not found for op type {:?} (ud:{}).",
              handler_fd, op_type, cqe_user_data
            );
          }
        }
        InternalOpType::EventFdPoll => {
          warn!("[CQE Proc] Reached InternalOpType::EventFdPoll (ud:{}) in final processing path. Should have been handled by event_fd_poller.handle_cqe_if_matches.", cqe_user_data);
        }
        InternalOpType::RingReadMultishot | InternalOpType::AsyncCancel => {
          warn!("[CQE Proc] Reached InternalOpType::{:?} (ud:{}) in final processing path. Should have been handled by delegate_cqe_to_multishot_reader.", op_type, cqe_user_data);
        }
      }
    } else if is_zc_initial_pending_notify
      || is_multishot_read_pending_more
      || was_delegated_to_multishot_handler
    {
      trace!("[CQE Proc] CQE (ud:{}) was peek-handled (ZC initial success OR Multishot F_MORE OR full MS delegate). No final processing here for this UD.", cqe_user_data);
    } else {
      trace!("[CQE Proc] CQE (ud:{}, res:{}, flags:{:x}) - UNKNOWN or ALREADY PROCESSED (no op_details and not peek/delegate-handled). Ignored.", cqe_user_data, cqe_result, cqe_flags);
    }
  }
  Ok(num_processed)
}
