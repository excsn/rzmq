#![cfg(feature = "io-uring")]

use super::internal_op_tracker::{
  InternalOpDetails, InternalOpPayload, InternalOpType, PinnedEgressBatch,
};
use crate::io_uring_backend::connection_handler::{
  HandlerIoOps, HandlerSqeBlueprint, UringWorkerInterface,
};
use crate::io_uring_backend::ops::{HANDLER_INTERNAL_SEND_OP_UD, UringOpCompletion, UserData};
use crate::io_uring_backend::worker::UringWorker;
use crate::io_uring_backend::worker::multishot_reader::IOURING_CQE_F_MORE;
use crate::message::{FrameBatch, Msg};
use crate::socket::connection_iface::DummyConnection;
use crate::{Command, ZmqError, uring};

use io_uring::cqueue::Entry;
use io_uring::{cqueue, opcode, squeue, types};
use std::mem;
use std::os::unix::io::RawFd;
use tracing::{debug, error, info, trace, warn};

#[cfg(feature = "io-uring")]
const CQE_F_NOTIFY_FLAG: u32 = 1 << 3; // (8) for kernels 6.0+

/// Submit a single blueprint SQE for `fd`. Returns `Err(blueprint)` if the submission
/// queue is full (race after caller's pre-check) so the caller can push it back to the front
/// of `pending_blueprints` and retry next cycle.
pub(crate) fn process_handler_blueprint(
  worker: &mut UringWorker,
  fd: RawFd,
  blueprint: HandlerSqeBlueprint,
) -> Result<(), HandlerSqeBlueprint> {
  let ring = &mut worker.ring;
  let sq = &mut unsafe { ring.submission_shared() };
  let external_ops = &mut worker.external_op_tracker;
  let internal_ops = &mut worker.internal_op_tracker;
  let handler_manager = &mut worker.handler_manager;
  let send_buffer_pool = &worker.send_buffer_pool;
  let worker_cfg_send_zerocopy_enabled = worker.cfg_send_zerocopy_enabled;

  if sq.is_full() {
    debug!(
      "CQE Processor: SQ full when processing blueprint for FD {} (race). Re-queueing.",
      fd
    );
    return Err(blueprint);
  }

  #[cfg(debug_assertions)]
  worker
    .metrics
    .sqes_submitted
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

  match blueprint {
    HandlerSqeBlueprint::RequestSetCork(enable) => {
      let cork_val: i32 = if enable { 1 } else { 0 };
      let mut entry = opcode::SetSockOpt::new(
        types::Fd(fd),
        libc::SOL_TCP as _,
        libc::TCP_CORK as _,
        &cork_val as *const _ as *const libc::c_void,
        std::mem::size_of_val(&cork_val) as u32,
      )
      .build();
      let user_data = internal_ops.new_op_id(
        fd,
        InternalOpType::GenericHandlerOp,
        InternalOpPayload::None,
      );
      entry = entry.user_data(user_data);

      unsafe {
        if sq.push(&entry).is_err() {
          debug!(
            "CQE Processor: SQ push failed for FD {} RequestSetCork (race). Re-queueing.",
            fd
          );
          internal_ops.take_op_details(user_data);
          return Err(HandlerSqeBlueprint::RequestSetCork(enable));
        } else {
          #[cfg(debug_assertions)]
          worker
            .metrics
            .sqe_op_other
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

          trace!(
            "CQE Processor: Queued SetSockOpt(cork={}) SQE (ud:{}) for FD {}.",
            enable, user_data, fd
          );
        }
      }
      Ok(())
    }

    HandlerSqeBlueprint::RequestSend {
      data,
      send_op_flags,
      originating_app_op_ud,
      batch_count,
    } => {
      #[cfg(debug_assertions)]
      worker.metrics.record_write_batch(batch_count as u64);
      let app_op_name_str = external_ops
        .get_op_context_ref(originating_app_op_ud)
        .map(|ctx| ctx.op_name.clone())
        .unwrap_or_else(|| "UnknownAppOpSend".to_string());
      let data_ptr = data.as_ptr();
      let data_len = data.len() as u32;
      let op_payload = InternalOpPayload::SendBuffer {
        buffer: data,
        send_op_flags,
        app_op_ud: Some(originating_app_op_ud),
        app_op_name: Some(app_op_name_str),
      };
      let mut entry = opcode::Send::new(types::Fd(fd), data_ptr, data_len)
        .flags(send_op_flags)
        .build();
      let user_data = internal_ops.new_op_id(fd, InternalOpType::Send, op_payload);
      entry = entry.user_data(user_data);

      unsafe {
        if sq.push(&entry).is_err() {
          debug!(
            "CQE Processor: SQ push failed for FD {} RequestSend (race). Re-queueing.",
            fd
          );
          let dropped = internal_ops.take_op_details(user_data);
          if let InternalOpPayload::SendBuffer {
            buffer,
            send_op_flags,
            app_op_ud,
            ..
          } = dropped.unwrap().payload
          {
            return Err(HandlerSqeBlueprint::RequestSend {
              data: buffer,
              send_op_flags,
              originating_app_op_ud: app_op_ud.unwrap_or(HANDLER_INTERNAL_SEND_OP_UD),
              batch_count,
            });
          }
          unreachable!("SendBuffer payload expected after RequestSend registration");
        } else {
          #[cfg(debug_assertions)]
          worker
            .metrics
            .sqe_op_write
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

          trace!(
            "CQE Processor: Queued Send SQE (ud:{}) for FD {}.",
            user_data, fd
          );
        }
      }
      Ok(())
    }

    HandlerSqeBlueprint::RequestSendZeroCopy {
      data_to_send,
      send_op_flags,
      originating_app_op_ud,
      batch_count,
    } => {
      #[cfg(debug_assertions)]
      worker.metrics.record_write_batch(batch_count as u64);
      let mut submitted_via_zc_path = false;
      let mut zc_entry: Option<squeue::Entry> = None;
      let mut op_payload = InternalOpPayload::None;
      let mut op_type = InternalOpType::Send;

      if worker_cfg_send_zerocopy_enabled {
        if let Some(pool_arc) = send_buffer_pool {
          if let Some((reg_buf_id, buffer_ptr, len_copied)) =
            pool_arc.acquire_and_prep_buffer(&data_to_send)
          {
            let app_op_name_str = external_ops
              .get_op_context_ref(originating_app_op_ud)
              .map(|ctx| ctx.op_name.clone())
              .unwrap_or_else(|| "UnknownAppOpSendZC".to_string());
            op_payload = InternalOpPayload::SendZeroCopy {
              send_buf_id: reg_buf_id,
              original_data: data_to_send.clone(),
              send_op_flags,
              app_op_ud: originating_app_op_ud,
              app_op_name: app_op_name_str,
            };
            zc_entry = Some(
              opcode::SendZc::new(types::Fd(fd), buffer_ptr, len_copied)
                .buf_index(Some(reg_buf_id.0))
                .flags(send_op_flags)
                .build(),
            );
            op_type = InternalOpType::SendZeroCopy;
            submitted_via_zc_path = true;
          } else {
            debug!(
              "CQE Processor: ZC Send - pool buffer unavailable for FD {}. Falling back.",
              fd
            );
          }
        } else {
          debug!(
            "CQE Processor: ZC Send - SendBufferPool not available for FD {}. Falling back.",
            fd
          );
        }
      }

      if !submitted_via_zc_path {
        let app_op_name_str_fb = external_ops
          .get_op_context_ref(originating_app_op_ud)
          .map(|ctx| ctx.op_name.clone())
          .unwrap_or_else(|| "UnknownAppOpSendFallback".to_string());
        let data_ptr = data_to_send.as_ptr();
        let data_len = data_to_send.len() as u32;
        op_payload = InternalOpPayload::SendBuffer {
          buffer: data_to_send,
          send_op_flags,
          app_op_ud: Some(originating_app_op_ud),
          app_op_name: Some(app_op_name_str_fb),
        };
        zc_entry = Some(
          opcode::Send::new(types::Fd(fd), data_ptr, data_len)
            .flags(send_op_flags)
            .build(),
        );
        op_type = InternalOpType::Send;
      }

      let mut entry = zc_entry.expect("entry set in both ZC and fallback paths");
      let user_data = internal_ops.new_op_id(fd, op_type, op_payload);
      entry = entry.user_data(user_data);

      unsafe {
        if sq.push(&entry).is_err() {
          debug!(
            "CQE Processor: SQ push failed for FD {} RequestSendZeroCopy (race). Re-queueing.",
            fd
          );
          let dropped = internal_ops.take_op_details(user_data).unwrap();
          let requeue = match dropped.payload {
            InternalOpPayload::SendZeroCopy {
              send_buf_id,
              original_data,
              send_op_flags,
              app_op_ud,
              ..
            } => {
              if let Some(pool) = &worker.send_buffer_pool {
                pool.release_buffer(send_buf_id);
              }
              HandlerSqeBlueprint::RequestSend {
                data: original_data,
                send_op_flags,
                originating_app_op_ud: app_op_ud,
                batch_count,
              }
            }
            InternalOpPayload::SendBuffer {
              buffer,
              send_op_flags,
              app_op_ud,
              ..
            } => HandlerSqeBlueprint::RequestSend {
              data: buffer,
              send_op_flags,
              originating_app_op_ud: app_op_ud.unwrap_or(HANDLER_INTERNAL_SEND_OP_UD),
              batch_count,
            },
            _ => unreachable!("unexpected payload after RequestSendZeroCopy registration"),
          };
          return Err(requeue);
        } else {
          #[cfg(debug_assertions)]
          worker
            .metrics
            .sqe_op_write
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          trace!(
            "CQE Processor: Queued SendZeroCopy/Send SQE (ud:{}) for FD {}.",
            user_data, fd
          );
        }
      }
      Ok(())
    }

    HandlerSqeBlueprint::RequestSendRawVectored {
      bufs,
      send_op_flags,
      batch_count,
    } => {
      #[cfg(debug_assertions)]
      worker.metrics.record_write_batch(batch_count as u64);
      let total_len: usize = bufs.iter().map(|b| b.len()).sum();

      // Convert to a Box before taking the pointer. `into_boxed_slice()` may
      // reallocate to trim excess Vec capacity; by boxing first the heap address
      // is final and stable before `iov_ptr` is captured.
      let iovecs_box: Box<[libc::iovec]> = bufs
        .iter()
        .map(|b| libc::iovec {
          iov_base: b.as_ptr() as *mut libc::c_void,
          iov_len: b.len(),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
      let iov_ptr = iovecs_box.as_ptr();
      let iov_len = iovecs_box.len() as u32;

      let batch = PinnedEgressBatch {
        iovecs: iovecs_box,
        payloads: bufs,
        total_len,
        send_op_flags,
      };
      let op_payload = InternalOpPayload::RawVectored(batch);
      let user_data = internal_ops.new_op_id(fd, InternalOpType::SendRawVectored, op_payload);
      let mut entry = opcode::Writev::new(types::Fd(fd), iov_ptr, iov_len).build();
      entry = entry.user_data(user_data);

      unsafe {
        if sq.push(&entry).is_err() {
          debug!(
            "CQE Processor: SQ push failed for FD {} RequestSendRawVectored (race). Re-queueing.",
            fd
          );
          let dropped = internal_ops.take_op_details(user_data).unwrap();
          if let InternalOpPayload::RawVectored(b) = dropped.payload {
            return Err(HandlerSqeBlueprint::RequestSendRawVectored {
              bufs: b.payloads,
              send_op_flags: b.send_op_flags,
              batch_count: 0,
            });
          }
          unreachable!();
        } else {
          #[cfg(debug_assertions)]
          worker
            .metrics
            .sqe_op_write
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

          trace!(
            "CQE Processor: Queued raw Writev SQE (ud:{}, iovecs:{}) for FD {}.",
            user_data, iov_len, fd
          );
        }
      }
      Ok(())
    }

    HandlerSqeBlueprint::RequestSendZeroCopyLeased { id, ptr, len } => {
      let op_payload = InternalOpPayload::SendZeroCopyLeased { send_buf_id: id };
      let mut entry = opcode::SendZc::new(types::Fd(fd), ptr, len)
        .buf_index(Some(id.0))
        .build();
      let user_data = internal_ops.new_op_id(fd, InternalOpType::SendZeroCopyLeased, op_payload);
      entry = entry.user_data(user_data);

      unsafe {
        if sq.push(&entry).is_err() {
          debug!(
            "CQE Processor: SQ push failed for FD {} RequestSendZeroCopyLeased (race). Re-queueing.",
            fd
          );
          let dropped = internal_ops.take_op_details(user_data).unwrap();
          if let InternalOpPayload::SendZeroCopyLeased { send_buf_id } = dropped.payload {
            if let Some(pool) = send_buffer_pool {
              pool.release_buffer(send_buf_id);
            }
          }
          return Err(HandlerSqeBlueprint::RequestSendZeroCopyLeased { id, ptr, len });
        } else {
          #[cfg(debug_assertions)]
          worker
            .metrics
            .sqe_op_write
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          trace!(
            "CQE Processor: Queued SendZcLeased SQE (ud:{}) for FD {}.",
            user_data, fd
          );
        }
      }
      Ok(())
    }

    HandlerSqeBlueprint::RequestClose => {
      let mut entry = opcode::Close::new(types::Fd(fd)).build();
      let user_data = internal_ops.new_op_id(fd, InternalOpType::CloseFd, InternalOpPayload::None);
      entry = entry.user_data(user_data);

      unsafe {
        if sq.push(&entry).is_err() {
          debug!(
            "CQE Processor: SQ push failed for FD {} RequestClose (race). Re-queueing.",
            fd
          );
          internal_ops.take_op_details(user_data);
          return Err(HandlerSqeBlueprint::RequestClose);
        } else {
          #[cfg(debug_assertions)]
          worker
            .metrics
            .sqe_op_other
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

          trace!(
            "CQE Processor: Queued Close SQE (ud:{}) for FD {}.",
            user_data, fd
          );
        }
      }
      Ok(())
    }

    HandlerSqeBlueprint::RequestNewRingReadMultishot { fd: bp_fd, bgid } => {
      if bp_fd != fd {
        error!(
          "CQE Processor: FD mismatch in RequestNewRingReadMultishot! Handler FD {}, Blueprint FD {}. Dropping.",
          fd, bp_fd
        );
        return Ok(());
      }
      let op_user_data = internal_ops.new_op_id(
        fd,
        InternalOpType::RingReadMultishot,
        InternalOpPayload::None,
      );
      let entry = opcode::RecvMulti::new(types::Fd(fd), bgid)
        .build()
        .user_data(op_user_data);

      unsafe {
        if sq.push(&entry).is_err() {
          debug!(
            "CQE Processor: SQ push failed for FD {} RecvMulti (race). Re-queueing.",
            fd
          );
          internal_ops.take_op_details(op_user_data);
          return Err(HandlerSqeBlueprint::RequestNewRingReadMultishot { fd: bp_fd, bgid });
        } else {
          #[cfg(debug_assertions)]
          worker
            .metrics
            .sqe_op_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

          trace!(
            "CQE Processor: Queued RecvMulti SQE (ud:{}) for FD {}.",
            op_user_data, fd
          );
          if let Some(handler) = handler_manager.get_mut(fd) {
            handler.inform_multishot_reader_op_submitted(op_user_data, false, None);
          } else {
            debug!(
              "CQE Processor: Handler for FD {} not found after submitting RecvMulti.",
              fd
            );
          }
        }
      }
      Ok(())
    }

    HandlerSqeBlueprint::RequestNewAsyncCancel {
      fd: bp_fd,
      target_user_data,
    } => {
      if bp_fd != fd {
        error!(
          "CQE Processor: FD mismatch in RequestNewAsyncCancel! Handler FD {}, Blueprint FD {}. Dropping.",
          fd, bp_fd
        );
        return Ok(());
      }
      let cancel_op_user_data = internal_ops.new_op_id(
        fd,
        InternalOpType::AsyncCancel,
        InternalOpPayload::CancelTarget { target_user_data },
      );
      let entry = opcode::AsyncCancel::new(target_user_data)
        .build()
        .user_data(cancel_op_user_data);

      unsafe {
        if sq.push(&entry).is_err() {
          debug!(
            "CQE Processor: SQ push failed for FD {} AsyncCancel target_ud:{} (race). Re-queueing.",
            fd, target_user_data
          );
          internal_ops.take_op_details(cancel_op_user_data);
          return Err(HandlerSqeBlueprint::RequestNewAsyncCancel {
            fd: bp_fd,
            target_user_data,
          });
        } else {
          #[cfg(debug_assertions)]
          worker
            .metrics
            .sqe_op_other
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

          trace!(
            "CQE Processor: Queued AsyncCancel SQE (ud:{}, target_ud:{}) for FD {}.",
            cancel_op_user_data, target_user_data, fd
          );
          if let Some(handler) = handler_manager.get_mut(fd) {
            handler.inform_multishot_reader_op_submitted(
              cancel_op_user_data,
              true,
              Some(target_user_data),
            );
          } else {
            debug!(
              "CQE Processor: Handler for FD {} not found after submitting AsyncCancel.",
              fd
            );
          }
        }
      }
      Ok(())
    }
  }
}

/// This function is now a "pure" processor for CQEs. It reads from the completion queue,
/// processes each entry, and returns a list of new I/O work (blueprints) that the
/// handlers generated in response. The main loop is then responsible for submitting this new work.
pub(crate) fn process_all_cqes(
  worker: &mut UringWorker,
  is_worker_shutting_down: bool,
) -> Result<(usize, Vec<(RawFd, Vec<HandlerSqeBlueprint>)>), ZmqError> {
  {
    let mut cq: cqueue::CompletionQueue<'_> = unsafe { worker.ring.completion_shared() };
    cq.sync();
    worker.cqe_scratch.clear();
    worker.cqe_scratch.extend(cq.into_iter());
  }
  let num_processed = worker.cqe_scratch.len();

  let mut new_work_generated: Vec<(RawFd, Vec<HandlerSqeBlueprint>)> = Vec::new();

  for i in 0..num_processed {
    let cqe = worker.cqe_scratch[i].clone(); // 16-byte struct clone — stack copy, no heap allocation
    let cqe_user_data = cqe.user_data();
    let cqe_result = cqe.result();
    let cqe_flags = cqe.flags();

    trace!(
      "[CQE Proc] CQE: ud={}, res={}, flags={:x}",
      cqe_user_data, cqe_result, cqe_flags
    );

    if worker.event_fd_poller.handle_cqe_if_matches(
      cqe_user_data,
      cqe_result,
      &mut worker.internal_op_tracker,
      is_worker_shutting_down,
    ) {
      worker.metrics.record_wakeup();
      if worker
        .internal_op_tracker
        .take_op_details(cqe_user_data)
        .is_none()
      {
        warn!(
          "[CQE Proc] EventFdPoll CQE (ud:{}) handled, but no InternalOpDetails found.",
          cqe_user_data
        );
      }
      continue;
    }

    if let Some(mut ext_op_ctx) = worker.external_op_tracker.take_op(cqe_user_data) {
      trace!(
        "[CQE Proc] EXTERNAL op '{}' (ud:{}, res:{})",
        ext_op_ctx.op_name, cqe_user_data, cqe_result
      );
      let completion_to_send: UringOpCompletion = if cqe_result < 0 {
        let zmq_err = ZmqError::from(std::io::Error::from_raw_os_error(-cqe_result));
        error!(
          "CQE for external op '{}' (ud:{}) KERNEL FAILED. Errno: {}, ZmqErr: {}",
          ext_op_ctx.op_name, cqe_user_data, -cqe_result, zmq_err
        );
        if ext_op_ctx.op_name == "Connect" {
          if let Some(fd_to_close) = ext_op_ctx.fd_created_for_connect_op.take() {
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
              .expect("Connect op context missing FD");
            let (peer_addr, local_addr) = crate::io_uring_backend::worker::get_peer_local_addr(
              connected_fd,
            )
            .unwrap_or_else(|e| {
              (
                crate::io_uring_backend::worker::dummy_socket_addr(),
                crate::io_uring_backend::worker::dummy_socket_addr(),
              )
            });
            if let Some(ref factory_id) = ext_op_ctx.protocol_handler_factory_id {
              let protocol_config = ext_op_ctx.protocol_config.as_ref().unwrap();
              let socket_mailbox = ext_op_ctx
                .socket_mailbox
                .take()
                .expect("Connect op context missing socket_mailbox");
              // The UringOpRequest::Connect path is not used by tcp.rs (which uses
              // RegisterExternalFd). Use DummyConnection and derive URIs from peer addr.
              let connect_endpoint_uri = format!("tcp://{}", peer_addr);
              let recv_buffer_manager = worker.buffer_manager.as_ref();
              let default_bgid_val_from_worker = worker.default_buffer_ring_group_id_val;
              match worker.handler_manager.create_and_add_handler(
                connected_fd,
                factory_id,
                protocol_config,
                false,
                socket_mailbox,
                connect_endpoint_uri,
                String::new(),
                std::sync::Arc::new(DummyConnection),
                recv_buffer_manager,
                default_bgid_val_from_worker,
                cqe_user_data,
              ) {
                Ok(initial_ops) => {
                  info!(
                    "CQE Processor: Connect successful (ext_ud:{}), new FD:{}. Handler created. Peer: {}, Local: {}",
                    cqe_user_data, connected_fd, peer_addr, local_addr
                  );
                  if !initial_ops.sqe_blueprints.is_empty() {
                    new_work_generated.push((connected_fd, initial_ops.sqe_blueprints));
                  }
                  if initial_ops.initiate_close_due_to_error {
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
          _ => UringOpCompletion::OpError {
            user_data: cqe_user_data,
            op_name: ext_op_ctx.op_name.clone(),
            error: ZmqError::Internal(format!("Unhandled external op: {}", ext_op_ctx.op_name)),
          },
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
    let mut is_multishot_read_pending_more = false;
    let mut was_delegated_to_multishot_handler = false;

    if let Some(peeked_details) = worker.internal_op_tracker.get_op_details(cqe_user_data) {
      let handler_fd_peeked = peeked_details.fd;
      let op_type_peeked = peeked_details.op_type;

      if op_type_peeked == InternalOpType::RingReadMultishot {
        if cqe_result >= 0 && (cqe_flags & IOURING_CQE_F_MORE) != 0 {
          is_multishot_read_pending_more = true;
        }
      }

      if matches!(
        op_type_peeked,
        InternalOpType::RingReadMultishot | InternalOpType::AsyncCancel
      ) {
        if let Some(handler) = worker.handler_manager.get_mut(handler_fd_peeked) {
          let recv_bm_ref = worker
            .buffer_manager
            .as_ref()
            .expect("Recv BufferManager missing for multishot op");
          let pending_egress = worker
            .work_map
            .get(&handler_fd_peeked)
            .map_or(0, |w| w.egress_blueprints.len());
          let io_config = handler.io_config().clone();
          let interface = UringWorkerInterface::new(
            handler_fd_peeked,
            &io_config,
            Some(recv_bm_ref),
            worker.default_buffer_ring_group_id_val,
            cqe_user_data,
            pending_egress,
            false,
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
                if !handler_ops.sqe_blueprints.is_empty() {
                  new_work_generated.push((handler_fd_peeked, handler_ops.sqe_blueprints));
                }
                if handler_ops.initiate_close_due_to_error {
                  worker
                    .fds_needing_close_initiated_pass
                    .push_back(handler_fd_peeked);
                }
                if should_cleanup_delegated_op_ud {
                  if worker
                    .internal_op_tracker
                    .take_op_details(cqe_user_data)
                    .is_none()
                  {
                    warn!(
                      "[CQE Proc] Multishot delegate requested cleanup for ud:{}, but it was already taken.",
                      cqe_user_data
                    );
                  }
                } else if op_type_peeked == InternalOpType::RingReadMultishot {
                  is_multishot_read_pending_more = true;
                }
              }
              Err(e) => {
                error!(
                  "[CQE Proc] Multishot delegate for FD {} (ud:{}) returned error: {}. Closing handler.",
                  handler_fd_peeked, cqe_user_data, e
                );
                worker
                  .fds_needing_close_initiated_pass
                  .push_back(handler_fd_peeked);
                worker.internal_op_tracker.take_op_details(cqe_user_data);
              }
            }
          }
        }
      }
    }

    if !is_multishot_read_pending_more && !was_delegated_to_multishot_handler {
      op_details_taken_for_final_processing =
        worker.internal_op_tracker.take_op_details(cqe_user_data);
    }

    if let Some(op_details) = op_details_taken_for_final_processing {
      let handler_fd = op_details.fd;
      let op_type = op_details.op_type;
      trace!(
        "[CQE Proc] INTERNAL op (ud:{}, type:{:?}, fd:{}, res:{}) - Final Processing",
        cqe_user_data, op_type, handler_fd, cqe_result
      );

      match op_type {
        InternalOpType::Accept => {
          if cqe_result >= 0 {
            let client_fd = cqe_result as RawFd;
            let listener_fd = handler_fd;
            let (factory_id_opt, protocol_config_opt, mailbox_opt) = {
              let meta_opt = worker.handler_manager.get_listener_metadata(listener_fd);
              (
                meta_opt.map(|m| m.factory_id_for_accepted_connections.clone()),
                meta_opt.map(|m| m.protocol_config_for_accepted.clone()),
                meta_opt.map(|m| m.socket_mailbox.clone()),
              )
            };
            if let (Some(factory_id), Some(protocol_config), Some(socket_mailbox)) =
              (factory_id_opt, protocol_config_opt, mailbox_opt)
            {
              // Derive peer URI from the accepted FD; use DummyConnection for the internal path.
              let peer_uri = crate::io_uring_backend::worker::get_peer_local_addr(client_fd)
                .map(|(peer, _)| format!("tcp://{}", peer))
                .unwrap_or_else(|_| format!("tcp-accepted-fd-{}", client_fd));
              match worker.handler_manager.create_and_add_handler(
                client_fd,
                &factory_id,
                &protocol_config,
                true,
                socket_mailbox,
                peer_uri,
                String::new(),
                std::sync::Arc::new(DummyConnection),
                worker.buffer_manager.as_ref(),
                worker.default_buffer_ring_group_id_val,
                0,
              ) {
                Ok(initial_ops) => {
                  if !initial_ops.sqe_blueprints.is_empty() {
                    new_work_generated.push((client_fd, initial_ops.sqe_blueprints));
                  }
                  if initial_ops.initiate_close_due_to_error {
                    worker.fds_needing_close_initiated_pass.push_back(client_fd);
                  }
                }
                Err(e) => {
                  unsafe {
                    libc::close(client_fd);
                  }
                  error!(
                    "Failed to create handler for accepted client_fd {}: {}",
                    client_fd, e
                  );
                }
              }
            } else {
              unsafe {
                libc::close(client_fd);
              }
              warn!(
                "Could not find factory_id/protocol_config for listener fd {}",
                listener_fd
              );
            }
            if !is_worker_shutting_down {
              let new_accept_ud = worker.internal_op_tracker.new_op_id(
                listener_fd,
                InternalOpType::Accept,
                InternalOpPayload::None,
              );
              let mut client_addr: libc::sockaddr_storage = unsafe { mem::zeroed() };
              let mut client_addr_len = mem::size_of_val(&client_addr) as libc::socklen_t;
              let accept_sqe = opcode::Accept::new(
                types::Fd(listener_fd),
                &mut client_addr as *mut _ as *mut _,
                &mut client_addr_len,
              )
              .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
              .build()
              .user_data(new_accept_ud);
              unsafe {
                if worker.ring.submission_shared().push(&accept_sqe).is_err() {
                  error!(
                    "Failed to re-submit Accept SQE for listener fd {}",
                    listener_fd
                  );
                  worker
                    .fds_needing_close_initiated_pass
                    .push_back(listener_fd);
                  worker.internal_op_tracker.take_op_details(new_accept_ud);
                }
              }
            }
          } else {
            // Handle accept error, maybe re-submit accept
          }
        }
        InternalOpType::CloseFd => {
          if cqe_result >= 0 {
            info!(
              "CQE Processor: Internal CloseFd op (ud:{}) for FD {} successful.",
              cqe_user_data, handler_fd
            );
            if worker.fd_to_mpsc_rx.remove(&handler_fd).is_some() {
              trace!("Removed MPSC receiver for closed FD {}", handler_fd);
            }
            if worker.fd_to_zmtp_egress_rx.remove(&handler_fd).is_some() {
              trace!("Removed ZMTP egress receiver for closed FD {}", handler_fd);
            }
            while let Some((ext_ud, ext_ctx)) = worker
              .external_op_tracker
              .take_op_if_shutdown_for_fd(handler_fd)
            {
              let ack = UringOpCompletion::ShutdownConnectionHandlerComplete {
                user_data: ext_ud,
                fd: handler_fd,
              };
              let _ = ext_ctx.reply_tx.send(Ok(ack));
            }
            // Clone mailbox and endpoint_uri BEFORE removing handler.
            let (mailbox_for_close_notify, endpoint_uri_for_notify) = worker
              .handler_manager
              .get_mut(handler_fd)
              .map(|h| {
                (
                  h.io_config().socket_mailbox.clone(),
                  h.io_config().endpoint_uri.clone(),
                )
              })
              .unzip();
            if let Some(mut h) = worker.handler_manager.remove_handler(handler_fd) {
              h.fd_has_been_closed();
            }
            // Drop all queued write blueprints and in-flight send buffer allocations
            // for this fd so they are freed immediately rather than leaking.
            worker.work_map.remove(&handler_fd);
            let removed_ops = worker.internal_op_tracker.remove_ops_for_fd(handler_fd);
            for op in removed_ops {
              match op.payload {
                InternalOpPayload::SendZeroCopy { send_buf_id, .. }
                | InternalOpPayload::SendZeroCopyLeased { send_buf_id } => {
                  if let Some(pool) = &worker.send_buffer_pool {
                    pool.release_buffer(send_buf_id);
                  }
                }
                _ => {}
              }
            }
            if let Some(mailbox) = mailbox_for_close_notify {
              let endpoint_uri = endpoint_uri_for_notify.unwrap_or_default();
              let _ = mailbox.try_send(Command::UringFdError {
                endpoint_uri,
                error: ZmqError::ConnectionClosed,
              });
            }
          } else {
            // Handle close error
            error!(
              "CQE Processor: Internal CloseFd op (ud:{}) for FD {} FAILED.",
              cqe_user_data, handler_fd
            );
          }
        }
        InternalOpType::Send
        | InternalOpType::SendZeroCopy
        | InternalOpType::SendRawVectored
        | InternalOpType::SendZeroCopyLeased => {
          let is_notify = (cqe_flags & CQE_F_NOTIFY_FLAG) != 0;
          let is_initial_with_more = !is_notify && (cqe_flags & IOURING_CQE_F_MORE) != 0;

          if is_notify {
            match op_details.payload {
              InternalOpPayload::SendZeroCopy { send_buf_id, .. }
              | InternalOpPayload::SendZeroCopyLeased { send_buf_id } => {
                if let Some(pool) = &worker.send_buffer_pool {
                  pool.release_buffer(send_buf_id);
                }
              }
              _ => {}
            }
            continue; // Buffer released, nothing more to do for F_NOTIFY.
          }

          if cqe_result < 0 {
            let errno = -cqe_result;

            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK || errno == libc::ENOBUFS {
              if errno == libc::ENOBUFS {
                worker.metrics.record_enobufs();
              } else {
                worker.metrics.record_eagain();
              }
              // Non-destructive recovery: move payload out and re-queue to front of work_map.
              let blueprint_to_requeue = match op_details.payload {
                InternalOpPayload::SendBuffer {
                  buffer,
                  send_op_flags,
                  app_op_ud,
                  ..
                } => Some(HandlerSqeBlueprint::RequestSend {
                  data: buffer,
                  send_op_flags,
                  originating_app_op_ud: app_op_ud.unwrap_or(HANDLER_INTERNAL_SEND_OP_UD),
                  batch_count: 0,
                }),
                InternalOpPayload::SendZeroCopy {
                  send_buf_id,
                  original_data,
                  send_op_flags,
                  app_op_ud,
                  ..
                } => {
                  if !is_initial_with_more {
                    if let Some(pool) = &worker.send_buffer_pool {
                      pool.release_buffer(send_buf_id);
                    }
                  } else {
                    worker.internal_op_tracker.reinsert_for_notification(
                      cqe_user_data,
                      InternalOpDetails {
                        fd: handler_fd,
                        op_type: InternalOpType::SendZeroCopyLeased,
                        payload: InternalOpPayload::SendZeroCopyLeased { send_buf_id },
                      },
                    );
                  }
                  Some(HandlerSqeBlueprint::RequestSend {
                    data: original_data,
                    send_op_flags,
                    originating_app_op_ud: app_op_ud,
                    batch_count: 0,
                  })
                }
                InternalOpPayload::RawVectored(batch) => {
                  Some(HandlerSqeBlueprint::RequestSendRawVectored {
                    bufs: batch.payloads,
                    send_op_flags: batch.send_op_flags,
                    batch_count: 0,
                  })
                }
                InternalOpPayload::SendZeroCopyLeased { send_buf_id } => {
                  if !is_initial_with_more {
                    if let Some(pool) = &worker.send_buffer_pool {
                      pool.release_buffer(send_buf_id);
                    }
                  } else {
                    worker.internal_op_tracker.reinsert_for_notification(
                      cqe_user_data,
                      InternalOpDetails {
                        fd: handler_fd,
                        op_type: InternalOpType::SendZeroCopyLeased,
                        payload: InternalOpPayload::SendZeroCopyLeased { send_buf_id },
                      },
                    );
                  }
                  error!(
                    fd = handler_fd,
                    errno, "EAGAIN on leased ZC send — no original data for retry. Closing."
                  );
                  worker
                    .fds_needing_close_initiated_pass
                    .push_back(handler_fd);
                  None
                }
                _ => None,
              };

              if let Some(blueprint) = blueprint_to_requeue {
                trace!(
                  fd = handler_fd,
                  errno,
                  "Send non-fatal error (EAGAIN/ENOBUFS): re-queuing payload to front of work_map."
                );
                worker
                  .work_map
                  .entry(handler_fd)
                  .or_default()
                  .egress_blueprints
                  .push_front(blueprint);
              } else {
                error!(
                  fd = handler_fd,
                  errno, "Send recovery failed: payload structure mismatch. Closing connection."
                );
                worker
                  .fds_needing_close_initiated_pass
                  .push_back(handler_fd);
              }
              continue;
            }

            // Fatal error: release payload-specific resources before closing
            match op_details.payload {
              InternalOpPayload::SendZeroCopy { send_buf_id, .. }
              | InternalOpPayload::SendZeroCopyLeased { send_buf_id } => {
                if !is_initial_with_more {
                  if let Some(pool) = &worker.send_buffer_pool {
                    pool.release_buffer(send_buf_id);
                  }
                } else {
                  worker.internal_op_tracker.reinsert_for_notification(
                    cqe_user_data,
                    InternalOpDetails {
                      fd: handler_fd,
                      op_type: InternalOpType::SendZeroCopyLeased,
                      payload: InternalOpPayload::SendZeroCopyLeased { send_buf_id },
                    },
                  );
                }
              }
              InternalOpPayload::RawVectored(_) => {}
              _ => {}
            }
            let zmq_err = ZmqError::from(std::io::Error::from_raw_os_error(errno));
            error!(fd = handler_fd, error = %zmq_err, "Fatal write error during Send operation. Closing connection.");
            worker
              .fds_needing_close_initiated_pass
              .push_back(handler_fd);
            continue;
          }

          let bytes_written = cqe_result as usize;
          let total_expected = match &op_details.payload {
            InternalOpPayload::SendBuffer { buffer, .. } => buffer.len(),
            InternalOpPayload::SendZeroCopy { original_data, .. } => original_data.len(),
            InternalOpPayload::RawVectored(batch) => batch.total_len,
            InternalOpPayload::SendZeroCopyLeased { .. } => 0, // no local data; skip partial check
            _ => 0,
          };

          if total_expected > 0 && bytes_written < total_expected {
            trace!(
              fd = handler_fd,
              bytes_written,
              total_expected,
              "Partial write: re-queuing remainder to front of work_map."
            );
            let requeue_blueprint = match op_details.payload {
              InternalOpPayload::SendBuffer {
                buffer,
                send_op_flags,
                app_op_ud,
                ..
              } => HandlerSqeBlueprint::RequestSend {
                data: buffer.slice(bytes_written..),
                send_op_flags,
                originating_app_op_ud: app_op_ud.unwrap_or(HANDLER_INTERNAL_SEND_OP_UD),
                batch_count: 0,
              },
              InternalOpPayload::SendZeroCopy {
                send_buf_id,
                original_data,
                send_op_flags,
                app_op_ud,
                ..
              } => {
                if !is_initial_with_more {
                  if let Some(pool) = &worker.send_buffer_pool {
                    pool.release_buffer(send_buf_id);
                  }
                } else {
                  worker.internal_op_tracker.reinsert_for_notification(
                    cqe_user_data,
                    InternalOpDetails {
                      fd: handler_fd,
                      op_type: InternalOpType::SendZeroCopyLeased,
                      payload: InternalOpPayload::SendZeroCopyLeased { send_buf_id },
                    },
                  );
                }
                HandlerSqeBlueprint::RequestSend {
                  data: original_data.slice(bytes_written..),
                  send_op_flags,
                  originating_app_op_ud: app_op_ud,
                  batch_count: 0,
                }
              }
              InternalOpPayload::RawVectored(batch) => {
                // Safe partial-write recovery using Bytes::slice() — no raw pointer arithmetic.
                let mut bytes_to_skip = bytes_written;
                let mut remaining: Vec<bytes::Bytes> = Vec::new();
                let mut skip_done = false;
                for payload in batch.payloads {
                  if skip_done {
                    remaining.push(payload);
                  } else if bytes_to_skip >= payload.len() {
                    bytes_to_skip -= payload.len(); // fully consumed, discard
                  } else {
                    remaining.push(payload.slice(bytes_to_skip..)); // zero-copy split
                    skip_done = true;
                  }
                }
                HandlerSqeBlueprint::RequestSendRawVectored {
                  bufs: remaining,
                  send_op_flags: batch.send_op_flags,
                  batch_count: 0,
                }
              }
              _ => {
                error!(
                  fd = handler_fd,
                  "Partial write on unexpected payload type. Closing."
                );
                worker
                  .fds_needing_close_initiated_pass
                  .push_back(handler_fd);
                continue;
              }
            };
            worker
              .work_map
              .entry(handler_fd)
              .or_default()
              .egress_blueprints
              .push_front(requeue_blueprint);
            continue;
          }

          // Complete write: extract ACK info then release payload-specific resources.
          let (app_op_ud, app_op_name) = match &op_details.payload {
            InternalOpPayload::SendBuffer {
              app_op_ud: Some(ud),
              app_op_name: Some(name),
              ..
            } => (*ud, name.clone()),
            InternalOpPayload::SendZeroCopy {
              app_op_ud,
              app_op_name,
              ..
            } => (*app_op_ud, app_op_name.clone()),
            _ => (HANDLER_INTERNAL_SEND_OP_UD, String::new()),
          };

          match op_details.payload {
            InternalOpPayload::SendZeroCopy { send_buf_id, .. }
            | InternalOpPayload::SendZeroCopyLeased { send_buf_id } => {
              if !is_initial_with_more {
                if let Some(pool) = &worker.send_buffer_pool {
                  pool.release_buffer(send_buf_id);
                }
              } else {
                worker.internal_op_tracker.reinsert_for_notification(
                  cqe_user_data,
                  InternalOpDetails {
                    fd: handler_fd,
                    op_type: InternalOpType::SendZeroCopyLeased,
                    payload: InternalOpPayload::SendZeroCopyLeased { send_buf_id },
                  },
                );
              }
            }
            _ => {}
          }

          if app_op_ud != HANDLER_INTERNAL_SEND_OP_UD {
            // Logic for notifying external operation completion
          } else if let Some(handler) = worker.handler_manager.get_mut(handler_fd) {
            let pending_egress = worker
              .work_map
              .get(&handler_fd)
              .map_or(0, |w| w.egress_blueprints.len());
            let io_config = handler.io_config().clone();
            let interface = UringWorkerInterface::new(
              handler_fd,
              &io_config,
              worker.buffer_manager.as_ref(),
              worker.default_buffer_ring_group_id_val,
              cqe_user_data,
              pending_egress,
              true,
            );
            let handler_output = handler.handle_internal_sqe_completion(
              cqe_user_data,
              cqe_result,
              cqe_flags,
              &interface,
            );
            if !handler_output.sqe_blueprints.is_empty() {
              new_work_generated.push((handler_fd, handler_output.sqe_blueprints));
            }
            if handler_output.initiate_close_due_to_error {
              worker
                .fds_needing_close_initiated_pass
                .push_back(handler_fd);
            }
          }
        }
        InternalOpType::RingRead | InternalOpType::GenericHandlerOp => {
          // Non-fatal: the kernel had no provided buffers available when the read completed.
          // The op tracker entry is already reaped, so Phase 3 will re-submit a new Read SQE
          // automatically on the next worker cycle once other connections release buffers.
          if op_type == InternalOpType::RingRead
            && cqe_result == -(libc::ENOBUFS as i32)
            && cqueue::buffer_select(cqe_flags).is_none()
          {
            trace!(
              fd = handler_fd,
              "RingRead: ENOBUFS — buffer ring temporarily exhausted; deferring to next cycle."
            );
            continue;
          }

          if let Some(handler) = worker.handler_manager.get_mut(handler_fd) {
            let pending_egress = worker
              .work_map
              .get(&handler_fd)
              .map_or(0, |w| w.egress_blueprints.len());
            let io_config = handler.io_config().clone();
            let interface = UringWorkerInterface::new(
              handler_fd,
              &io_config,
              worker.buffer_manager.as_ref(),
              worker.default_buffer_ring_group_id_val,
              cqe_user_data,
              pending_egress,
              false,
            );
            let handler_output: HandlerIoOps = if op_type == InternalOpType::RingRead
              && cqueue::buffer_select(cqe_flags).is_some()
            {
              if cqe_result >= 0 {
                let bid = cqueue::buffer_select(cqe_flags).unwrap();
                let bytes_read = cqe_result as usize;
                if let Some(bm) = worker.buffer_manager.as_ref() {
                  if bytes_read == 0 {
                    // EOF with buffer consumed — reprovide inline and send EOF sentinel.
                    if let Err(e) = bm.reprovide_buffer(bid) {
                      tracing::warn!(
                        "cqe_processor: reprovide_buffer({}) on EOF failed: {:?}",
                        bid,
                        e
                      );
                    }
                    handler.process_ring_read_bytes(bytes::Bytes::new(), &interface)
                  } else {
                    match bm.take_and_replenish_buffer(bid, bytes_read) {
                      Ok(owned_bytes) => handler.process_ring_read_bytes(owned_bytes, &interface),
                      Err(_e) => {
                        worker
                          .fds_needing_close_initiated_pass
                          .push_back(handler_fd);
                        HandlerIoOps::default().set_error_close()
                      }
                    }
                  }
                } else {
                  worker
                    .fds_needing_close_initiated_pass
                    .push_back(handler_fd);
                  HandlerIoOps::default().set_error_close()
                }
              } else {
                handler.handle_internal_sqe_completion(
                  cqe_user_data,
                  cqe_result,
                  cqe_flags,
                  &interface,
                )
              }
            } else {
              handler.handle_internal_sqe_completion(
                cqe_user_data,
                cqe_result,
                cqe_flags,
                &interface,
              )
            };

            if !handler_output.sqe_blueprints.is_empty() {
              new_work_generated.push((handler_fd, handler_output.sqe_blueprints));
            }
            if handler_output.initiate_close_due_to_error {
              worker
                .fds_needing_close_initiated_pass
                .push_back(handler_fd);
            }
          }
        }
        InternalOpType::RingReadMultishot => {
          // This arm is only reached when a multishot read CQE was NOT delegated to its
          // registered handler — indicating state-tracking desynchronisation.
          tracing::error!(
            fd = handler_fd,
            ud = cqe_user_data,
            "Handshake Safety Gate: multishot CQE not delegated to handler — forcing close"
          );
          worker
            .fds_needing_close_initiated_pass
            .push_back(handler_fd);
        }
        _ => { /* Other op types handled by peek-logic or are errors */ }
      }
    }
  }

  Ok((num_processed, new_work_generated))
}
