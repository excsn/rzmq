// core/src/io_uring_backend/worker/cqe_processor.rs

#![cfg(feature = "io-uring")]

use super::external_op_tracker::{ExternalOpTracker};
use super::eventfd_poller::EventFdPoller;
use super::handler_manager::HandlerManager;
use super::internal_op_tracker::{InternalOpDetails, InternalOpTracker, InternalOpType, InternalOpPayload};
use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::io_uring_backend::ops::{UringOpCompletion, UserData};
use crate::io_uring_backend::connection_handler::{
    WorkerIoConfig,
    UringWorkerInterface,
    HandlerIoOps, HandlerSqeBlueprint,
};
use crate::ZmqError;

use io_uring::{cqueue, types, opcode, squeue, IoUring}; // Added IoUring
use std::os::unix::io::RawFd;
use std::mem;
use std::collections::VecDeque; // Added VecDeque
use tracing::{debug, error, info, trace, warn};

// process_handler_blueprints (signature confirmed from previous discussions)
pub(crate) fn process_handler_blueprints(
    fd: RawFd,
    ops_output: HandlerIoOps,
    internal_ops: &mut InternalOpTracker,
    sq: &mut squeue::SubmissionQueue<'_>,
    default_bgid_for_read_sqe: Option<u16>,
    fds_to_initiate_close_queue: &mut VecDeque<RawFd>,
    pending_sqe_retry_queue: &mut VecDeque<(RawFd, HandlerSqeBlueprint)>,
) {
    if ops_output.initiate_close_due_to_error {
        warn!("CQE Processor: Handler for FD {} requested error close via HandlerIoOps.", fd);
        if !fds_to_initiate_close_queue.contains(&fd) { 
            fds_to_initiate_close_queue.push_back(fd);
        }
    }

    for (idx, blueprint_ref) in ops_output.sqe_blueprints.iter().enumerate() {

        if unsafe { sq.is_full() } {
            warn!(
                "CQE Processor: Submission queue full while processing blueprints for FD {}. Blueprint {:?} will be requeued.",
                fd, blueprint_ref
            );
            // Requeue the blueprint instead of dropping it.
            // Need to clone the blueprint as we only have a reference.
            pending_sqe_retry_queue.push_back((fd, (*blueprint_ref).clone()));
            continue;
        }

        let mut op_payload_for_tracker = InternalOpPayload::None;
        if let HandlerSqeBlueprint::RequestSend { data } = blueprint_ref { // Use blueprint directly
            op_payload_for_tracker = InternalOpPayload::SendBuffer(data.clone());
        }

        let sqe_build_result: Result<(squeue::Entry, InternalOpType), String> = match blueprint_ref { // Use blueprint
            HandlerSqeBlueprint::RequestRingRead => {
                if let Some(bgid) = default_bgid_for_read_sqe {
                    let read_op_builder = opcode::Read::new(types::Fd(fd), std::ptr::null_mut(), 0)
                        .offset(u64::MAX) 
                        .buf_group(bgid);

                    let mut entry: squeue::Entry = read_op_builder.build();
                    entry = entry.flags(squeue::Flags::BUFFER_SELECT);
                    Ok((entry, InternalOpType::RingRead))
                } else {
                    let err_msg = "RequestRingRead blueprint: No default_buffer_ring_group_id configured/passed for SQE construction.".to_string();
                    Err(err_msg)
                }
            }
            HandlerSqeBlueprint::RequestSend { data } => {
                let ptr = data.as_ptr();
                let len = data.len() as u32;
                let entry = opcode::Send::new(types::Fd(fd), ptr, len).build();
                Ok( (entry, InternalOpType::Send) )
            }
            HandlerSqeBlueprint::RequestClose => {
                let entry = opcode::Close::new(types::Fd(fd)).build();
                Ok( (entry, InternalOpType::CloseFd) )
            }
        };

        match sqe_build_result {
            Ok((mut entry_to_submit, op_type)) => {
                let user_data = internal_ops.new_op_id(fd, op_type, op_payload_for_tracker);
                entry_to_submit = entry_to_submit.user_data(user_data);

                unsafe {
                    let push_result = sq.push(&entry_to_submit);
                    
                    if push_result.is_err() { 
                        warn!("CQE Processor: SQ push failed for FD {} blueprint {:?} (race condition or SQ became full?). Op requeued.", fd, entry_to_submit);
                        
                        // Requeue if push fails even after initial SQ full check (race condition).
                        // Also, need to clean up the internal_op_tracker entry for this failed push.
                        let _dropped_details = internal_ops.take_op_details(user_data); // Clean up tracker
                        pending_sqe_retry_queue.push_back((fd, blueprint_ref.clone())); // Requeue
                    } else {
                        trace!("CQE Processor: Queued SQE (ud:{}) for FD {} from blueprint: {:?}", user_data, fd, entry_to_submit);
                    }
                }
            }
            Err(s) => {
                error!("CQE Processor: Failed to build SQE for FD {} from blueprint: {}. Blueprint dropped.", fd, s);
            }
        }
    }
}


pub(crate) fn process_all_cqes(
    ring: &mut IoUring, // <<< Now takes &mut IoUring
    external_ops: &mut ExternalOpTracker,
    internal_ops: &mut InternalOpTracker,
    handler_manager: &mut HandlerManager,
    buffer_manager: Option<&BufferRingManager>,
    worker_io_config: &WorkerIoConfig,
    default_bgid_val_from_worker: Option<u16>,
    fds_to_initiate_close_queue: &mut VecDeque<RawFd>,
    pending_sqe_retry_queue: &mut VecDeque<(RawFd, HandlerSqeBlueprint)>,
    event_fd_poller: &mut EventFdPoller,
) -> Result<(), ZmqError> {

    // Get a local, short-lived CompletionQueue reference from the ring.
    let mut cq = unsafe { ring.completion_shared() };
    // It's good practice to call sync on the CQ if SQEs might have been submitted
    // without an immediate ring.enter() or submit_and_wait(), though in a single-threaded
    // worker like this where submit() is called before this, it might be redundant.
    // However, it doesn't hurt.
    cq.sync();

    // Iterate over all available CQEs.
    // The `&mut cq` allows `cqueue::Entry` to be taken by value.
    for cqe in &mut cq {
        let cqe_user_data = cqe.user_data();
        let cqe_result = cqe.result();
        let cqe_flags = cqe.flags();

        trace!("CQE Processor: Received CQE UserData: {}, Result: {}, Flags: {:x}", cqe_user_data, cqe_result, cqe_flags);

        if event_fd_poller.handle_cqe_if_matches(cqe_user_data, cqe_result, internal_ops) {
            // The EventFdPoller handled it (read from eventfd, prepared for next poll).
            // We also need to ensure the InternalOpTracker entry for this UserData is removed,
            // as the UserData has served its purpose for this specific poll instance.
            // The EventFdPoller itself does not directly call `take_op_details`.
            if let Some(_details) = internal_ops.take_op_details(cqe_user_data) {
                trace!("[CQE Processor] Consumed InternalOpDetails for handled EventFdPoll CQE (ud: {}).", cqe_user_data);
            } else {
                // This case should be rare if UserData generation and tracking is correct.
                warn!("[CQE Processor] EventFdPoller handled CQE (ud: {}), but no corresponding InternalOpDetails found to consume.", cqe_user_data);
            }
            // The main_loop will call event_fd_poller.try_submit_initial_poll_sqe()
            // in the next iteration to re-submit the poll with the new UserData.
            continue; // Move to the next CQE
        }

        // --- 1. Check if it's an External Operation Completion ---
        if let Some(mut ext_op_ctx) = external_ops.take_op(cqe_user_data) {
            trace!("CQE Processor: Matched EXTERNAL op '{}', ud: {}, res: {}", ext_op_ctx.op_name, cqe_user_data, cqe_result);
            let completion_to_send: UringOpCompletion = if cqe_result < 0 {
                let zmq_err = ZmqError::from(std::io::Error::from_raw_os_error(-cqe_result));
                error!("CQE for external op '{}' (ud:{}) KERNEL FAILED. Errno: {}, ZmqErr: {}", ext_op_ctx.op_name, cqe_user_data, -cqe_result, zmq_err);
                if ext_op_ctx.op_name == "Connect" {
                    if let Some(fd_to_close) = ext_op_ctx.fd_created_for_connect_op.take() {
                        debug!("Closing FD {} from failed Connect CQE (ud:{})", fd_to_close, cqe_user_data);
                        // For a failed connect, we might not want to queue a Close SQE as the FD might not be fully registered.
                        // Direct libc::close is simpler here.
                        unsafe { libc::close(fd_to_close); }
                    }
                }
                UringOpCompletion::OpError { user_data: cqe_user_data, op_name: ext_op_ctx.op_name.clone(), error: zmq_err }
            } else {
                // Successful kernel operation for external request
                match ext_op_ctx.op_name.as_str() {
                    "Connect" => {
                        let connected_fd = ext_op_ctx.fd_created_for_connect_op.expect("Connect op context missing FD after successful CQE");
                        let (peer_addr, local_addr) = super::get_peer_local_addr(connected_fd) // Assuming super:: refers to worker/mod.rs
                            .unwrap_or_else(|e| {
                                warn!("CQE ConnectSuccess: Failed to get peer/local addr for connected_fd {}: {}", connected_fd, e);
                                (super::dummy_socket_addr(), super::dummy_socket_addr())
                            });

                        if let Some(ref factory_id) = ext_op_ctx.protocol_handler_factory_id {
                            let protocol_config = ext_op_ctx.protocol_config.clone();

                            match handler_manager.create_and_add_handler(
                                connected_fd,
                                factory_id,
                                protocol_config.as_ref().unwrap(),
                                false,
                                buffer_manager, // Pass Option<&BufferRingManager>
                                default_bgid_val_from_worker, // Pass the default bgid
                            ) {
                                Ok(initial_ops) => {
                                    info!("CQE Processor: Connect successful (ext_ud:{}), new FD:{}. Handler created. Peer: {}, Local: {}", cqe_user_data, connected_fd, peer_addr, local_addr);
                                    // Get a new SQ ref for processing these initial blueprints
                                    let mut sq_for_initial_ops = unsafe { ring.submission_shared() };
                                    process_handler_blueprints(
                                        connected_fd,
                                        initial_ops,
                                        internal_ops,
                                        &mut sq_for_initial_ops,
                                        default_bgid_val_from_worker,
                                        fds_to_initiate_close_queue,
                                        pending_sqe_retry_queue,
                                    );
                                    // sq_for_initial_ops drops here
                                    UringOpCompletion::ConnectSuccess { user_data: cqe_user_data, connected_fd, peer_addr, local_addr }
                                }
                                Err(err_msg) => {
                                    error!("CQE Processor: Connect successful (ext_ud:{}), FD:{}, but handler creation failed: {}", cqe_user_data, connected_fd, err_msg);
                                    unsafe { libc::close(connected_fd); } // Clean up FD
                                    UringOpCompletion::OpError { user_data: cqe_user_data, op_name: ext_op_ctx.op_name.clone(), error: ZmqError::Internal(err_msg) }
                                }
                            }
                        } else {
                            let err_msg = "Missing factory_id for Connect op context";
                            error!("CQE Processor: Connect successful (ext_ud:{}), FD:{}, but: {}", cqe_user_data, connected_fd, err_msg);
                            unsafe { libc::close(connected_fd); } // Clean up FD
                            UringOpCompletion::OpError { user_data: cqe_user_data, op_name: ext_op_ctx.op_name.clone(), error: ZmqError::Internal(err_msg.into()) }
                        }
                    }
                    "Nop" => UringOpCompletion::NopSuccess { user_data: cqe_user_data },
                    "RegisterRawBuffers" => UringOpCompletion::RegisterRawBuffersSuccess { user_data: cqe_user_data },
                    // ListenSuccess is replied directly by UringWorker::handle_external_op_request_submission, not from a CQE.
                    _ => {
                        warn!("CQE Processor: Unhandled successful external op CQE: name='{}', ud={}", ext_op_ctx.op_name, cqe_user_data);
                        UringOpCompletion::OpError { user_data: cqe_user_data, op_name: ext_op_ctx.op_name.clone(), error: ZmqError::Internal(format!("Unhandled successful external op: {}", ext_op_ctx.op_name)) }
                    }
                }
            };
            // Send completion back to the original requester
            if ext_op_ctx.reply_tx.take_and_send_sync(Ok(completion_to_send)).is_none() {
                 tracing::warn!("CQE Processor: Reply_tx for external op ud {} was already taken.", cqe_user_data);
            }
            continue; // Processed as external op, move to next CQE
        }

        // --- 2. If not External, it must be an Internal Operation Completion ---
        let op_details_option = internal_ops.take_op_details(cqe_user_data);

        if let Some(op_details) = op_details_option {
            let handler_fd = op_details.fd;
            let op_type = op_details.op_type;
            // op_details.payload is available here, e.g., for Send op, to release buffer if needed by tracker.

            trace!("CQE Processor: Matched INTERNAL op ud: {}, type: {:?}, fd: {}, res: {}", cqe_user_data, op_type, handler_fd, cqe_result);

            match op_type {
                InternalOpType::Accept => {
                    if cqe_result >= 0 { // Successful accept
                        let client_fd = cqe_result as RawFd;
                        let listener_fd = handler_fd; // FD of the listening socket
                        info!("CQE Processor: Internal Accept CQE (ud:{}) on listener FD {} -> new client FD {}", cqe_user_data, listener_fd, client_fd);

                        let mut factory_id = None;
                        let mut protocol_config = None;

                        {
                            if let Some(listener_info) = handler_manager.get_listener_metadata(listener_fd) {
                                factory_id = Some(listener_info.factory_id_for_accepted_connections.to_string());
                                protocol_config = Some(listener_info.protocol_config_for_accepted.clone());
                            }
                        }
                        if factory_id.is_some() {
                            match handler_manager.create_and_add_handler(
                                client_fd,
                                &factory_id.unwrap(),
                                &protocol_config.unwrap(),
                                true, // is_server = true for accepted connections
                                buffer_manager,
                                default_bgid_val_from_worker,
                            ) {
                                Ok(initial_ops) => {
                                     let mut sq_for_accept_initial_ops = unsafe { ring.submission_shared() };
                                     process_handler_blueprints(
                                         client_fd,
                                         initial_ops,
                                         internal_ops,
                                         &mut sq_for_accept_initial_ops,
                                         default_bgid_val_from_worker,
                                         fds_to_initiate_close_queue,
                                        pending_sqe_retry_queue,
                                     );
                                     // sq_for_accept_initial_ops drops
                                }
                                Err(e) => {
                                    error!("Worker: Failed to create handler for accepted client_fd {} from listener {}: {}", client_fd, listener_fd, e);
                                    unsafe { libc::close(client_fd); } // Clean up new FD
                                }
                            }
                        } else {
                            warn!("Worker: Could not find factory_id for listener fd {} to create handler for client_fd {}", listener_fd, client_fd);
                            unsafe { libc::close(client_fd); } // Clean up new FD
                        }

                        // Re-submit Accept SQE for the listener_fd to continue accepting
                        let new_accept_internal_ud = internal_ops.new_op_id(listener_fd, InternalOpType::Accept, InternalOpPayload::None);
                        let mut client_addr_storage: libc::sockaddr_storage = unsafe { mem::zeroed() }; // Fresh storage for next accept
                        let mut client_addr_len = mem::size_of_val(&client_addr_storage) as libc::socklen_t;
                        let accept_sqe = opcode::Accept::new(types::Fd(listener_fd), &mut client_addr_storage as *mut _ as *mut _, &mut client_addr_len)
                                             .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC) // Standard flags for accept
                                             .build()
                                             .user_data(new_accept_internal_ud);
                        // Get a new SQ ref for this single re-submission
                        let mut sq_for_reaccept = unsafe { ring.submission_shared() };
                        unsafe {
                            if sq_for_reaccept.push(&accept_sqe).is_err() {
                                error!("Worker: Failed to re-submit Accept SQE for listener fd {} (SQ FULL?). Listener may stop accepting.", listener_fd);
                                internal_ops.take_op_details(new_accept_internal_ud); // Clean up tracker if push failed
                                if !fds_to_initiate_close_queue.contains(&listener_fd) { fds_to_initiate_close_queue.push_back(listener_fd); }
                            } else {
                                trace!("Worker: Re-submitted Accept SQE (ud:{}) for listener fd {}", new_accept_internal_ud, listener_fd);
                            }
                        }
                        // sq_for_reaccept drops
                    } else { // Accept failed
                        let accept_err = std::io::Error::from_raw_os_error(-cqe_result);
                        error!("CQE Processor: Internal Accept op (ud:{}) for listener FD {} FAILED with errno {}: {}", cqe_user_data, handler_fd, -cqe_result, accept_err);
                        // TODO: Handle specific accept errors. Some might be fatal for the listener.
                        // For now, attempt to re-submit accept.
                        let new_accept_internal_ud_on_err = internal_ops.new_op_id(handler_fd, InternalOpType::Accept, InternalOpPayload::None);
                        let mut client_addr_storage_err: libc::sockaddr_storage = unsafe { mem::zeroed() };
                        let mut client_addr_len_err = mem::size_of_val(&client_addr_storage_err) as libc::socklen_t;
                        let accept_sqe_on_err = opcode::Accept::new(types::Fd(handler_fd), &mut client_addr_storage_err as *mut _ as *mut _, &mut client_addr_len_err)
                                             .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
                                             .build()
                                             .user_data(new_accept_internal_ud_on_err);
                        let mut sq_for_reaccept_err = unsafe { ring.submission_shared() };
                        unsafe {
                            if sq_for_reaccept_err.push(&accept_sqe_on_err).is_err() {
                                error!("Worker: Failed to re-submit Accept SQE after error for listener_fd {} (SQ FULL?). Listener may stop accepting.", handler_fd);
                                internal_ops.take_op_details(new_accept_internal_ud_on_err);
                                if !fds_to_initiate_close_queue.contains(&handler_fd) { fds_to_initiate_close_queue.push_back(handler_fd); }
                            }
                        }
                        // sq_for_reaccept_err drops
                    }
                }
                InternalOpType::CloseFd => {
                    if cqe_result >= 0 {
                        info!("CQE Processor: Internal CloseFd op (ud:{}) for FD {} successful.", cqe_user_data, handler_fd);
                        // If this CloseFd was triggered by an external ShutdownConnectionHandler request, notify its reply_tx.
                        if let Some((ext_ud, ext_ctx)) = external_ops.take_op_if_shutdown_for_fd(handler_fd) {
                             let ack = UringOpCompletion::ShutdownConnectionHandlerComplete {user_data: ext_ud, fd: handler_fd};
                             if ext_ctx.reply_tx.take_and_send_sync(Ok(ack)).is_none() {
                                 tracing::warn!("CQE Processor: Reply_tx for external Shutdown op (ext_ud {}) was already taken.", ext_ud);
                             }
                        }
                        // Remove handler after FD is confirmed closed by kernel.
                        if let Some(mut h) = handler_manager.remove_handler(handler_fd){
                            h.fd_has_been_closed(); // Notify handler
                            info!("CQE Processor: Handler for FD {} removed after successful CloseFd.", handler_fd);
                        } else {
                             warn!("CQE Processor: No handler found for FD {} to remove after CloseFd (ud:{}), possibly already removed.", handler_fd, cqe_user_data);
                        }
                    } else {
                        let close_err = std::io::Error::from_raw_os_error(-cqe_result);
                        error!("CQE Processor: Internal CloseFd op (ud:{}) for FD {} FAILED: {}. Forcing handler removal.", cqe_user_data, handler_fd, close_err);
                        // Even if close failed, the FD state is uncertain. Remove handler.
                        if let Some(mut h) = handler_manager.remove_handler(handler_fd){
                            h.fd_has_been_closed();
                        }
                        // TODO: If an external ShutdownConnectionHandler was waiting, it should be notified of error.
                        if let Some((ext_ud, ext_ctx)) = external_ops.take_op_if_shutdown_for_fd(handler_fd) {
                             let err_completion = UringOpCompletion::OpError{
                                user_data: ext_ud,
                                op_name: "ShutdownConnectionHandler (via CloseFd failure)".to_string(),
                                error: ZmqError::from(close_err),
                             };
                             let _ = ext_ctx.reply_tx.take_and_send_sync(Ok(err_completion));
                        }
                    }
                }
                InternalOpType::RingRead | InternalOpType::Send | InternalOpType::GenericHandlerOp => {
                    // Note: op_details.payload (like SendBuffer) is dropped when op_details goes out of scope
                    // at the end of this `if let Some(op_details)` block. This is correct, as the CQE means
                    // the kernel is done with any buffer associated with the SQE.

                    if let Some(handler) = handler_manager.get_mut(handler_fd) {
                        let interface_for_handler_call = UringWorkerInterface::new(
                            handler_fd,
                            worker_io_config,
                            buffer_manager,
                            default_bgid_val_from_worker,
                        );
                        let handler_output: HandlerIoOps;

                        let is_buffer_select_cqe = io_uring::cqueue::buffer_select(cqe_flags).is_some();

                        if op_type == InternalOpType::RingRead && is_buffer_select_cqe {
                            if cqe_result >= 0 { // Successful ring read
                                if let Some(bid) = io_uring::cqueue::buffer_select(cqe_flags) {
                                    let bytes_read = cqe_result as usize;
                                    if bytes_read > 0 {
                                        if let Some(bm) = buffer_manager {
                                            // Borrow the buffer. This is unsafe because we are dealing with raw kernel interaction.
                                            // The `bid` and `bytes_read` must be correct.
                                            match unsafe { bm.borrow_kernel_filled_buffer(bid, bytes_read) } {
                                                Ok(borrowed_buffer) => {
                                                    // `borrowed_buffer` is a `BorrowedBuffer<BytesMut>`. It derefs to `&[u8]`.
                                                    // It must be dropped to return the buffer to the ring.
                                                    handler_output = handler.process_ring_read_data(&borrowed_buffer[..bytes_read], bid, &interface_for_handler_call);
                                                    // `borrowed_buffer` is dropped here.
                                                }
                                                Err(e) => {
                                                    error!("Worker: Failed to borrow buffer ID {} ({} bytes) for fd {}: {:?}", bid, bytes_read, handler_fd, e);
                                                    if !fds_to_initiate_close_queue.contains(&handler_fd) { fds_to_initiate_close_queue.push_back(handler_fd); }
                                                    handler_output = HandlerIoOps::default().set_error_close();
                                                }
                                            }
                                        } else {
                                            error!("Worker: BufferManager not configured, but received RingRead CQE for fd {}", handler_fd);
                                            if !fds_to_initiate_close_queue.contains(&handler_fd) { fds_to_initiate_close_queue.push_back(handler_fd); }
                                            handler_output = HandlerIoOps::default().set_error_close();
                                        }
                                    } else { // bytes_read == 0 (EOF)
                                        info!("Worker: EOF on fd {} during ring read (ud {}). Notifying handler.", handler_fd, cqe_user_data);
                                        // Pass empty slice to handler. Handler should see this and initiate close.
                                        handler_output = handler.process_ring_read_data(&[], bid, &interface_for_handler_call);
                                    }
                                } else { // Should not happen if CQE_F_BUFFER is set.
                                    error!("Worker: CQE_F_BUFFER set for RingRead on fd {} but buffer_select() returned None (ud:{}, flags:{:x}, res:{})", handler_fd, cqe_user_data, cqe_flags, cqe_result);
                                    if !fds_to_initiate_close_queue.contains(&handler_fd) { fds_to_initiate_close_queue.push_back(handler_fd); }
                                    handler_output = HandlerIoOps::default().set_error_close();
                                }
                            } else { // cqe_result < 0 for RingRead
                                let read_err = std::io::Error::from_raw_os_error(-cqe_result);
                                error!("Worker: RingRead op (ud:{}) for FD {} FAILED: {}. Notifying handler.", cqe_user_data, handler_fd, read_err);
                                // Notify handler of the error.
                                handler_output = handler.handle_internal_sqe_completion(
                                    cqe_user_data, cqe_result, cqe_flags, &interface_for_handler_call,
                                );
                            }
                        } else { // Not a RingRead with CQE_F_BUFFER, or RingRead failed early
                            // This handles Send completions, GenericHandlerOp, or RingRead that didn't use CQE_F_BUFFER (shouldn't happen for our RingRead SQEs)
                            handler_output = handler.handle_internal_sqe_completion(
                                cqe_user_data, cqe_result, cqe_flags, &interface_for_handler_call,
                            );
                            if cqe_result < 0 { // For Send or other ops
                                let op_err = std::io::Error::from_raw_os_error(-cqe_result);
                                warn!("Worker: Internal op (ud:{}, type:{:?}) for FD {} FAILED: {}. Handler notified.", cqe_user_data, op_type, handler_fd, op_err);
                                // The handler's `handle_internal_sqe_completion` might already set `initiate_close_due_to_error` in `handler_output`.
                            }
                        }
                        // Process blueprints returned by the handler
                        let mut sq_for_handler_resp_ops = unsafe { ring.submission_shared() };
                        process_handler_blueprints(
                            handler_fd,
                            handler_output,
                            internal_ops,
                            &mut sq_for_handler_resp_ops,
                            default_bgid_val_from_worker,
                            fds_to_initiate_close_queue,
                            pending_sqe_retry_queue,
                        );
                        // sq_for_handler_resp_ops drops
                    } else { // Handler not found for this FD
                        warn!("Worker: CQE for internal op user_data {} - handler for FD {} not found (already removed?).", cqe_user_data, handler_fd);
                        // If payload was SendBuffer, it's dropped with op_details.
                    }
                } // End RingRead/Send/GenericHandlerOp arm

                // This case should ideally not be reached if `event_fd_poller.handle_cqe_if_matches` works correctly.
                // But as a defensive measure or if the UserData ranges were to overlap accidentally.
                InternalOpType::EventFdPoll => {
                    warn!(
                        "CQE Processor: Reached InternalOpType::EventFdPoll (ud: {}) directly. This should have been handled by EventFdPoller. Investigating.",
                        cqe_user_data
                    );
                    // Attempt to delegate to the poller again, just in case.
                    if !event_fd_poller.handle_cqe_if_matches(cqe_user_data, cqe_result, internal_ops) {
                        error!(
                            "CQE Processor: FATAL - EventFdPoll CQE (ud: {}) was not handled by EventFdPoller even on second attempt within internal ops match.",
                            cqe_user_data
                        );
                        // This is a serious logic error. The UserData for EventFdPoll should be unique
                        // and managed by EventFdPoller.
                    }
                    // The poller, if it handled it, would have set up for the next poll.
                }
            } // End match op_type
        } else { // user_data did not match an external or internal op
            warn!("Worker: CQE for UNKNOWN user_data: {} (result: {}, flags: {:x}) - Not external or mapped internal op. CQE Ignored.", cqe_user_data, cqe_result, cqe_flags);
        }
    } // End loop over CQEs

    // CQ is dropped here, releasing its borrow on the ring.
    Ok(())
}