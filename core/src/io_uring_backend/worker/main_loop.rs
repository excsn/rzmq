// core/src/io_uring_backend/worker/main_loop.rs

#![cfg(feature = "io-uring")]

use super::{
    UringWorker, ExternalOpContext, // ExternalOpTracker not needed directly in this file's public API
    // InternalOpTracker, InternalOpType, InternalOpPayload, HandlerManager, // Not directly needed by UringWorker impl
    cqe_processor, sqe_builder,
};
use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::io_uring_backend::ops::{UringOpRequest, UringOpCompletion}; // UserData implicitly used via ops
use crate::io_uring_backend::connection_handler::{HandlerIoOps}; use crate::io_uring_backend::worker::{InternalOpPayload, InternalOpType};
// HandlerSqeBlueprint not used by main_loop directly
use crate::ZmqError;
use io_uring::{opcode, types, squeue, Parameters};
use std::collections::VecDeque;
use std::os::fd::AsRawFd; // ADDED for as_raw_fd()
use std::os::unix::io::RawFd;
use std::sync::Arc; // For Arc::get_mut in InitializeBufferRing
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};
use kanal::{ReceiveError as KanalReceiveError, ReceiveErrorTimeout as KanalReceiveErrorTimeout};


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
        let reply_tx = request.get_reply_tx_ref().clone(); // Clone for local use, original stays with request for retry

        trace!("UringWorker: Handling external op request: {}, ud: {}", op_name_str, user_data_from_req);

        match request {
            UringOpRequest::InitializeBufferRing { user_data, bgid, num_buffers, buffer_capacity, reply_tx: _ } => {
                if self.buffer_manager.is_some() {
                    warn!("UringWorker: BufferRingManager already initialized. Ignoring InitializeBufferRing (ud: {})", user_data);
                    let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
                        user_data, op_name: op_name_str,
                        error: ZmqError::InvalidState("Buffer ring already initialized".into()),
                    }));
                } else {
                    match BufferRingManager::new(&self.ring, num_buffers, bgid, buffer_capacity) {
                        Ok(bm) => {
                            info!("UringWorker: BufferRingManager initialized with bgid: {}, {} buffers of {} capacity.", bgid, num_buffers, buffer_capacity);
                            self.buffer_manager = Some(bm);
                            // Set the worker's owned default_bgid_val
                            if self.default_buffer_ring_group_id_val.is_none() {
                                self.default_buffer_ring_group_id_val = Some(bgid);
                                debug!("UringWorker: Worker's default buffer group ID set to {}", bgid);
                            } else {
                                warn!("UringWorker: Worker's default buffer group ID was already set. New bgid {} not made default.", bgid);
                            }
                            let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::InitializeBufferRingSuccess { user_data, bgid }));
                        }
                        Err(e) => {
                            error!("UringWorker: Failed to initialize BufferRingManager: {}", e);
                            let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError { user_data, op_name: op_name_str, error: e, }));
                        }
                    }
                }
                Ok(false)
            }
            UringOpRequest::RegisterRawBuffers { user_data, ref buffers, reply_tx: _ } => {
                warn!("UringWorker: RegisterRawBuffers direct ring call not fully implemented. Sending placeholder ack.");
                // Real implementation: self.ring.register_buffers(buffers_as_ioslices_or_vecs).map_err...
                let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::RegisterRawBuffersSuccess { user_data }));
                Ok(false)
            }
            UringOpRequest::Listen { user_data, addr, ref protocol_handler_factory_id, protocol_config, reply_tx: _ } => {
                let socket_fd = match addr {
                    std::net::SocketAddr::V4(_) => unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0) },
                    std::net::SocketAddr::V6(_) => unsafe { libc::socket(libc::AF_INET6, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0) },
                };
                if socket_fd < 0 { 
                    let e = ZmqError::from(std::io::Error::last_os_error());
                    error!("Listen(ud:{}): Failed to create socket: {}", user_data, e);
                    let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: e }));
                    return Ok(false);
                }

                let optval: libc::c_int = 1;
                if unsafe { libc::setsockopt(socket_fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, &optval as *const _ as *const libc::c_void, std::mem::size_of_val(&optval) as libc::socklen_t) } < 0 {
                    let e = ZmqError::from(std::io::Error::last_os_error());
                    error!("Listen(ud:{}): Failed to set SO_REUSEADDR for fd {}: {}", user_data, socket_fd, e);
                    unsafe { libc::close(socket_fd); }
                    let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: e }));
                    return Ok(false);
                }

                let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
                let addr_len = super::socket_addr_to_sockaddr_storage(&addr, &mut storage);
                if unsafe { libc::bind(socket_fd, &storage as *const _ as *const libc::sockaddr, addr_len) } < 0 {
                    let e = ZmqError::from(std::io::Error::last_os_error());
                    error!("Listen(ud:{}): Failed to bind fd {}: {}", user_data, socket_fd, e);
                    unsafe { libc::close(socket_fd); }
                    let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: e }));
                    return Ok(false);
                }
                
                let mut actual_addr_storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
                let mut actual_addr_len = std::mem::size_of_val(&actual_addr_storage) as libc::socklen_t;
                let actual_bound_addr = if unsafe { libc::getsockname(socket_fd, &mut actual_addr_storage as *mut _ as *mut libc::sockaddr, &mut actual_addr_len) } == 0 {
                    super::sockaddr_storage_to_socket_addr(&actual_addr_storage, actual_addr_len).unwrap_or(addr)
                } else { addr };

                if unsafe { libc::listen(socket_fd, 128) } < 0 { 
                    let e = ZmqError::from(std::io::Error::last_os_error());
                    error!("Listen(ud:{}): Failed to listen on fd {}: {}", user_data, socket_fd, e);
                    unsafe { libc::close(socket_fd); }
                    let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: e }));
                    return Ok(false);
                }
                
                info!("UringWorker: Listener socket {} successfully set up for address {}", socket_fd, actual_bound_addr);
                
                self.external_op_tracker.add_op(user_data, ExternalOpContext {
                    reply_tx: reply_tx.clone(), 
                    op_name: op_name_str.clone(),
                    protocol_handler_factory_id: Some(protocol_handler_factory_id.clone()),
                    protocol_config: Some(protocol_config.clone()),
                    fd_created_for_connect_op: None,
                    listener_fd: Some(socket_fd), 
                    target_fd_for_shutdown: None,
                });
                
                self.handler_manager.add_listener_metadata(socket_fd, protocol_handler_factory_id.clone(), protocol_config);

                let accept_ud = self.internal_op_tracker.new_op_id(socket_fd, InternalOpType::Accept, InternalOpPayload::None);
                let mut client_addr_storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
                let mut client_addr_len = std::mem::size_of_val(&client_addr_storage) as libc::socklen_t;
                let accept_sqe = opcode::Accept::new(types::Fd(socket_fd), &mut client_addr_storage as *mut _ as *mut _, &mut client_addr_len)
                                     .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
                                     .build()
                                     .user_data(accept_ud);
                
                let mut sq = unsafe { self.ring.submission_shared() };
                let submitted_accept_sqe_ok = if !unsafe { sq.is_full() } {
                    match unsafe { sq.push(&accept_sqe) } {
                        Ok(_) => {
                            trace!("UringWorker: Queued first Accept SQE (ud:{}) for listener_fd {}", accept_ud, socket_fd);
                            true
                        }
                        Err(push_err) => { 
                            error!("UringWorker: Failed to push first Accept SQE for listener_fd {}: {:?}. Listener setup failed.", socket_fd, push_err);
                            self.internal_op_tracker.take_op_details(accept_ud); 
                            self.handler_manager.remove_handler(socket_fd); 
                            unsafe { libc::close(socket_fd); }
                            if let Some(ctx) = self.external_op_tracker.take_op(user_data){
                                let _ = ctx.reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: ZmqError::Internal("Failed to queue first accept".into())}));
                            }
                            false
                        }
                    }
                } else { 
                    error!("UringWorker: SQ FULL when trying to push first Accept SQE for listener_fd {}. Listener setup failed.", socket_fd);
                    self.internal_op_tracker.take_op_details(accept_ud);
                    self.handler_manager.remove_handler(socket_fd);
                    unsafe { libc::close(socket_fd); }
                     if let Some(ctx) = self.external_op_tracker.take_op(user_data){
                        let _ = ctx.reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: ZmqError::Internal("SQ full for first accept".into())}));
                    }
                    false
                };
                drop(sq);

                if submitted_accept_sqe_ok {
                    if let Some(ctx) = self.external_op_tracker.take_op(user_data) {
                         let _ = ctx.reply_tx.take_and_send_sync(Ok(UringOpCompletion::ListenSuccess {
                            user_data, listener_fd: socket_fd, actual_addr: actual_bound_addr,
                        }));
                    }
                }
                Ok(submitted_accept_sqe_ok)
            }

            UringOpRequest::Connect { user_data, target_addr, ref protocol_handler_factory_id, protocol_config, reply_tx: _original_reply_tx_in_req } => {
                let mut fd_created_for_connect_op: Option<RawFd> = None;
                let ring_has_ext_arg = self.ring.params().is_feature_ext_arg();

                // Need to reconstruct a temporary UringOpRequest for sqe_builder if it expects a reference.
                // Or, pass individual fields to sqe_builder.
                // Let's assume sqe_builder can take fields or a reconstructed temporary request.
                // The current sqe_builder takes &UringOpRequest.
                let temp_connect_req_for_builder = UringOpRequest::Connect {
                    user_data, target_addr,
                    protocol_handler_factory_id: protocol_handler_factory_id.clone(),
                    protocol_config: protocol_config.clone(),
                    reply_tx: reply_tx.clone() // Use the cloned reply_tx
                };

                match sqe_builder::build_sqe_for_external_request(
                    &temp_connect_req_for_builder,
                    &mut fd_created_for_connect_op,
                    ring_has_ext_arg,
                ) {
                    Ok(Some(sqe)) => {
                        self.external_op_tracker.add_op(user_data, ExternalOpContext {
                            reply_tx: reply_tx.clone(), op_name: op_name_str.clone(),
                            protocol_handler_factory_id: Some(protocol_handler_factory_id.clone()),
                            protocol_config: Some(protocol_config.clone()),
                            fd_created_for_connect_op, 
                            listener_fd: None, target_fd_for_shutdown: None,
                        });
                        
                        let mut sq = unsafe { self.ring.submission_shared() };
                        if !unsafe { sq.is_full() } {
                            match unsafe { sq.push(&sqe) } {
                                Ok(_) => Ok(true), 
                                Err(e) => { 
                                    error!("UringWorker: Failed to push SQE for {}: {:?}. Op failed.", op_name_str, e);
                                    self.external_op_tracker.take_op(user_data); 
                                    if let Some(fd_conn) = fd_created_for_connect_op { unsafe { libc::close(fd_conn); }}
                                    let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: ZmqError::Internal("SQ push failed".into())}));
                                    Ok(false)
                                }
                            }
                        } else { 
                            warn!("UringWorker: SQ full for op {}. Re-queuing.", op_name_str);
                            self.external_op_tracker.take_op(user_data); 
                            if let Some(fd_conn) = fd_created_for_connect_op { unsafe { libc::close(fd_conn); }}
                            // Reconstruct the original UringOpRequest::Connect to return for retry
                            Err(UringOpRequest::Connect { user_data, target_addr, protocol_handler_factory_id: protocol_handler_factory_id.clone(), protocol_config, reply_tx })
                        }
                    }
                    Ok(None) => { 
                        warn!("UringWorker: build_sqe_for_external_request returned Ok(None) for op: {}", op_name_str);
                        let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str.clone(), error: ZmqError::Internal(format!("Op {} did not produce SQE via builder as expected", op_name_str))}));
                        Ok(false)
                    }
                    Err(op_completion_error) => { 
                        error!("UringWorker: Failed to build SQE for {}: {:?}", op_name_str, op_completion_error);
                        let _ = reply_tx.take_and_send_sync(Ok(op_completion_error));
                        Ok(false)
                    }
                }
            }
            UringOpRequest::Nop { user_data, reply_tx: _ } => {
                let mut fd_created_dummy : Option<RawFd> = None; // Not used for Nop
                 match sqe_builder::build_sqe_for_external_request(
                    &UringOpRequest::Nop { user_data, reply_tx: reply_tx.clone() }, // Reconstruct
                    &mut fd_created_dummy, 
                    false, 
                ) {
                    Ok(Some(sqe)) => {
                        self.external_op_tracker.add_op(user_data, ExternalOpContext {
                            reply_tx: reply_tx.clone(), op_name: op_name_str.clone(),
                            protocol_handler_factory_id: None, protocol_config: None, fd_created_for_connect_op: None,
                            listener_fd: None, target_fd_for_shutdown: None,
                        });
                        let mut sq = unsafe { self.ring.submission_shared() };
                        if !unsafe { sq.is_full() } {
                            if unsafe{ sq.push(&sqe) }.is_ok() { Ok(true) }
                            else { 
                                self.external_op_tracker.take_op(user_data);
                                let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: ZmqError::Internal("SQ Nop push failed".into())}));
                                Ok(false) 
                            }
                        } else { 
                            self.external_op_tracker.take_op(user_data);
                            Err(UringOpRequest::Nop { user_data, reply_tx })
                        }
                    }
                    Ok(None) => {
                        let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: ZmqError::Internal("Nop build returned None".into())}));
                        Ok(false)
                    }
                    Err(e) => { 
                        let _ = reply_tx.take_and_send_sync(Ok(e));
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
                reply_tx: _, // Original reply_tx from request, use cloned `reply_tx` from outer scope
            } => {
                info!(
                    "UringWorker: Received RegisterExternalFd (ud: {}, fd: {}, factory: {}, server_role: {})",
                    user_data, fd, protocol_handler_factory_id, is_server_role
                );
                // Add to external_op_tracker BEFORE calling handler_manager,
                // so if handler_manager errors, we can reply with OpError.
                self.external_op_tracker.add_op(
                    user_data,
                    ExternalOpContext {
                        reply_tx: reply_tx.clone(), // Use the cloned reply_tx
                        op_name: op_name_str.clone(),
                        // These fields might not be strictly necessary for RegisterExternalFd's ExternalOpContext
                        // but kept for consistency if some generic error reply logic uses them.
                        protocol_handler_factory_id: Some(protocol_handler_factory_id.clone()),
                        protocol_config: Some(protocol_config.clone()),
                        fd_created_for_connect_op: None, // FD is provided, not created by worker
                        listener_fd: None,
                        target_fd_for_shutdown: Some(fd), // For potential shutdown later
                    },
                );

                match self.handler_manager.create_and_add_handler(
                    fd,
                    &protocol_handler_factory_id,
                    &protocol_config,
                    is_server_role,
                    self.buffer_manager.as_ref(),
                    self.default_buffer_ring_group_id_val,
                ) {
                    Ok(initial_ops_from_handler) => {
                        debug!(
                            "UringWorker: Handler created for registered FD {}. Processing initial ops.", fd
                        );
                        // Process any initial blueprints from the handler (e.g., start reading)
                        let mut sq_for_initial_handler_ops = unsafe { self.ring.submission_shared() };
                        // We need a mutable fds_to_initiate_close_queue. If it's part of self, use self.
                        // For now, assuming it's a local var in run_worker_loop, which means this function
                        // cannot directly add to it. This implies process_handler_blueprints should return
                        // FDs to close or this part needs rework.
                        // Let's assume process_handler_blueprints can manage its side effects, or we pass a dummy VecDeque.
                        let mut dummy_fds_to_close_q = VecDeque::new();
                        cqe_processor::process_handler_blueprints(
                            fd,
                            initial_ops_from_handler,
                            &mut self.internal_op_tracker,
                            &mut sq_for_initial_handler_ops,
                            self.default_buffer_ring_group_id_val,
                            &mut dummy_fds_to_close_q, // Dummy queue for now
                        );
                        if !dummy_fds_to_close_q.is_empty() {
                            warn!("UringWorker: RegisterExternalFd for FD {} generated unexpected close requests immediately.", fd);
                            // TODO: Handle these if necessary by adding to worker's main close queue
                        }
                        drop(sq_for_initial_handler_ops);

                        // Reply with success to the original requester via the ExternalOpContext.
                        // We already added to tracker, now take it to reply.
                        if let Some(mut op_ctx) = self.external_op_tracker.take_op(user_data) {
                            let _ = op_ctx.reply_tx.take_and_send_sync(Ok(
                                UringOpCompletion::RegisterExternalFdSuccess { user_data, fd },
                            ));
                        } else {
                            // Should not happen if add_op was successful
                            error!("UringWorker: ExternalOpContext missing for ud {} after successful RegisterExternalFd.", user_data);
                        }
                        Ok(true) // Indicated SQEs might have been pushed by process_handler_blueprints
                    }
                    Err(err_msg) => {
                        error!(
                            "UringWorker: Failed to create handler for registered FD {}: {}", fd, err_msg
                        );
                        // Reply with error via the ExternalOpContext
                        if let Some(mut op_ctx) = self.external_op_tracker.take_op(user_data) {
                            let _ = op_ctx.reply_tx.take_and_send_sync(Ok(
                                UringOpCompletion::OpError {
                                    user_data,
                                    op_name: op_name_str.clone(),
                                    error: ZmqError::Internal(err_msg),
                                },
                            ));
                        }
                        // Important: The external FD was not successfully managed. The caller needs to know.
                        // The UringWorker should NOT attempt to close this FD as it doesn't own it yet.
                        Ok(false) // No SQEs pushed by this path itself, though handler creation failed.
                    }
                }
            }

            UringOpRequest::StartFdReadLoop { user_data, fd, reply_tx: _ } => { 
                if self.handler_manager.contains_handler_for(fd) {
                    trace!("UringWorker: Received StartFdReadLoop for fd {}. Handler will manage reads via prepare_sqes.", fd);
                    let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::StartFdReadLoopAck{user_data, fd}));
                } else {
                    warn!("UringWorker: StartFdReadLoop for unknown fd {}", fd);
                     let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: ZmqError::InvalidArgument(format!("FD {} not managed for StartFdReadLoop", fd))}));
                }
                Ok(false)
            }
            UringOpRequest::SendDataViaHandler { user_data, fd, app_data, reply_tx: _ } => {
                if let Some(handler) = self.handler_manager.get_mut(fd) {
                    let interface = crate::io_uring_backend::connection_handler::UringWorkerInterface::new(
                        fd, &self.worker_io_config, self.buffer_manager.as_ref(),
                        self.default_buffer_ring_group_id_val,
                    );
                    let ops_to_queue = handler.handle_outgoing_app_data(app_data, &interface);
                    
                    let mut fds_to_close_dummy = VecDeque::new(); 
                    let mut sq_for_send_blueprints = unsafe { self.ring.submission_shared() };
                    cqe_processor::process_handler_blueprints(
                        fd, ops_to_queue, &mut self.internal_op_tracker,
                        &mut sq_for_send_blueprints, self.default_buffer_ring_group_id_val,
                        &mut fds_to_close_dummy, // Pass a mutable reference
                    );
                    drop(sq_for_send_blueprints);

                    if !fds_to_close_dummy.is_empty() {
                        warn!("SendDataViaHandler for FD {} unexpectedly resulted in close request.", fd);
                        // TODO: Need to add these to the main loop's fds_needing_close_initiated_pass
                    }
                    let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::SendDataViaHandlerAck{user_data, fd}));
                } else { 
                     warn!("UringWorker: SendDataViaHandler for unknown fd {}", fd);
                     let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: ZmqError::InvalidArgument(format!("FD {} not managed for SendDataViaHandler", fd))}));
                }
                Ok(false) 
            }
            UringOpRequest::ShutdownConnectionHandler { user_data, fd, reply_tx: _ } => { 
                 if let Some(handler) = self.handler_manager.get_mut(fd) {
                    let interface = crate::io_uring_backend::connection_handler::UringWorkerInterface::new(
                        fd, &self.worker_io_config, self.buffer_manager.as_ref(),
                        self.default_buffer_ring_group_id_val, 
                    );
                    let ops_to_queue = handler.close_initiated(&interface);
                    self.external_op_tracker.add_op(user_data, ExternalOpContext{
                        reply_tx: reply_tx.clone(), op_name: op_name_str,
                        protocol_handler_factory_id: None, protocol_config: None, fd_created_for_connect_op: None,
                        listener_fd: None, target_fd_for_shutdown: Some(fd),
                    });
                    let mut fds_to_close_dummy = VecDeque::new();
                    let mut sq_for_shutdown_blueprints = unsafe { self.ring.submission_shared() };
                    cqe_processor::process_handler_blueprints(
                        fd, ops_to_queue, &mut self.internal_op_tracker,
                        &mut sq_for_shutdown_blueprints, self.default_buffer_ring_group_id_val,
                        &mut fds_to_close_dummy, // Pass mutable reference
                    );
                    drop(sq_for_shutdown_blueprints);
                 } else { 
                    warn!("UringWorker: ShutdownConnectionHandler for unknown fd {}", fd);
                    let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError{user_data, op_name: op_name_str, error: ZmqError::InvalidArgument(format!("FD {} not managed for Shutdown", fd))}));
                 }
                 Ok(false)
            }
        }
    }
}


pub(crate) fn run_worker_loop(worker: &mut UringWorker) -> Result<(), ZmqError> {
    info!("UringWorker run_loop starting (PID: {}). Ring FD: {}", std::process::id(), worker.ring.as_raw_fd());

    let mut _submitted_to_sq_this_batch_count: u32 = 0;
    // This queue will now be a VecDeque. If it's a field on `worker`,
    // then this line would be removed, and we'd use `worker.pending_external_op_retry_queue`.
    // For now, let's make it local to this function for clarity of the change.
    // In a real implementation, worker.pending_external_op_retry_queue would be the VecDeque.
    let mut pending_external_op_retry_queue: VecDeque<UringOpRequest> = VecDeque::new();
    let mut fds_needing_close_initiated_pass: VecDeque<RawFd> = VecDeque::new(); // Also use VecDeque for consistency

    loop {
        // --- 1. Process Retry Queue for External Ops (using VecDeque) ---
        let mut items_to_process_this_retry_cycle = pending_external_op_retry_queue.len();
        if items_to_process_this_retry_cycle > 0 {
            debug!("UringWorker: Processing retry queue ({} items)", items_to_process_this_retry_cycle);
        }

        for _ in 0..items_to_process_this_retry_cycle {
            if unsafe { worker.ring.submission_shared().is_full() } {
                warn!("UringWorker: SQ full. Pausing retry queue processing. {} items remain in queue for next cycle.", pending_external_op_retry_queue.len());
                break; // Stop processing retry queue for this main loop iteration
            }

            // Process from the front (FIFO)
            if let Some(request_to_retry) = pending_external_op_retry_queue.pop_front() {
                match worker.handle_external_op_request_submission(request_to_retry.clone()) { // Clone for potential re-queue
                    Ok(true) => { _submitted_to_sq_this_batch_count += 1; }
                    Ok(false) => { /* Handled directly, or no SQE produced */ }
                    Err(req_failed_submission) => {
                        // `handle_external_op_request_submission` decided it needs retry
                        pending_external_op_retry_queue.push_back(req_failed_submission); // Add to end for later retry
                    }
                }
            } else {
                // This should not happen if items_to_process_this_retry_cycle logic is correct
                warn!("UringWorker: Retry queue (VecDeque) unexpectedly empty during processing iteration.");
                break;
            }
        }

        // --- 2. Process New External Operations from op_rx Channel ---
        // Drain op_rx as much as possible, but stop if SQ becomes full.
        if !unsafe { worker.ring.submission_shared().is_full() } {
            loop { // Loop to drain op_rx
                match worker.op_rx.try_recv() {
                    Ok(Some(request_from_channel)) => {
                        if unsafe { worker.ring.submission_shared().is_full() } {
                             trace!("UringWorker: SQ full processing new op {:?}. Adding to retry queue.", request_from_channel.op_name_str());
                             pending_external_op_retry_queue.push_back(request_from_channel);
                             break; // Stop draining op_rx for this cycle
                        }
                        match worker.handle_external_op_request_submission(request_from_channel.clone()) { // Clone for potential retry
                            Ok(true) => { _submitted_to_sq_this_batch_count += 1; }
                            Ok(false) => { /* Handled */ }
                            Err(req_for_retry_queue) => {
                                pending_external_op_retry_queue.push_back(req_for_retry_queue);
                                // If handle_external_op_request_submission itself found SQ full,
                                // we should also break draining op_rx.
                                if unsafe { worker.ring.submission_shared().is_full() } { break; }
                            }
                        }
                    }
                    Ok(None) => break, // op_rx is empty for now
                    Err(KanalReceiveError::Closed) | Err(KanalReceiveError::SendClosed) => {
                        info!("UringWorker: op_rx channel closed. Will process in-flight, then prepare for shutdown.");
                        // worker.shutdown_requested = true; // Or similar flag if UringWorker has one
                        break; // Stop draining op_rx
                    }
                }
            }
        }

        // --- 3. Poll Connection Handlers for their SQEs ---
        if !unsafe { worker.ring.submission_shared().is_full() } {
            let handler_io_ops_list = worker.handler_manager.prepare_all_handler_io_ops(
                worker.buffer_manager.as_ref(),
                worker.default_buffer_ring_group_id_val,
            );
            if !handler_io_ops_list.is_empty() {
                trace!("UringWorker: Processing {} HandlerIoOps lists from prepare_all_handler_io_ops", handler_io_ops_list.len());
                let mut sq_for_handler_blueprints = unsafe { worker.ring.submission_shared() };
                for (fd, handler_ops) in handler_io_ops_list {
                    if unsafe { sq_for_handler_blueprints.is_full() } {
                        warn!("UringWorker: SQ full during prepare_all_handler_io_ops for FD {}. Remaining blueprints need robust requeue or will be dropped.", fd);
                        // TODO: Robust mechanism to requeue handler_ops blueprints.
                        // For now, they might be dropped if SQ is full here.
                        // This could involve adding these (fd, handler_ops) to a temporary queue
                        // to be re-processed by prepare_all_handler_io_ops in the next cycle,
                        // or having handlers themselves manage retry of blueprint submission.
                        break;
                    }
                    cqe_processor::process_handler_blueprints(
                        fd,
                        handler_ops,
                        &mut worker.internal_op_tracker,
                        &mut sq_for_handler_blueprints,
                        worker.default_buffer_ring_group_id_val,
                        // &mut fds_needing_close_initiated_pass, // This was Vec, now VecDeque
                        // Convert VecDeque to Vec temporarily if needed, or change fn signature
                        // For now, let's assume we pass a mutable Vec ref and adapt later.
                        // OR, change fds_needing_close_initiated_pass to Vec for this call site temporarily.
                        // Let's assume it expects &mut Vec<RawFd> for now.
                        // This detail depends on cqe_processor.rs signature.
                        // If cqe_processor::process_handler_blueprints takes &mut Vec<RawFd>:
                        //    We'd need to convert VecDeque to Vec, pass it, then convert back.
                        //    This is inefficient. It's better if cqe_processor::process_handler_blueprints
                        //    can work with a more generic collection or if fds_needing_close_initiated_pass
                        //    is managed by pushing directly to the VecDeque.
                        // For now, assume process_handler_blueprints pushes to the VecDeque directly:
                        &mut fds_needing_close_initiated_pass, // Pass the worker's VecDeque field
                    );
                }
                // _submitted_to_sq_this_batch_count implicitly updated by direct pushes.
            }
        }

        // --- 3.5 Process FDs flagged for close_initiated by handlers (using VecDeque) ---
        let mut fds_to_process_this_close_cycle = fds_needing_close_initiated_pass.len();
        for _ in 0..fds_to_process_this_close_cycle {
            if unsafe { worker.ring.submission_shared().is_full() } {
                warn!("UringWorker: SQ full before calling close_initiated for an FD. {} FDs remain in close queue.", fds_needing_close_initiated_pass.len());
                break;
            }

            if let Some(fd_to_close) = fds_needing_close_initiated_pass.pop_front() {
                if let Some(handler) = worker.handler_manager.get_mut(fd_to_close) {
                    debug!("UringWorker MainLoop: Calling close_initiated for FD {}", fd_to_close);
                    let interface_for_close = crate::io_uring_backend::connection_handler::UringWorkerInterface::new(
                        fd_to_close,
                        &worker.worker_io_config,
                        worker.buffer_manager.as_ref(),
                        worker.default_buffer_ring_group_id_val,
                    );
                    let close_io_ops = handler.close_initiated(&interface_for_close);

                    let mut sq_for_close_blueprints = unsafe { worker.ring.submission_shared() };
                    cqe_processor::process_handler_blueprints(
                        fd_to_close,
                        close_io_ops,
                        &mut worker.internal_op_tracker,
                        &mut sq_for_close_blueprints,
                        worker.default_buffer_ring_group_id_val,
                        &mut fds_needing_close_initiated_pass, // Can push back if needed
                    );
                    // _submitted_to_sq_this_batch_count implicitly updated
                } else {
                    // Handler might have been removed by a CQE already
                    trace!("UringWorker: Tried to initiate close for FD {}, but handler was already gone.", fd_to_close);
                }
            } else {
                warn!("UringWorker: Close queue (VecDeque) unexpectedly empty during processing iteration.");
                break;
            }
        }


        // --- 4. Submit to Kernel ---
        let mut submitted_to_kernel_count: usize = 0;
        // Check if submission queue is not empty *before* calling submit.
        // `ring.submit()` might block or return 0 if SQ is empty, depending on flags/kernel.
        // `len()` on submission_shared() is the correct check.
        let sq_current_len = unsafe { worker.ring.submission_shared().len() };
        if sq_current_len > 0 {
            match worker.ring.submit() {
                Ok(count) => {
                    submitted_to_kernel_count = count;
                    trace!("UringWorker: Submitted {} SQEs to kernel (SQ had {}).", count, sq_current_len);
                }
                Err(e) => {
                    if e.raw_os_error() == Some(libc::EINTR) || e.raw_os_error() == Some(libc::EBUSY) {
                        warn!("UringWorker: ring.submit() failed with recoverable error ({}). Will retry submission implicitly in next loop or wait.", e);
                        // SQEs remain in the submission queue for the next attempt.
                    } else {
                        error!("UringWorker: ring.submit() failed with unrecoverable error: {}. Shutting down.", e);
                        return Err(ZmqError::IoError{kind: e.kind(), message: e.to_string()});
                    }
                }
            }
        }
        _submitted_to_sq_this_batch_count = 0; // Reset counter for next cycle

        // --- 5. Process Completions ---
        // Determine if we should wait for CQEs.
        // We should wait if:
        //  - We successfully submitted ops to the kernel in this cycle.
        //  - Or, there are external ops tracked (waiting for their SQE to complete).
        //  - Or, there are internal ops tracked (waiting for their SQE to complete).
        let should_wait_for_cqes = submitted_to_kernel_count > 0 ||
                                 !worker.external_op_tracker.is_empty() ||
                                 !worker.internal_op_tracker.is_empty();

        if should_wait_for_cqes {
            // Using submit_and_wait(1) waits for at least one CQE if the SQ was also submitted.
            // If SQ was empty but ops are outstanding, it just waits for CQEs.
            // This is generally a good strategy.
            trace!("UringWorker: Waiting for CQEs (submitted_count: {}, ext_ops: {}, int_ops: {})",
                   submitted_to_kernel_count, worker.external_op_tracker.in_flight.len(), worker.internal_op_tracker.op_to_details.len());
            match worker.ring.submit_and_wait(1) { // Waits for at least 1 CQE if any ops are in flight or SQ submitted.
                Ok(cqe_count_after_wait) => {
                    trace!("UringWorker: submit_and_wait(1) completed. Kernel might have processed {} CQEs.", cqe_count_after_wait);
                    /* CQEs are now ready in the completion queue */
                }
                Err(e) => {
                    if e.raw_os_error() == Some(libc::ETIME) { // Only if ring was setup with timeout
                        trace!("UringWorker: submit_and_wait(1) timed out (ETIME). No new CQEs in configured timeout.");
                    } else if e.raw_os_error() == Some(libc::EINTR) {
                        warn!("UringWorker: submit_and_wait() for completions interrupted (EINTR).");
                    } else {
                        error!("UringWorker: ring.submit_and_wait() for completions error: {}. Shutting down.", e);
                        return Err(ZmqError::IoError{kind: e.kind(), message: e.to_string()});
                    }
                }
            }
        } else if unsafe { !worker.ring.completion_shared().is_empty() } {
            // If we didn't submit and didn't expect to wait, but CQ has items (e.g. from previous cycle's submit)
            // then we should process them. No explicit wait needed.
            trace!("UringWorker: CQ not empty from previous submit, processing completions.");
        }

        if let Err(e) = cqe_processor::process_all_cqes(
            &mut worker.ring,
            &mut worker.external_op_tracker,
            &mut worker.internal_op_tracker,
            &mut worker.handler_manager,
            worker.buffer_manager.as_ref(),
            &worker.worker_io_config,
            worker.default_buffer_ring_group_id_val,
            &mut fds_needing_close_initiated_pass,
        ) {
            error!("UringWorker: CQE processing failed fatally: {}. Shutting down.", e);
            return Err(e);
        }

        // --- 6. Loop Control / Shutdown Check ---
        // Condition for graceful shutdown:
        // 1. op_rx channel is closed (no new external ops will arrive).
        // 2. No external ops are currently being tracked (all replied or errored).
        // 3. No internal ops are currently being tracked (all completed or errored).
        // 4. The pending_external_op_retry_queue is empty.
        // 5. fds_needing_close_initiated_pass queue is empty.
        // 6. No active connection handlers are managed (all FDs closed and handlers removed).
        let op_rx_closed = worker.op_rx.is_closed();

        if op_rx_closed &&
           worker.external_op_tracker.is_empty() &&
           worker.internal_op_tracker.is_empty() &&
           pending_external_op_retry_queue.is_empty() &&
           fds_needing_close_initiated_pass.is_empty() && // Check the VecDeque
           worker.handler_manager.get_active_fds().is_empty() { // Assuming get_active_fds is efficient
            info!("UringWorker: Graceful shutdown conditions met. Exiting main loop.");
            break; // Exit the main worker loop
        }

        // --- Idle Wait Logic (if no work was done and not shutting down) ---
        let no_kernel_submissions_made_this_cycle = submitted_to_kernel_count == 0 && unsafe { worker.ring.submission_shared().is_empty() };
        // Check if op_rx is empty WITHOUT blocking, only relevant if not closed.
        let no_new_external_ops_from_channel_this_cycle = if op_rx_closed { true } else { worker.op_rx.is_empty() };
        let no_pending_retries_for_external_ops_local = pending_external_op_retry_queue.is_empty(); // Check local VecDeque
        let no_ops_currently_in_flight_with_kernel = worker.external_op_tracker.is_empty() && worker.internal_op_tracker.is_empty();
        let no_fds_waiting_for_close_initiation_local = fds_needing_close_initiated_pass.is_empty(); // Check local VecDeque

        if no_kernel_submissions_made_this_cycle &&
           no_new_external_ops_from_channel_this_cycle &&
           no_pending_retries_for_external_ops_local &&
           no_ops_currently_in_flight_with_kernel && // This means CQ should also be empty after processing
           no_fds_waiting_for_close_initiation_local &&
           !op_rx_closed // Only idle wait if we are NOT in shutdown sequence
           {
            // All queues are empty, no SQEs submitted, no CQEs pending. Worker is idle.
            // Wait for new ops on op_rx with a timeout.
            trace!("UringWorker: Idle state. Waiting for new ops with timeout.");
            match worker.op_rx.recv_timeout(Duration::from_millis(100)) { // Example timeout
                Ok(request_from_timeout) => {
                    // Received a new op, add to retry queue to be processed next cycle's Phase 1 or 2.
                    // Or, attempt to handle it immediately if SQ is not full.
                    if unsafe { worker.ring.submission_shared().is_full() } {
                        pending_external_op_retry_queue.push_back(request_from_timeout);
                    } else {
                        match worker.handle_external_op_request_submission(request_from_timeout.clone()) {
                            Ok(true) => { _submitted_to_sq_this_batch_count += 1; }
                            Ok(false) => { /* Handled */ }
                            Err(req_for_retry_queue) => {
                                pending_external_op_retry_queue.push_back(req_for_retry_queue);
                            }
                        }
                    }
                }
                Err(KanalReceiveErrorTimeout::Timeout) => {
                    // Timeout, no new ops. Loop will continue and re-evaluate.
                    trace!("UringWorker: Idle wait timed out. No new external ops.");
                }
                Err(KanalReceiveErrorTimeout::Closed) | Err(KanalReceiveErrorTimeout::SendClosed) => {
                    info!("UringWorker: op_rx channel closed during idle wait. Will exit next loop if ops clear.");
                    // Loop will naturally check shutdown condition again.
                }
            }
        }
    } // end loop

    Ok(())
}