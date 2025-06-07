// core/src/io_uring_backend/worker/main_loop.rs

#![cfg(feature = "io-uring")]

use super::{cqe_processor, sqe_builder, ExternalOpContext, UringWorker};
use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::io_uring_backend::connection_handler::{HandlerIoOps, UringWorkerInterface};
use crate::io_uring_backend::ops::{UringOpCompletion, UringOpRequest};
use crate::io_uring_backend::worker::external_op_tracker::MultipartSendState;
use crate::io_uring_backend::worker::{InternalOpPayload, InternalOpType};
use crate::profiler::LoopProfiler;
use crate::ZmqError;

use std::any::Any;
use std::collections::VecDeque;
use std::os::fd::AsRawFd;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Duration;

use fibre::{RecvError, TryRecvError};
use io_uring::{opcode, types};
use tracing::{debug, error, info, trace, warn};

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
  fn handle_external_op_request_submission(&mut self, request: UringOpRequest) -> Result<bool, UringOpRequest> {
    let user_data_from_req = request.get_user_data_ref();
    let op_name_str = request.op_name_str();
    let original_request_reply_tx = request.get_reply_tx_ref().clone(); // Clone for local use, original stays with request for retry

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
          let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
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
              // Set the worker's owned default_bgid_val
              if self.default_buffer_ring_group_id_val.is_none() {
                self.default_buffer_ring_group_id_val = Some(bgid);
                debug!("UringWorker: Worker's default buffer group ID set to {}", bgid);
              } else {
                warn!(
                  "UringWorker: Worker's default buffer group ID was already set. New bgid {} not made default.",
                  bgid
                );
              }
              let _ =
                reply_tx.take_and_send_sync(Ok(UringOpCompletion::InitializeBufferRingSuccess { user_data, bgid }));
            }
            Err(e) => {
              error!("UringWorker: Failed to initialize BufferRingManager: {}", e);
              let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
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
        ref buffers,
        reply_tx,
      } => {
        warn!("UringWorker: RegisterRawBuffers direct ring call not fully implemented. Sending placeholder ack.");
        // Real implementation: self.ring.register_buffers(buffers_as_ioslices_or_vecs).map_err...
        let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::RegisterRawBuffersSuccess { user_data }));
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
          let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
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
          let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: e,
          }));
          return Ok(false);
        }

        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let addr_len = super::socket_addr_to_sockaddr_storage(&addr, &mut storage);
        if unsafe { libc::bind(socket_fd, &storage as *const _ as *const libc::sockaddr, addr_len) } < 0 {
          let e = ZmqError::from(std::io::Error::last_os_error());
          error!("Listen(ud:{}): Failed to bind fd {}: {}", user_data, socket_fd, e);
          unsafe {
            libc::close(socket_fd);
          }
          let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
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
          super::sockaddr_storage_to_socket_addr(&actual_addr_storage, actual_addr_len).unwrap_or(addr)
        } else {
          addr
        };

        if unsafe { libc::listen(socket_fd, 128) } < 0 {
          let e = ZmqError::from(std::io::Error::last_os_error());
          error!("Listen(ud:{}): Failed to listen on fd {}: {}", user_data, socket_fd, e);
          unsafe {
            libc::close(socket_fd);
          }
          let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
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

        self
          .handler_manager
          .add_listener_metadata(socket_fd, protocol_handler_factory_id.clone(), protocol_config);

        let accept_ud = self
          .internal_op_tracker
          .new_op_id(socket_fd, InternalOpType::Accept, InternalOpPayload::None);
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
                let _ = ctx.reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
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
            let _ = ctx.reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
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
            let _ = ctx.reply_tx.take_and_send_sync(Ok(UringOpCompletion::ListenSuccess {
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

        // Need to reconstruct a temporary UringOpRequest for sqe_builder if it expects a reference.
        // Or, pass individual fields to sqe_builder.
        // Let's assume sqe_builder can take fields or a reconstructed temporary request.
        // The current sqe_builder takes &UringOpRequest.
        let temp_connect_req_for_builder = UringOpRequest::Connect {
          user_data,
          target_addr,
          protocol_handler_factory_id: protocol_handler_factory_id.clone(),
          protocol_config: protocol_config.clone(),
          reply_tx: reply_tx.clone(), // Use the cloned reply_tx
        };

        match sqe_builder::build_sqe_for_external_request(
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
                  let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
                    user_data,
                    op_name: op_name_str,
                    error: ZmqError::Internal("SQ push failed".into()),
                  }));
                  Ok(false)
                }
              }
            } else {
              warn!("UringWorker: SQ full for op {}. Re-queuing.", op_name_str);
              self.external_op_tracker.take_op(user_data);
              if let Some(fd_conn) = fd_created_for_connect_op {
                unsafe {
                  libc::close(fd_conn);
                }
              }
              // Reconstruct the original UringOpRequest::Connect to return for retry
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
            let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
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
            let _ = reply_tx.take_and_send_sync(Ok(op_completion_error));
            Ok(false)
          }
        }
      }
      UringOpRequest::Nop { user_data, reply_tx } => {
        let mut fd_created_dummy: Option<RawFd> = None; // Not used for Nop
        match sqe_builder::build_sqe_for_external_request(
          &UringOpRequest::Nop {
            user_data,
            reply_tx: reply_tx.clone(),
          }, // Reconstruct
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
                let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
                  user_data,
                  op_name: op_name_str,
                  error: ZmqError::Internal("SQ Nop push failed".into()),
                }));
                Ok(false)
              }
            } else {
              self.external_op_tracker.take_op(user_data);
              Err(UringOpRequest::Nop { user_data, reply_tx })
            }
          }
          Ok(None) => {
            let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
              user_data,
              op_name: op_name_str,
              error: ZmqError::Internal("Nop build returned None".into()),
            }));
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
        reply_tx, // Original reply_tx from request, use cloned `reply_tx` from outer scope
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
            multipart_state: None,
          },
        );

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
            // Process any initial blueprints from the handler (e.g., start reading)
            let mut sq_for_initial_handler_ops = unsafe { self.ring.submission_shared() };
            cqe_processor::process_handler_blueprints(
              fd,
              initial_ops_from_handler,
              &mut self.internal_op_tracker,
              &mut sq_for_initial_handler_ops,
              self.default_buffer_ring_group_id_val,
              &mut self.fds_needing_close_initiated_pass,
              &mut self.pending_sqe_retry_queue,
              &mut self.handler_manager,
              self.cfg_send_zerocopy_enabled,
              &self.send_buffer_pool,
              &self.external_op_tracker,
            );
            if !self.fds_needing_close_initiated_pass.is_empty() {
              warn!(
                "UringWorker: RegisterExternalFd for FD {} generated unexpected close requests immediately.",
                fd
              );
              // TODO: Handle these if necessary by adding to worker's main close queue
            }
            drop(sq_for_initial_handler_ops);

            // Reply with success to the original requester via the ExternalOpContext.
            // We already added to tracker, now take it to reply.
            if let Some(mut op_ctx) = self.external_op_tracker.take_op(user_data) {
              let _ = op_ctx
                .reply_tx
                .take_and_send_sync(Ok(UringOpCompletion::RegisterExternalFdSuccess { user_data, fd }));
            } else {
              // Should not happen if add_op was successful
              error!(
                "UringWorker: ExternalOpContext missing for ud {} after successful RegisterExternalFd.",
                user_data
              );
            }
            Ok(true) // Indicated SQEs might have been pushed by process_handler_blueprints
          }
          Err(err_msg) => {
            error!(
              "UringWorker: Failed to create handler for registered FD {}: {}",
              fd, err_msg
            );
            // Reply with error via the ExternalOpContext
            if let Some(mut op_ctx) = self.external_op_tracker.take_op(user_data) {
              let _ = op_ctx.reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
                user_data,
                op_name: op_name_str.clone(),
                error: ZmqError::Internal(err_msg),
              }));
            }
            // Important: The external FD was not successfully managed. The caller needs to know.
            // The UringWorker should NOT attempt to close this FD as it doesn't own it yet.
            Ok(false) // No SQEs pushed by this path itself, though handler creation failed.
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
          let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::StartFdReadLoopAck { user_data, fd }));
        } else {
          warn!("UringWorker: StartFdReadLoop for unknown fd {}", fd);
          let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: ZmqError::InvalidArgument(format!("FD {} not managed for StartFdReadLoop", fd)),
          }));
        }
        Ok(false)
      }
      UringOpRequest::SendDataViaHandler {
        user_data,
        fd,
        app_data,
        reply_tx,
      } => {
        // reply_tx is moved here
        let op_name_for_error = "SendDataViaHandler"; // Local const string for errors

        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx,
            op_name: op_name_str.clone(), // op_name_str was derived from request.op_name_str()
            protocol_handler_factory_id: None, // Not relevant for this op type
            protocol_config: None,        // Not relevant
            fd_created_for_connect_op: None, // Not relevant
            listener_fd: None,            // Not relevant
            target_fd_for_shutdown: Some(fd), // Associate with the FD for context
            multipart_state: None,
          },
        );

        if let Some(handler) = self.handler_manager.get_mut(fd) {
          let interface = UringWorkerInterface::new(
            // Ensure correct module path
            fd,
            &self.worker_io_config,
            self.buffer_manager.as_ref(),
            self.default_buffer_ring_group_id_val,
            user_data,
          );
          let ops_output_from_handler = handler.handle_outgoing_app_data(app_data, &interface);

          let mut local_fds_to_close_queue = VecDeque::new();

          let mut sq_for_blueprints = unsafe { self.ring.submission_shared() };
          cqe_processor::process_handler_blueprints(
            fd,
            ops_output_from_handler,
            &mut self.internal_op_tracker,
            &mut sq_for_blueprints,
            self.default_buffer_ring_group_id_val,
            &mut local_fds_to_close_queue,
            &mut self.pending_sqe_retry_queue,
            &mut self.handler_manager,
            self.cfg_send_zerocopy_enabled,
            &self.send_buffer_pool,
            &self.external_op_tracker,
          );
          drop(sq_for_blueprints);

          // Ensure local_fds_to_close_queue is processed even if we don't send an immediate ACK here
          self.fds_needing_close_initiated_pass.extend(local_fds_to_close_queue);

          if !self.cfg_send_zerocopy_enabled {
            // If zero-copy is OFF, ACK immediately.
            // The ExternalOpContext's reply_tx is consumed here.
            // cqe_processor will later find no (or already used) reply_tx for this app_op_ud.
            if let Some(ctx_for_immediate_ack) = self.external_op_tracker.take_op(user_data) {
              if ctx_for_immediate_ack
                .reply_tx
                .take_and_send_sync(Ok(UringOpCompletion::SendDataViaHandlerAck { user_data, fd }))
                .is_none()
              {
                tracing::warn!("UringWorker (No ZC): Reply_tx for SendDataViaHandler (ud {}) was already taken before immediate ACK.", user_data);
              }
            } else {
              // Should ideally not happen if add_op above succeeded.
              tracing::error!(
                "UringWorker (No ZC): ExternalOpContext not found for immediate ACK for SendDataViaHandler (ud {}).",
                user_data
              );
            }
          }

          Ok(true)
        } else {
          warn!("UringWorker: SendDataViaHandler for unknown fd {}", fd);
          // Clean up external op tracker if handler not found, and reply with error
          if let Some(ctx) = self.external_op_tracker.take_op(user_data) {
            let _ = ctx.reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
              user_data,
              op_name: op_name_for_error.to_string(),
              error: ZmqError::InvalidArgument(format!("FD {} not managed for SendDataViaHandler", fd)),
            }));
          }
          Ok(false)
        }
      }

      UringOpRequest::SendDataMultipartViaHandler {
        user_data,
        fd,
        app_data_parts,
        reply_tx,
      } => {
        let op_name_for_error = "SendDataMultipartViaHandler";

        let num_parts = app_data_parts.len();
        self.external_op_tracker.add_op(
          user_data,
          ExternalOpContext {
            reply_tx,                          // Clone the OneShotSender for the tracker
            op_name: op_name_str.clone(),      // op_name_str was derived from request.op_name_str()
            protocol_handler_factory_id: None, // Not relevant for this op type
            protocol_config: None,             // Not relevant
            fd_created_for_connect_op: None,   // Not relevant
            listener_fd: None,                 // Not relevant
            target_fd_for_shutdown: Some(fd),  // Associate with the FD for context
            multipart_state: if self.cfg_send_zerocopy_enabled {
              // Only track multipart if ZC is on
              Some(MultipartSendState {
                total_parts: num_parts,
                completed_parts: 0,
              })
            } else {
              None // No multipart state tracking needed if ZC is off (immediate ACK)
            },
          },
        );
        if let Some(handler) = self.handler_manager.get_mut(fd) {
          let interface = UringWorkerInterface::new(
            fd,
            &self.worker_io_config,
            self.buffer_manager.as_ref(),
            self.default_buffer_ring_group_id_val,
            user_data,
          );
          // UringConnectionHandler::handle_outgoing_app_data expects Arc<dyn Any + Send + Sync>
          // We pass the Arc<Vec<Msg>> as the Arc<dyn Any...>. The handler will downcast.
          let ops_output_from_handler =
            handler.handle_outgoing_app_data(app_data_parts as Arc<dyn Any + Send + Sync>, &interface);

          let mut local_fds_to_close_queue_multi = VecDeque::new();

          let mut sq_for_multi_blueprints = unsafe { self.ring.submission_shared() };
          cqe_processor::process_handler_blueprints(
            fd,
            ops_output_from_handler,
            &mut self.internal_op_tracker,
            &mut sq_for_multi_blueprints,
            self.default_buffer_ring_group_id_val,
            &mut local_fds_to_close_queue_multi,
            &mut self.pending_sqe_retry_queue,
            &mut self.handler_manager,
            self.cfg_send_zerocopy_enabled,
            &self.send_buffer_pool,
            &self.external_op_tracker,
          );
          drop(sq_for_multi_blueprints);

          // Ensure local_fds_to_close_queue is processed
          self
            .fds_needing_close_initiated_pass
            .extend(local_fds_to_close_queue_multi);

          if !self.cfg_send_zerocopy_enabled {
            // If zero-copy is OFF, ACK immediately for the whole multipart.
            if let Some(ctx_for_immediate_ack) = self.external_op_tracker.take_op(user_data) {
              if ctx_for_immediate_ack
                .reply_tx
                .take_and_send_sync(Ok(UringOpCompletion::SendDataViaHandlerAck { user_data, fd }))
                .is_none()
              {
                tracing::warn!("UringWorker (No ZC): Reply_tx for SendDataMultipartViaHandler (ud {}) was already taken before immediate ACK.", user_data);
              }
            } else {
              tracing::error!("UringWorker (No ZC): ExternalOpContext not found for immediate ACK for SendDataMultipartViaHandler (ud {}).", user_data);
            }
          }

          Ok(true)
        } else {
          warn!("UringWorker: SendDataMultipartViaHandler for unknown fd {}", fd);
          if let Some(ctx) = self.external_op_tracker.take_op(user_data) {
            let _ = ctx.reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
              user_data,
              op_name: op_name_for_error.to_string(),
              error: ZmqError::InvalidArgument(format!("FD {} not managed for SendDataMultipartViaHandler", fd)),
            }));
          }
          Ok(false)
        }
      }

      UringOpRequest::ShutdownConnectionHandler {
        user_data,
        fd,
        reply_tx,
      } => {
        if let Some(handler) = self.handler_manager.get_mut(fd) {
          let interface = crate::io_uring_backend::connection_handler::UringWorkerInterface::new(
            fd,
            &self.worker_io_config,
            self.buffer_manager.as_ref(),
            self.default_buffer_ring_group_id_val,
            user_data,
          );
          let ops_to_queue = handler.close_initiated(&interface);
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
          let mut sq_for_shutdown_blueprints = unsafe { self.ring.submission_shared() };
          cqe_processor::process_handler_blueprints(
            fd,
            ops_to_queue,
            &mut self.internal_op_tracker,
            &mut sq_for_shutdown_blueprints,
            self.default_buffer_ring_group_id_val,
            &mut self.fds_needing_close_initiated_pass, // Pass mutable reference
            &mut self.pending_sqe_retry_queue,
            &mut self.handler_manager,
            self.cfg_send_zerocopy_enabled,
            &self.send_buffer_pool,
            &self.external_op_tracker,
          );
          drop(sq_for_shutdown_blueprints);
        } else {
          warn!("UringWorker: ShutdownConnectionHandler for unknown fd {}", fd);
          let _ = reply_tx.take_and_send_sync(Ok(UringOpCompletion::OpError {
            user_data,
            op_name: op_name_str,
            error: ZmqError::InvalidArgument(format!("FD {} not managed for Shutdown", fd)),
          }));
        }
        Ok(false)
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

  let mut profiler = LoopProfiler::new(Duration::from_millis(10), 10000); // Log if loop > 10ms, or every 10000th iteration otherwise
  let mut pending_external_op_retry_queue: VecDeque<UringOpRequest> = VecDeque::new();

  const KERNEL_POLL_INITIAL: Duration = Duration::from_micros(1000);
  let mut kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;
  const KERNEL_POLL_MAX_DURATION: Duration = Duration::from_millis(64);

  loop {
    profiler.loop_start();

    let mut user_space_activity_this_cycle = false; // Tracks if user-space queues/handlers had items/generated ops
    let mut sqes_submitted_to_kernel_this_batch = 0;
    let mut cqe_processed_this_batch = 0;

    profiler.mark_segment_end_and_start_new("submit_eventfd_poll_if_needed");
    if !worker.shutdown_requested {
      // Only attempt if not shutting down
      let mut sq_for_eventfd_poll = unsafe { worker.ring.submission_shared() };
      // try_submit_initial_poll_sqe handles the `is_poll_submitted` check internally.
      // It will also handle SQ full condition by returning false, leading to retry next loop.
      let _eventfd_poll_submitted_or_already_active = worker
        .event_fd_poller
        .try_submit_initial_poll_sqe(&mut sq_for_eventfd_poll);
      // We don't strictly need to track if an SQE was pushed here for user_space_activity_this_cycle,
      // as the main SQ submit phase will catch it.
      // However, if it *was* submitted, it implies activity.
      if _eventfd_poll_submitted_or_already_active && !worker.event_fd_poller.is_poll_submitted {
        // This means it *tried* to submit but SQ was full.
        // Actual submission success is tracked by event_fd_poller.is_poll_submitted
      }
      drop(sq_for_eventfd_poll);
    }

    // --- PHASE 0: Process Pending SQE Retry Queue ---

    // Only attempt if SQ is not full and queue has items.
    let items_to_process_from_sqe_retry_queue = worker.pending_sqe_retry_queue.len();
    if items_to_process_from_sqe_retry_queue > 0 {
      trace!(
        "UringWorker: Processing SQE retry queue ({} items)",
        items_to_process_from_sqe_retry_queue
      );
      user_space_activity_this_cycle = true;
    }
    profiler.mark_segment_end_and_start_new("process_sqe_retry_q");

    for _ in 0..items_to_process_from_sqe_retry_queue {
      if unsafe { worker.ring.submission_shared().is_full() } {
        warn!(
          "UringWorker: SQ full during SQE retry queue processing. {} items remain.",
          worker.pending_sqe_retry_queue.len()
        );
        break;
      }
      if let Some((fd, blueprint_to_retry)) = worker.pending_sqe_retry_queue.pop_front() {
        // Re-process this blueprint. process_handler_blueprints handles SQ full by re-adding to the *same* queue.
        // This is okay as long as progress is made eventually.
        // We pass a temporary HandlerIoOps containing just this one blueprint.
        let single_blueprint_ops = HandlerIoOps {
          sqe_blueprints: vec![blueprint_to_retry], // clone might not be needed if it was already owned. For now, assume it's cheap or it was already moved to queue.
          initiate_close_due_to_error: false,
        };
        let mut sq_for_retry_blueprint = unsafe { worker.ring.submission_shared() };
        cqe_processor::process_handler_blueprints(
          fd,
          single_blueprint_ops,
          &mut worker.internal_op_tracker,
          &mut sq_for_retry_blueprint,
          worker.default_buffer_ring_group_id_val,
          &mut worker.fds_needing_close_initiated_pass,
          &mut worker.pending_sqe_retry_queue, // Pass it back in case SQ is still full
          &mut worker.handler_manager,
          worker.cfg_send_zerocopy_enabled,
          &worker.send_buffer_pool,
          &worker.external_op_tracker,
        );
        // sq_for_retry_blueprint is dropped
      } else {
        break;
      } // Should not happen if len > 0
    }

    profiler.mark_segment_end_and_start_new("process_ext_op_retry_q");
    // --- PHASE 1: Handle User-Space Requests and Handler Logic (Generate SQEs) ---

    // 1a. Process Retry Queue
    let items_to_process_this_retry_cycle = pending_external_op_retry_queue.len();
    if items_to_process_this_retry_cycle > 0 {
      debug!(
        "UringWorker: Processing retry queue ({} items)",
        items_to_process_this_retry_cycle
      );
      user_space_activity_this_cycle = true;
    }
    for _ in 0..items_to_process_this_retry_cycle {
      if unsafe { worker.ring.submission_shared().is_full() } {
        warn!(
          "UringWorker: SQ full during retry queue. {} items remain.",
          pending_external_op_retry_queue.len()
        );
        break;
      }
      if let Some(request_to_retry) = pending_external_op_retry_queue.pop_front() {
        match worker.handle_external_op_request_submission(request_to_retry.clone()) {
          Ok(_sqe_produced) => { /* SQEs are pushed by handle_external_op_request_submission if needed */ }
          Err(req_failed_submission) => {
            pending_external_op_retry_queue.push_back(req_failed_submission);
          }
        }
      } else {
        break;
      }
    }

    // <<<MODIFIED>>>
    profiler.mark_segment_end_and_start_new("process_new_op_rx");
    // 1b. Process New External Ops from op_rx
    loop {
      if unsafe { worker.ring.submission_shared().is_full() } {
        trace!("UringWorker: SQ full processing new ops from op_rx.");
        break;
      }
      match worker.op_rx.try_recv() {
        Ok(request_from_channel) => {
          user_space_activity_this_cycle = true;
          match worker.handle_external_op_request_submission(request_from_channel.clone()) {
            Ok(_sqe_produced) => {}
            Err(req_for_retry_queue) => {
              pending_external_op_retry_queue.push_back(req_for_retry_queue);
            }
          }
        }
        Err(TryRecvError::Empty) => {
          break;
        }
        Err(TryRecvError::Disconnected) => {
          info!("UringWorker: op_rx channel closed. Setting shutdown_requested = true.");
          worker.shutdown_requested = true;
          break;
        }
      }
    }

    // <<<MODIFIED>>>
    profiler.mark_segment_end_and_start_new("poll_conn_handlers");
    // 1c. Poll Connection Handlers
    if !worker.shutdown_requested && !unsafe { worker.ring.submission_shared().is_full() } {
      let handler_io_ops_list = worker
        .handler_manager
        .prepare_all_handler_io_ops(worker.buffer_manager.as_ref(), worker.default_buffer_ring_group_id_val);
      if !handler_io_ops_list.is_empty() {
        user_space_activity_this_cycle = true;
        let mut sq_for_handler_blueprints = unsafe { worker.ring.submission_shared() };
        for (fd, handler_ops) in handler_io_ops_list {
          if !handler_ops.sqe_blueprints.is_empty() && sq_for_handler_blueprints.is_full() {
            warn!("UringWorker: SQ full for HandlerIoOps for FD {}.", fd);

            // Requeue blueprints from handler if SQ is full
            for bp_to_requeue in handler_ops.sqe_blueprints {
              worker.pending_sqe_retry_queue.push_back((fd, bp_to_requeue));
            }
            if handler_ops.initiate_close_due_to_error && !worker.fds_needing_close_initiated_pass.contains(&fd) {
              worker.fds_needing_close_initiated_pass.push_back(fd);
            }
            continue;
          }
          cqe_processor::process_handler_blueprints(
            fd,
            handler_ops,
            &mut worker.internal_op_tracker,
            &mut sq_for_handler_blueprints,
            worker.default_buffer_ring_group_id_val,
            &mut worker.fds_needing_close_initiated_pass,
            &mut worker.pending_sqe_retry_queue,
            &mut worker.handler_manager,
            worker.cfg_send_zerocopy_enabled,
            &worker.send_buffer_pool,
            &worker.external_op_tracker,
          );
        }
      }
    }

    profiler.mark_segment_end_and_start_new("process_fds_to_close");
    // 1d. Process FDs flagged for close
    if !worker.shutdown_requested {
      let fds_to_process_this_close_cycle = worker.fds_needing_close_initiated_pass.len();
      if fds_to_process_this_close_cycle > 0 {
        user_space_activity_this_cycle = true;
      }
      for _ in 0..fds_to_process_this_close_cycle {
        if unsafe { worker.ring.submission_shared().is_full() } {
          break;
        }
        if let Some(fd_to_close) = worker.fds_needing_close_initiated_pass.pop_front() {
          if let Some(handler) = worker.handler_manager.get_mut(fd_to_close) {
            // For close_initiated not directly tied to an external op reply,
            // pass a sentinel UserData or a conventionally understood "internal" UD.
            // 0 is often used for internal/unspecific ops if not otherwise reserved.
            const INTERNAL_OP_SENTINEL_UD: u64 = 0;
            let interface_for_close = UringWorkerInterface::new(
              fd_to_close,
              &worker.worker_io_config,
              worker.buffer_manager.as_ref(),
              worker.default_buffer_ring_group_id_val,
              INTERNAL_OP_SENTINEL_UD,
            );
            let close_io_ops = handler.close_initiated(&interface_for_close);
            let mut sq_for_close_blueprints = unsafe { worker.ring.submission_shared() };
            cqe_processor::process_handler_blueprints(
              fd_to_close,
              close_io_ops,
              &mut worker.internal_op_tracker,
              &mut sq_for_close_blueprints,
              worker.default_buffer_ring_group_id_val,
              &mut worker.fds_needing_close_initiated_pass,
              &mut worker.pending_sqe_retry_queue,
              &mut worker.handler_manager,
              worker.cfg_send_zerocopy_enabled,
              &worker.send_buffer_pool,
              &worker.external_op_tracker,
            );
          }
        } else {
          break;
        }
      }
    }

    profiler.mark_segment_end_and_start_new("submit_sqes_to_kernel");
    // --- PHASE 2: Submit SQEs to Kernel ---
    let sq_len_at_submit_time = unsafe { worker.ring.submission_shared().len() };
    if sq_len_at_submit_time > 0 {
      user_space_activity_this_cycle = true;
      match worker.ring.submitter().submit() {
        Ok(submitted_count) => {
          sqes_submitted_to_kernel_this_batch = submitted_count;
          trace!(
            "UringWorker: Submitted {} SQEs to kernel (SQ had {}).",
            submitted_count,
            sq_len_at_submit_time
          );
        }
        Err(e) => {
          /* ... (handle EBUSY/EINTR or fatal error) ... */
          if e.raw_os_error() == Some(libc::EBUSY) || e.raw_os_error() == Some(libc::EINTR) {
            warn!("UringWorker: ring.submit() failed with EBUSY/EINTR. SQEs remain.");
          } else {
            profiler.log_and_reset_for_next_loop();
            error!("UringWorker: ring.submit() failed critically: {}. Shutting down.", e);
            return Err(ZmqError::IoError {
              kind: e.kind(),
              message: e.to_string(),
            });
          }
        }
      }
    }

    profiler.mark_segment_end_and_start_new("process_kernel_cqes");
    // --- PHASE 3: Process Kernel Completions (CQEs) ---
    let cq_len_before_processing = unsafe { worker.ring.completion_shared().len() };
    // worker.ring.completion().sync(); // Not strictly needed if iterating directly after submit

    if let Err(e) = cqe_processor::process_all_cqes(
      &mut worker.ring,
      &mut worker.external_op_tracker,
      &mut worker.internal_op_tracker,
      &mut worker.handler_manager,
      worker.buffer_manager.as_ref(),
      &worker.send_buffer_pool,
      &worker.worker_io_config,
      worker.default_buffer_ring_group_id_val,
      &mut worker.fds_needing_close_initiated_pass,
      &mut worker.pending_sqe_retry_queue,
      &mut worker.event_fd_poller,
      worker.cfg_send_zerocopy_enabled,
    ) {
      profiler.log_and_reset_for_next_loop();
      return Err(e);
    }
    let cq_len_after_processing = unsafe { worker.ring.completion_shared().len() };
    cqe_processed_this_batch = cq_len_before_processing.saturating_sub(cq_len_after_processing);
    if cqe_processed_this_batch > 0 {
      trace!("UringWorker: Processed {} CQEs this batch.", cqe_processed_this_batch);
      kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;
    }

    profiler.mark_segment_end_and_start_new("shutdown_check_and_idle_wait");
    // --- PHASE 4: Shutdown Check ---
    if worker.shutdown_requested
      && worker.external_op_tracker.is_empty()
      && worker.internal_op_tracker.is_empty()
      && pending_external_op_retry_queue.is_empty()
      && worker.pending_sqe_retry_queue.is_empty()
      && worker.fds_needing_close_initiated_pass.is_empty()
      && worker.handler_manager.get_active_fds().is_empty()
    {
      info!("UringWorker: Graceful shutdown conditions met. Exiting main loop.");
      profiler.log_and_reset_for_next_loop();
      break;
    }

    // --- PHASE 5: Idle Wait Strategy ---
    let had_any_sq_submission_this_batch = sqes_submitted_to_kernel_this_batch > 0;
    let any_effective_work_done_this_cycle =
      had_any_sq_submission_this_batch || cqe_processed_this_batch > 0 || user_space_activity_this_cycle;

    if !any_effective_work_done_this_cycle {
      let kernel_ops_are_pending = !worker.internal_op_tracker.is_empty() || !worker.external_op_tracker.is_empty();

      let eventfd_poll_is_active = worker.event_fd_poller.is_poll_submitted;

      if !worker.shutdown_requested || eventfd_poll_is_active {
        if kernel_ops_are_pending {
          // Kernel ops are outstanding.
          // Strategy:
          // 1. Try a non-blocking check on op_rx. If something is there, process it next loop.
          // 2. If op_rx is empty, then briefly ask kernel for events.
          // This gives op_rx higher priority than a blocking kernel wait.

          match worker.op_rx.try_recv() {
            Ok(request_from_channel) => {
              trace!("UringWorker: Polled op_rx while kernel ops pending; got new request. Will process next iter.");
              match worker.handle_external_op_request_submission(request_from_channel) {
                Ok(_sqe_produced) => {}
                Err(req_for_retry_queue) => {
                  pending_external_op_retry_queue.push_back(req_for_retry_queue);
                }
              }
              kernel_poll_timeout_duration = KERNEL_POLL_INITIAL; // Activity, reset kernel poll
            }
            Err(TryRecvError::Empty) => {
              // op_rx is empty
              trace!(
                                "UringWorker: op_rx empty. Kernel ops pending (int:{}, ext:{}, ev_poll:{}) or eventfd poll active. Waiting for CQEs with timeout: {:?}.",
                                worker.internal_op_tracker.op_to_details.len(),
                                worker.external_op_tracker.in_flight.len(),
                                eventfd_poll_is_active,
                                kernel_poll_timeout_duration
                            );
              let timespec_for_wait = types::Timespec::from(kernel_poll_timeout_duration);
              let submit_args = types::SubmitArgs::new().timespec(&timespec_for_wait);

              // We are waiting for *existing* ops, so wait_nr = 1 is appropriate.
              // submit_with_args will also submit any SQEs if the SQ wasn't empty (though it should be here).
              match worker.ring.submitter().submit_with_args(1, &submit_args) {
                Ok(reaped_count) => {
                  trace!(
                    "UringWorker: submit_with_args (timed wait for pending kernel ops) reaped {} CQEs.",
                    reaped_count
                  );
                  if reaped_count > 0 {
                    // Kernel activity
                    kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;
                  } else {
                    // Kernel timed out or reaped 0
                    kernel_poll_timeout_duration = (kernel_poll_timeout_duration * 2).min(KERNEL_POLL_MAX_DURATION);
                    trace!(
                      "UringWorker: Kernel wait timed out/reaped 0. New kernel_poll_timeout: {:?}",
                      kernel_poll_timeout_duration
                    );
                  }
                }
                Err(e) => {
                  /* ... (handle ETIME, EINTR, critical errors as before) ... */
                  if e.kind() == std::io::ErrorKind::TimedOut || e.raw_os_error() == Some(libc::ETIME) {
                    trace!(
                      "UringWorker: submit_with_args (timed wait for pending kernel ops) resulted in ETIME/TimedOut."
                    );
                    kernel_poll_timeout_duration = (kernel_poll_timeout_duration * 2).min(KERNEL_POLL_MAX_DURATION);
                  } else if e.raw_os_error() == Some(libc::EINTR) {
                    warn!("UringWorker: submit_with_args (timed wait for pending kernel ops) interrupted (EINTR).");
                    kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;
                  } else {
                    error!("UringWorker: ring.submit_with_args (timed wait for pending kernel ops) critical error: {}. Shutting down.", e);

                    profiler.mark_segment_end_and_start_new("kernel_wait_submit_with_args");
                    profiler.log_and_reset_for_next_loop();
                    return Err(ZmqError::IoError {
                      kind: e.kind(),
                      message: e.to_string(),
                    });
                  }
                }
              }

              profiler.mark_segment_end_and_start_new("kernel_wait_submit_with_args");
            }
            Err(TryRecvError::Disconnected) => {
              info!("UringWorker: op_rx channel closed while checking (kernel ops pending branch).");
              worker.shutdown_requested = true;
            }
          }
        } else {
          // Truly idle: No user-space work, no kernel ops outstanding.
          trace!(
                        "UringWorker: No user-space work, no other kernel ops. Waiting for eventfd or kernel ops with timeout: {:?}.",
                        kernel_poll_timeout_duration
                    );
          let timespec_for_wait = types::Timespec::from(kernel_poll_timeout_duration);
          let submit_args = types::SubmitArgs::new().timespec(&timespec_for_wait);
          match worker.ring.submitter().submit_with_args(1, &submit_args) {
            Ok(reaped_count) => {
              if reaped_count > 0 {
                kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;
              } else {
                kernel_poll_timeout_duration = (kernel_poll_timeout_duration * 2).min(KERNEL_POLL_MAX_DURATION);
              }
            }
            Err(e) => {
              /* Same error handling as above */
              if e.kind() == std::io::ErrorKind::TimedOut || e.raw_os_error() == Some(libc::ETIME) {
                kernel_poll_timeout_duration = (kernel_poll_timeout_duration * 2).min(KERNEL_POLL_MAX_DURATION);
              } else if e.raw_os_error() == Some(libc::EINTR) {
                kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;
              } else {
                error!(
                  "UringWorker: ring.submit_with_args (idle branch) critical error: {}. Shutting down.",
                  e
                );
                profiler.log_and_reset_for_next_loop();
                return Err(ZmqError::IoError {
                  kind: e.kind(),
                  message: e.to_string(),
                });
              }
            }
          }

          profiler.mark_segment_end_and_start_new("unified_kernel_wait");
        }
      } // end if !worker.shutdown_requested
    } else {
      // Work *was* done in this cycle. Reset kernel poll timeout to be responsive.
      kernel_poll_timeout_duration = KERNEL_POLL_INITIAL;
    }

    profiler.log_and_reset_for_next_loop();
  } // end main loop

  Ok(())
}
