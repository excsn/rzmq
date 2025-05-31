// core/src/io_uring_backend/worker/sqe_builder.rs

#![cfg(feature = "io-uring")]

use crate::io_uring_backend::ops::{UringOpRequest, UringOpCompletion, UserData};
use crate::ZmqError;
use io_uring::{opcode, squeue, types};
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::mem;

// Import the helper function from the parent module (worker/mod.rs)
// This assumes worker/mod.rs will pub(crate) use it or define it there.
// For now, let's assume it will be in `super`
use super::socket_addr_to_sockaddr_storage;
// If InternalOpTracker is needed here (e.g., for Listen's first Accept), it would also be `use super::internal_op_tracker::InternalOpTracker;`
// However, as per our refined plan, Listen setup (including first Accept) is in UringWorker::handle_external_op_request_submission.

/// Builds an SQE for an external UringOpRequest if it maps to a direct kernel call.
///
/// # Arguments
/// * `request`: The external operation request.
/// * `out_connect_fd`: If the request is `Connect`, this will be populated with the
///   `RawFd` of the socket created for the connect attempt. This FD is needed by
///   the `cqe_processor` to manage the socket (e.g., close it on connect failure).
/// * `_ring_features_has_ext_arg`: Example of passing ring features if needed by specific opcodes.
///
/// # Returns
/// * `Ok(Some(squeue::Entry))` if an SQE was successfully built.
/// * `Ok(None)` if the operation does not produce a direct SQE from this function
///   (e.g., `InitializeBufferRing`, `Listen` setup, `StartFdReadLoop`, etc., which are
///   handled by worker state changes or by handlers).
/// * `Err(UringOpCompletion::OpError)` if building the SQE failed (e.g., syscall error
///   during socket creation for Connect) or if the request type is unhandled here but
///   was expected to produce an SQE.
pub(crate) fn build_sqe_for_external_request(
    request: &UringOpRequest,
    out_connect_fd: &mut Option<RawFd>,
    _ring_features_has_ext_arg: bool, // Parameter kept for future use if needed
) -> Result<Option<squeue::Entry>, UringOpCompletion> {
    // Ensure out_connect_fd is reset for each call if it's reused.
    // However, it's better if the caller manages its state.
    // This function will only set it if it creates an FD for Connect.
    // *out_connect_fd = None; // Caller should initialize this to None before calling.

    let user_data = request.get_user_data_ref(); // Use the helper

    match request {
        UringOpRequest::Nop { .. } => {
            Ok(Some(opcode::Nop::new().build().user_data(user_data)))
        }
        UringOpRequest::Connect { target_addr, .. } => {
            let socket_fd = match target_addr {
                SocketAddr::V4(_) => unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0) },
                SocketAddr::V6(_) => unsafe { libc::socket(libc::AF_INET6, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0) },
            };

            if socket_fd < 0 {
                let e = std::io::Error::last_os_error();
                tracing::error!("Connect: Failed to create socket: {}", e);
                return Err(UringOpCompletion::OpError {
                    user_data,
                    op_name: "ConnectSocketCreate".to_string(),
                    error: ZmqError::from(e),
                });
            }
            *out_connect_fd = Some(socket_fd); // Store the created FD

            let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
            let addr_len = socket_addr_to_sockaddr_storage(target_addr, &mut storage);

            if addr_len == 0 {
                // This case should ideally be caught by SocketAddr parsing earlier,
                // but as a safeguard if socket_addr_to_sockaddr_storage fails.
                unsafe { libc::close(socket_fd); } // Clean up the created socket
                *out_connect_fd = None;             // Clear the stored FD
                tracing::error!("Connect: Address conversion failed for target_addr: {:?}", target_addr);
                return Err(UringOpCompletion::OpError {
                    user_data,
                    op_name: "ConnectAddrConversion".to_string(),
                    error: ZmqError::InvalidArgument(format!("Address conversion failed for {:?}", target_addr)),
                });
            }

            Ok(Some(
                opcode::Connect::new(types::Fd(socket_fd), &storage as *const _ as *const libc::sockaddr, addr_len)
                    .build()
                    .user_data(user_data), // UserData for Connect SQE is the external UserData
            ))
        }
        UringOpRequest::RegisterRawBuffers { .. } => {
            // This operation itself (registering buffers with IORING_REGISTER_BUFFERS)
            // is usually done by the IoUring::register_buffers method directly on the ring,
            // not as a typical SQE that goes through the submission queue for I/O.
            // If the intent was a different kind of buffer registration, it needs clarification.
            // For now, assume it's handled by UringWorker directly or not an SQE from here.
            tracing::trace!("build_sqe_for_external_request: RegisterRawBuffers is handled directly by worker, not as an SQE from here.");
            Ok(None)
        }

        // Operations handled by worker state changes or delegated to handlers, not by direct SQE from this function:
        UringOpRequest::InitializeBufferRing { .. } |
        UringOpRequest::Listen { .. } | // Listen setup is complex, first Accept SQE is internal.
        UringOpRequest::RegisterExternalFd { .. } |
        UringOpRequest::StartFdReadLoop { .. } |
        UringOpRequest::SendDataViaHandler { .. } |
        UringOpRequest::ShutdownConnectionHandler { .. } => {
            tracing::trace!(
                "build_sqe_for_external_request: Op '{}' does not produce a direct SQE from this function.",
                request.op_name_str()
            );
            Ok(None) // These ops don't produce an SQE *through this builder*.
                     // Their logic is in UringWorker::handle_external_op_request_submission.
        }
        // Note: If any new UringOpRequest variants are added that *should* produce an SQE here,
        // they need to be explicitly handled.
    }
}
