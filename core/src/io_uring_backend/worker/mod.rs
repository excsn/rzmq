#![cfg(feature = "io-uring")]

// Declare internal worker sub-modules
mod cqe_processor;
mod external_op_tracker;
mod eventfd_poller;
mod handler_manager;
mod internal_op_tracker;
mod main_loop;
mod sqe_builder;

use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::io_uring_backend::connection_handler::{
    ProtocolHandlerFactory, WorkerIoConfig, HandlerSqeBlueprint,
};
use crate::io_uring_backend::ops::UringOpRequest;
use crate::io_uring_backend::signaling_op_sender::SignalingOpSender;
use crate::message::Msg;
use crate::ZmqError;

use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;

use io_uring::IoUring;
use io_uring::opcode;
use io_uring::types;
use kanal::{Receiver as KanalReceiver, AsyncSender as KanalAsyncSender, Sender as KanalSyncSender};
use tracing::{error, info, trace, warn};

// Publicly re-export for use within io_uring_backend module
pub(crate) use eventfd_poller::EventFdPoller; 
pub(crate) use external_op_tracker::{ExternalOpContext, ExternalOpTracker};
pub(crate) use handler_manager::HandlerManager;
pub(crate) use internal_op_tracker::{InternalOpTracker, InternalOpPayload, InternalOpType};

pub struct UringWorker {
    ring: IoUring,
    op_rx: KanalReceiver<UringOpRequest>,

    buffer_manager: Option<BufferRingManager>, // Option because it's initialized via UringOpRequest
    handler_manager: HandlerManager,

    external_op_tracker: ExternalOpTracker,
    internal_op_tracker: InternalOpTracker,
    
    // WorkerIoConfig is Arc'd because it's shared with handlers created by HandlerManager
    worker_io_config: Arc<WorkerIoConfig>,
    default_buffer_ring_group_id_val: Option<u16>,
    fds_needing_close_initiated_pass: VecDeque<RawFd>,
    shutdown_requested: bool, 
    // Added queue for SQEs that couldn't be submitted immediately.
    // Stores (FD to operate on, Blueprint of the SQE to retry)
    pending_sqe_retry_queue: VecDeque<(RawFd, HandlerSqeBlueprint)>,
    pub(crate) event_fd_poller: EventFdPoller,
}

impl fmt::Debug for UringWorker {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("UringWorker")
      .field("ring_fd", &self.ring.as_raw_fd()) // as_raw_fd() on IoUring
      .field("op_rx_len", &self.op_rx.len())
      .field("op_rx_is_closed", &self.op_rx.is_closed())
      .field("buffer_manager_is_some", &self.buffer_manager.is_some()) 
      .field("external_op_tracker_len", &self.external_op_tracker.in_flight.len())
      .field("internal_op_tracker_len", &self.internal_op_tracker.op_to_details.len())
      .field("default_buffer_ring_group_id_val", &self.default_buffer_ring_group_id_val)
      .field("shutdown_requested", &self.shutdown_requested) 
      .field("pending_sqe_retry_queue_len", &self.pending_sqe_retry_queue.len())
      .field("event_fd_poller", &self.event_fd_poller)
      .finish_non_exhaustive()
  }
}

impl UringWorker {
  pub fn spawn(
    ring_entries: u32, // Number of entries for the io_uring SQ/CQ
    factories: Vec<Arc<dyn ProtocolHandlerFactory>>,
    // Provide the pre-created sender for parsed ZMTP messages
    parsed_msg_tx_zmtp: KanalSyncSender<(RawFd, Result<Msg, ZmqError>)>,
  ) -> Result<(SignalingOpSender, std::thread::JoinHandle<Result<(), ZmqError>>), ZmqError> {

    let (op_tx_sync, op_rx) = kanal::unbounded::<UringOpRequest>();
    let op_tx_async_for_signaler: KanalAsyncSender<UringOpRequest> = op_tx_sync.to_async();

    let worker_io_config = Arc::new(WorkerIoConfig {
        parsed_msg_tx_zmtp,
    });

    let event_fd_master_instance = eventfd::EventFD::new(
        0, // initval
        eventfd::EfdFlags::EFD_CLOEXEC | eventfd::EfdFlags::EFD_NONBLOCK,
    ).map_err(|e| {
        error!("Failed to create master EventFD for UringWorker: {}", e);
        ZmqError::Internal(format!("Master EventFD creation failed: {}", e))
    })?;

    let signaling_op_sender = SignalingOpSender::new(
        op_tx_async_for_signaler,
        event_fd_master_instance.clone() // SignalingOpSender gets a clone
    );

    let worker_thread_join_handle = std::thread::Builder::new()
      .name("rzmq-io-uring-worker".into())
      .spawn(move || { // worker_io_config is moved here
        match IoUring::new(ring_entries) {
          Ok(ring) => {


            let mut internal_tracker = InternalOpTracker::new(); // Create tracker instance first

            let event_fd_poller_instance = EventFdPoller::new_with_fd(
                event_fd_master_instance, // Master instance moved here
                &mut internal_tracker,
            );

            let mut worker = UringWorker {
              ring,
              op_rx,
              buffer_manager: None, // Initialized via op request
              handler_manager: HandlerManager::new(factories, worker_io_config.clone()), // Clone Arc
              external_op_tracker: ExternalOpTracker::new(),
              internal_op_tracker: internal_tracker, // Use the initialized tracker
              event_fd_poller: event_fd_poller_instance, 
              worker_io_config,
              default_buffer_ring_group_id_val: None,
              fds_needing_close_initiated_pass: VecDeque::new(),
              shutdown_requested: false,
              pending_sqe_retry_queue: VecDeque::new(),
            };
            
            let loop_result = main_loop::run_worker_loop(&mut worker);
            
            // --- Graceful Shutdown of Handlers and Ring ---
            info!("UringWorker (PID: {}) run_loop exited. Performing final cleanup.", std::process::id());
            // 1. Inform all handlers their FDs are being closed (if not already done)
            //    and attempt to queue Close SQEs for any remaining active FDs.
            //    This is tricky because the main loop might have exited due to an error
            //    where pushing to SQ is not possible.
            //    A best-effort approach:
            if loop_result.is_err() {
                warn!("UringWorker: Loop exited with error, cleanup might be partial.");
            }

            // Drain remaining external ops and notify them of shutdown/error
            for (_ud, ext_op_ctx) in worker.external_op_tracker.drain_all() {
                let _ = ext_op_ctx.reply_tx.take_and_send_sync(Err(ZmqError::Internal("UringWorker shutting down".into())));
            }

            // Close all managed FDs by handlers
            // This requires iterating handlers, calling close_initiated, and then processing RequestClose blueprints
            // This is complex to do robustly outside the main loop.
            // A simpler approach for now:
            let active_fds = worker.handler_manager.get_active_fds();
            let mut sq_cleanup = unsafe { worker.ring.submission_shared() };
            for fd in active_fds {
                if let Some(handler) = worker.handler_manager.get_mut(fd) {
                     let interface = crate::io_uring_backend::connection_handler::UringWorkerInterface::new(
                        fd,
                        &worker.worker_io_config,
                        worker.buffer_manager.as_ref(),
                        worker.default_buffer_ring_group_id_val,
                    );
                    let close_blueprints = handler.close_initiated(&interface);
                    // Attempt to submit close SQEs
                    for bp in close_blueprints.sqe_blueprints {
                        if let crate::io_uring_backend::connection_handler::HandlerSqeBlueprint::RequestClose = bp {
                            let close_ud = worker.internal_op_tracker.new_op_id(fd, InternalOpType::CloseFd, InternalOpPayload::None);
                            let close_sqe = opcode::Close::new(types::Fd(fd)).build().user_data(close_ud);
                            unsafe { let _ = sq_cleanup.push(&close_sqe); } // Best effort
                        }
                    }
                }
            }
            if !unsafe { sq_cleanup.is_empty() } {
                 match worker.ring.submit_and_wait(1) { // Try to submit and process these close ops
                    Ok(count) => trace!("UringWorker cleanup: Submitted {} final SQEs.", count),
                    Err(e) => warn!("UringWorker cleanup: Error submitting final SQEs: {}", e),
                }
                // Could attempt to process CQEs for these closes, but it's complex here.
            }
            drop(sq_cleanup);


            // Final call to fd_has_been_closed for all handlers that might still be around
            for mut handler_box in worker.handler_manager.drain_all_handlers_calling_closed() {
                handler_box.fd_has_been_closed(); // Final notification
            }


            // BufferRingManager's Drop will unregister buffers if it holds the ring ref,
            // but IoUringBufRing unregisters on its own Drop.
            // Dropping worker.ring (which happens when worker goes out of scope) closes the uring fd.
            info!("rzmq-io-uring-worker OS thread (PID: {}) finished.", std::process::id());
            loop_result
          }
          Err(e) => {
            error!("Failed to initialize IoUring (entries: {}): {}. UringWorker thread cannot start.", ring_entries, e);
            Err(ZmqError::Internal(format!("IoUring init failed: {}", e)))
          }
        }
      })
      .map_err(|e| ZmqError::Internal(format!("Failed to spawn UringWorker thread: {:?}", e)))?;

    Ok((signaling_op_sender, worker_thread_join_handle))
  }
}

// --- Helper functions for address conversion (moved from sqe_builder or other places) ---
pub(crate) fn socket_addr_to_sockaddr_storage(addr: &SocketAddr, storage: &mut libc::sockaddr_storage) -> libc::socklen_t {
    unsafe {
        // Zero out the storage first to avoid garbage in padding bytes
        // especially for sockaddr_in.
        *(storage as *mut _ as *mut [u8; std::mem::size_of::<libc::sockaddr_storage>()]) =
            [0; std::mem::size_of::<libc::sockaddr_storage>()];

        match addr {
            SocketAddr::V4(v4_addr) => {
                let sockaddr_in: &mut libc::sockaddr_in = mem::transmute(storage);
                sockaddr_in.sin_family = libc::AF_INET as libc::sa_family_t;
                sockaddr_in.sin_port = v4_addr.port().to_be();
                sockaddr_in.sin_addr = libc::in_addr { s_addr: u32::from_ne_bytes(v4_addr.ip().octets()).to_be() };
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
            }
            SocketAddr::V6(v6_addr) => {
                let sockaddr_in6: &mut libc::sockaddr_in6 = mem::transmute(storage);
                sockaddr_in6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
                sockaddr_in6.sin6_port = v6_addr.port().to_be();
                sockaddr_in6.sin6_addr = libc::in6_addr { s6_addr: v6_addr.ip().octets() };
                sockaddr_in6.sin6_flowinfo = v6_addr.flowinfo(); // Already in network byte order from std
                sockaddr_in6.sin6_scope_id = v6_addr.scope_id(); // Already in network byte order from std
                mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
            }
        }
    }
}

#[allow(dead_code)] // Used in cqe_processor for unwrap_or_else
pub(crate) fn dummy_socket_addr() -> SocketAddr {
    SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))
}

pub(crate) fn get_peer_local_addr(fd: RawFd) -> Result<(SocketAddr, SocketAddr), std::io::Error> {
    let mut peer_storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let mut peer_addrlen = mem::size_of_val(&peer_storage) as libc::socklen_t;
    if unsafe { libc::getpeername(fd, &mut peer_storage as *mut _ as *mut libc::sockaddr, &mut peer_addrlen) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let peer_saddr = sockaddr_storage_to_socket_addr(&peer_storage, peer_addrlen)?;

    let mut local_storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let mut local_addrlen = mem::size_of_val(&local_storage) as libc::socklen_t;
    if unsafe { libc::getsockname(fd, &mut local_storage as *mut _ as *mut libc::sockaddr, &mut local_addrlen) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let local_saddr = sockaddr_storage_to_socket_addr(&local_storage, local_addrlen)?;

    Ok((peer_saddr, local_saddr))
}

pub(crate) fn sockaddr_storage_to_socket_addr(storage: &libc::sockaddr_storage, len: libc::socklen_t) -> std::io::Result<SocketAddr> {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            if len as usize >= mem::size_of::<libc::sockaddr_in>() {
                let sa = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
                let ip = Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr)); // s_addr is in network byte order
                let port = u16::from_be(sa.sin_port); // sin_port is in network byte order
                Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
            } else {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "sockaddr_in length too small"))
            }
        }
        libc::AF_INET6 => {
             if len as usize >= mem::size_of::<libc::sockaddr_in6>() {
                let sa = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
                let ip = Ipv6Addr::from(sa.sin6_addr.s6_addr); // s6_addr is network order
                let port = u16::from_be(sa.sin6_port); // sin6_port is network order
                let flowinfo = u32::from_be(sa.sin6_flowinfo); // sin6_flowinfo is network order
                let scope_id = u32::from_be(sa.sin6_scope_id); // sin6_scope_id is network order
                Ok(SocketAddr::V6(SocketAddrV6::new(ip, port, flowinfo, scope_id)))
            } else {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "sockaddr_in6 length too small"))
            }
        }
        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid socket address family")),
    }
}