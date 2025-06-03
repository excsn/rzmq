#![cfg(feature = "io-uring")]

// Declare internal worker sub-modules
mod cqe_processor;
mod eventfd_poller;
mod external_op_tracker;
mod handler_manager;
mod internal_op_tracker;
mod main_loop;
mod multishot_reader;
mod sqe_builder;

use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::io_uring_backend::connection_handler::{
  HandlerSqeBlueprint, HandlerUpstreamEvent, ProtocolHandlerFactory, WorkerIoConfig,
};
use crate::io_uring_backend::ops::UringOpRequest;
use crate::io_uring_backend::send_buffer_pool::SendBufferPool;
use crate::io_uring_backend::signaling_op_sender::SignalingOpSender;
use crate::uring::UringConfig;
use crate::ZmqError;

use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;

use io_uring::opcode;
use io_uring::types;
use io_uring::IoUring;
use kanal::{AsyncSender as KanalAsyncSender, Receiver as KanalReceiver, Sender as KanalSyncSender};
use tracing::{error, info, trace, warn};

// Publicly re-export for use within io_uring_backend module
pub(crate) use eventfd_poller::EventFdPoller;
pub(crate) use external_op_tracker::{ExternalOpContext, ExternalOpTracker};
pub(crate) use handler_manager::HandlerManager;
pub(crate) use internal_op_tracker::{InternalOpPayload, InternalOpTracker, InternalOpType};
pub(crate) use multishot_reader::MultishotReader;

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
  send_buffer_pool: Option<Arc<SendBufferPool>>, // For zero-copy sends
  // Configuration values passed at spawn time or from global settings
  cfg_send_zerocopy_enabled: bool,
  cfg_send_buffer_count: usize,
  cfg_send_buffer_size: usize,
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
      .field(
        "default_buffer_ring_group_id_val",
        &self.default_buffer_ring_group_id_val,
      )
      .field("shutdown_requested", &self.shutdown_requested)
      .field("pending_sqe_retry_queue_len", &self.pending_sqe_retry_queue.len())
      .field("event_fd_poller", &self.event_fd_poller)
      .finish_non_exhaustive()
  }
}

impl UringWorker {
  pub fn spawn_with_config(
    config: UringConfig,
    factories: Vec<Arc<dyn ProtocolHandlerFactory>>,
    upstream_event_tx: KanalSyncSender<(RawFd, HandlerUpstreamEvent)>,
  ) -> Result<(SignalingOpSender, std::thread::JoinHandle<Result<(), ZmqError>>), ZmqError> {
    let (op_tx_sync, op_rx) = kanal::unbounded::<UringOpRequest>();
    let op_tx_async_for_signaler: KanalAsyncSender<UringOpRequest> = op_tx_sync.to_async();

    let worker_io_config = Arc::new(WorkerIoConfig { upstream_event_tx });

    let event_fd_master_instance =
      eventfd::EventFD::new(0, eventfd::EfdFlags::EFD_CLOEXEC | eventfd::EfdFlags::EFD_NONBLOCK).map_err(|e| {
        error!("Failed to create master EventFD for UringWorker: {}", e);
        ZmqError::Internal(format!("Master EventFD creation failed: {}", e))
      })?;

    let signaling_op_sender = SignalingOpSender::new(op_tx_async_for_signaler, event_fd_master_instance.clone());

    let worker_thread_join_handle = std::thread::Builder::new()
            .name("rzmq-io-uring-worker".into())
            .spawn(move || { // config is moved here
                match IoUring::new(config.ring_entries) {
                    Ok(ring) => {
                        let mut internal_tracker = InternalOpTracker::new();
                        let event_fd_poller_instance = EventFdPoller::new_with_fd(
                            event_fd_master_instance,
                            &mut internal_tracker,
                        );

                        // --- SendBufferPool Initialization ---
                        let mut worker_send_buffer_pool: Option<Arc<SendBufferPool>> = None;
                        // This variable will hold the *actual* state of ZC enablement for this worker instance.
                        let mut effective_send_zerocopy_enabled_for_worker = config.default_send_zerocopy;

                        if config.default_send_zerocopy {
                            if config.default_send_buffer_count > 0 && config.default_send_buffer_size > 0 {
                                // TODO: Consider RLIMIT_MEMLOCK check here or ensure it's documented.
                                match SendBufferPool::new(&ring, config.default_send_buffer_count, config.default_send_buffer_size) {
                                    Ok(pool) => {
                                        info!("UringWorker: SendBufferPool initialized (count: {}, size: {}). Zero-copy send enabled.", config.default_send_buffer_count, config.default_send_buffer_size);
                                        worker_send_buffer_pool = Some(Arc::new(pool));
                                    }
                                    Err(e) => {
                                        error!("UringWorker: Failed to initialize SendBufferPool from config: {}. Disabling ZC send for this worker.", e);
                                        effective_send_zerocopy_enabled_for_worker = false;
                                    }
                                }
                            } else {
                                info!("UringWorker: Zero-copy send requested by config, but pool count/size is zero. Disabling ZC send for this worker.");
                                effective_send_zerocopy_enabled_for_worker = false;
                            }
                        } else {
                            info!("UringWorker: Zero-copy send not enabled by config.");
                        }

                        // --- Default BufferRingManager Initialization (for bgid 0) ---
                        let mut default_worker_buffer_manager: Option<BufferRingManager> = None;
                        let mut default_worker_bgid_val: Option<u16> = None;

                        // The UringConfig.default_recv_multishot enables/disables the *creation* of the
                        // default (bgid 0) provided buffer ring at worker startup.
                        // Individual handlers can still choose to use (or not use) multishot reads
                        // based on their specific configuration (e.g., ZmtpEngineConfig.use_recv_multishot).
                        // If a handler wants multishot but the default ring wasn't created, it might fail
                        // or need to request creation of a specific bgid.
                        if config.default_recv_multishot {
                            if config.default_recv_buffer_count > 0 && config.default_recv_buffer_size > 0 {
                                match BufferRingManager::new(&ring, config.default_recv_buffer_count as u16, 0, config.default_recv_buffer_size) {
                                    Ok(bm) => {
                                        info!("UringWorker: Default BufferRingManager (bgid 0) initialized (count: {}, size: {}). Default multishot recv path enabled.", config.default_recv_buffer_count, config.default_recv_buffer_size);
                                        default_worker_buffer_manager = Some(bm);
                                        default_worker_bgid_val = Some(0); // Default ring uses bgid 0
                                    }
                                    Err(e) => {
                                        error!("UringWorker: Failed to initialize default BufferRingManager from config: {}. Provided buffer reads on bgid 0 may fail.", e);
                                    }
                                }
                            } else {
                                info!("UringWorker: Default multishot recv requested by config, but buffer count/size is zero. Default ring (bgid 0) not created.");
                            }
                        } else {
                             info!("UringWorker: Default multishot recv not enabled by config. Default ring (bgid 0) not created at spawn.");
                        }

                        let mut worker = UringWorker {
                            ring,
                            op_rx,
                            buffer_manager: default_worker_buffer_manager,
                            handler_manager: HandlerManager::new(factories, worker_io_config.clone()),
                            external_op_tracker: ExternalOpTracker::new(),
                            internal_op_tracker: internal_tracker,
                            event_fd_poller: event_fd_poller_instance,
                            worker_io_config,
                            default_buffer_ring_group_id_val: default_worker_bgid_val,
                            fds_needing_close_initiated_pass: VecDeque::new(),
                            shutdown_requested: false,
                            pending_sqe_retry_queue: VecDeque::new(),
                            send_buffer_pool: worker_send_buffer_pool,
                            cfg_send_zerocopy_enabled: effective_send_zerocopy_enabled_for_worker,
                            // Store original config values for reference if needed, though behavior is driven by effective values.
                            cfg_send_buffer_count: config.default_send_buffer_count,
                            cfg_send_buffer_size: config.default_send_buffer_size,
                        };

                        let loop_result = main_loop::run_worker_loop(&mut worker);
                        
                        info!("UringWorker (PID: {}) run_loop exited. Performing final cleanup.", std::process::id());
                        if loop_result.is_err() {
                            warn!("UringWorker: Loop exited with error, cleanup might be partial.");
                        }
                        for (_ud, ext_op_ctx) in worker.external_op_tracker.drain_all() {
                            let _ = ext_op_ctx.reply_tx.take_and_send_sync(Err(ZmqError::Internal("UringWorker shutting down".into())));
                        }
                        let active_fds = worker.handler_manager.get_active_fds();
                        let mut sq_cleanup = unsafe { worker.ring.submission_shared() };
                        for fd in active_fds {
                            if let Some(handler) = worker.handler_manager.get_mut(fd) {
                                const WORKER_SHUTDOWN_SENTINEL_UD: u64 = 0;
                                 let interface = crate::io_uring_backend::connection_handler::UringWorkerInterface::new(
                                    fd,
                                    &worker.worker_io_config,
                                    worker.buffer_manager.as_ref(), // Pass the default bgid 0 manager
                                    worker.default_buffer_ring_group_id_val,
                                    WORKER_SHUTDOWN_SENTINEL_UD,
                                );
                                let close_blueprints = handler.close_initiated(&interface);
                                for bp in close_blueprints.sqe_blueprints {
                                    if let crate::io_uring_backend::connection_handler::HandlerSqeBlueprint::RequestClose = bp {
                                        let close_ud = worker.internal_op_tracker.new_op_id(fd, InternalOpType::CloseFd, InternalOpPayload::None);
                                        let close_sqe = opcode::Close::new(types::Fd(fd)).build().user_data(close_ud);
                                        unsafe { let _ = sq_cleanup.push(&close_sqe); }
                                    }
                                }
                            }
                        }
                        if !sq_cleanup.is_empty() {
                             match worker.ring.submit_and_wait(1) {
                                Ok(count) => trace!("UringWorker cleanup: Submitted {} final SQEs.", count),
                                Err(e) => warn!("UringWorker cleanup: Error submitting final SQEs: {}", e),
                            }
                        }
                        drop(sq_cleanup);
                        for mut handler_box in worker.handler_manager.drain_all_handlers_calling_closed() {
                            handler_box.fd_has_been_closed();
                        }
                        if let Some(pool_arc) = &worker.send_buffer_pool {
                            unsafe {
                                if let Err(e) = pool_arc.unregister_all(&worker.ring) {
                                    error!("UringWorker: Error unregistering send buffers on shutdown: {}", e);
                                }
                            }
                        }
                        // Drop buffer_manager if it was created. Its Drop impl handles unregistering.
                        drop(worker.buffer_manager);

                        info!("rzmq-io-uring-worker OS thread (PID: {}) finished.", std::process::id());
                        loop_result
                    }
                    Err(e) => {
                        error!("Failed to initialize IoUring (entries: {}): {}. UringWorker thread cannot start.", config.ring_entries, e);
                        Err(ZmqError::Internal(format!("IoUring init failed: {}", e)))
                    }
                }
            })
            .map_err(|e| ZmqError::Internal(format!("Failed to spawn UringWorker thread: {:?}", e)))?;

    Ok((signaling_op_sender, worker_thread_join_handle))
  }
}

// --- Helper functions for address conversion (moved from sqe_builder or other places) ---
pub(crate) fn socket_addr_to_sockaddr_storage(
  addr: &SocketAddr,
  storage: &mut libc::sockaddr_storage,
) -> libc::socklen_t {
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
  if unsafe {
    libc::getpeername(
      fd,
      &mut peer_storage as *mut _ as *mut libc::sockaddr,
      &mut peer_addrlen,
    )
  } != 0
  {
    return Err(std::io::Error::last_os_error());
  }
  let peer_saddr = sockaddr_storage_to_socket_addr(&peer_storage, peer_addrlen)?;

  let mut local_storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
  let mut local_addrlen = mem::size_of_val(&local_storage) as libc::socklen_t;
  if unsafe {
    libc::getsockname(
      fd,
      &mut local_storage as *mut _ as *mut libc::sockaddr,
      &mut local_addrlen,
    )
  } != 0
  {
    return Err(std::io::Error::last_os_error());
  }
  let local_saddr = sockaddr_storage_to_socket_addr(&local_storage, local_addrlen)?;

  Ok((peer_saddr, local_saddr))
}

pub(crate) fn sockaddr_storage_to_socket_addr(
  storage: &libc::sockaddr_storage,
  len: libc::socklen_t,
) -> std::io::Result<SocketAddr> {
  match storage.ss_family as libc::c_int {
    libc::AF_INET => {
      if len as usize >= mem::size_of::<libc::sockaddr_in>() {
        let sa = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
        let ip = Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr)); // s_addr is in network byte order
        let port = u16::from_be(sa.sin_port); // sin_port is in network byte order
        Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
      } else {
        Err(std::io::Error::new(
          std::io::ErrorKind::InvalidInput,
          "sockaddr_in length too small",
        ))
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
        Err(std::io::Error::new(
          std::io::ErrorKind::InvalidInput,
          "sockaddr_in6 length too small",
        ))
      }
    }
    _ => Err(std::io::Error::new(
      std::io::ErrorKind::InvalidInput,
      "invalid socket address family",
    )),
  }
}
