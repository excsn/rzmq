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
  HandlerSqeBlueprint, OutgoingMessage, ProtocolHandlerFactory,
};
use crate::io_uring_backend::ops::UringOpRequest;
use crate::io_uring_backend::send_buffer_pool::SendBufferPool;
use crate::io_uring_backend::signaling_op_sender::SignalingOpSender;
use crate::io_uring_backend::UserData;
use crate::uring::{UringPollingStrategy, global_state, UringConfig};
use crate::ZmqError;

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use fibre::mpmc::{unbounded, AsyncSender, Sender as SyncSender, Receiver as SyncReceiver};
use fibre::mpsc;
use io_uring::opcode;
use io_uring::IoUring;
use tracing::{debug, error, info, trace, warn};

// Publicly re-export for use within io_uring_backend module
pub(crate) use eventfd_poller::EventFdPoller;
pub(crate) use external_op_tracker::{ExternalOpContext, ExternalOpTracker};
pub(crate) use handler_manager::HandlerManager;
pub(crate) use internal_op_tracker::{InternalOpPayload, InternalOpTracker, InternalOpType};
pub(crate) use multishot_reader::MultishotReader;


#[derive(Debug, Default)]
struct FdWork {
  /// Non-blocking ingress (read setup / cancel) blueprints — never gated by write_in_flight.
  pub(crate) ingress_blueprints: VecDeque<HandlerSqeBlueprint>,
  /// Sequential egress (data writes + cork + close) blueprints — write-serialized.
  pub(crate) egress_blueprints: VecDeque<HandlerSqeBlueprint>,
  /// Serialization gate: true while a write SQE has been submitted but its CQE not yet reaped.
  pub(crate) write_in_flight: bool,
}

impl FdWork {
  pub(crate) fn route_blueprints(&mut self, blueprints: impl IntoIterator<Item = HandlerSqeBlueprint>) {
    for bp in blueprints {
      if bp.is_ingress() {
        self.ingress_blueprints.push_back(bp);
      } else {
        self.egress_blueprints.push_back(bp);
      }
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerState {
  Running,
  Draining, // Shutdown initiated, processing in-flight completions only
  CleaningUp, 
  Stopped,
}

pub struct UringWorker {
  pub(crate) state: WorkerState,
  ring: IoUring,
  op_rx: SyncReceiver<UringOpRequest>,

  pub(crate) work_map: HashMap<RawFd, FdWork>,
  buffer_manager: Option<BufferRingManager>, // Option because it's initialized via UringOpRequest
  handler_manager: HandlerManager,

  external_op_tracker: ExternalOpTracker,
  internal_op_tracker: InternalOpTracker,

  default_buffer_ring_group_id_val: Option<u16>,
  fds_needing_close_initiated_pass: VecDeque<RawFd>,
  pub(crate) event_fd_poller: EventFdPoller,
  send_buffer_pool: Option<Arc<SendBufferPool>>, // For zero-copy sends
  pub(crate) fd_to_mpsc_rx: HashMap<RawFd, Arc<mpsc::BoundedReceiver<OutgoingMessage>>>,
  // Configuration values passed at spawn time or from global settings
  cfg_send_zerocopy_enabled: bool,
  cfg_send_buffer_count: usize, //TODO revisit
  cfg_send_buffer_size: usize,
  /// True when the io_uring ring was successfully initialized with `IORING_SETUP_SQPOLL`.
  /// Controls whether the main loop bypasses `submit()` when the kernel polling thread is active.
  pub(crate) cfg_sqpoll_active: bool,
  /// User-space spinning strategy applied when `needs_wait` is true.
  pub(crate) cfg_polling_strategy: UringPollingStrategy,
  // Shared flag: true while the worker is blocked in submit_with_args waiting for kernel events.
  // Connections check this before writing to eventfd to avoid redundant syscalls.
  pub(crate) worker_asleep: Arc<AtomicBool>,
}

impl fmt::Debug for UringWorker {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("UringWorker")
      .field("ring_fd", &self.ring.as_raw_fd())
      .field("op_rx_len", &self.op_rx.len())
      .field("op_rx_is_closed", &self.op_rx.is_closed())
      .field("buffer_manager_is_some", &self.buffer_manager.is_some())
      .field("external_op_tracker_len", &self.external_op_tracker.in_flight.len())
      .field("internal_op_tracker_len", &self.internal_op_tracker.op_to_details.len())
      .field("default_buffer_ring_group_id_val", &self.default_buffer_ring_group_id_val)
      .field("event_fd_poller", &self.event_fd_poller)
      .field("fd_to_mpsc_rx_len", &self.fd_to_mpsc_rx.len())
      .finish_non_exhaustive()
  }
}

impl UringWorker {
  pub fn spawn_with_config(
    config: UringConfig,
    factories: Vec<Arc<dyn ProtocolHandlerFactory>>,
  ) -> Result<(SignalingOpSender, std::thread::JoinHandle<Result<(), ZmqError>>), ZmqError> {
    let (op_tx_sync, op_rx) = unbounded::<UringOpRequest>();
    let op_tx_async_for_signaler: AsyncSender<UringOpRequest> = op_tx_sync.to_async();

    let event_fd_master_instance =
      eventfd::EventFD::new(0, eventfd::EfdFlags::EFD_CLOEXEC | eventfd::EfdFlags::EFD_NONBLOCK).map_err(|e| {
        error!("Failed to create master EventFD for UringWorker: {}", e);
        ZmqError::Internal(format!("Master EventFD creation failed: {}", e))
      })?;

    let worker_asleep = Arc::new(AtomicBool::new(false));
    let pool_slot: Arc<once_cell::sync::OnceCell<Arc<crate::io_uring_backend::send_buffer_pool::SendBufferPool>>> =
      Arc::new(once_cell::sync::OnceCell::new());
    let signaling_op_sender = SignalingOpSender::new(
      op_tx_async_for_signaler,
      event_fd_master_instance.clone(),
      Arc::clone(&worker_asleep),
      Arc::clone(&pool_slot),
    );

    let worker_thread_join_handle = std::thread::Builder::new()
      .name("rzmq-io-uring-worker".into())
      .spawn(move || { // config is moved here
        // Build the ring, optionally with SQPOLL, with graceful privilege fallback.
        let mut actual_sqpoll_enabled = config.sqpoll_enabled;
        let ring_result: Result<IoUring, ZmqError> = {
          let mut builder = io_uring::IoUring::builder();
          if config.sqpoll_enabled {
            builder.setup_sqpoll(config.sqpoll_idle_ms);
          }
          match builder.build(config.ring_entries) {
            Ok(r) => Ok(r),
            Err(e)
              if config.sqpoll_enabled
                && (e.raw_os_error() == Some(libc::EPERM)
                  || e.raw_os_error() == Some(libc::EACCES)) =>
            {
              warn!(
                "UringWorker: SQPOLL initialization failed (EPERM/EACCES — insufficient privileges \
                 or kernel < 5.11). Falling back to standard non-SQPOLL mode."
              );
              actual_sqpoll_enabled = false;
              io_uring::IoUring::builder()
                .build(config.ring_entries)
                .map_err(|e| ZmqError::Internal(format!("io_uring fallback build failed: {}", e)))
            }
            Err(e) => Err(ZmqError::Internal(format!("IoUring init failed: {}", e))),
          }
        };
        match ring_result {
          Ok(ring) => {
            info!(
              "UringWorker: io_uring ring initialized. entries={}, sqpoll={}",
              config.ring_entries, actual_sqpoll_enabled
            );
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
                            let pool_arc = Arc::new(pool);
                            let _ = pool_slot.set(Arc::clone(&pool_arc));
                            worker_send_buffer_pool = Some(pool_arc);
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

            // Create the default buffer ring if buffers are configured.
            // This ring is used for both single-shot and multi-shot provided-buffer reads.
            // The `config.default_recv_multishot` flag will control which *type* of read is *attempted*,
            // but the infrastructure (the buffer ring) should exist for either.
            if config.default_recv_buffer_count > 0 && config.default_recv_buffer_size > 0 {
                match BufferRingManager::new(&ring, config.default_recv_buffer_count as u16, 0, config.default_recv_buffer_size) {
                    Ok(bm) => {
                        info!("UringWorker: Default BufferRingManager (bgid 0) initialized (count: {}, size: {}). Provided-buffer reads are enabled.", config.default_recv_buffer_count, config.default_recv_buffer_size);
                        default_worker_buffer_manager = Some(bm);
                        default_worker_bgid_val = Some(0); // Default ring uses bgid 0
                    }
                    Err(e) => {
                        error!("UringWorker: Failed to initialize default BufferRingManager from config: {}. Provided-buffer reads may fail.", e);
                    }
                }
            } else {
                  info!("UringWorker: Default provided-buffer recv ring not configured (count/size is zero).");
            }

            let mut worker = UringWorker {
              state: WorkerState::Running,
              ring,
              op_rx,
              work_map: HashMap::new(),
              buffer_manager: default_worker_buffer_manager,
              handler_manager: HandlerManager::new(factories),
              external_op_tracker: ExternalOpTracker::new(),
              internal_op_tracker: internal_tracker,
              event_fd_poller: event_fd_poller_instance,
              default_buffer_ring_group_id_val: default_worker_bgid_val,
              fds_needing_close_initiated_pass: VecDeque::new(),
              send_buffer_pool: worker_send_buffer_pool,
              fd_to_mpsc_rx: HashMap::new(),
              cfg_send_zerocopy_enabled: effective_send_zerocopy_enabled_for_worker,
              cfg_send_buffer_count: config.default_send_buffer_count,
              cfg_send_buffer_size: config.default_send_buffer_size,
              cfg_sqpoll_active: actual_sqpoll_enabled,
              cfg_polling_strategy: config.polling_strategy,
              worker_asleep,
            };

            {
              let loop_result = main_loop::run_worker_loop(&mut worker);
              if loop_result.is_err() {
                warn!("UringWorker: Loop exited with error, cleanup might be partial.");
              }
            }
            
            info!("rzmq-io-uring-worker OS thread (PID: {}) finished.", std::process::id());
            Ok(()) // The loop_result is no longer returned, just Ok(())
          }
          Err(e) => {
            error!("UringWorker: io_uring initialization failed — cannot start: {}", e);
            Err(e)
          }
        }
      })
      .map_err(|e| ZmqError::Internal(format!("Failed to spawn UringWorker thread: {:?}", e)))?;

    Ok((signaling_op_sender, worker_thread_join_handle))
  }
}

impl UringWorker {
  fn transition_to_draining(&mut self) {
    info!("UringWorker: Transitioning to DRAINING state.");
    self.state = WorkerState::Draining;
    
    let mut sq_for_shutdown = unsafe { self.ring.submission_shared() };

    // Cancel all in-flight internal kernel operations.
    let internal_ops_to_cancel: Vec<UserData> =
      self.internal_op_tracker.op_to_details.keys().copied().collect();

    info!(
      "UringWorker: Draining state - Cancelling {} in-flight internal operations.",
      internal_ops_to_cancel.len()
    );

    for op_ud in internal_ops_to_cancel {
      if sq_for_shutdown.is_full() {
        warn!("UringWorker draining transition: SQ full, cannot submit all cancel ops. CQE processing will need to handle the rest.");
        break;
      }
      trace!(
        "UringWorker draining transition: Submitting AsyncCancel for internal op_ud {}",
        op_ud
      );
      // The cancel op itself doesn't need its own tracker entry,
      // as we will be draining all CQEs anyway. We can give it a sentinel UD.
      let cancel_sqe = opcode::AsyncCancel::new(op_ud).build().user_data(0); // Sentinel UD for cancel op
      unsafe {
        let _ = sq_for_shutdown.push(&cancel_sqe);
      }
    }
    drop(sq_for_shutdown);

    // After submitting cancellations, we must submit the ring to make sure the kernel sees them.
    if let Err(e) = self.ring.submitter().submit() {
      warn!(
        "UringWorker draining transition: Error submitting cancellation SQEs: {}",
        e
      );
    }
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
