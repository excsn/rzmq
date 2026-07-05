#![cfg(feature = "io-uring")]

use crate::io_uring_backend::signaling_op_sender::SignalingOpSender;
use crate::{error::ZmqError, uring::URING_BACKEND_INITIALIZED};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle as StdThreadJoinHandle;

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tracing::{debug, error, info};

/// The set of spawned `UringWorker` senders plus a lock-free round-robin cursor.
/// Each fd is assigned a worker at registration via [`pick_worker`] and stays on it for life.
pub(crate) struct UringWorkerPool {
  workers: Box<[SignalingOpSender]>,
  next: AtomicUsize,
}

impl UringWorkerPool {
  pub(crate) fn new(workers: Vec<SignalingOpSender>) -> Self {
    debug_assert!(!workers.is_empty());
    Self {
      workers: workers.into_boxed_slice(),
      next: AtomicUsize::new(0),
    }
  }

  /// Round-robin pick, same idiom as `LoadBalancer::get_next_connection`.
  pub(crate) fn pick(&self) -> SignalingOpSender {
    let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
    self.workers[idx].clone()
  }

  /// Consumes the pool for shutdown: each sender gets an explicit `ShutdownWorker` op.
  pub(crate) fn into_senders(self) -> Vec<SignalingOpSender> {
    self.workers.into_vec()
  }
}

static URING_WORKER_POOL: OnceCell<Mutex<Option<UringWorkerPool>>> = OnceCell::new();
static URING_WORKER_JOIN_HANDLES: OnceCell<Mutex<Vec<StdThreadJoinHandle<Result<(), ZmqError>>>>> =
  OnceCell::new();

#[doc(hidden)]
pub(crate) fn get_uring_worker_pool_mutex() -> &'static Mutex<Option<UringWorkerPool>> {
  URING_WORKER_POOL.get_or_init(Default::default)
}

#[doc(hidden)]
pub(crate) fn get_uring_worker_join_handles_mutex(
) -> &'static Mutex<Vec<StdThreadJoinHandle<Result<(), ZmqError>>>> {
  URING_WORKER_JOIN_HANDLES.get_or_init(Default::default)
}

pub(crate) fn ensure_global_uring_systems_started() -> Result<(), ZmqError> {
  if !URING_BACKEND_INITIALIZED.load(std::sync::atomic::Ordering::SeqCst) {
    info!("GlobalUringState: io_uring backend not yet initialized by user. Initializing with default configuration.");
    crate::uring::initialize_uring_backend(crate::uring::UringConfig::default())?;
  } else {
    debug!("GlobalUringState: io_uring backend already initialized.");
  }
  Ok(())
}

/// Round-robin a worker from the pool. Use for `RegisterExternalZmtpFd`; the chosen worker owns
/// the fd for its lifetime, and post-registration control ops must use the sender embedded in the
/// connection object, not this.
pub(crate) fn pick_worker() -> Result<SignalingOpSender, ZmqError> {
  let guard = get_uring_worker_pool_mutex().lock();
  guard.as_ref().map(|pool| pool.pick()).ok_or_else(|| {
    error!("Global UringWorkerPool not available or already taken. Ensure backend is initialized and not shut down.");
    ZmqError::Internal("UringWorkerPool unavailable".into())
  })
}

/// Liveness accessor: confirms the pool exists and returns a sender. Kept for init/liveness
/// checks only (context init, accept-loop startup) — not for fd-targeted ops.
pub(crate) fn get_global_uring_worker_op_tx() -> Result<SignalingOpSender, ZmqError> {
  pick_worker()
}
