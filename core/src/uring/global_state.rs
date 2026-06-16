#![cfg(feature = "io-uring")]

use crate::io_uring_backend::signaling_op_sender::SignalingOpSender;
use crate::{error::ZmqError, uring::URING_BACKEND_INITIALIZED};

use std::thread::JoinHandle as StdThreadJoinHandle;

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tracing::{debug, error, info};

static URING_WORKER_OP_TX: OnceCell<Mutex<Option<SignalingOpSender>>> = OnceCell::new();
static URING_WORKER_JOIN_HANDLE: OnceCell<
  Mutex<Option<StdThreadJoinHandle<Result<(), ZmqError>>>>,
> = OnceCell::new();

#[doc(hidden)]
pub(crate) fn get_uring_worker_op_tx_mutex() -> &'static Mutex<Option<SignalingOpSender>> {
  URING_WORKER_OP_TX.get_or_init(Default::default)
}

#[doc(hidden)]
pub(crate) fn get_uring_worker_join_handle_mutex(
) -> &'static Mutex<Option<StdThreadJoinHandle<Result<(), ZmqError>>>> {
  URING_WORKER_JOIN_HANDLE.get_or_init(Default::default)
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

pub(crate) fn get_global_uring_worker_op_tx() -> Result<SignalingOpSender, ZmqError> {
  let guard = get_uring_worker_op_tx_mutex().lock();
  guard.as_ref().cloned().ok_or_else(|| {
    error!("Global UringWorker OP_TX not available or already taken. Ensure backend is initialized and not shut down.");
    ZmqError::Internal("UringWorker OP_TX unavailable".into())
  })
}
