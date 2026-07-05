#![cfg(feature = "io-uring")]

pub mod global_state;

use crate::error::ZmqError;
use crate::io_uring_backend::connection_handler::ProtocolHandlerFactory;
use crate::io_uring_backend::worker::UringWorker;
use crate::socket::options::calculate_required_slot_size;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use once_cell::sync::OnceCell;
use tokio::task::spawn_blocking;
use tracing::{debug, error, info, warn};

#[cfg(feature = "io-uring")]
pub const DEFAULT_IO_URING_SND_BUFFER_COUNT: usize = 16;
/// Default size (in bytes) for each buffer in the io_uring send buffer pool.
#[cfg(feature = "io-uring")]
pub const DEFAULT_IO_URING_SND_BUFFER_SIZE: usize = 65536;

#[cfg(feature = "io-uring")]
pub const DEFAULT_IO_URING_RECV_BUFFER_COUNT: usize = 16;
/// Default size (in bytes) for each buffer in the io_uring multishot receive pool.
#[cfg(feature = "io-uring")]
pub const DEFAULT_IO_URING_RECV_BUFFER_SIZE: usize = 65536;

/// Controls how the `UringWorker` thread behaves when there is no immediate work.
///
/// The tiered strategy inserts user-space spinning before entering a blocking kernel sleep,
/// trading CPU cycles for reduced wakeup latency. During all spin phases, `worker_asleep`
/// remains `false`, so `UringStream` never fires an expensive EventFD write.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UringPollingStrategy {
  /// Skip all user-space spinning and go directly to a blocking `io_uring_enter` wait.
  /// Lowest CPU usage; highest idle latency.
  ImmediateSleep,
  /// Execute sequential spin phases before sleeping.
  Tiered {
    /// Iterations of tight CPU spin with no yield hints (highest responsiveness).
    aggressive_spin_limit: u32,
    /// Iterations with `std::hint::spin_loop()` — notifies the CPU pipeline to relax.
    cooperative_spin_limit: u32,
    /// Iterations with `std::thread::yield_now()` — cooperates with the OS scheduler.
    os_yield_limit: u32,
    /// If `false`, never enter deep sleep (pins CPU at 100%); if `true`, fall through to
    /// `submit_with_args` after all spin phases exhaust.
    deep_sleep_fallback: bool,
  },
}

impl UringPollingStrategy {
  /// No spinning. Best for power-constrained or oversubscribed environments.
  pub fn low_power() -> Self {
    Self::ImmediateSleep
  }

  /// Moderate spinning before sleeping. Good general-purpose default.
  pub fn balanced() -> Self {
    Self::Tiered {
      aggressive_spin_limit: 64,
      cooperative_spin_limit: 32,
      os_yield_limit: 16,
      deep_sleep_fallback: true,
    }
  }

  /// Maximum spinning, no kernel sleep. Best for sustained high-frequency bursts.
  ///
  /// **Warning**: pins the `UringWorker` OS thread at 100% CPU.
  pub fn ultra_low_latency() -> Self {
    Self::Tiered {
      aggressive_spin_limit: 10000, // Pure register spin
      cooperative_spin_limit: 5000, // CPU pipeline pause hint (PAUSE instruction)
      os_yield_limit: 0,            // Change from 100 to 0 (ELIMINATE SYSCALL STORM)
      deep_sleep_fallback: false,   // Stay in user-space
    }
  }
}

#[derive(Debug, Clone, Copy)]
pub struct UringConfig {
  pub ring_entries: u32,
  pub default_send_zerocopy: bool,
  pub default_recv_multishot: bool,
  pub default_recv_buffer_count: usize,
  pub default_recv_buffer_size: usize,
  pub default_send_buffer_count: usize,
  pub default_send_buffer_size: usize,
  /// Enable `IORING_SETUP_SQPOLL`: the kernel spawns a dedicated thread that polls
  /// the submission queue, eliminating `io_uring_enter` syscalls under load.
  /// Requires `CAP_SYS_ADMIN`/`CAP_SYS_NICE` or Linux ≥ 5.11 for unprivileged use.
  /// Falls back to non-SQPOLL mode on `EPERM`/`EACCES`.
  pub sqpoll_enabled: bool,
  /// Milliseconds of inactivity before the SQPOLL kernel thread sleeps.
  /// After sleeping, one wakeup syscall is needed before polling resumes.
  pub sqpoll_idle_ms: u32,
  /// Controls the user-space spinning behavior when the worker thread is idle.
  pub polling_strategy: UringPollingStrategy,
  /// Number of `UringWorker` threads (each with its own ring, buffer pools, and fd set).
  /// Connections are assigned round-robin at registration and stay on their worker for life.
  /// Note: buffer pools are registered per ring, so pinned memory scales with this value,
  /// and with `sqpoll_enabled` each worker gets its own kernel poll thread.
  pub num_workers: usize,
}

/// Cores-based default worker count: `available_parallelism() - 2`, clamped to `[1, 8]`.
pub fn default_uring_num_workers() -> usize {
  std::thread::available_parallelism()
    .map(|n| n.get())
    .unwrap_or(1)
    .saturating_sub(2)
    .clamp(1, 8)
}

impl Default for UringConfig {
  fn default() -> Self {
    Self {
      ring_entries: 256,
      default_send_zerocopy: false,
      default_recv_multishot: true,
      default_recv_buffer_count: DEFAULT_IO_URING_RECV_BUFFER_COUNT,
      default_recv_buffer_size: calculate_required_slot_size(
        DEFAULT_IO_URING_RECV_BUFFER_SIZE,
        crate::socket::options::DEFAULT_RCVBATCH_COUNT,
      ),
      default_send_buffer_count: DEFAULT_IO_URING_SND_BUFFER_COUNT,
      default_send_buffer_size: calculate_required_slot_size(
        DEFAULT_IO_URING_SND_BUFFER_SIZE,
        crate::socket::options::DEFAULT_SNDBATCH_COUNT,
      ),
      sqpoll_enabled: false,
      sqpoll_idle_ms: 1000,
      polling_strategy: UringPollingStrategy::balanced(),
      num_workers: default_uring_num_workers(),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn balanced_preset_fields() {
    assert!(matches!(
      UringPollingStrategy::balanced(),
      UringPollingStrategy::Tiered {
        aggressive_spin_limit: 64,
        cooperative_spin_limit: 32,
        os_yield_limit: 16,
        deep_sleep_fallback: true,
      }
    ));
  }

  #[test]
  fn ultra_low_latency_never_sleeps() {
    assert!(matches!(
      UringPollingStrategy::ultra_low_latency(),
      UringPollingStrategy::Tiered {
        deep_sleep_fallback: false,
        ..
      }
    ));
  }

  #[test]
  fn low_power_is_immediate_sleep() {
    assert_eq!(
      UringPollingStrategy::low_power(),
      UringPollingStrategy::ImmediateSleep
    );
  }

  #[test]
  fn default_config_uses_balanced() {
    let cfg = UringConfig::default();
    assert!(matches!(
      cfg.polling_strategy,
      UringPollingStrategy::Tiered {
        deep_sleep_fallback: true,
        ..
      }
    ));
  }
}

static URING_INIT_RESULT: OnceCell<Result<(), ZmqError>> = OnceCell::new();
pub static URING_BACKEND_INITIALIZED: AtomicBool = AtomicBool::new(false);

const NOT_INITIALIZED_ERROR_MSG: &str = "io_uring backend not initialized.";

pub fn initialize_uring_backend(config: UringConfig) -> Result<(), ZmqError> {
  let init_result_ref: Result<&Result<(), ZmqError>, ZmqError> =
    URING_INIT_RESULT.get_or_try_init(|| -> Result<Result<(), ZmqError>, ZmqError> {
      info!(
        "Initializing global io_uring backend with config: {:?}",
        config
      );

      let num_workers = config.num_workers.max(1);
      let mut senders = Vec::with_capacity(num_workers);
      let mut handles = Vec::with_capacity(num_workers);
      for worker_idx in 0..num_workers {
        let factories: Vec<Arc<dyn ProtocolHandlerFactory>> = vec![];
        match UringWorker::spawn_with_config(config, factories) {
          Ok((signaling_op_tx, worker_join_handle)) => {
            senders.push(signaling_op_tx);
            handles.push(worker_join_handle);
          }
          Err(e) => {
            // Rings and their registered buffer pools are pinned memory; later workers can
            // hit limits (RLIMIT_MEMLOCK / memcg ENOMEM) that the first did not. A smaller
            // pool beats no backend — degrade to the workers that spawned.
            if senders.is_empty() {
              error!("Failed to spawn UringWorker 1/{}: {}", num_workers, e);
              return Err(e);
            }
            warn!(
              "Failed to spawn UringWorker {}/{}: {}. Continuing with {} worker(s).",
              worker_idx + 1,
              num_workers,
              e,
              senders.len()
            );
            break;
          }
        }
      }

      let spawned = senders.len();
      *global_state::get_uring_worker_pool_mutex().lock() =
        Some(global_state::UringWorkerPool::new(senders));
      *global_state::get_uring_worker_join_handles_mutex().lock() = handles;
      debug!(
        "io_uring::initialize: {} UringWorker(s) spawned and their handles stored.",
        spawned
      );

      URING_BACKEND_INITIALIZED.store(true, Ordering::SeqCst);
      info!("Global io_uring backend successfully initialized.");

      Ok(Ok(()))
    });

  match init_result_ref {
    Ok(stored_result_ref) => (*stored_result_ref).clone(),
    Err(init_error) => Err(init_error.clone()),
  }
}

pub async fn shutdown_uring_backend() -> Result<(), ZmqError> {
  if URING_INIT_RESULT.get().is_none() || !URING_BACKEND_INITIALIZED.load(Ordering::SeqCst) {
    warn!("{}", NOT_INITIALIZED_ERROR_MSG);
    return Ok(());
  }

  if !URING_BACKEND_INITIALIZED.swap(false, Ordering::SeqCst) {
    warn!("io_uring backend shutdown already in progress or completed.");
    return Ok(());
  }

  info!("Shutting down global io_uring backend...");

  // Signal all workers to stop. Connection objects (and each worker's own self_op_tx) hold
  // sender clones, so channel closure cannot be the drain trigger — send the explicit
  // ShutdownWorker op instead. The send also wakes a sleeping worker via its eventfd.
  let taken_pool = global_state::get_uring_worker_pool_mutex().lock().take();
  if let Some(pool) = taken_pool {
    let mut senders = pool.into_senders();
    let n = senders.len();
    for sender in senders.iter_mut() {
      if let Err(e) = sender.send(crate::io_uring_backend::ops::UringOpRequest::ShutdownWorker).await {
        warn!("io_uring::shutdown: ShutdownWorker send failed (worker already gone?): {}", e);
      }
    }
    drop(senders);
    debug!(
      "io_uring::shutdown: Sent ShutdownWorker to {} UringWorker(s) and dropped pool senders.",
      n
    );
  }

  // Join all worker threads.
  let worker_handles: Vec<_> =
    std::mem::take(&mut *global_state::get_uring_worker_join_handles_mutex().lock());
  if worker_handles.is_empty() {
    warn!("io_uring::shutdown: No UringWorker JoinHandles found. Cannot join.");
  } else {
    debug!(
      "io_uring::shutdown: Joining {} UringWorker thread(s)...",
      worker_handles.len()
    );
    spawn_blocking(move || {
      for (idx, worker_handle) in worker_handles.into_iter().enumerate() {
        match worker_handle.join() {
          Ok(Ok(())) => info!("UringWorker thread {} joined successfully.", idx),
          Ok(Err(e)) => error!("UringWorker thread {} exited with error: {}", idx, e),
          Err(e) => error!("Failed to join UringWorker thread {} (panic): {:?}", idx, e),
        }
      }
    })
    .await
    .map_err(|e| ZmqError::Internal(format!("spawn_blocking for worker join failed: {}", e)))?;
  }

  info!("Global io_uring backend shutdown complete.");
  Ok(())
}
