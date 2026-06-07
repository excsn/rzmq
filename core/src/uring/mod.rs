#![cfg(feature = "io-uring")]

pub mod global_state;

use crate::error::ZmqError;
use crate::io_uring_backend::connection_handler::ProtocolHandlerFactory;
use crate::io_uring_backend::worker::UringWorker;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
      aggressive_spin_limit: 1000,
      cooperative_spin_limit: 500,
      os_yield_limit: 100,
      deep_sleep_fallback: false,
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
}

impl Default for UringConfig {
  fn default() -> Self {
    Self {
      ring_entries: 256,
      default_send_zerocopy: false,
      default_recv_multishot: true,
      default_recv_buffer_count: DEFAULT_IO_URING_RECV_BUFFER_COUNT,
      default_recv_buffer_size: DEFAULT_IO_URING_RECV_BUFFER_SIZE,
      default_send_buffer_count: DEFAULT_IO_URING_SND_BUFFER_COUNT,
      default_send_buffer_size: calculate_required_slot_size(DEFAULT_IO_URING_SND_BUFFER_SIZE, crate::socket::options::DEFAULT_SNDBATCH_COUNT),
      sqpoll_enabled: true,
      sqpoll_idle_ms: 2000,
      polling_strategy: UringPollingStrategy::balanced(),
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
      UringPollingStrategy::Tiered { deep_sleep_fallback: false, .. }
    ));
  }

  #[test]
  fn low_power_is_immediate_sleep() {
    assert_eq!(UringPollingStrategy::low_power(), UringPollingStrategy::ImmediateSleep);
  }

  #[test]
  fn default_config_uses_balanced() {
    let cfg = UringConfig::default();
    assert!(matches!(
      cfg.polling_strategy,
      UringPollingStrategy::Tiered { deep_sleep_fallback: true, .. }
    ));
  }
}

/// Calculates the minimum physical send-buffer slot size needed to hold a fully-framed
/// ZMTP batch without allocation fallback.
///
/// Each ZMTP long frame (payload ≥ 256 bytes) costs 9 bytes of overhead; each short frame
/// costs 2 bytes. This function computes the worst-case total and rounds up to a 4 KB page
/// boundary for kernel/MMU efficiency.
///
/// Example: `calculate_required_slot_size(65_536, 128)` → 69_632 bytes (68 KB).
pub fn calculate_required_slot_size(target_payload_bytes: usize, max_batch_count: usize) -> usize {
  let max_long_frames = std::cmp::min(max_batch_count, target_payload_bytes / 256);
  let long_frame_overhead = max_long_frames * 9;
  let short_frame_overhead = max_batch_count.saturating_sub(max_long_frames) * 2;
  let raw_physical_size = target_payload_bytes + long_frame_overhead + short_frame_overhead;
  const PAGE_SIZE: usize = 4096;
  ((raw_physical_size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE
}

static URING_INIT_RESULT: OnceCell<Result<(), ZmqError>> = OnceCell::new();
pub static URING_BACKEND_INITIALIZED: AtomicBool = AtomicBool::new(false);

const NOT_INITIALIZED_ERROR_MSG: &str = "io_uring backend not initialized.";

pub fn initialize_uring_backend(config: UringConfig) -> Result<(), ZmqError> {
  let init_result_ref: Result<&Result<(), ZmqError>, ZmqError> =
    URING_INIT_RESULT.get_or_try_init(|| -> Result<Result<(), ZmqError>, ZmqError> {
      info!("Initializing global io_uring backend with config: {:?}", config);

      let factories: Vec<Arc<dyn ProtocolHandlerFactory>> = vec![];
      let (signaling_op_tx, worker_join_handle) =
        UringWorker::spawn_with_config(config, factories).map_err(|e| {
          error!("Failed to spawn UringWorker: {}", e);
          e
        })?;

      *global_state::get_uring_worker_op_tx_mutex().lock() = Some(signaling_op_tx);
      *global_state::get_uring_worker_join_handle_mutex().lock() = Some(worker_join_handle);
      debug!("io_uring::initialize: Global UringWorker spawned and its handles stored.");

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

  // Signal the worker to stop by closing its op channel.
  let taken_op_tx = global_state::get_uring_worker_op_tx_mutex().lock().take();
  if taken_op_tx.is_some() {
    debug!("io_uring::shutdown: Signaled UringWorker to stop by closing its op channel.");
  }
  drop(taken_op_tx);

  // Join the worker thread.
  if let Some(worker_handle) = global_state::get_uring_worker_join_handle_mutex().lock().take() {
    debug!("io_uring::shutdown: Joining UringWorker thread...");
    spawn_blocking(move || match worker_handle.join() {
      Ok(Ok(())) => info!("UringWorker thread joined successfully."),
      Ok(Err(e)) => error!("UringWorker thread exited with error: {}", e),
      Err(e) => error!("Failed to join UringWorker thread (panic): {:?}", e),
    })
    .await
    .map_err(|e| ZmqError::Internal(format!("spawn_blocking for worker join failed: {}", e)))?;
  } else {
    warn!("io_uring::shutdown: UringWorker JoinHandle was None. Cannot join.");
  }

  info!("Global io_uring backend shutdown complete.");
  Ok(())
}
