pub(crate) mod macros;

#[cfg(feature = "io-uring")]
pub mod uring;

#[cfg(feature = "diagnostics")]
pub(crate) mod active {
  use std::sync::atomic::AtomicU64;

  pub(crate) struct RZmqMetrics {
    pub(crate) global_drained_msgs: AtomicU64,
    pub(crate) global_msgs_pulled: AtomicU64,
    pub(crate) global_bytes_pulled: AtomicU64,
    pub(crate) last_bytes_sent: AtomicU64,
  }

  pub(crate) static METRICS: RZmqMetrics = RZmqMetrics {
    global_drained_msgs: AtomicU64::new(0),
    global_msgs_pulled: AtomicU64::new(0),
    global_bytes_pulled: AtomicU64::new(0),
    last_bytes_sent: AtomicU64::new(0),
  };

  /// RAII guard that prints a cancellation warning if dropped before `complete()` is called.
  ///
  /// Use via the `cancel_guard!` / `cancel_guard_complete!` macros so call sites
  /// compile to nothing in release builds without the `diagnostics` feature.
  pub(crate) struct CancelGuard {
    pub(crate) label: &'static str,
    pub(crate) completed: bool,
  }

  impl CancelGuard {
    pub(crate) fn new(label: &'static str) -> Self {
      Self { label, completed: false }
    }

    pub(crate) fn complete(&mut self) {
      self.completed = true;
    }
  }

  impl Drop for CancelGuard {
    fn drop(&mut self) {
      if !self.completed {
        println!(
          "[CANCEL-DETECTED pid={}] future dropped mid-flight at: {}",
          std::process::id(),
          self.label,
        );
      }
    }
  }
}
