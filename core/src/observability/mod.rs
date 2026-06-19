pub(crate) mod macros;

#[cfg(feature = "io-uring")]
pub mod uring;

#[cfg(any(debug_assertions, feature = "diagnostics"))]
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
}
