use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub(crate) struct CountDownLatch {
  count: Arc<AtomicUsize>,
  notify: Arc<Notify>,
}

impl CountDownLatch {
  /// Creates a new latch initialized with the given count.
  pub fn new(count: usize) -> Self {
    // If the initial count is already zero, notify immediately.
    let notify = Arc::new(Notify::new());
    if count == 0 {
      notify.notify_one();
    }
    Self {
      count: Arc::new(AtomicUsize::new(count)),
      notify,
    }
  }

  /// Decrements the count of the latch, releasing all waiting threads if
  /// the count reaches zero.
  pub async fn count_down(&self) {
    // Use AcqRel to synchronize with the acquire load in await_
    if self.count.fetch_sub(1, Ordering::AcqRel) == 1 {
      // We were the last one, notify waiters
      self.notify.notify_waiters();
    }
  }

  /// Causes the current task to wait until the latch has counted down to zero.
  /// If the count is already zero, returns immediately.
  pub async fn await_(&self) {
    // Fast path: Check if already zero. Acquire load synchronizes with AcqRel fetch_sub.
    if self.count.load(Ordering::Acquire) == 0 {
      return;
    }

    // Slow path: Wait for notification
    loop {
      self.notify.notified().await;
      // Spurious wakeup check: Re-verify count after being notified.
      if self.count.load(Ordering::Acquire) == 0 {
        return;
      }
    }
  }

  /// Returns the current count. Primarily for debugging/testing.
  #[allow(dead_code)]
  pub fn get_count(&self) -> usize {
    self.count.load(Ordering::Relaxed)
  }
}
