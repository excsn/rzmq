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

#[cfg(test)]
mod additional_latch_tests {
  use super::*;
  use std::sync::Arc;
  use std::time::Duration;
  use tokio::time::timeout;

  #[tokio::test]
  async fn test_latch_zero_initial_count() {
    let latch = CountDownLatch::new(0);
    let res = timeout(Duration::from_millis(50), latch.await_()).await;
    assert!(res.is_ok(), "Zero-initialized latch should not block");
  }

  #[tokio::test]
  async fn test_latch_countdown_synchronization() {
    let latch = Arc::new(CountDownLatch::new(3));

    let latch_clone = latch.clone();
    let mut waiter = tokio::spawn(async move {
      latch_clone.await_().await;
    });

    // Count = 3: waiter should still be blocked.
    assert!(
      timeout(Duration::from_millis(50), &mut waiter).await.is_err(),
      "Waiter should be blocked while count > 0"
    );

    latch.count_down().await; // count → 2
    latch.count_down().await; // count → 1

    // Count = 1: still blocked.
    assert!(
      timeout(Duration::from_millis(50), &mut waiter).await.is_err(),
      "Waiter should still be blocked at count = 1"
    );

    latch.count_down().await; // count → 0 → notifies waiters

    let res = timeout(Duration::from_millis(200), waiter).await;
    assert!(
      res.is_ok(),
      "Waiter should have resolved after final countdown"
    );
  }

  #[tokio::test]
  async fn test_latch_concurrent_countdown() {
    let count = 10;
    let latch = Arc::new(CountDownLatch::new(count));
    let mut tasks = vec![];

    for _ in 0..count {
      let l = latch.clone();
      tasks.push(tokio::spawn(async move {
        l.count_down().await;
      }));
    }

    let res = timeout(Duration::from_millis(200), latch.await_()).await;
    assert!(res.is_ok(), "Concurrent countdown failed to unblock the latch");

    for t in tasks {
      assert!(t.await.is_ok());
    }
  }
}
