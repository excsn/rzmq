// src/runtime/waitgroup.rs

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// An asynchronous WaitGroup, similar to Go's `sync.WaitGroup`.
///
/// Allows multiple tasks to register themselves with the group (`add`)
/// and signal completion (`done`). Another task can wait (`wait`) until
/// all registered tasks have completed (counter returns to zero).
#[derive(Debug, Clone)]
pub(crate) struct WaitGroup {
  count: Arc<AtomicUsize>,
  notify_on_zero: Arc<Notify>,
}

impl WaitGroup {
  /// Creates a new WaitGroup with an initial count of zero.
  pub fn new() -> Self {
    Self {
      count: Arc::new(AtomicUsize::new(0)),
      notify_on_zero: Arc::new(Notify::new()),
    }
  }

  /// Adds a delta to the WaitGroup counter.
  ///
  /// Typically used with `add(1)` when starting a new task associated
  /// with the group.
  pub fn add(&self, delta: usize) {
    if delta == 0 {
      return;
    }
    // Increment the counter. Relaxed ordering is sufficient for increments.
    let old_count = self.count.fetch_add(delta, Ordering::Relaxed);

    // If the count *was* zero before adding, it means waiters might have been
    // notified spuriously or the latch was already released. We need to ensure
    // that subsequent `wait` calls will block correctly. `Notify` handles this:
    // If notify_waiters() was called when count was 0, new permits are created.
    // If we increment from 0, subsequent waits will correctly block until notified again.
    // No specific action needed here regarding the Notify state reset.
    if old_count == 0 {
      // Optional: log when transitioning from zero
      tracing::trace!(delta, new_count = delta, "WaitGroup count increased from zero");
    }
  }

  /// Decrements the WaitGroup counter by one.
  ///
  /// Typically called when a task associated with the group completes.
  /// If the counter reaches zero, all tasks waiting on `wait()` are notified.
  ///
  /// Panics if the counter would drop below zero.
  pub fn done(&self) {
    // Use AcqRel ordering:
    // - Acquire ensures that subsequent reads (in wait) see the decremented value.
    // - Release ensures that operations before done() are visible to tasks released by wait().
    let old_count = self.count.fetch_sub(1, Ordering::AcqRel);

    if old_count == 0 {
      // This should not happen if add/done are used correctly.
      // Restore count and panic or log error.
      self.count.fetch_add(1, Ordering::Relaxed); // Try to restore
      panic!("WaitGroup::done() called when count was already zero!");
    } else if old_count == 1 {
      // We decremented the counter to zero. Notify waiters.
      self.notify_on_zero.notify_waiters();
      tracing::trace!("WaitGroup count reached zero, notifying waiters");
    }
    // If old_count > 1, do nothing further.
  }

  /// Waits asynchronously until the WaitGroup counter becomes zero.
  ///
  /// If the counter is already zero when called, returns immediately.
  pub async fn wait(&self) {
    // Fast path: Check if already zero.
    // Acquire load synchronizes with the AcqRel fetch_sub in done().
    if self.count.load(Ordering::Acquire) == 0 {
      tracing::trace!("WaitGroup::wait() called when count is already zero");
      return;
    }

    // Slow path: Wait for notification.
    loop {
      // Wait until notified. notified() consumes a permit.
      self.notify_on_zero.notified().await;

      // Check count again after notification (spurious wakeup or race check).
      if self.count.load(Ordering::Acquire) == 0 {
        tracing::trace!("WaitGroup::wait() released after notification");
        return;
      }
      tracing::trace!("WaitGroup::wait() woke, but count is non-zero; re-waiting");
      // If count is still non-zero, loop and wait again.
    }
  }

  /// Returns the current count. Primarily for debugging/testing.
  #[allow(dead_code)]
  pub fn get_count(&self) -> usize {
    self.count.load(Ordering::Relaxed)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::time::Duration;
  use tokio::time::timeout;

  #[rzmq::main]
  async fn test_waitgroup_add_done_wait() {
    let wg = WaitGroup::new();
    assert_eq!(wg.get_count(), 0);

    wg.add(2); // Register two tasks
    assert_eq!(wg.get_count(), 2);

    // Task 1: completes after 10ms
    let wg_clone1 = wg.clone();
    let task1 = tokio::spawn(async move {
      tokio::time::sleep(Duration::from_millis(10)).await;
      wg_clone1.done();
      "Task 1 Done"
    });

    // Barrier to control Task 2
    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();

    // Task 2: waits on barrier before completing
    let wg_clone2 = wg.clone();
    let task2 = tokio::spawn(async move {
      notify_clone.notified().await;
      wg_clone2.done();
      "Task 2 Done"
    });

    // WaitGroup wait task
    let wg_clone_wait = wg.clone();
    let mut wait_task = tokio::spawn(async move {
      wg_clone_wait.wait().await;
      "Wait Finished"
    });

    // Allow only Task 1 to complete
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(wg.get_count(), 1);

    // Ensure wait_task is still blocked
    assert!(
      timeout(Duration::from_millis(5), &mut wait_task).await.is_err(),
      "Wait task should not have finished yet"
    );

    // Release Task 2
    notify.notify_one();

    // Collect results
    let task1_res = task1.await.unwrap();
    let task2_res = task2.await.unwrap();
    let wait_res = timeout(Duration::from_millis(50), wait_task).await;

    // Final assertions
    assert_eq!(task1_res, "Task 1 Done");
    assert_eq!(task2_res, "Task 2 Done");
    assert_eq!(wg.get_count(), 0);
    assert!(wait_res.is_ok(), "Wait task should finish after task 2 completes");
    assert_eq!(wait_res.unwrap().unwrap(), "Wait Finished");
  }

  #[rzmq::main]
  async fn test_waitgroup_wait_on_zero() {
    let wg = WaitGroup::new();
    let start = tokio::time::Instant::now();
    wg.wait().await; // Should return immediately
    assert!(start.elapsed() < Duration::from_millis(10));
    assert_eq!(wg.get_count(), 0);
  }

  #[rzmq::main]
  async fn test_waitgroup_add_after_wait_starts() {
    let wg = WaitGroup::new();
    wg.add(1); // Count is 1

    let wg_clone_wait = wg.clone();
    let mut wait_task = tokio::spawn(async move {
      // <<< Marked mut >>>
      wg_clone_wait.wait().await; // Should block
    });

    tokio::time::sleep(Duration::from_millis(10)).await; // Let wait start

    wg.add(1); // Count is 2
    assert_eq!(wg.get_count(), 2);

    wg.done(); // Count is 1
    assert_eq!(wg.get_count(), 1);
    // Wait should still be blocked
    // <<< MODIFIED START: Correct check >>>
    assert!(
      timeout(Duration::from_millis(10), &mut wait_task).await.is_err(),
      "Wait task should still be blocked after one done()"
    );
    // <<< MODIFIED END >>>

    wg.done(); // Count is 0
    assert_eq!(wg.get_count(), 0);

    // Wait should now complete
    assert!(
      timeout(Duration::from_millis(50), wait_task).await.is_ok(),
      "Wait task should complete after second done()"
    );
  }

  #[rzmq::main]
  #[should_panic]
  async fn test_waitgroup_done_panic_on_zero() {
    let wg = WaitGroup::new();
    wg.done(); // Should panic
  }
}
