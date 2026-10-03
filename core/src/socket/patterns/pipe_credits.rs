use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

/// Send credits for one PUB peer pipe.
///
/// The pipe's capacity is split into credits. A producer takes one before it
/// enqueues a message and the pipe's consumer returns one for every message it
/// removes, so a producer holding a credit always finds a free slot. Every
/// write to a credited pipe must go through a credit.
#[derive(Debug)]
pub(crate) struct PipeCredits {
  available: AtomicUsize,
  closed: AtomicBool,
  drained: Notify,
}

impl PipeCredits {
  pub(crate) fn new(capacity: usize) -> Self {
    Self {
      available: AtomicUsize::new(capacity),
      closed: AtomicBool::new(false),
      drained: Notify::new(),
    }
  }

  pub(crate) fn try_acquire(&self) -> bool {
    let mut current = self.available.load(Ordering::Relaxed);
    loop {
      if current == 0 {
        return false;
      }
      match self
        .available
        .compare_exchange_weak(current, current - 1, Ordering::Acquire, Ordering::Relaxed)
      {
        Ok(_) => return true,
        Err(actual) => current = actual,
      }
    }
  }

  /// Returns `n` credits. Waiters are woken only on the edge from zero.
  pub(crate) fn release(&self, n: usize) {
    if n == 0 {
      return;
    }
    if self.available.fetch_add(n, Ordering::Release) == 0 {
      self.drained.notify_waiters();
    }
  }

  pub(crate) fn has_credit(&self) -> bool {
    self.available.load(Ordering::Acquire) > 0
  }

  pub(crate) fn close(&self) {
    self.closed.store(true, Ordering::Release);
    self.drained.notify_waiters();
  }

  pub(crate) fn is_closed(&self) -> bool {
    self.closed.load(Ordering::Acquire)
  }

  /// A waiter must `enable()` the returned future and then re-check
  /// `has_credit`/`is_closed` before awaiting it, or a release that lands
  /// between its failed acquire and the await is missed.
  pub(crate) fn drained(&self) -> Notified<'_> {
    self.drained.notified()
  }

  #[cfg(test)]
  pub(crate) fn available(&self) -> usize {
    self.available.load(Ordering::Acquire)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;
  use std::time::Duration;

  #[test]
  fn acquire_stops_at_zero_and_release_restores() {
    let credits = PipeCredits::new(2);
    assert!(credits.try_acquire());
    assert!(credits.try_acquire());
    assert!(!credits.try_acquire());
    credits.release(1);
    assert!(credits.try_acquire());
    assert_eq!(credits.available(), 0);
  }

  #[tokio::test]
  async fn release_from_zero_wakes_an_enabled_waiter() {
    let credits = Arc::new(PipeCredits::new(1));
    assert!(credits.try_acquire());

    let waiter_credits = credits.clone();
    let waiter = tokio::spawn(async move {
      let notified = waiter_credits.drained();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if !waiter_credits.has_credit() {
        notified.await;
      }
      waiter_credits.try_acquire()
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    credits.release(1);
    assert!(tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap());
  }

  #[tokio::test]
  async fn close_wakes_an_enabled_waiter() {
    let credits = Arc::new(PipeCredits::new(1));
    assert!(credits.try_acquire());

    let waiter_credits = credits.clone();
    let waiter = tokio::spawn(async move {
      let notified = waiter_credits.drained();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if !waiter_credits.is_closed() {
        notified.await;
      }
      waiter_credits.is_closed()
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    credits.close();
    assert!(tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap());
  }
}
