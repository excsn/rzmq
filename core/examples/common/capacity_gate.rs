use futures_intrusive::sync::{GenericSemaphoreReleaser, Semaphore};
use parking_lot::RawMutex;
use std::{future::Future, sync::Arc};

// New struct for the owned permit. It holds an Arc to the gate,
// making it 'static.
#[derive(Debug)]
pub(crate) struct OwnedPermitGuard {
    gate: Arc<CapacityGate>,
}

// When the owned guard is dropped, it releases the permit.
impl Drop for OwnedPermitGuard {
    fn drop(&mut self) {
        self.gate.release();
    }
}

#[derive(Debug)]
pub(crate) struct CapacityGate {
  capacity: usize,
  semaphore: Semaphore,
}

pub(crate) type PermitGuard<'a> = GenericSemaphoreReleaser<'a, RawMutex>;

impl CapacityGate {
  pub fn new(capacity: usize) -> Self {
    Self {
      capacity,
      semaphore: Semaphore::new(true, capacity),
    }
  }

  /// Acquires a permit, returning a future that resolves to a RAII guard.
  pub fn acquire(&self) -> impl Future<Output = PermitGuard<'_>> {
    self.semaphore.acquire(1)
  }

  /// NEW METHOD: Acquires an owned permit.
  /// The returned future and the resulting guard are 'static.
  pub fn acquire_owned(self: Arc<Self>) -> impl Future<Output = OwnedPermitGuard> {
    async move {
      // Await the underlying semaphore using a temporary borrow.
      let _temporary_guard = self.semaphore.acquire(1).await;
      // Forget the temporary guard so it doesn't release the permit.
      std::mem::forget(_temporary_guard);
      // Return our new owned guard, which will release the permit on drop.
      OwnedPermitGuard { gate: self }
    }
  }

  /// Releases a permit back to the gate.
  /// This is made `pub(crate)` to be visible to `task_queue.rs`.
  pub fn release(&self) {
    self.semaphore.release(1);
  }

  /// Returns the number of currently available permits.
  /// The `#[cfg(test)]` attribute is removed so it can be used by non-test code
  /// like the Debug implementation in `task_queue.rs`.
  pub fn get_permits(&self) -> usize {
    self.semaphore.permits()
  }

  /// Returns the total capacity of the gate.
  pub fn capacity(&self) -> usize {
    self.capacity
  }
}

// The test module remains unchanged.
#[cfg(test)]
mod tests {
  use super::*;
  use std::{sync::Arc, time::Duration};

  #[tokio::test]
  async fn new_gate_has_correct_initial_permits() {
    let gate = CapacityGate::new(5);
    assert_eq!(gate.capacity(), 5);
    assert_eq!(gate.get_permits(), 5);
  }

  #[tokio::test]
  async fn acquire_and_release_on_drop() {
    let gate = CapacityGate::new(2);
    let p1 = gate.acquire().await;
    let p2 = gate.acquire().await;
    assert_eq!(gate.get_permits(), 0);
    drop(p1);
    assert_eq!(gate.get_permits(), 1);
    drop(p2);
    assert_eq!(gate.get_permits(), 2);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn concurrent_acquire() {
    const CAPACITY: usize = 4;
    const NUM_TASKS: usize = 64;

    let gate = Arc::new(CapacityGate::new(CAPACITY));
    let mut handles = Vec::new();

    for i in 0..NUM_TASKS {
      let gate_clone = gate.clone();
      let task = async move {
        let _permit = gate_clone.acquire().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
      };
      handles.push(tokio::spawn(task));
    }

    for handle in handles {
      handle.await.unwrap();
    }

    assert_eq!(gate.get_permits(), CAPACITY);
  }
}
