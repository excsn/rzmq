use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ZmqError;

type PipeId = usize;

#[derive(Debug)]
pub(crate) struct PipeState {
  semaphore: Arc<Semaphore>,
  is_closing: Arc<AtomicBool>,
}

#[derive(Debug)]
pub struct WritePipeCoordinator {
  pipe_states: RwLock<HashMap<PipeId, Arc<PipeState>>>,
}

impl WritePipeCoordinator {
  pub fn new() -> Self {
    Self {
      pipe_states: RwLock::new(HashMap::new()),
    }
  }

  // Called when a pipe is attached to the RouterSocket
  pub async fn add_pipe(&self, pipe_id: PipeId) {
    let mut states_guard = self.pipe_states.write();
    states_guard.entry(pipe_id).or_insert_with(|| {
      Arc::new(PipeState {
        semaphore: Arc::new(Semaphore::new(1)),
        is_closing: Arc::new(AtomicBool::new(false)),
      })
    });

    tracing::trace!(
      pipe_id,
      "WritePipeCoordinator: Added/Ensured semaphore for pipe."
    );
  }

  // Called when a pipe is detached
  pub async fn remove_pipe(&self, pipe_id: PipeId) -> Option<Arc<PipeState>> {
    let mut states_guard = self.pipe_states.write();
    let removed_state = states_guard.remove(&pipe_id);
    if let Some(ref state) = removed_state {
      // Mark as closing *before* closing the semaphore.
      state.is_closing.store(true, Ordering::SeqCst);
      // Close the semaphore to wake up any waiters immediately.
      state.semaphore.close();
      tracing::trace!(
        pipe_id,
        "WritePipeCoordinator: Marked pipe as closing and removed."
      );
    }
    removed_state
  }

  // Acquires a send permit for a specific pipe.
  // This will block until the permit is available for that pipe.
  // Returns an OwnedSemaphorePermit which must be held while sending to that pipe
  // and dropped to release the lock.
  pub async fn acquire_send_permit(
    &self,
    pipe_id: PipeId,
    timeout_opt: Option<Duration>,
  ) -> Result<OwnedSemaphorePermit, ZmqError> {
    let pipe_state_arc = {
      let states_guard = self.pipe_states.read();
      states_guard.get(&pipe_id).cloned()
    };

    match pipe_state_arc {
      Some(state) => {
        // Check if the pipe is already marked for closing.
        if state.is_closing.load(Ordering::Relaxed) {
          return Err(ZmqError::HostUnreachable(
            "Target pipe is closing or has been detached".into(),
          ));
        }

        let permit_result = if let Some(duration) = timeout_opt {
          if duration.is_zero() {
            // Non-blocking attempt
            return match state.semaphore.clone().try_acquire_owned() {
              Ok(permit) => Ok(permit),
              Err(_) => Err(ZmqError::ResourceLimitReached),
            };
          }
          // Optimistic try before registering a timer
          match state.semaphore.clone().try_acquire_owned() {
            Ok(permit) => return Ok(permit),
            Err(_) => {}
          }
          // Timed wait
          tokio::time::timeout(duration, state.semaphore.clone().acquire_owned()).await
        } else {
          // Infinite wait
          Ok(state.semaphore.clone().acquire_owned().await)
        };

        match permit_result {
          // Successfully acquired
          Ok(Ok(permit)) => Ok(permit),
          // Semaphore was closed while waiting
          Ok(Err(_closed_err)) => {
            tracing::debug!(
              pipe_id,
              "Woke from acquire on a closed semaphore; peer detached."
            );
            Err(ZmqError::HostUnreachable(
              "Target pipe was detached during send operation".into(),
            ))
          }
          // Timed out
          Err(_timeout_elapsed) => {
            tracing::debug!(
              pipe_id,
              ?timeout_opt,
              "WritePipeCoordinator: Timeout acquiring send permit."
            );
            Err(ZmqError::Timeout)
          }
        }
      }
      None => {
        tracing::warn!(
          pipe_id,
          "WritePipeCoordinator: Attempted to acquire permit for unknown/detached pipe."
        );
        Err(ZmqError::HostUnreachable(
          "Target pipe for send not available or recently detached".into(),
        ))
      }
    }
  }
}

#[cfg(test)]
mod coordinator_tests {
  use super::*;
  use std::sync::Arc;
  use std::time::Duration;
  use tokio::time::sleep;

  #[tokio::test]
  async fn test_concurrent_acquisitions() {
    let coordinator = Arc::new(WritePipeCoordinator::new());
    let pipe_id: PipeId = 42;
    coordinator.add_pipe(pipe_id).await;

    let mut tasks = vec![];
    for _ in 0..5 {
      let coord_clone = coordinator.clone();
      tasks.push(tokio::spawn(async move {
        let _permit = coord_clone
          .acquire_send_permit(pipe_id, Some(Duration::from_secs(1)))
          .await
          .expect("Should successfully acquire permit");
        sleep(Duration::from_millis(5)).await;
      }));
    }

    for task in tasks {
      task.await.expect("Task finished without panicking");
    }
  }

  #[tokio::test]
  async fn test_pipe_detachment_unblocks_waiters() {
    let coordinator = Arc::new(WritePipeCoordinator::new());
    let pipe_id: PipeId = 99;
    coordinator.add_pipe(pipe_id).await;

    // Hold the sole permit to block all subsequent acquisitions.
    let _holder_permit = coordinator
      .acquire_send_permit(pipe_id, None)
      .await
      .expect("First permit acquisition should succeed");

    // Spawn a task that will block indefinitely waiting for the permit.
    let coord_clone = coordinator.clone();
    let waiter_task = tokio::spawn(async move {
      coord_clone.acquire_send_permit(pipe_id, None).await
    });

    // Yield so the waiter task has a chance to block on the semaphore.
    sleep(Duration::from_millis(10)).await;

    // Remove the pipe — closes the semaphore and wakes all waiters.
    coordinator.remove_pipe(pipe_id).await;

    let res = waiter_task.await.expect("Waiter task panicked");
    assert!(
      matches!(res, Err(ZmqError::HostUnreachable(_))),
      "Expected Err(HostUnreachable) due to pipe detachment, got {:?}",
      res
    );
  }
}
