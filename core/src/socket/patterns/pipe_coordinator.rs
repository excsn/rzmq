use parking_lot::RwLock;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ZmqError;

type PipeId = usize;

#[derive(Debug)]
pub struct WritePipeCoordinator {
  pipe_semaphores: RwLock<HashMap<PipeId, Arc<Semaphore>>>,
}

impl WritePipeCoordinator {
  pub fn new() -> Self {
    Self {
      pipe_semaphores: RwLock::new(HashMap::new()),
    }
  }

  // Called when a pipe is attached to the RouterSocket
  pub async fn add_pipe(&self, pipe_id: PipeId) {
    let mut semaphores_guard = self.pipe_semaphores.write();
    semaphores_guard
      .entry(pipe_id)
      .or_insert_with(|| Arc::new(Semaphore::new(1)));
    tracing::trace!(pipe_id, "WritePipeCoordinator: Added/Ensured semaphore for pipe.");
  }

  // Called when a pipe is detached
  pub async fn remove_pipe(&self, pipe_id: PipeId) -> Option<Arc<Semaphore>> {
    let mut semaphores_guard = self.pipe_semaphores.write();
    let removed = semaphores_guard.remove(&pipe_id);
    if removed.is_some() {
      tracing::trace!(pipe_id, "WritePipeCoordinator: Removed semaphore for pipe.");
    }
    removed
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
    let semaphore_arc = {
      let semaphores_guard = self.pipe_semaphores.read();
      semaphores_guard.get(&pipe_id).cloned()
    };

    match semaphore_arc {
      Some(semaphore) => {
        if let Some(duration) = timeout_opt {
          if duration.is_zero() {
            // Handle ZMQ's non-blocking send case (SNDTIMEO=0)
            match semaphore.try_acquire_owned() {
              Ok(permit) => Ok(permit),
              Err(_try_acquire_error) => {
                // Permit not available
                tracing::debug!(
                  pipe_id,
                  "WritePipeCoordinator: try_acquire_owned failed (ResourceLimitReached)."
                );
                Err(ZmqError::ResourceLimitReached)
              }
            }
          } else {
            // Timed wait
            match tokio::time::timeout(duration, semaphore.acquire_owned()).await {
              Ok(Ok(permit)) => Ok(permit),
              Ok(Err(_closed_err)) => {
                tracing::error!(pipe_id, "WritePipeCoordinator: Semaphore closed unexpectedly.");
                Err(ZmqError::Internal("Send semaphore closed".into()))
              }
              Err(_timeout_elapsed) => {
                tracing::debug!(
                  pipe_id,
                  ?duration,
                  "WritePipeCoordinator: Timeout acquiring send permit."
                );
                Err(ZmqError::Timeout)
              }
            }
          }
        } else {
          // Infinite wait
          match semaphore.acquire_owned().await {
            Ok(permit) => Ok(permit),
            Err(_closed_err) => {
              tracing::error!(
                pipe_id,
                "WritePipeCoordinator: Semaphore closed unexpectedly (infinite wait)."
              );
              Err(ZmqError::Internal("Send semaphore closed".into()))
            }
          }
        }
      }
      None => {
        tracing::error!(
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
