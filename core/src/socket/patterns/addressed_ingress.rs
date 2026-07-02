use crate::message::FrameBatch;
use crate::ZmqError;
use crate::socket::patterns::ready_pipe_queue::{PipeMessageSender, ReadyPipeQueue};

use parking_lot::Mutex;
use std::collections::VecDeque;

/// Consumer-local cache of already-popped `(pipe_id, batch)` entries plus the
/// reusable drain buffer handed to `try_pop_batch`.
struct AddressedCache {
  entries: VecDeque<(usize, FrameBatch)>,
  scratch: Vec<FrameBatch>,
}

pub(crate) struct AddressedIngressEngine {
  queue: ReadyPipeQueue<FrameBatch>,
  /// Cache fill target per top-up (rcvbatch_count).
  batch_max: usize,
  cache: Mutex<AddressedCache>,
}

impl AddressedIngressEngine {
  pub fn new(activation_capacity: usize, batch_max: usize) -> Self {
    Self {
      queue: ReadyPipeQueue::new(activation_capacity),
      batch_max: batch_max.max(1),
      cache: Mutex::new(AddressedCache {
        entries: VecDeque::new(),
        scratch: Vec::new(),
      }),
    }
  }

  pub fn register_pipe(&self, pipe_id: usize, capacity: usize, drain_delta: usize) -> PipeMessageSender {
    let sender = self.queue.register_pipe(pipe_id, capacity, drain_delta);
    PipeMessageSender::DirectAddressed { sender }
  }

  pub fn deregister_pipe(&self, pipe_id: usize) {
    self.queue.deregister_pipe(pipe_id);
    // Drop only the dead pipe's cached messages; other peers' entries survive.
    self.cache.lock().entries.retain(|(pid, _)| *pid != pipe_id);
  }

  pub fn close(&self) {
    self.queue.close();
    self.cache.lock().entries.clear();
  }

  fn pop_cached(&self) -> Option<(usize, FrameBatch)> {
    self.cache.lock().entries.pop_front()
  }

  /// Opportunistically drains whatever is immediately ready into the cache,
  /// up to `batch_max` entries. Fully synchronous and performed UNDER the
  /// cache lock: with concurrent consumers, fills serialize in token order,
  /// so the cache sequence preserves per-pipe FIFO (a drain outside the lock
  /// could commit a pipe's later messages ahead of its earlier ones).
  fn top_up_cache(&self) {
    let mut cache = self.cache.lock();
    while cache.entries.len() < self.batch_max {
      let budget = self.batch_max - cache.entries.len();
      let mut scratch = std::mem::take(&mut cache.scratch);
      match self.queue.try_pop_batch(&mut scratch, budget) {
        Some((pipe_id, _n)) => {
          cache.entries.extend(scratch.drain(..).map(|b| (pipe_id, b)));
          cache.scratch = scratch;
        }
        None => {
          cache.scratch = scratch;
          break;
        }
      }
    }
  }

  /// Blocking pop of the next ready `(pipe_id, batch)`, with no timeout.
  ///
  /// Cancel-safe: the head message comes from a single `ReadyPipeQueue::pop`
  /// (dropping the future before completion does not consume a message); the
  /// batch top-up runs synchronously only after the head pop completes. Used
  /// by the ROUTER to race a queue pop against an identity-finalized signal.
  pub async fn pop(&self) -> Result<(usize, FrameBatch), ZmqError> {
    if let Some(item) = self.pop_cached() {
      return Ok(item);
    }
    let head = self.queue.pop().await?;
    self.top_up_cache();
    Ok(head)
  }

  /// Non-blocking pop through the cache.
  fn try_pop(&self) -> Option<(usize, FrameBatch)> {
    if let Some(item) = self.pop_cached() {
      return Some(item);
    }
    let head = self.queue.try_pop()?;
    self.top_up_cache();
    Some(head)
  }

  pub async fn recv_logical_message(
    &self,
    rcvtimeo_opt: Option<std::time::Duration>,
  ) -> Result<(usize, FrameBatch), ZmqError> {
    match rcvtimeo_opt {
      Some(d) if d.is_zero() => self.try_pop().ok_or(ZmqError::ResourceLimitReached),
      Some(d) => tokio::time::timeout(d, self.pop())
        .await
        .map_err(|_| ZmqError::Timeout)?,
      None => self.pop().await,
    }
  }
}

impl std::fmt::Debug for AddressedIngressEngine {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("AddressedIngressEngine").finish_non_exhaustive()
  }
}
