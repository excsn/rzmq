use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::Mutex;
use crate::message::{Msg, FrameBatch};
use crate::ZmqError;
use crate::socket::patterns::ready_pipe_queue::{PipeMessageSender, ReadyPipeQueue};

/// Consumer-local buffers. `frames` holds flattened frames from already-popped
/// batches (message boundaries preserved by the MORE flag); `scratch` is the
/// reusable batch-drain buffer handed to `ReadyPipeQueue::pop_batch`.
struct IngressCache {
  frames: VecDeque<Msg>,
  scratch: Vec<FrameBatch>,
}

pub(crate) struct AnonymousIngressEngine {
  queue: ReadyPipeQueue<FrameBatch>,
  /// Max messages drained per ready-token acquisition (rcvbatch_count).
  batch_max: usize,
  cache: Mutex<IngressCache>,
}

impl AnonymousIngressEngine {
  pub fn new(activation_capacity: usize, batch_max: usize) -> Self {
    Self {
      queue: ReadyPipeQueue::new(activation_capacity),
      batch_max: batch_max.max(1),
      cache: Mutex::new(IngressCache {
        frames: VecDeque::new(),
        scratch: Vec::new(),
      }),
    }
  }

  pub fn register_pipe(&self, pipe_id: usize, capacity: usize, drain_delta: usize) -> PipeMessageSender {
    let sender = self.queue.register_pipe(pipe_id, capacity, drain_delta);
    PipeMessageSender::DirectAnonymous(sender)
  }

  pub fn register_pipe_filtered(
    &self,
    pipe_id: usize,
    capacity: usize,
    trie: Arc<crate::socket::patterns::SubscriptionTrie>,
    drain_delta: usize,
  ) -> PipeMessageSender {
    let sender = self.queue.register_pipe(pipe_id, capacity, drain_delta);
    PipeMessageSender::FilteredAnonymous { sender, trie }
  }

  pub fn deregister_pipe(&self, pipe_id: usize) {
    self.queue.deregister_pipe(pipe_id);
    self.cache.lock().frames.clear();
  }

  pub fn close(&self) {
    self.queue.close();
    self.cache.lock().frames.clear();
  }

  /// Drains up to `batch_max` messages from the ready queue into `scratch`.
  async fn pop_batch(
    &self,
    scratch: &mut Vec<FrameBatch>,
    rcvtimeo_opt: Option<std::time::Duration>,
  ) -> Result<(), ZmqError> {
    match rcvtimeo_opt {
      Some(d) if d.is_zero() => self
        .queue
        .try_pop_batch(scratch, self.batch_max)
        .map(|_| ())
        .ok_or(ZmqError::ResourceLimitReached),
      Some(d) => tokio::time::timeout(d, self.queue.pop_batch(scratch, self.batch_max))
        .await
        .map_err(|_| ZmqError::Timeout)?
        .map(|_| ()),
      None => self.queue.pop_batch(scratch, self.batch_max).await.map(|_| ()),
    }
  }

  pub async fn recv(&self, rcvtimeo_opt: Option<std::time::Duration>) -> Result<Msg, ZmqError> {
    let mut scratch = {
      let mut cache = self.cache.lock();
      if let Some(msg) = cache.frames.pop_front() {
        return Ok(msg);
      }
      std::mem::take(&mut cache.scratch)
    };

    let pop_res = self.pop_batch(&mut scratch, rcvtimeo_opt).await;

    let mut cache = self.cache.lock();
    cache.frames.extend(scratch.drain(..).flatten());
    cache.scratch = scratch;
    pop_res?;

    // A popped batch with zero frames degrades to an empty Msg (legacy edge).
    Ok(cache.frames.pop_front().unwrap_or_else(Msg::new))
  }

  pub async fn recv_multipart(&self, rcvtimeo_opt: Option<std::time::Duration>) -> Result<FrameBatch, ZmqError> {
    let mut scratch = {
      let mut cache = self.cache.lock();
      if !cache.frames.is_empty() {
        if let Some(batch) = Self::assemble_message(&mut cache.frames) {
          return Ok(batch);
        }
        // Partial message without a boundary (mixed recv/recv_multipart use):
        // discarded, matching the previous cache behavior.
      }
      std::mem::take(&mut cache.scratch)
    };

    let pop_res = self.pop_batch(&mut scratch, rcvtimeo_opt).await;

    let mut cache = self.cache.lock();
    cache.frames.extend(scratch.drain(..).flatten());
    cache.scratch = scratch;
    pop_res?;

    Ok(Self::assemble_message(&mut cache.frames).unwrap_or_else(FrameBatch::new))
  }

  /// Pops one logical message (frames up to and including the first frame
  /// without MORE) off the front of `frames`. Returns `None` — leaving
  /// `frames` drained — if no complete boundary is present.
  fn assemble_message(frames: &mut VecDeque<Msg>) -> Option<FrameBatch> {
    let mut batch = FrameBatch::new();
    while let Some(msg) = frames.pop_front() {
      let is_more = msg.is_more();
      batch.push(msg);
      if !is_more {
        return Some(batch);
      }
    }
    None
  }
}

impl std::fmt::Debug for AnonymousIngressEngine {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("AnonymousIngressEngine").finish_non_exhaustive()
  }
}
