use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::Mutex;
use crate::message::{Msg, FrameBatch};
use crate::ZmqError;
use crate::socket::patterns::ready_pipe_queue::{PipeMessageSender, ReadyPipeQueue};

pub(crate) struct AnonymousIngressEngine {
  queue: ReadyPipeQueue<FrameBatch>,
  local_cache: Mutex<Option<VecDeque<Msg>>>,
}

impl AnonymousIngressEngine {
  pub fn new() -> Self {
    Self {
      queue: ReadyPipeQueue::new(4096),
      local_cache: Mutex::new(None),
    }
  }

  pub fn register_pipe(&self, pipe_id: usize, capacity: usize) -> PipeMessageSender {
    let sender = self.queue.register_pipe(pipe_id, capacity);
    PipeMessageSender::DirectAnonymous(sender)
  }

  pub fn register_pipe_filtered(
    &self,
    pipe_id: usize,
    capacity: usize,
    trie: Arc<crate::socket::patterns::SubscriptionTrie>,
  ) -> PipeMessageSender {
    let sender = self.queue.register_pipe(pipe_id, capacity);
    PipeMessageSender::FilteredAnonymous { sender, trie }
  }

  pub fn deregister_pipe(&self, pipe_id: usize) {
    self.queue.deregister_pipe(pipe_id);
    *self.local_cache.lock() = None;
  }

  pub fn close(&self) {
    self.queue.close();
    *self.local_cache.lock() = None;
  }

  pub async fn recv(&self, rcvtimeo_opt: Option<std::time::Duration>) -> Result<Msg, ZmqError> {
    {
      let mut cache = self.local_cache.lock();
      if let Some(ref mut deque) = *cache {
        if let Some(msg) = deque.pop_front() {
          if deque.is_empty() { *cache = None; }
          return Ok(msg);
        }
        *cache = None;
      }
    }

    let (_, mut batch) = match rcvtimeo_opt {
      Some(d) if d.is_zero() => self.queue.try_pop().ok_or(ZmqError::ResourceLimitReached)?,
      Some(d) => tokio::time::timeout(d, self.queue.pop())
        .await
        .map_err(|_| ZmqError::Timeout)??,
      None => self.queue.pop().await?,
    };

    if batch.is_empty() {
      return Ok(Msg::new());
    }
    if batch.len() == 1 {
      return Ok(batch.remove(0));
    }
    let mut deque: VecDeque<Msg> = batch.into_iter().collect();
    let first = deque.pop_front().unwrap();
    *self.local_cache.lock() = Some(deque);
    Ok(first)
  }

  pub async fn recv_multipart(&self, rcvtimeo_opt: Option<std::time::Duration>) -> Result<FrameBatch, ZmqError> {
    {
      let mut cache = self.local_cache.lock();
      if let Some(ref mut deque) = *cache {
        let mut batch = FrameBatch::new();
        let mut completed = false;
        while let Some(msg) = deque.pop_front() {
          let is_more = msg.is_more();
          batch.push(msg);
          if !is_more {
            completed = true;
            break;
          }
        }
        if deque.is_empty() { *cache = None; }
        if completed {
          return Ok(batch);
        }
      }
    }

    let (_, batch) = match rcvtimeo_opt {
      Some(d) if d.is_zero() => self.queue.try_pop().ok_or(ZmqError::ResourceLimitReached)?,
      Some(d) => tokio::time::timeout(d, self.queue.pop())
        .await
        .map_err(|_| ZmqError::Timeout)??,
      None => self.queue.pop().await?,
    };

    Ok(batch)
  }
}

impl std::fmt::Debug for AnonymousIngressEngine {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("AnonymousIngressEngine").finish_non_exhaustive()
  }
}
