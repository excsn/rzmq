use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::socket::patterns::fair_queue::FairQueue;
#[allow(unused_imports)]
use crate::Blob;
use dashmap::DashMap;
use parking_lot::Mutex as ParkingMutex;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::timeout as tokio_timeout;

/// Max items pulled from the FairQueue per batch wait; the surplus beyond the first
/// is cached in `pending_logical_messages` and served without further channel waits.
const RECV_REFILL_MAX: usize = 64;

#[derive(Debug, Clone)]
pub(crate) enum AppFrames {
  Single(Msg),
  Multiple(Vec<Msg>),
}

#[derive(Debug)]
pub(crate) struct IncomingMessageOrchestrator<QItem: Send + 'static> {
  socket_core_handle: usize,
  main_incoming_queue: FairQueue<QItem>,
  partial_pipe_messages: DashMap<usize, Vec<Msg>>,
  // Buffer for delivering frames of a single QItem one-by-one via recv_message()
  current_recv_frames_buffer: ParkingMutex<VecDeque<Msg>>,
  // Cache of pre-fetched logical messages; serves recv()/recv_multipart() without channel waits
  pending_logical_messages: ParkingMutex<VecDeque<QItem>>,
}

impl<QItem: Send + 'static> IncomingMessageOrchestrator<QItem> {
  pub fn new(core_handle: usize, rcvhwm: usize) -> Self {
    Self {
      socket_core_handle: core_handle,
      main_incoming_queue: FairQueue::new(rcvhwm.max(1)),
      partial_pipe_messages: DashMap::new(),
      current_recv_frames_buffer: ParkingMutex::new(VecDeque::new()),
      pending_logical_messages: ParkingMutex::new(VecDeque::new()),
    }
  }

  #[inline(always)]
  pub fn has_partial_message(&self, pipe_read_id: usize) -> bool {
    self.partial_pipe_messages.contains_key(&pipe_read_id)
  }

  pub fn accumulate_pipe_frame(&self, pipe_read_id: usize, incoming_frame: Msg) -> Result<Option<Vec<Msg>>, ZmqError> {
    let is_last_frame_of_zmtp_message = !incoming_frame.is_more();
    if is_last_frame_of_zmtp_message {
      let mut assembled_message = self
        .partial_pipe_messages
        .remove(&pipe_read_id)
        .map(|(_key, val)| val)
        .unwrap_or_else(Vec::new);
      assembled_message.push(incoming_frame);
      if assembled_message.is_empty() {
        tracing::warn!(
          "Orchestrator: Assembled an empty ZMTP message for pipe {}.",
          pipe_read_id
        );
      }
      Ok(Some(assembled_message))
    } else {
      let mut pipe_buffer_entry = self.partial_pipe_messages.entry(pipe_read_id).or_insert_with(Vec::new);
      pipe_buffer_entry.value_mut().push(incoming_frame);
      Ok(None)
    }
  }

  pub async fn queue_item(&self, pipe_read_id_for_logging: usize, item_to_queue: QItem) -> Result<(), ZmqError> {
    match self.main_incoming_queue.push_item(item_to_queue).await {
      Ok(()) => Ok(()),
      Err(ZmqError::Internal(ref msg)) if msg.contains("FairQueue channel closed") => Err(ZmqError::Internal(
        "Main incoming queue channel unexpectedly closed".into(),
      )),
      Err(e) => {
        tracing::error!(handle = self.socket_core_handle, pipe_id = pipe_read_id_for_logging, error = %e, "Orchestrator: Unexpected error pushing item.");
        Err(e)
      }
    }
  }

  /// Pushes a batch of assembled logical messages to the FairQueue in one batch
  /// operation (single channel pass + coalesced wakeups), blocking on HWM as needed.
  pub async fn queue_batch(&self, _pipe_read_id: usize, mut items: Vec<QItem>) -> Result<(), ZmqError> {
    self.main_incoming_queue.push_batch(&mut items).await
  }

  pub(crate) async fn fetch_next_logical_item(
    &self,
    rcvtimeo_opt: Option<Duration>,
  ) -> Result<QItem, ZmqError> {
    if let Some(item) = self.pending_logical_messages.lock().pop_front() {
      return Ok(item);
    }
    self.recv_item_from_main_queue(rcvtimeo_opt).await
  }

  /// Helper to pop a QItem from the main FairQueue with timeout logic.
  ///
  /// Pops in batches: the first item is returned and the surplus is cached in
  /// `pending_logical_messages` (served by `fetch_next_logical_item` before any
  /// channel wait), so the per-message async channel cost is amortized.
  pub(crate) async fn recv_item_from_main_queue(&self, rcvtimeo_opt: Option<Duration>) -> Result<QItem, ZmqError> {
    let pop_future = self.pop_batch_and_cache();
    match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => match tokio_timeout(duration, pop_future).await {
        Ok(Ok(Some(item))) => Ok(item),
        Ok(Ok(None)) => Err(ZmqError::InvalidState("Socket terminated".into())),
        Ok(Err(e)) => Err(e),
        Err(_timeout_elapsed) => Err(ZmqError::Timeout),
      },
      _ => {
        if rcvtimeo_opt == Some(Duration::ZERO) {
          let mut popped: Vec<QItem> = Vec::new();
          match self.main_incoming_queue.try_pop_batch(&mut popped, RECV_REFILL_MAX) {
            Ok(0) => Err(ZmqError::ResourceLimitReached),
            Ok(_) => {
              let mut iter = popped.into_iter();
              let first = iter.next().unwrap();
              if iter.len() > 0 {
                self.pending_logical_messages.lock().extend(iter);
              }
              Ok(first)
            }
            Err(ZmqError::Internal(msg)) if msg.contains("closed") => {
              Err(ZmqError::InvalidState("Socket terminated".into()))
            }
            Err(e) => Err(e),
          }
        } else {
          // Infinite wait
          match pop_future.await {
            Ok(Some(item)) => Ok(item),
            Ok(None) => Err(ZmqError::InvalidState("Socket terminated".into())),
            Err(e) => Err(e),
          }
        }
      }
    }
  }

  /// Pops up to `RECV_REFILL_MAX` items from the FairQueue in one batch wait,
  /// returning the first and caching the rest in `pending_logical_messages`.
  /// Cancel-safe: the underlying batch receive future is cancel-safe, so a
  /// timeout cancellation cannot lose items.
  async fn pop_batch_and_cache(&self) -> Result<Option<QItem>, ZmqError> {
    match self.main_incoming_queue.pop_batch(RECV_REFILL_MAX).await? {
      Some(items) => {
        let mut iter = items.into_iter();
        let first = iter.next();
        debug_assert!(first.is_some(), "pop_batch returned an empty batch");
        if iter.len() > 0 {
          self.pending_logical_messages.lock().extend(iter);
        }
        Ok(first)
      }
      None => Ok(None),
    }
  }

  /// For ISocket::recv_multipart(). Fetches the next logical item and transforms it.
  pub async fn recv_logical_message(
    &self,
    rcvtimeo_opt: Option<Duration>,
    qitem_to_app_frames: impl FnOnce(QItem) -> AppFrames,
  ) -> Result<Vec<Msg>, ZmqError> {
    {
      let mut buffer_guard = self.current_recv_frames_buffer.lock();
      if !buffer_guard.is_empty() {
        tracing::warn!(
          handle = self.socket_core_handle,
          "recv_logical_message called while recv_message buffer was active. Clearing buffer."
        );
        *buffer_guard = VecDeque::new();
      }
    }

    let q_item = self.fetch_next_logical_item(rcvtimeo_opt).await?;
    match qitem_to_app_frames(q_item) {
      AppFrames::Single(msg) => Ok(vec![msg]),
      AppFrames::Multiple(msgs) => Ok(msgs),
    }
  }

  /// For ISocket::recv(). Fetches frames one by one for a logical message.
  pub async fn recv_message(
    &self,
    rcvtimeo_opt: Option<Duration>,
    qitem_to_app_frames: impl FnOnce(QItem) -> AppFrames,
  ) -> Result<Msg, ZmqError> {
    // Fast path: serve buffered frame from a previous multipart fetch
    {
      let mut buffer_guard = self.current_recv_frames_buffer.lock();
      if let Some(mut frame) = buffer_guard.pop_front() {
        if !buffer_guard.is_empty() {
          frame.set_flags(frame.flags() | MsgFlags::MORE);
        } else {
          frame.set_flags(frame.flags() & !MsgFlags::MORE);
        }
        return Ok(frame);
      }
    }

    let q_item = self.fetch_next_logical_item(rcvtimeo_opt).await?;
    match qitem_to_app_frames(q_item) {
      AppFrames::Single(msg) => Ok(msg),
      AppFrames::Multiple(mut msgs) => {
        if msgs.is_empty() {
          return Ok(Msg::new());
        }
        if msgs.len() == 1 {
          return Ok(msgs.pop().unwrap());
        }
        let mut app_frames_deque = VecDeque::from(msgs);
        let first_frame = app_frames_deque.pop_front().unwrap();
        {
          let mut buffer_guard = self.current_recv_frames_buffer.lock();
          *buffer_guard = app_frames_deque;
        }
        Ok(first_frame)
      }
    }
  }

  /// Clears the internal buffer used by `recv_message`.
  pub async fn reset_recv_message_buffer(&self) {
    let mut buffer_guard = self.current_recv_frames_buffer.lock();
    if !buffer_guard.is_empty() {
      tracing::trace!(
        handle = self.socket_core_handle,
        "Orchestrator: Resetting current_recv_frames_buffer."
      );
      *buffer_guard = VecDeque::new();
    }
  }

  pub async fn clear_pipe_state(&self, pipe_read_id: usize) {
    if self.partial_pipe_messages.remove(&pipe_read_id).is_some() {
      tracing::debug!(
        handle = self.socket_core_handle,
        pipe_id = pipe_read_id,
        "Orchestrator: Cleared partial ZMTP message buffer for detached pipe"
      );
    }
    // Also clear the main recv buffer if a pipe detaches.
    // This is a simplification; a more advanced system might only clear if the buffered message
    // originated from the detached pipe (would require QItem to carry pipe_id or similar context).
    // For now, this ensures no stale data from a disconnected pipe is accidentally delivered.
    self.reset_recv_message_buffer().await;
  }

  pub async fn close(&self) {
    self.main_incoming_queue.close();
    self.partial_pipe_messages.clear();
    self.pending_logical_messages.lock().clear();
    self.current_recv_frames_buffer.lock().clear();
  }
}
