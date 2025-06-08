use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::socket::core::SocketCore; // Not used in constructor anymore, core_handle is passed
use crate::socket::patterns::fair_queue::{FairQueue, PushError};
#[allow(unused_imports)] // Blob might not be directly used by generic orchestrator
use crate::Blob;
use dashmap::DashMap;
use std::collections::VecDeque; // <<< ADDED [For current_recv_frames_buffer] >>>
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex; // <<< ADDED [For current_recv_frames_buffer] >>>
use tokio::time::timeout as tokio_timeout;

// <<< MODIFIED START [Generic IncomingMessageOrchestrator<QItem> with internal buffering for recv()] >>>
#[derive(Debug)]
pub(crate) struct IncomingMessageOrchestrator<QItem: Send + 'static> {
  socket_core_handle: usize,
  main_incoming_queue: FairQueue<QItem>,
  queue_push_timeout: Option<Duration>,
  partial_pipe_messages: DashMap<usize, Vec<Msg>>,
  // Buffer for delivering frames of a single QItem one-by-one via recv_message()
  current_recv_frames_buffer: TokioMutex<VecDeque<Msg>>,
}

impl<QItem: Send + 'static> IncomingMessageOrchestrator<QItem> {
  pub fn new(core_handle: usize, rcvhwm: usize) -> Self {
    Self {
      socket_core_handle: core_handle,
      main_incoming_queue: FairQueue::new(rcvhwm.max(1)),
      queue_push_timeout: Some(Duration::from_millis(1000)),
      partial_pipe_messages: DashMap::new(),
      current_recv_frames_buffer: TokioMutex::new(VecDeque::new()),
    }
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
    let push_result = match self.queue_push_timeout {
      None => self.main_incoming_queue.push_item(item_to_queue).await,
      Some(duration) if duration.is_zero() => match self.main_incoming_queue.try_push_item(item_to_queue) {
        Ok(()) => Ok(()),
        Err(PushError::Full(_returned_item)) => {
          tracing::warn!(
            handle = self.socket_core_handle,
            pipe_id = pipe_read_id_for_logging,
            "Orchestrator try_push item to main queue failed: Full. Item dropped."
          );
          Ok(())
        }
        Err(PushError::Closed(_returned_item)) => {
          tracing::error!(
            handle = self.socket_core_handle,
            pipe_id = pipe_read_id_for_logging,
            "Orchestrator try_push item to main queue failed: Closed."
          );
          Err(ZmqError::Internal("Main incoming queue closed".into()))
        }
      },
      Some(duration) => match tokio_timeout(duration, self.main_incoming_queue.push_item(item_to_queue)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_timeout_elapsed) => {
          tracing::warn!(
            handle = self.socket_core_handle,
            pipe_id = pipe_read_id_for_logging,
            "Orchestrator timed push of item to main queue failed: Timeout. Item dropped."
          );
          Ok(())
        }
      },
    };

    match push_result {
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

  /// Helper to pop a QItem from the main FairQueue with timeout logic.
  pub(crate) async fn recv_item_from_main_queue(&self, rcvtimeo_opt: Option<Duration>) -> Result<QItem, ZmqError> {
    let pop_future = self.main_incoming_queue.pop_item();
    match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => match tokio_timeout(duration, pop_future).await {
        Ok(Ok(Some(item))) => Ok(item),
        Ok(Ok(None)) => Err(ZmqError::Internal(
          "Orchestrator: Main receive queue closed while popping item".into(),
        )),
        Ok(Err(e)) => Err(e),
        Err(_timeout_elapsed) => Err(ZmqError::Timeout),
      },
      _ => {
        if rcvtimeo_opt == Some(Duration::ZERO) {
          match self.main_incoming_queue.try_pop_item() {
            Ok(Some(item)) => Ok(item),
            Ok(None) => Err(ZmqError::ResourceLimitReached),
            Err(e) => Err(e),
          }
        } else {
          // Infinite wait
          match pop_future.await? {
            Some(item) => Ok(item),
            None => Err(ZmqError::Internal(
              "Orchestrator: Main receive queue closed (inf wait)".into(),
            )),
          }
        }
      }
    }
  }

  /// For ISocket::recv_multipart(). Fetches the next logical item and transforms it.
  pub async fn recv_logical_message(
    &self,
    rcvtimeo_opt: Option<Duration>,
    // Closure to transform QItem into the Vec<Msg> the application expects for multipart.
    // e.g., for Router: (Blob, Vec<Msg>) -> [identity_frame, payload_frames...]
    qitem_to_app_frames: impl FnOnce(QItem) -> Vec<Msg>,
  ) -> Result<Vec<Msg>, ZmqError> {
    let mut buffer_guard = self.current_recv_frames_buffer.lock().await;
    if !buffer_guard.is_empty() {
      // A `recv_message()` sequence was in progress. Application is mixing API calls.
      // Clear buffer and fetch a new full message.
      tracing::warn!(
        handle = self.socket_core_handle,
        "recv_logical_message called while recv_message buffer was active. Clearing buffer."
      );
      *buffer_guard = VecDeque::new();
    }
    // Drop guard before await
    drop(buffer_guard);

    let q_item = self.recv_item_from_main_queue(rcvtimeo_opt).await?;
    let app_frames = qitem_to_app_frames(q_item);
    Ok(app_frames)
  }

  /// For ISocket::recv(). Fetches frames one by one for a logical message.
  pub async fn recv_message(
    &self,
    rcvtimeo_opt: Option<Duration>,
    // Closure to transform QItem into Vec<Msg> for buffering if buffer is empty.
    qitem_to_app_frames: impl FnOnce(QItem) -> Vec<Msg>,
  ) -> Result<Msg, ZmqError> {
    let mut buffer_guard = self.current_recv_frames_buffer.lock().await;

    if let Some(mut frame) = buffer_guard.pop_front() {
      // Set MORE flag based on whether more frames remain *in this buffer*
      if !buffer_guard.is_empty() {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
      return Ok(frame);
    }

    // Buffer is empty, need to fetch and populate.
    // Drop guard before await on recv_item_from_main_queue
    drop(buffer_guard);

    let q_item = self.recv_item_from_main_queue(rcvtimeo_opt).await?;
    let app_frames_vec = qitem_to_app_frames(q_item);

    // Re-acquire lock to update buffer
    let mut buffer_guard = self.current_recv_frames_buffer.lock().await;

    if app_frames_vec.is_empty() {
      // This indicates the QItem transformed into an empty Vec<Msg>, which is unusual
      // unless it represents an intentionally empty message from the peer.
      // ZMQ recv on an empty message might block or have specific behavior.
      // For now, return error or a single empty Msg.
      tracing::warn!(
        handle = self.socket_core_handle,
        "recv_message: Transformed QItem resulted in empty app_frames. Returning empty Msg."
      );
      // Or an error: return Err(ZmqError::InvalidMessage("Received an empty logical message".into()));
      return Ok(Msg::new()); // Return a single empty message without MORE flag.
    }

    let mut app_frames_deque = VecDeque::from(app_frames_vec);
    let first_frame = app_frames_deque.pop_front().unwrap(); // Safe due to is_empty check above

    *buffer_guard = app_frames_deque; // Store remaining frames

    // The MORE flag on first_frame should have been set correctly by qitem_to_app_frames
    // based on whether there were subsequent frames in *its* sequence.
    Ok(first_frame)
  }

  /// Clears the internal buffer used by `recv_message`.
  /// Should be called when the socket pattern knows that any partially consumed
  /// message is no longer valid (e.g., REQ socket sending a new request).
  pub async fn reset_recv_message_buffer(&self) {
    let mut buffer_guard = self.current_recv_frames_buffer.lock().await;
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
}
