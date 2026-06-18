use std::collections::VecDeque;
use std::time::Duration;

use parking_lot::Mutex as ParkingMutex;
use tokio::time::timeout as tokio_timeout;

use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg, MsgFlags};

use super::ready_pipe_queue::{ReadyPipeQueue, ReadyPipeSender};

/// Activation-queue capacity for the underlying `ReadyPipeQueue`.
/// Sized to accommodate typical peak concurrent active connections.
const ACTIVATION_CAPACITY: usize = 4096;

// ---------------------------------------------------------------------------
// AppFrames — the consumer-facing message shape
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) enum AppFrames {
  Single(Msg),
  Multiple(FrameBatch),
}

// ---------------------------------------------------------------------------
// IncomingMessageOrchestrator — non-generic, always FrameBatch internally
// ---------------------------------------------------------------------------

/// Coordinates ingress message delivery for a ZMQ socket.
///
/// Internally uses a `ReadyPipeQueue<FrameBatch>` where each enqueued item is
/// already a *complete* logical ZMQ message (multipart assembly is done by the
/// connection actor via `ZmqMessageProcessor`). Per-pipe channels provide
/// backpressure that flows all the way back to TCP.
///
/// `recv_frame` and `recv_logical_message` are the two consumer interfaces.
/// Complex sockets (ROUTER, REP) call `recv_logical_message` and handle
/// post-processing (identity prepend, routing prefix extraction) inline.
/// Simple sockets (PULL, SUB, DEALER, REQ) use `recv_frame` for single-frame
/// delivery and `recv_logical_message` for full multipart delivery.
pub(crate) struct IncomingMessageOrchestrator {
  socket_core_handle: usize,
  main_incoming_queue: ReadyPipeQueue<FrameBatch>,
  /// Remaining frames + originating pipe_id from a partially delivered
  /// logical message (used by `recv_frame` for frame-at-a-time delivery).
  current_recv_buffer: ParkingMutex<Option<(usize, VecDeque<Msg>)>>,
}

impl std::fmt::Debug for IncomingMessageOrchestrator {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("IncomingMessageOrchestrator")
      .field("socket_core_handle", &self.socket_core_handle)
      .finish_non_exhaustive()
  }
}

impl IncomingMessageOrchestrator {
  pub fn new(core_handle: usize) -> Self {
    Self {
      socket_core_handle: core_handle,
      main_incoming_queue: ReadyPipeQueue::new(ACTIVATION_CAPACITY),
      current_recv_buffer: ParkingMutex::new(None),
    }
  }

  // ------------------------------------------------------------------
  // Pipe registration
  // ------------------------------------------------------------------

  /// Register a new connection pipe. Returns the producer handle which the
  /// connection actor holds for direct push.
  pub fn register_connection_pipe(
    &self,
    pipe_id: usize,
    rcvhwm: usize,
  ) -> ReadyPipeSender<FrameBatch> {
    tracing::trace!(
      handle = self.socket_core_handle,
      pipe_id,
      rcvhwm,
      "Orchestrator: registering connection pipe"
    );
    self.main_incoming_queue.register_pipe(pipe_id, rcvhwm)
  }

  /// Deregister a connection pipe (e.g. on peer disconnect).
  /// Stale activation signals for this pipe are silently skipped in `pop`.
  pub fn deregister_connection_pipe(&self, pipe_id: usize) {
    tracing::trace!(
      handle = self.socket_core_handle,
      pipe_id,
      "Orchestrator: deregistering connection pipe"
    );
    self.main_incoming_queue.deregister_pipe(pipe_id);

    // Clear the recv buffer if it was mid-delivery from the detached pipe.
    let mut guard = self.current_recv_buffer.lock();
    if let Some((buffered_id, _)) = guard.as_ref() {
      if *buffered_id == pipe_id {
        *guard = None;
      }
    }
  }

  // ------------------------------------------------------------------
  // Consumer interface
  // ------------------------------------------------------------------

  /// Fetch a complete logical message from any active connection pipe.
  /// Returns `(pipe_id, batch)` where `batch` contains all frames of one
  /// logical ZMQ message. The `pipe_id` lets complex sockets (ROUTER, REP)
  /// look up per-peer routing metadata.
  ///
  /// Clears the frame buffer before waiting so that a previous multipart
  /// delivery does not interfere with the next message.
  pub async fn recv_logical_message(
    &self,
    rcvtimeo_opt: Option<Duration>,
  ) -> Result<(usize, FrameBatch), ZmqError> {
    // Discard any partially buffered frame from a prior recv_frame call.
    *self.current_recv_buffer.lock() = None;
    self.pop_with_timeout(rcvtimeo_opt).await
  }

  /// Deliver one frame at a time from the next logical message.
  /// On the first call for a new message, blocks on the queue; subsequent
  /// calls for the same message are served from an internal frame buffer
  /// without touching the queue again.
  ///
  /// Returns `(pipe_id, frame)`. The pipe_id is the same for every frame of
  /// one logical message. Sockets that do not need the pipe_id may discard it.
  pub async fn recv_frame(&self, rcvtimeo_opt: Option<Duration>) -> Result<(usize, Msg), ZmqError> {
    // Fast path: serve from buffer if mid-multipart.
    {
      let mut guard = self.current_recv_buffer.lock();
      if let Some((pipe_id, frames)) = guard.as_mut() {
        if let Some(mut frame) = frames.pop_front() {
          // Set MORE flag on all but the last buffered frame.
          if frames.is_empty() {
            frame.set_flags(frame.flags() & !MsgFlags::MORE);
          } else {
            frame.set_flags(frame.flags() | MsgFlags::MORE);
          }
          return Ok((*pipe_id, frame));
        }
        *guard = None;
      }
    }

    // Slow path: fetch next complete message.
    let (pipe_id, mut batch) = self.pop_with_timeout(rcvtimeo_opt).await?;

    if batch.is_empty() {
      return Ok((pipe_id, Msg::new()));
    }

    if batch.len() == 1 {
      return Ok((pipe_id, batch.remove(0)));
    }

    let mut deque: VecDeque<Msg> = batch.into_iter().collect();
    let first = deque.pop_front().unwrap();
    *self.current_recv_buffer.lock() = Some((pipe_id, deque));
    Ok((pipe_id, first))
  }

  /// Clear the internal frame buffer (call on close or when resetting state).
  pub fn reset_recv_buffer(&self) {
    *self.current_recv_buffer.lock() = None;
  }

  /// Close the queue so pending `recv_*` calls return an error.
  pub async fn close(&self) {
    self.main_incoming_queue.close();
    *self.current_recv_buffer.lock() = None;
  }

  // ------------------------------------------------------------------
  // Internal helpers
  // ------------------------------------------------------------------

  async fn pop_with_timeout(
    &self,
    rcvtimeo_opt: Option<Duration>,
  ) -> Result<(usize, FrameBatch), ZmqError> {
    match rcvtimeo_opt {
      // Non-blocking (RCVTIMEO = 0): true non-blocking via try_pop
      Some(d) if d.is_zero() => self
        .main_incoming_queue
        .try_pop()
        .ok_or(ZmqError::ResourceLimitReached),
      // Timed wait
      Some(duration) => match tokio_timeout(duration, self.main_incoming_queue.pop()).await {
        Ok(Ok(item)) => Ok(item),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(ZmqError::Timeout),
      },
      // Infinite wait
      None => self
        .main_incoming_queue
        .pop()
        .await
        .map_err(|_| ZmqError::InvalidState("Socket terminated".into())),
    }
  }
}
