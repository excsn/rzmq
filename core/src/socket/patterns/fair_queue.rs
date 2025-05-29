use crate::error::ZmqError;
use crate::message::Msg;
use async_channel::{Receiver, Sender, TryRecvError, TrySendError};

#[derive(Debug)]
pub(crate) enum PushError {
  Full(Msg),   // Queue was full, message is returned to the caller
  Closed(Msg), // Queue was closed, message is returned to the caller
}

/// Buffers incoming messages from multiple pipes in a single queue
/// for fair consumption by the socket's `recv()` method.
#[derive(Debug)]
pub(crate) struct FairQueue {
  // Use an async channel internally. `recv()` will await on this.
  // The capacity acts as the effective RCVHWM for the socket.
  receiver: Receiver<Msg>,
  sender: Sender<Msg>,
  // Optional: Track attached pipes for debugging or specific logic?
  // pipe_count: Arc<AtomicUsize>,
  hwm: usize, // Store the HWM capacity
}

impl FairQueue {
  /// Creates a new fair queue with a specific capacity (RCVHWM).
  pub fn new(capacity: usize) -> Self {
    let (sender, receiver) = async_channel::bounded(capacity.max(1));
    Self {
      receiver,
      sender,
      hwm: capacity.max(1),
    }
  }

  /// Called when a pipe delivering messages to this queue is attached.
  /// (Might not be strictly needed if just using the queue).
  pub fn pipe_attached(&self, pipe_read_id: usize) {
    // self.pipe_count.fetch_add(1, Ordering::Relaxed);
    tracing::trace!(pipe_id = pipe_read_id, hwm = self.hwm, "FairQueue pipe attached");
  }

  /// Called when a pipe delivering messages to this queue is detached.
  pub fn pipe_detached(&self, pipe_read_id: usize) {
    // self.pipe_count.fetch_sub(1, Ordering::Relaxed);
    tracing::trace!(pipe_id = pipe_read_id, "FairQueue pipe detached");
  }

  /// Pushes a message received from a pipe reader task into the fair queue.
  /// Called by the ISocket implementation's `handle_pipe_event` for `PipeMessageReceived`.
  /// Returns error if queue is closed (shouldn't happen normally).
  /// Note: `async-channel` handles backpressure via `send().await`. This call is async.
  pub async fn push_message(&self, msg: Msg) -> Result<(), ZmqError> {
    self.sender.send(msg).await.map_err(|e| {
      tracing::error!("FairQueue send error (queue closed?): {:?}", e);
      ZmqError::Internal("FairQueue channel closed unexpectedly".into())
    })
  }

  /// Pops the next available message from the queue for the user's `recv()` call.
  /// Awaits if the queue is empty. Returns `None` if the queue is closed.
  pub async fn pop_message(&self) -> Result<Option<Msg>, ZmqError> {
    match self.receiver.recv().await {
      Ok(msg) => Ok(Some(msg)),
      Err(async_channel::RecvError) => Ok(None), // Channel closed
    }
  }

  /// Attempts to push a message into the fair queue without blocking.
  ///
  /// Returns:
  /// - `Ok(())` if the message was successfully sent (Msg is consumed).
  /// - `Err(PushError::Full(Msg))` if the queue was full; the original `Msg` is returned.
  /// - `Err(PushError::Closed(Msg))` if the queue was closed; the original `Msg` is returned.
  pub fn try_push_message(&self, msg: Msg) -> Result<(), PushError> {
    match self.sender.try_send(msg) {
      Ok(()) => Ok(()),
      Err(TrySendError::Full(returned_msg)) => Err(PushError::Full(returned_msg)),
      Err(TrySendError::Closed(returned_msg)) => {
        // Log the error, as channel closure is usually unexpected here.
        tracing::error!("FairQueue try_send failed: Channel was closed.");
        Err(PushError::Closed(returned_msg))
      }
    }
  }

  /// Attempts to pop a message without blocking.
  /// Returns `Ok(Some(Msg))` if a message is available.
  /// Returns `Ok(None)` if the queue is currently empty.
  /// Returns `Err(ZmqError::Internal)` if the channel is closed.
  pub fn try_pop_message(&self) -> Result<Option<Msg>, ZmqError> {
    match self.receiver.try_recv() {
      Ok(msg) => Ok(Some(msg)),
      Err(TryRecvError::Empty) => Ok(None),
      Err(TryRecvError::Closed) => {
        tracing::error!("FairQueue try_recv error: Channel closed");
        Err(ZmqError::Internal("FairQueue channel closed unexpectedly".into()))
      }
    }
  }

  /// Returns the capacity (RCVHWM) of the queue.
  pub fn capacity(&self) -> usize {
    self.hwm
  }

  /// Returns the current number of messages in the queue.
  pub fn len(&self) -> usize {
    self.receiver.len()
  }

  /// Returns true if the queue is empty.
  pub fn is_empty(&self) -> bool {
    self.receiver.is_empty()
  }
}
