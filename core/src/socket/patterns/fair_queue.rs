use crate::error::ZmqError;
use async_channel::{Receiver, Sender, TryRecvError, TrySendError};

#[derive(Debug)]
pub(crate) enum PushError<T: Send + 'static> {
  Full(T),
  Closed(T),
}

/// Buffers incoming items (generic T) from multiple sources in a single queue
/// for fair consumption.
#[derive(Debug)]
pub(crate) struct FairQueue<T: Send + 'static> {
  receiver: Receiver<T>,
  sender: Sender<T>,
  hwm: usize,
}

impl<T: Send + 'static> FairQueue<T> {
  /// Creates a new fair queue with a specific capacity (HWM).
  pub fn new(capacity: usize) -> Self {
    let (sender, receiver) = async_channel::bounded(capacity.max(1));
    Self {
      receiver,
      sender,
      hwm: capacity.max(1),
    }
  }

  /// Called when a pipe delivering messages to this queue is attached.
  pub fn pipe_attached(&self, pipe_read_id: usize) {
    tracing::trace!(pipe_id = pipe_read_id, hwm = self.hwm, "FairQueue pipe attached");
  }

  /// Called when a pipe delivering messages to this queue is detached.
  pub fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::trace!(pipe_id = pipe_read_id, "FairQueue pipe detached");
  }

  /// Pushes an item into the fair queue.
  pub async fn push_item(&self, item: T) -> Result<(), ZmqError> {
    self.sender.send(item).await.map_err(|e| {
      tracing::error!("FairQueue send error (queue closed?): {:?}", e);
      ZmqError::Internal("FairQueue channel closed unexpectedly".into())
    })
  }

  /// Pops the next available item from the queue.
  pub async fn pop_item(&self) -> Result<Option<T>, ZmqError> {
    match self.receiver.recv().await {
      Ok(item) => Ok(Some(item)),
      Err(async_channel::RecvError) => Ok(None), // Channel closed
    }
  }

  /// Attempts to push an item into the fair queue without blocking.
  pub fn try_push_item(&self, item: T) -> Result<(), PushError<T>> {
    match self.sender.try_send(item) {
      Ok(()) => Ok(()),
      Err(TrySendError::Full(returned_item)) => Err(PushError::Full(returned_item)),
      Err(TrySendError::Closed(returned_item)) => {
        tracing::error!("FairQueue try_send failed: Channel was closed.");
        Err(PushError::Closed(returned_item))
      }
    }
  }

  /// Attempts to pop an item without blocking.
  pub fn try_pop_item(&self) -> Result<Option<T>, ZmqError> {
    match self.receiver.try_recv() {
      Ok(item) => Ok(Some(item)),
      Err(TryRecvError::Empty) => Ok(None),
      Err(TryRecvError::Closed) => {
        tracing::error!("FairQueue try_recv error: Channel closed");
        Err(ZmqError::Internal("FairQueue channel closed unexpectedly".into()))
      }
    }
  }

  /// Returns the capacity (HWM) of the queue.
  pub fn capacity(&self) -> usize {
    self.hwm
  }

  /// Returns the current number of items in the queue.
  pub fn len(&self) -> usize {
    self.receiver.len()
  }

  /// Returns true if the queue is empty.
  pub fn is_empty(&self) -> bool {
    self.receiver.is_empty()
  }

  pub fn close(&self) {
    self.sender.close();
    self.receiver.close();
  }
}
