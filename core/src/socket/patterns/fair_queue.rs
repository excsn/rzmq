use crate::error::ZmqError;
use fibre::{mpmc::{bounded_async, AsyncReceiver, AsyncSender}, TryRecvError, TrySendError, RecvError};

#[derive(Debug)]
pub(crate) enum PushError<T: Send + 'static> {
  Full(T),
  Closed(T),
}

/// Buffers incoming items (generic T) from multiple sources in a single queue
/// for fair consumption.
#[derive(Debug)]
pub(crate) struct FairQueue<T: Send + 'static> {
  receiver: AsyncReceiver<T>,
  sender: AsyncSender<T>,
  hwm: usize,
}

impl<T: Send + 'static> FairQueue<T> {
  /// Creates a new fair queue with a specific capacity (HWM).
  pub fn new(capacity: usize) -> Self {
    let (sender, receiver) = bounded_async(capacity.max(1));
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
      Err(RecvError::Disconnected) => Ok(None), // Channel closed
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
      _ => unreachable!(),
    }
  }

  /// Attempts to pop an item without blocking.
  pub fn try_pop_item(&self) -> Result<Option<T>, ZmqError> {
    match self.receiver.try_recv() {
      Ok(item) => Ok(Some(item)),
      Err(TryRecvError::Empty) => Ok(None),
      Err(TryRecvError::Disconnected) => {
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
  }
}

#[cfg(test)]
mod additional_fair_queue_tests {
  use super::*;

  #[test]
  fn test_fair_queue_try_push_and_pop() {
    let queue = FairQueue::new(2);

    queue.try_push_item("item-1").expect("Push 1 should succeed");
    queue.try_push_item("item-2").expect("Push 2 should succeed");

    assert_eq!(queue.len(), 2);

    let item = queue
      .try_pop_item()
      .expect("Should not error")
      .expect("Should return an item");
    assert_eq!(item, "item-1");
  }

  #[test]
  fn test_fair_queue_exhaustion() {
    let queue = FairQueue::new(1);
    queue.try_push_item("item-1").unwrap();

    let res = queue.try_push_item("item-2");
    assert!(matches!(res, Err(PushError::Full(_))));
  }

  #[tokio::test]
  async fn test_fair_queue_close() {
    let queue = FairQueue::new(2);
    queue.try_push_item("item-1").unwrap();

    queue.close();

    // Writes after close must fail
    let res = queue.try_push_item("item-2");
    assert!(matches!(res, Err(PushError::Closed(_))));

    // Existing items can still be drained
    let item = queue.try_pop_item().expect("Should not error");
    assert_eq!(item, Some("item-1"));

    // Empty closed queue returns None
    let drained = queue.pop_item().await.expect("Should not error on closed empty");
    assert!(drained.is_none());
  }
}
