use crate::message::FrameBatch;
use fibre::mpmc::AsyncReceiver;

pub(crate) struct EgressScheduler {
  rx: AsyncReceiver<FrameBatch>,
  batch_limit: usize,
  scratch_buffer: Vec<FrameBatch>,
}

impl EgressScheduler {
  pub fn new(rx: AsyncReceiver<FrameBatch>, batch_limit: usize) -> Self {
    Self {
      rx,
      batch_limit,
      scratch_buffer: Vec::with_capacity(batch_limit),
    }
  }

  pub fn drained_batches(&self) -> &[FrameBatch] {
    &self.scratch_buffer
  }

  /// Clears the local scratch space and greedily pulls up to `batch_limit` batches
  /// from the egress channel. Returns the total count of batches drained.
  pub fn drain_batch(&mut self) -> usize {
    self.scratch_buffer.clear();

    let mut drained = self
      .rx
      .try_recv_batch_mut(&mut self.scratch_buffer, self.batch_limit)
      .unwrap_or(0);

    if drained > 0 && drained < self.batch_limit {
      std::hint::spin_loop();

      let remaining_capacity = self.batch_limit - drained;
      let additional = self
        .rx
        .try_recv_batch_mut(&mut self.scratch_buffer, remaining_capacity)
        .unwrap_or(0);

      drained += additional;
    }

    drained
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::message::Msg;
  use fibre::mpmc::bounded_async;
  use smallvec::smallvec;

  fn create_test_batch(payload: &'static [u8]) -> FrameBatch {
    smallvec![Msg::from_static(payload)]
  }

  #[tokio::test]
  async fn test_scheduler_empty_drain() {
    let (_tx, rx) = bounded_async::<FrameBatch>(10);
    let mut scheduler = EgressScheduler::new(rx, 5);

    assert_eq!(scheduler.drain_batch(), 0);
    assert!(scheduler.drained_batches().is_empty());
  }

  #[tokio::test]
  async fn test_scheduler_greedy_limit_drain() {
    let (tx, rx) = bounded_async::<FrameBatch>(10);
    let mut scheduler = EgressScheduler::new(rx, 3);

    tx.try_send(create_test_batch(b"1")).unwrap();
    tx.try_send(create_test_batch(b"2")).unwrap();
    tx.try_send(create_test_batch(b"3")).unwrap();
    tx.try_send(create_test_batch(b"4")).unwrap();
    tx.try_send(create_test_batch(b"5")).unwrap();

    let count = scheduler.drain_batch();
    assert_eq!(count, 3);

    let drained = scheduler.drained_batches();
    assert_eq!(drained.len(), 3);
    assert_eq!(drained[0][0].data().unwrap(), b"1");
    assert_eq!(drained[2][0].data().unwrap(), b"3");
  }

  #[tokio::test]
  async fn test_scheduler_opportunistic_spin_drain() {
    let (tx, rx) = bounded_async::<FrameBatch>(10);
    let mut scheduler = EgressScheduler::new(rx, 5);

    tx.try_send(create_test_batch(b"immediate")).unwrap();

    let tx_clone = tx.clone();
    tokio::spawn(async move {
      tokio::time::sleep(std::time::Duration::from_micros(1)).await;
      let _ = tx_clone.try_send(create_test_batch(b"delayed"));
    });

    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    let count = scheduler.drain_batch();
    assert_eq!(count, 2);

    let drained = scheduler.drained_batches();
    assert_eq!(drained[0][0].data().unwrap(), b"immediate");
    assert_eq!(drained[1][0].data().unwrap(), b"delayed");
  }
}
