use std::collections::{HashMap, VecDeque};
#[cfg(feature = "io-uring")]
use std::sync::OnceLock;
use std::sync::{
  Arc, Weak,
  atomic::{AtomicBool, AtomicUsize, Ordering},
};

use parking_lot::RwLock;

use fibre::mpmc::{AsyncReceiver, AsyncSender, bounded_async};
use fibre::{RecvError, TryRecvError, TrySendError};

use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::socket::patterns::trie::SubscriptionTrie;
use crate::{cancel_guard, cancel_guard_complete, log_rpq_spin_deadlock};

#[cfg(feature = "io-uring")]
use crate::io_uring_backend::ops::{WAKEUP_STATE_SIGNALED, WAKEUP_STATE_SLEEPING};

// ---------------------------------------------------------------------------
// io_uring direct-wakeup payload
// ---------------------------------------------------------------------------

#[cfg(feature = "io-uring")]
#[derive(Clone)]
pub(crate) struct UringWakeup {
  pub event_fd: eventfd::EventFD,
  pub worker_asleep: Arc<std::sync::atomic::AtomicU8>,
}

// ---------------------------------------------------------------------------
// Per-pipe slot
// ---------------------------------------------------------------------------

/// Compute the low-water mark for a pipe: the threshold below which the pipe
/// is considered "drained" and the io_uring worker is woken to resume sending.
///
/// L = max(capacity / 2, capacity − drain_delta)
///
/// The `capacity / 2` floor prevents thrashing on small queues (e.g. capacity=2
/// with drain_delta=64 would give L=0, causing immediate re-congestion).
/// The `capacity − drain_delta` term scales L upward for large queues so that
/// the worker is woken before the pipe empties completely, absorbing one full
/// receive batch of back-pressure without an extra round-trip.
pub(crate) fn pipe_lwm(capacity: usize, drain_delta: usize) -> usize {
  (capacity / 2).max(capacity.saturating_sub(drain_delta))
}

/// Diagnostic invariant audit (debug / `diagnostics` builds only).
///
/// Invariant: every item physically present in `rx` must have a live reservation,
/// i.e. `reserved_count >= rx.len()` at all times — a reservation is taken *before*
/// an item is enqueued and only released *after* it is dequeued. `rx.len()` is read
/// *before* `reserved_count` so a concurrent producer (reserve-then-enqueue) or
/// consumer (dequeue-then-release) cannot fabricate a false positive.
///
/// Fires at most once per slot, on the first violation, naming the `site` — this
/// pinpoints the exact operation that breaks the accounting behind the PULL-ingress
/// deadlock (`rx` full while `queued`/`reserved` read 0). Silent in the happy path,
/// so it adds no log throughput until something is actually wrong.
#[inline]
fn audit_slot<T: Send + 'static>(slot: &PipeSlot<T>, site: &str) {
  #[cfg(any(debug_assertions, feature = "diagnostics"))]
  {
    let rxlen = slot.rx.len();
    let reserved = slot.reserved_count.load(Ordering::Acquire);
    if reserved < rxlen && !slot.audit_reported.swap(true, Ordering::AcqRel) {
      let queued = slot.queued_count.load(Ordering::Acquire);
      println!(
        "[RPQ-DESYNC pid={} pipe={} site={}] reserved({}) < rx.len({}) \
         — item(s) in channel with no backing reservation; queued={}",
        std::process::id(),
        slot.pipe_id,
        site,
        reserved,
        rxlen,
        queued,
      );
    }
  }
  #[cfg(not(any(debug_assertions, feature = "diagnostics")))]
  {
    let _ = (slot, site);
  }
}

pub(crate) struct PipeSlot<T: Send + 'static> {
  pub(crate) pipe_id: usize,
  pub(crate) tx: AsyncSender<T>,
  pub(crate) rx: AsyncReceiver<T>,
  /// Active send reservations: in-flight (not yet committed) + committed messages.
  /// Incremented at the START of every send attempt (before the channel write).
  /// Decremented on cancellation (RAII) or on consumer pop.
  /// Invariant: reserved_count >= queued_count at all times.
  pub(crate) reserved_count: AtomicUsize,
  /// Committed messages physically present in `rx`.
  /// Incremented AFTER a successful channel write; decremented on consumer pop.
  /// Invariant: queued_count == reserved_count when no sends are in flight.
  pub(crate) queued_count: AtomicUsize,
  /// Pre-computed low-water mark: wakeup fires when len() drops below this.
  pub(crate) lwm: usize,
  /// Diagnostic latch ensuring the desync audit prints at most once per slot.
  #[allow(dead_code)]
  pub(crate) audit_reported: AtomicBool,
  #[cfg(feature = "io-uring")]
  pub(crate) uring_wakeup: Arc<OnceLock<UringWakeup>>,
}

impl<T: Send + 'static> PipeSlot<T> {
  pub fn len(&self) -> usize {
    self.rx.len()
  }

  pub fn capacity(&self) -> usize {
    self.rx.capacity().unwrap_or(usize::MAX)
  }

  pub fn is_congested(&self) -> bool {
    self.len() >= self.capacity()
  }

  pub fn is_drained(&self) -> bool {
    self.len() < self.lwm
  }
}

// ---------------------------------------------------------------------------
// Diagnostic cancel detector
//
// Placed around every `.await` inside `send()` and `pop()` that publishes or
// re-enqueues a pipe. If the surrounding future is cancelled mid-await the
// drop fires and prints a loud warning with the exact location.
// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// Private RAII send reservation
//
// Created before every send attempt (incrementing reserved_count).
// On Drop: if not committed, rolls back reserved_count.
// On commit(): marks the reservation permanent; consumer pop handles cleanup.
// ---------------------------------------------------------------------------

struct SendReservation<T: Send + 'static> {
  slot: Arc<PipeSlot<T>>,
  committed: bool,
}

impl<T: Send + 'static> SendReservation<T> {
  fn new(slot: Arc<PipeSlot<T>>) -> Self {
    slot.reserved_count.fetch_add(1, Ordering::AcqRel);
    Self { slot, committed: false }
  }

  fn commit(&mut self) {
    self.committed = true;
  }
}

impl<T: Send + 'static> Drop for SendReservation<T> {
  fn drop(&mut self) {
    if !self.committed {
      // Cancelled or errored — roll back the reservation.
      self.slot.reserved_count.fetch_sub(1, Ordering::AcqRel);
    }
    // Committed reservations are released by the consumer on pop.
  }
}

// ---------------------------------------------------------------------------
// ReadyPipeQueue — consumer side
// ---------------------------------------------------------------------------

pub(crate) struct ReadyPipeQueue<T: Send + 'static> {
  pub(crate) pipes: Arc<RwLock<HashMap<usize, Arc<PipeSlot<T>>>>>,
  pub(crate) ready_rx: AsyncReceiver<Arc<PipeSlot<T>>>,
  ready_tx: AsyncSender<Arc<PipeSlot<T>>>,
}

impl<T: Send + 'static> ReadyPipeQueue<T> {
  /// `ready_capacity` must be at least the maximum number of registered pipes:
  /// each pipe occupies at most one slot in the ready list at a time.
  pub fn new(ready_capacity: usize) -> Self {
    let (tx, rx) = bounded_async(ready_capacity.max(1));
    Self {
      pipes: Arc::new(RwLock::new(HashMap::new())),
      ready_rx: rx,
      ready_tx: tx,
    }
  }

  pub fn register_pipe(&self, pipe_id: usize, capacity: usize, drain_delta: usize) -> ReadyPipeSender<T> {
    let mut pipes = self.pipes.write();

    if let Some(slot) = pipes.get(&pipe_id) {
      return ReadyPipeSender {
        slot: Arc::downgrade(slot),
        ready_tx: self.ready_tx.clone(),
      };
    }

    let (tx, rx) = bounded_async(capacity.max(1));
    #[cfg(feature = "io-uring")]
    let uring_wakeup = Arc::new(OnceLock::new());

    let slot = Arc::new(PipeSlot {
      pipe_id,
      tx,
      rx,
      reserved_count: AtomicUsize::new(0),
      queued_count: AtomicUsize::new(0),
      lwm: pipe_lwm(capacity, drain_delta),
      audit_reported: AtomicBool::new(false),
      #[cfg(feature = "io-uring")]
      uring_wakeup,
    });

    pipes.insert(pipe_id, Arc::clone(&slot));

    ReadyPipeSender {
      slot: Arc::downgrade(&slot),
      ready_tx: self.ready_tx.clone(),
    }
  }

  pub fn deregister_pipe(&self, pipe_id: usize) {
    self.pipes.write().remove(&pipe_id);
  }

  pub async fn pop(&self) -> Result<(usize, T), ZmqError> {
    loop {
      let slot = match self.ready_rx.recv().await {
        Ok(s) => s,
        Err(RecvError::Disconnected) => {
          return Err(ZmqError::InvalidState("ready queue closed"));
        }
      };

      match slot.rx.try_recv() {
        Ok(item) => {
          let prev = slot.queued_count.fetch_sub(1, Ordering::AcqRel);
          slot.reserved_count.fetch_sub(1, Ordering::AcqRel);
          debug_assert!(prev > 0);
          audit_slot(&slot, "pop");

          if prev > 1 {
            cancel_guard!(guard, "ReadyPipeQueue::pop → ready_tx.send");

            // More committed messages remain — keep this pipe on the ready list.
            let _ = self.ready_tx.send(Arc::clone(&slot)).await;

            cancel_guard_complete!(guard);
          }

          #[cfg(feature = "io-uring")]
          if slot.is_drained() {
            if let Some(wakeup) = slot.uring_wakeup.get() {
              if wakeup.worker_asleep.load(Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
                if wakeup
                  .worker_asleep
                  .compare_exchange(
                    WAKEUP_STATE_SLEEPING,
                    WAKEUP_STATE_SIGNALED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                  )
                  .is_ok()
                {
                  let _ = wakeup.event_fd.write(1);
                }
              }
            }
          }

          return Ok((slot.pipe_id, item));
        }
        Err(TryRecvError::Empty) => {
          // Stale ready signal (deregistration or close race). queued_count is
          // authoritative; if the channel is empty the signal is invalid — discard.
          continue;
        }
        Err(TryRecvError::Disconnected) => continue,
      }
    }
  }

  pub fn try_pop(&self) -> Option<(usize, T)> {
    loop {
      let slot = match self.ready_rx.try_recv() {
        Ok(s) => s,
        Err(_) => return None,
      };

      match slot.rx.try_recv() {
        Ok(item) => {
          let prev = slot.queued_count.fetch_sub(1, Ordering::AcqRel);
          slot.reserved_count.fetch_sub(1, Ordering::AcqRel);
          debug_assert!(prev > 0);
          audit_slot(&slot, "try_pop");

          if prev > 1 {
            let _ = self.ready_tx.try_send(Arc::clone(&slot));
          }

          #[cfg(feature = "io-uring")]
          if slot.is_drained() {
            if let Some(wakeup) = slot.uring_wakeup.get() {
              if wakeup.worker_asleep.load(Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
                if wakeup
                  .worker_asleep
                  .compare_exchange(
                    WAKEUP_STATE_SLEEPING,
                    WAKEUP_STATE_SIGNALED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                  )
                  .is_ok()
                {
                  let _ = wakeup.event_fd.write(1);
                }
              }
            }
          }

          return Some((slot.pipe_id, item));
        }
        Err(TryRecvError::Empty) => {
          // Stale ready signal — discard, let caller yield.
          return None;
        }
        Err(TryRecvError::Disconnected) => continue,
      }
    }
  }

  pub fn close(&self) {
    self.pipes.write().clear();
    self.ready_tx.close();
  }
}

// ---------------------------------------------------------------------------
// ReadyPipeSender — producer side
// Weak<PipeSlot<T>> prevents an Arc cycle with the queue's HashMap.
// ---------------------------------------------------------------------------

pub(crate) struct ReadyPipeSender<T: Send + 'static> {
  slot: Weak<PipeSlot<T>>,
  ready_tx: AsyncSender<Arc<PipeSlot<T>>>,
}

impl<T: Send + 'static> ReadyPipeSender<T> {
  #[cfg(feature = "io-uring")]
  pub fn bind_uring_wakeup(&self, wakeup: UringWakeup) {
    if let Some(slot) = self.slot.upgrade() {
      let _ = slot.uring_wakeup.set(wakeup);
    }
  }

  pub async fn send(&self, item: T) -> Result<(), ZmqError> {
    let slot = self.slot.upgrade().ok_or(ZmqError::ConnectionClosed)?;

    // Reservation increments reserved_count before any channel write.
    // If this future is dropped (tokio::select! picks another branch),
    // the guard's Drop rolls back reserved_count — no leak.
    let mut reservation = SendReservation::new(Arc::clone(&slot));

    match slot.tx.try_send(item) {
      Ok(()) => {}
      Err(TrySendError::Closed(_)) => return Err(ZmqError::ConnectionClosed),
      Err(TrySendError::Full(returned)) => {
        // Block here. If cancelled mid-await, Drop runs on the reservation.
        slot
          .tx
          .send(returned)
          .await
          .map_err(|_| ZmqError::ConnectionClosed)?;
      }
      Err(TrySendError::Sent(_)) => unreachable!(),
    }

    // Message is committed to the channel. Seal the reservation so Drop
    // does not roll it back; the consumer's pop() will release it instead.
    let prev = slot.queued_count.fetch_add(1, Ordering::AcqRel);
    reservation.commit();

    if prev == 0 {
      cancel_guard!(cd, "ReadyPipeSender::send → ready_tx.send");
      self
        .ready_tx
        .send(Arc::clone(&slot))
        .await
        .map_err(|_| ZmqError::ConnectionClosed)?;
      cancel_guard_complete!(cd);
    }

    audit_slot(&slot, "send");
    Ok(())
  }

  pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
    let slot = match self.slot.upgrade() {
      Some(s) => s,
      None => return Err(TrySendError::Closed(item)),
    };

    let mut reservation = SendReservation::new(Arc::clone(&slot));

    // If this returns an error, the reservation is dropped (rolled back).
    slot.tx.try_send(item)?;

    let prev = slot.queued_count.fetch_add(1, Ordering::AcqRel);
    reservation.commit();

    if prev == 0 {
      // 0→1 transition: ready queue capacity must be >= max registered
      // pipes so this should never spin more than one iteration.
      let mut spins = 0usize;
      while let Err(e) = self.ready_tx.try_send(Arc::clone(&slot)) {
        spins += 1;
        log_rpq_spin_deadlock!(spins, "try_send spinning", e);
        std::thread::yield_now();
      }
    }

    audit_slot(&slot, "try_send");
    Ok(())
  }

  /// Synchronously pushes as many items as the channel will accept, performing
  /// `queued_count` updates inline (prevents consumer underflow) and coalescing
  /// the ready-queue wakeup to exactly one spin-retry at the end.
  ///
  /// Returns the total weight of items consumed from `items` (both sent and, in
  /// the filtered case, discarded). Items that could not be sent due to backpressure
  /// remain at the front of `items` in FIFO order.
  pub fn try_send_batch(&self, items: &mut VecDeque<T>, get_weight: impl Fn(&T) -> usize) -> usize {
    let slot = match self.slot.upgrade() {
      Some(s) => s,
      None => return 0,
    };

    let n = items.len();
    if n == 0 {
      return 0;
    }

    // Bulk reservation upfront — one atomic instead of N.
    slot.reserved_count.fetch_add(n, Ordering::AcqRel);

    let mut sent_batches = 0usize;
    let mut total_weight = 0usize;
    let mut had_zero_transition = false;

    while let Some(item) = items.pop_front() {
      let weight = get_weight(&item);
      match slot.tx.try_send(item) {
        Ok(()) => {
          sent_batches += 1;
          total_weight += weight;
          // Inline increment — consumer may pop the item before the batch ends;
          // updating immediately keeps queued_count >= physical channel occupancy.
          let prev = slot.queued_count.fetch_add(1, Ordering::AcqRel);
          if prev == 0 {
            had_zero_transition = true;
          }
        }
        Err(TrySendError::Full(returned)) => {
          items.push_front(returned);
          break;
        }
        Err(TrySendError::Closed(returned)) => {
          items.push_front(returned);
          break;
        }
        Err(TrySendError::Sent(_)) => unreachable!(),
      }
    }

    // Roll back any reservations for items we couldn't push.
    if sent_batches < n {
      slot.reserved_count.fetch_sub(n - sent_batches, Ordering::AcqRel);
    }

    // Guaranteed wakeup on 0→1 transition. ready_capacity >= max registered
    // pipes, so the spin almost never executes more than one iteration.
    if had_zero_transition {
      let mut spins = 0usize;
      while let Err(e) = self.ready_tx.try_send(Arc::clone(&slot)) {
        spins += 1;
        log_rpq_spin_deadlock!(spins, "try_send_batch spinning on ready_tx", e);
        std::thread::yield_now();
      }
    }

    audit_slot(&slot, "try_send_batch");
    total_weight
  }

  pub fn queued_count(&self) -> usize {
    self
      .slot
      .upgrade()
      .map(|s| s.queued_count.load(Ordering::Relaxed))
      .unwrap_or(0)
  }

  pub fn reserved_count(&self) -> usize {
    self
      .slot
      .upgrade()
      .map(|s| s.reserved_count.load(Ordering::Relaxed))
      .unwrap_or(0)
  }

  pub fn len(&self) -> usize {
    self.slot.upgrade().map(|s| s.len()).unwrap_or(0)
  }

  pub fn capacity(&self) -> usize {
    self
      .slot
      .upgrade()
      .map(|s| s.capacity())
      .unwrap_or(usize::MAX)
  }

  pub fn is_congested(&self) -> bool {
    self
      .slot
      .upgrade()
      .map(|s| s.is_congested())
      .unwrap_or(false)
  }

  pub fn is_drained(&self) -> bool {
    self.slot.upgrade().map(|s| s.is_drained()).unwrap_or(true)
  }
}

// ---------------------------------------------------------------------------
// PipeMessageSender
// ---------------------------------------------------------------------------

pub(crate) enum PipeMessageSender {
  DirectAnonymous(ReadyPipeSender<FrameBatch>),
  FilteredAnonymous {
    sender: ReadyPipeSender<FrameBatch>,
    trie: Arc<SubscriptionTrie>,
  },
  DirectAddressed {
    sender: ReadyPipeSender<FrameBatch>,
  },
}

impl PipeMessageSender {
  #[cfg(feature = "io-uring")]
  pub fn bind_uring_wakeup(&mut self, wakeup: UringWakeup) {
    match self {
      Self::DirectAnonymous(s) => s.bind_uring_wakeup(wakeup),
      Self::FilteredAnonymous { sender, .. } => sender.bind_uring_wakeup(wakeup),
      Self::DirectAddressed { sender } => sender.bind_uring_wakeup(wakeup),
    }
  }

  pub async fn send(&self, batch: FrameBatch) -> Result<(), ZmqError> {
    match self {
      Self::DirectAnonymous(s) => s.send(batch).await,
      Self::FilteredAnonymous { sender, trie } => {
        let topic: &[u8] = batch.first().and_then(|m| m.data()).unwrap_or(&[]);
        if trie.matches(topic) {
          sender.send(batch).await
        } else {
          Ok(())
        }
      }
      Self::DirectAddressed { sender } => sender.send(batch).await,
    }
  }

  pub fn try_send_sync(&self, batch: FrameBatch) -> Result<(), TrySendError<FrameBatch>> {
    match self {
      Self::DirectAnonymous(s) => s.try_send(batch),
      Self::FilteredAnonymous { sender, trie } => {
        let topic: &[u8] = batch.first().and_then(|m| m.data()).unwrap_or(&[]);
        if trie.matches(topic) {
          sender.try_send(batch)
        } else {
          Ok(())
        }
      }
      Self::DirectAddressed { sender } => sender.try_send(batch),
    }
  }

  /// Synchronously drains as many `FrameBatch`es from `items` as possible,
  /// applying subscription filtering for `FilteredAnonymous` senders.
  ///
  /// Returns the total frame count consumed (sent + discarded). Backpressured
  /// items remain at the front of `items` in FIFO order.
  pub fn try_send_batch(&self, items: &mut VecDeque<FrameBatch>) -> usize {
    match self {
      Self::DirectAnonymous(s) => s.try_send_batch(items, |b| b.len()),

      Self::FilteredAnonymous { sender, trie } => {
        let n = items.len();
        if n == 0 {
          return 0;
        }

        // Pre-scan to get exact match count for a precise coalesced reservation.
        let match_count = items
          .iter()
          .filter(|b| trie.matches(b.first().and_then(|m| m.data()).unwrap_or(&[])))
          .count();

        // Fast path: nothing passes the filter — bulk discard.
        if match_count == 0 {
          let total = items.iter().map(|b| b.len()).sum::<usize>();
          items.clear();
          return total;
        }

        let slot = match sender.slot.upgrade() {
          Some(s) => s,
          None => return 0,
        };

        slot.reserved_count.fetch_add(match_count, Ordering::AcqRel);

        let mut sent_batches = 0usize;
        let mut total_frames = 0usize;
        let mut had_zero_transition = false;

        while let Some(item) = items.pop_front() {
          let topic: &[u8] = item.first().and_then(|m| m.data()).unwrap_or(&[]);
          if trie.matches(topic) {
            let frame_count = item.len();
            match slot.tx.try_send(item) {
              Ok(()) => {
                sent_batches += 1;
                total_frames += frame_count;
                let prev = slot.queued_count.fetch_add(1, Ordering::AcqRel);
                if prev == 0 {
                  had_zero_transition = true;
                }
              }
              Err(TrySendError::Full(returned)) => {
                items.push_front(returned);
                break;
              }
              Err(TrySendError::Closed(returned)) => {
                items.push_front(returned);
                break;
              }
              _ => unreachable!(),
            }
          } else {
            // Non-matching frames are discarded; count them as processed.
            total_frames += item.len();
          }
        }

        if sent_batches < match_count {
          slot.reserved_count.fetch_sub(match_count - sent_batches, Ordering::AcqRel);
        }

        if had_zero_transition {
          let mut spins = 0usize;
          while let Err(e) = sender.ready_tx.try_send(Arc::clone(&slot)) {
            spins += 1;
            log_rpq_spin_deadlock!(spins, "try_send_batch filtered spinning", e);
            std::thread::yield_now();
          }
        }

        total_frames
      }

      Self::DirectAddressed { sender } => sender.try_send_batch(items, |b| b.len()),
    }
  }

  pub fn queued_count(&self) -> usize {
    match self {
      Self::DirectAnonymous(s) => s.queued_count(),
      Self::FilteredAnonymous { sender, .. } => sender.queued_count(),
      Self::DirectAddressed { sender } => sender.queued_count(),
    }
  }

  pub fn reserved_count(&self) -> usize {
    match self {
      Self::DirectAnonymous(s) => s.reserved_count(),
      Self::FilteredAnonymous { sender, .. } => sender.reserved_count(),
      Self::DirectAddressed { sender } => sender.reserved_count(),
    }
  }

  pub fn len(&self) -> usize {
    match self {
      Self::DirectAnonymous(s) => s.len(),
      Self::FilteredAnonymous { sender, .. } => sender.len(),
      Self::DirectAddressed { sender } => sender.len(),
    }
  }

  pub fn capacity(&self) -> usize {
    match self {
      Self::DirectAnonymous(s) => s.capacity(),
      Self::FilteredAnonymous { sender, .. } => sender.capacity(),
      Self::DirectAddressed { sender } => sender.capacity(),
    }
  }

  pub fn is_congested(&self) -> bool {
    match self {
      Self::DirectAnonymous(s) => s.is_congested(),
      Self::FilteredAnonymous { sender, .. } => sender.is_congested(),
      Self::DirectAddressed { sender } => sender.is_congested(),
    }
  }

  pub fn is_drained(&self) -> bool {
    match self {
      Self::DirectAnonymous(s) => s.is_drained(),
      Self::FilteredAnonymous { sender, .. } => sender.is_drained(),
      Self::DirectAddressed { sender } => sender.is_drained(),
    }
  }
}

impl std::fmt::Debug for PipeMessageSender {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::DirectAnonymous(_) => write!(f, "PipeMessageSender::DirectAnonymous"),
      Self::FilteredAnonymous { .. } => write!(f, "PipeMessageSender::FilteredAnonymous"),
      Self::DirectAddressed { .. } => write!(f, "PipeMessageSender::DirectAddressed"),
    }
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use super::*;
  use fibre::TrySendError;
  use std::sync::Arc;
  use std::sync::atomic::{AtomicBool, Ordering};

  /// Checks that the lost-wakeup invariant holds under concurrent producers.
  /// Uses reserved_count (not queued_count) as the authoritative "a send is
  /// in flight or committed" signal, so transient counter lag does not
  /// generate false positives.
  #[test]
  fn test_ready_pipe_queue_try_pop_lost_wakeup_repro() {
    const NUM_PRODUCERS: usize = 4;
    const ATTEMPTS_PER_PRODUCER: usize = 500_000;

    let queue = Arc::new(ReadyPipeQueue::<usize>::new(128));
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut senders = Vec::new();
    let mut producer_handles = Vec::new();

    for pipe_id in 0..NUM_PRODUCERS {
      let sender = Arc::new(queue.register_pipe(pipe_id, 1, 0));
      senders.push(sender.clone());

      let sender_clone = sender.clone();
      let stop_clone = stop_signal.clone();

      producer_handles.push(std::thread::spawn(move || {
        let mut seq = 0;
        while !stop_clone.load(Ordering::Relaxed) && seq < ATTEMPTS_PER_PRODUCER {
          match sender_clone.try_send(seq) {
            Ok(()) => seq += 1,
            Err(TrySendError::Full(_)) => std::thread::yield_now(),
            Err(_) => break,
          }
        }
      }));
    }

    let start_time = std::time::Instant::now();
    let mut lost_wakeup_detected = false;

    while start_time.elapsed() < std::time::Duration::from_secs(5) {
      if let Some((_, _item)) = queue.try_pop() {
        // drained successfully
      } else {
        let pipes = queue.pipes.read();
        for pipe_id in 0..NUM_PRODUCERS {
          if let Some(slot) = pipes.get(&pipe_id) {
            let has_items = !slot.rx.is_empty();
            // reserved_count covers both in-flight and committed messages so
            // a non-zero value means a wakeup signal is guaranteed to arrive.
            let reserved = slot.reserved_count.load(Ordering::Acquire);
            let has_ready_signal = !queue.ready_rx.is_empty();

            if has_items && reserved == 0 && !has_ready_signal {
              println!(
                "\n[LOST WAKEUP] pipe={} rx_len={} reserved={} queued={} ready_rx_len={}",
                pipe_id,
                slot.rx.len(),
                reserved,
                slot.queued_count.load(Ordering::Acquire),
                queue.ready_rx.len()
              );
              lost_wakeup_detected = true;
              break;
            }
          }
        }
        if lost_wakeup_detected {
          break;
        }
        std::thread::yield_now();
      }
    }

    stop_signal.store(true, Ordering::Release);
    queue.close();
    for h in producer_handles {
      let _ = h.join();
    }

    assert!(
      !lost_wakeup_detected,
      "REGRESSION: A lost-wakeup deadlock was detected!"
    );
  }

  #[test]
  fn test_ready_pipe_queue_pipe_deregistration_cleanup() {
    let queue = ReadyPipeQueue::<i32>::new(10);
    let sender = queue.register_pipe(1, 10, 0);
    assert_eq!(queue.pipes.read().len(), 1);

    queue.deregister_pipe(1);
    assert_eq!(queue.pipes.read().len(), 0);

    // Weak::upgrade returns None after the HashMap drops the last strong Arc.
    let res = sender.try_send(42);
    assert!(res.is_err(), "sending on a deregistered pipe must fail");
  }
}

#[cfg(test)]
mod livelock_repro_tests {
  use super::*;
  use std::sync::Arc;
  use std::time::Duration;
  use tokio::time::timeout;

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn test_ready_pipe_queue_livelock_repro() {
    let queue = Arc::new(ReadyPipeQueue::<i32>::new(10));

    // Pipe with capacity 1 so the second send blocks.
    let sender = Arc::new(queue.register_pipe(1, 1, 0));

    // Fill the channel.
    sender.try_send(100).unwrap();

    // Spawn a sender that will block on the full channel.
    let sender_clone = sender.clone();
    let blocked_sender = tokio::spawn(async move {
      let _ = sender_clone.send(200).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Pop 100. queued_count drops 1→0, pipe is NOT re-enqueued.
    let (id, val) = queue.pop().await.unwrap();
    assert_eq!(id, 1);
    assert_eq!(val, 100);

    // The blocked sender wakes, commits 200 (queued_count 0→1), publishes pipe.
    // pop() must complete — not spin forever.
    let queue_clone = queue.clone();
    let pop_task = tokio::spawn(async move { queue_clone.pop().await.unwrap() });

    let result = timeout(Duration::from_secs(2), pop_task).await;

    blocked_sender.abort();

    assert!(
      result.is_ok(),
      "pop() spun indefinitely instead of waiting for the blocked sender"
    );
  }
}

#[cfg(test)]
mod pop_counter_desync_regression {
  use super::*;
  use std::collections::VecDeque;
  use std::sync::Arc;
  use std::sync::atomic::Ordering;
  use std::time::Duration;
  use tokio::time::timeout;

  /// Push `buf` into `sender` exactly the way the session ingress path
  /// (`IngressDriver`) does: a bulk synchronous `try_send_batch`, then an async
  /// `send` of the still-blocked front frame when the channel is full. Returns
  /// when the whole buffer has been delivered.
  async fn ingress_style_push(sender: &ReadyPipeSender<usize>, buf: &mut VecDeque<usize>) {
    loop {
      sender.try_send_batch(buf, |_| 1);
      match buf.front().copied() {
        None => return,
        Some(front) => {
          sender.send(front).await.expect("blocked send must succeed");
          buf.pop_front();
        }
      }
    }
  }

  /// Regression for the PULL-ingress deadlock.
  ///
  /// Under sustained backpressure (channel pinned at `RCVHWM`), a race between a
  /// producer enqueue and a consumer `pop()` let `queued_count`/`reserved_count`
  /// fall one behind the physical `rx` occupancy (`[RPQ-DESYNC site=pop]
  /// reserved(99) < rx.len(100)`). The skew ratcheted down until `queued_count`
  /// reached 0 with items still in `rx`; `pop()`'s `prev > 1` re-enqueue then
  /// stopped firing, the pipe was never re-armed on `ready_tx`, and the consumer
  /// deadlocked — losing messages mid-stream (observed as a PULL receiver timing
  /// out short of the sent count in `test_push_pull_concurrent_shutdown_race`).
  ///
  /// This drives the identical workload — one producer using the ingress push
  /// pattern, one `pop()` consumer, capacity == RCVHWM — and asserts every
  /// message is delivered and the counters are clean at the end. On the buggy
  /// code the consumer stalls and this fails via the per-pop timeout.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_pop_counter_desync_deadlock_regression() {
    const CAP: usize = 100; // mirrors RCVHWM in the failing integration test
    const BATCH: usize = 128; // mirrors rcvbatch_count
    const TOTAL: usize = 300_000; // enough volume to hit the enqueue/pop race

    let queue = Arc::new(ReadyPipeQueue::<usize>::new(8));
    let sender = Arc::new(queue.register_pipe(0, CAP, 0));

    let producer = {
      let sender = sender.clone();
      tokio::spawn(async move {
        let mut next = 0usize;
        let mut buf: VecDeque<usize> = VecDeque::with_capacity(BATCH);
        while next < TOTAL {
          let end = (next + BATCH).min(TOTAL);
          buf.extend(next..end);
          next = end;
          ingress_style_push(&sender, &mut buf).await;
        }
      })
    };

    let consumer = {
      let queue = queue.clone();
      tokio::spawn(async move {
        let mut got = 0usize;
        while got < TOTAL {
          match timeout(Duration::from_secs(5), queue.pop()).await {
            Ok(Ok(_)) => got += 1,
            Ok(Err(e)) => panic!("pop() errored after {got}/{TOTAL}: {e:?}"),
            Err(_) => panic!(
              "DEADLOCK: pop() stalled after {got}/{TOTAL} messages — \
               queued_count/reserved_count desynced from rx (the [RPQ-DESYNC] bug)"
            ),
          }
        }
        got
      })
    };

    producer.await.expect("producer task");
    let got = consumer.await.expect("consumer task");
    assert_eq!(got, TOTAL, "messages were lost in the ready pipe queue");

    // Drained and balanced: no leaked reservations / counts, channel empty.
    let pipes = queue.pipes.read();
    let slot = pipes.get(&0).expect("pipe slot present");
    assert_eq!(slot.rx.len(), 0, "rx not fully drained");
    assert_eq!(
      slot.queued_count.load(Ordering::Acquire),
      0,
      "queued_count leaked"
    );
    assert_eq!(
      slot.reserved_count.load(Ordering::Acquire),
      0,
      "reserved_count leaked"
    );
  }
}

#[cfg(test)]
mod cancellation_safety_tests {
  use super::*;
  use std::sync::Arc;
  use std::time::Duration;
  use tokio::time::timeout;

  /// A cancelled send must not inflate reserved_count or queued_count.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn test_cancellation_rollback() {
    let queue = Arc::new(ReadyPipeQueue::<i32>::new(10));
    let sender = queue.register_pipe(1, 1, 0);

    // Fill the pipe so the next send blocks.
    sender.send(100).await.unwrap();

    let pipes = queue.pipes.read();
    let slot = pipes.get(&1).unwrap().clone();
    drop(pipes);

    let reserved_before = slot.reserved_count.load(Ordering::Acquire);
    let queued_before = slot.queued_count.load(Ordering::Acquire);

    // Drop the blocking future mid-flight.
    let _ = timeout(Duration::from_millis(20), sender.send(200)).await;

    let reserved_after = slot.reserved_count.load(Ordering::Acquire);
    let queued_after = slot.queued_count.load(Ordering::Acquire);

    assert_eq!(
      reserved_after, reserved_before,
      "cancelled send must not leave a reservation: before={} after={}",
      reserved_before, reserved_after
    );
    assert_eq!(
      queued_after, queued_before,
      "cancelled send must not inflate queued_count"
    );
  }

  /// 1000 cancelled futures must leave reserved_count == queued_count.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_massive_cancellation_storm() {
    let queue = Arc::new(ReadyPipeQueue::<i32>::new(10));
    let sender = Arc::new(queue.register_pipe(1, 1, 0));

    // Fill the pipe so every send blocks.
    sender.send(0).await.unwrap();

    let pipes = queue.pipes.read();
    let slot = pipes.get(&1).unwrap().clone();
    drop(pipes);

    // Launch 1000 send futures and immediately cancel every one.
    let mut tasks = Vec::new();
    for i in 1..=1000 {
      let s = sender.clone();
      tasks.push(tokio::spawn(async move {
        let _ = timeout(Duration::from_millis(1), s.send(i)).await;
      }));
    }
    for t in tasks {
      let _ = t.await;
    }

    // Give any racing completions a moment to settle.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let reserved = slot.reserved_count.load(Ordering::Acquire);
    let queued = slot.queued_count.load(Ordering::Acquire);

    assert_eq!(
      reserved, queued,
      "after all cancellations reserved_count ({}) must equal queued_count ({})",
      reserved, queued
    );
  }

  /// Concurrent send/cancel cycles must leave no phantom readiness.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_concurrent_send_cancel_race() {
    let queue = Arc::new(ReadyPipeQueue::<i32>::new(10));
    let sender = Arc::new(queue.register_pipe(1, 4, 0));

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tasks = Vec::new();

    // Senders: alternate short-lived (cancellable) and committed sends.
    for i in 0..8 {
      let s = sender.clone();
      let stop2 = stop.clone();
      tasks.push(tokio::spawn(async move {
        let mut seq = i;
        while !stop2.load(Ordering::Relaxed) {
          // Alternate cancellable and normal sends.
          if seq % 2 == 0 {
            let _ = timeout(Duration::from_micros(10), s.send(seq)).await;
          } else {
            let _ = s.send(seq).await;
          }
          seq += 8;
          tokio::task::yield_now().await;
        }
      }));
    }

    // Consumer: drain for 1 second.
    let queue2 = queue.clone();
    let consumer = tokio::spawn(async move {
      let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
      while tokio::time::Instant::now() < deadline {
        tokio::select! {
          biased;
          _ = queue2.pop() => {}
          _ = tokio::time::sleep(Duration::from_millis(1)) => {}
        }
      }
    });

    consumer.await.unwrap();
    stop.store(true, Ordering::Release);
    // Abort producers that may be blocked in slot.tx.send().await after the
    // consumer exited. RAII SendReservation rolls back reserved_count on abort.
    for t in &tasks {
      t.abort();
    }
    for t in tasks {
      let _ = t.await;
    }

    // Drain whatever remains.
    while queue.try_pop().is_some() {}

    let pipes = queue.pipes.read();
    let slot = pipes.get(&1).unwrap();
    let reserved = slot.reserved_count.load(Ordering::Acquire);
    let queued = slot.queued_count.load(Ordering::Acquire);
    drop(pipes);

    assert_eq!(
      reserved, queued,
      "after concurrent send/cancel storm reserved={} queued={}",
      reserved, queued
    );
  }

  /// A cancelled send future must not leave the pipe stuck in a perpetual
  /// pop() spin (the original cancellation-leak livelock).
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn test_cancellation_safe_no_livelock() {
    let queue = Arc::new(ReadyPipeQueue::<i32>::new(10));
    let sender = queue.register_pipe(1, 1, 0);

    // Fill the channel. queued_count → 1, reserved_count → 1.
    sender.send(100).await.unwrap();

    // Drop a blocking send mid-flight. With RAII the reservation rolls back.
    let _ = timeout(Duration::from_millis(50), sender.send(200)).await;

    // Pop 100. queued_count/reserved_count → 0. Pipe is NOT re-enqueued.
    let (id, val) = queue.pop().await.unwrap();
    assert_eq!(id, 1);
    assert_eq!(val, 100);

    // Channel is genuinely empty; no phantom reservation remains.
    // A second pop() must block (not spin), so we expect a timeout here.
    let queue2 = queue.clone();
    let pop_task = tokio::spawn(async move { queue2.pop().await.unwrap() });

    let result = timeout(Duration::from_millis(200), pop_task).await;
    assert!(
      result.is_err(),
      "pop() returned unexpectedly — phantom reservation or ghost message present"
    );
  }
}
