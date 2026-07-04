use std::cell::UnsafeCell;
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "io-uring")]
use std::sync::OnceLock;
use std::sync::{
  Arc, Weak,
  atomic::{AtomicBool, AtomicUsize, Ordering},
};

use parking_lot::RwLock;

use fibre::mpmc::{AsyncReceiver, AsyncSender, bounded_async};
use fibre::spsc;
use fibre::{RecvError, TryRecvError, TrySendError};

use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::socket::patterns::sub_matcher::{PrefixMatcher, SubscriptionMatcher};
use crate::log_rpq_spin_deadlock;

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
// ExclusiveCell — mutex-shaped cell without the lock
// ---------------------------------------------------------------------------

/// Interior-mutability cell for the fibre spsc handles inside `PipeSlot`.
///
/// fibre takes `&mut self` on every ring-touching spsc op to enforce exclusive
/// producer/consumer access at the type level. `PipeSlot` provides that
/// exclusivity dynamically instead:
/// - `rx`: a slot occupies the ready list at most once (0→1 `queued_count`
///   transition on send / `prev > 1` re-enqueue on pop), so only the holder of
///   the ready token touches `rx`, and the mpmc ready channel's send/recv
///   provides the happens-before edge between successive holders.
/// - `tx`: exactly one producer task per pipe holds the `ReadyPipeSender`.
struct ExclusiveCell<T>(UnsafeCell<T>);

unsafe impl<T: Send> Send for ExclusiveCell<T> {}
unsafe impl<T: Send> Sync for ExclusiveCell<T> {}

impl<T> ExclusiveCell<T> {
  fn new(v: T) -> Self {
    Self(UnsafeCell::new(v))
  }

  /// SAFETY: caller must be the exclusive owner at this instant — the ready
  /// token holder for `rx`, or the single registered producer for `tx` (see
  /// the type-level docs).
  #[allow(clippy::mut_from_ref)]
  unsafe fn get_mut(&self) -> &mut T {
    unsafe { &mut *self.0.get() }
  }
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
/// Invariant: every committed item must have a live reservation, i.e.
/// `reserved_count >= occupancy` at all times — a reservation is taken *before*
/// an item is enqueued and only released *after* it is dequeued. `occupancy` is
/// read *before* `reserved_count` so a concurrent producer (reserve-then-enqueue)
/// or consumer (dequeue-then-release) cannot fabricate a false positive.
///
/// `occupancy` is a closure because the measure differs per side: the pop site
/// holds the ready token and may read the physical `rx.len()`; producer sites
/// must not touch `rx` (the token holder may hold `&mut rx`) and pass
/// `queued_count` instead.
///
/// Fires at most once per slot, on the first violation, naming the `site` — this
/// pinpoints the exact operation that breaks the accounting behind the PULL-ingress
/// deadlock (`rx` full while `queued`/`reserved` read 0). Silent in the happy path,
/// so it adds no log throughput until something is actually wrong.
#[inline]
fn audit_slot<T: Send + 'static>(slot: &PipeSlot<T>, site: &str, occupancy: impl FnOnce() -> usize) {
  #[cfg(feature = "diagnostics")]
  {
    let occ = occupancy();
    let reserved = slot.reserved_count.load(Ordering::Acquire);
    if reserved < occ && !slot.audit_reported.swap(true, Ordering::AcqRel) {
      let queued = slot.queued_count.load(Ordering::Acquire);
      println!(
        "[RPQ-DESYNC pid={} pipe={} site={}] reserved({}) < occupancy({}) \
         — item(s) in channel with no backing reservation; queued={}",
        std::process::id(),
        slot.pipe_id,
        site,
        reserved,
        occ,
        queued,
      );
    }
  }
  #[cfg(not(feature = "diagnostics"))]
  let _ = (slot, site, occupancy);
}

pub(crate) struct PipeSlot<T: Send + 'static> {
  pub(crate) pipe_id: usize,
  tx: ExclusiveCell<spsc::BoundedAsyncSender<T>>,
  rx: ExclusiveCell<spsc::BoundedAsyncReceiver<T>>,
  /// Channel capacity, mirrored here so observers never touch `rx`/`tx`.
  capacity: usize,
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
  /// Committed occupancy. Uses `queued_count` rather than the physical
  /// `rx.len()`: observers on both sides call this concurrently, and touching
  /// `rx` here would alias the ready-token holder's `&mut rx`. `queued_count`
  /// tracks exactly the committed messages present in `rx` (transiently
  /// lagging by the commit window), which is sufficient for the
  /// congestion/drain heuristics built on it.
  pub fn len(&self) -> usize {
    self.queued_count.load(Ordering::Acquire)
  }

  pub fn capacity(&self) -> usize {
    self.capacity
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
    Self {
      slot,
      committed: false,
    }
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

  pub fn register_pipe(
    &self,
    pipe_id: usize,
    capacity: usize,
    drain_delta: usize,
  ) -> ReadyPipeSender<T> {
    let mut pipes = self.pipes.write();

    if let Some(slot) = pipes.get(&pipe_id) {
      return ReadyPipeSender {
        slot: Arc::downgrade(slot),
        ready_tx: self.ready_tx.clone(),
      };
    }

    let (tx, rx) = spsc::bounded_async(capacity.max(1));
    #[cfg(feature = "io-uring")]
    let uring_wakeup = Arc::new(OnceLock::new());

    let slot = Arc::new(PipeSlot {
      pipe_id,
      tx: ExclusiveCell::new(tx),
      rx: ExclusiveCell::new(rx),
      capacity: capacity.max(1),
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

      // SAFETY: we hold this slot's ready token (received it off `ready_rx`
      // just above), so we are the exclusive consumer right now.
      match unsafe { slot.rx.get_mut() }.try_recv() {
        Ok(item) => {
          let prev = slot.queued_count.fetch_sub(1, Ordering::AcqRel);
          slot.reserved_count.fetch_sub(1, Ordering::AcqRel);
          debug_assert!(prev > 0);
          audit_slot(&slot, "pop", || unsafe { slot.rx.get_mut() }.len());

          if prev > 1 {
            // More committed messages remain — keep this pipe on the ready list.
            let mut spins = 0usize;
            loop {
              match self.ready_tx.try_send(Arc::clone(&slot)) {
                Ok(()) => break,
                Err(TrySendError::Full(_)) => {
                  spins += 1;
                  log_rpq_spin_deadlock!(spins, "pop spinning on ready_tx", "Full");
                  std::thread::yield_now();
                }
                Err(TrySendError::Closed(_)) => break,
                Err(TrySendError::Sent(_)) => unreachable!(),
              }
            }
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

      // SAFETY: we hold this slot's ready token (received it off `ready_rx`
      // just above), so we are the exclusive consumer right now.
      match unsafe { slot.rx.get_mut() }.try_recv() {
        Ok(item) => {
          let prev = slot.queued_count.fetch_sub(1, Ordering::AcqRel);
          slot.reserved_count.fetch_sub(1, Ordering::AcqRel);
          debug_assert!(prev > 0);
          audit_slot(&slot, "try_pop", || unsafe { slot.rx.get_mut() }.len());

          if prev > 1 {
            let mut spins = 0usize;
            loop {
              match self.ready_tx.try_send(Arc::clone(&slot)) {
                Ok(()) => break,
                Err(TrySendError::Full(_)) => {
                  spins += 1;
                  log_rpq_spin_deadlock!(spins, "try_pop spinning on ready_tx", "Full");
                  std::thread::yield_now();
                }
                Err(TrySendError::Closed(_)) => break,
                Err(TrySendError::Sent(_)) => unreachable!(),
              }
            }
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

  /// Pops up to `max` messages from the next ready pipe while holding its
  /// ready token, appending them to `out`. One token round-trip (and at most
  /// one re-enqueue) is paid for the whole batch instead of per message.
  /// Returns the pipe id and the number of messages appended (>= 1).
  pub async fn pop_batch(&self, out: &mut Vec<T>, max: usize) -> Result<(usize, usize), ZmqError> {
    loop {
      let slot = match self.ready_rx.recv().await {
        Ok(s) => s,
        Err(RecvError::Disconnected) => {
          return Err(ZmqError::InvalidState("ready queue closed"));
        }
      };

      if let Some(res) = self.drain_slot(&slot, out, max) {
        return Ok(res);
      }
      // Stale ready signal — discard and wait for the next token.
    }
  }

  /// Non-blocking `pop_batch`. Returns `None` when no pipe is ready.
  pub fn try_pop_batch(&self, out: &mut Vec<T>, max: usize) -> Option<(usize, usize)> {
    loop {
      let slot = match self.ready_rx.try_recv() {
        Ok(s) => s,
        Err(_) => return None,
      };

      if let Some(res) = self.drain_slot(&slot, out, max) {
        return Some(res);
      }
    }
  }

  /// Drains up to `max` committed messages from `slot` into `out` while the
  /// caller holds the slot's ready token. Returns `None` for a stale token.
  fn drain_slot(&self, slot: &Arc<PipeSlot<T>>, out: &mut Vec<T>, max: usize) -> Option<(usize, usize)> {
    // Only committed messages may be popped: capping at queued_count keeps the
    // counter decrement below from racing a producer's post-write increment.
    // Committed items are always physically present in rx (the increment
    // happens after the channel write), so the batch read cannot come short.
    let committed = slot.queued_count.load(Ordering::Acquire);
    let cap = committed.min(max.max(1));
    if cap == 0 {
      return None;
    }

    // SAFETY: we hold this slot's ready token, so we are the exclusive
    // consumer right now.
    let got = match unsafe { slot.rx.get_mut() }.try_recv_batch_mut(out, cap) {
      Ok(n) => n,
      Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return None,
    };
    debug_assert!(got > 0 && got <= committed);

    let prev = slot.queued_count.fetch_sub(got, Ordering::AcqRel);
    slot.reserved_count.fetch_sub(got, Ordering::AcqRel);
    audit_slot(slot, "pop_batch", || unsafe { slot.rx.get_mut() }.len());

    if prev > got {
      // More committed messages remain — keep this pipe on the ready list.
      let mut spins = 0usize;
      loop {
        match self.ready_tx.try_send(Arc::clone(slot)) {
          Ok(()) => break,
          Err(TrySendError::Full(_)) => {
            spins += 1;
            log_rpq_spin_deadlock!(spins, "pop_batch spinning on ready_tx", "Full");
            std::thread::yield_now();
          }
          Err(TrySendError::Closed(_)) => break,
          Err(TrySendError::Sent(_)) => unreachable!(),
        }
      }
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

    Some((slot.pipe_id, got))
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

    // SAFETY: this ReadyPipeSender is the pipe's single producer.
    let tx = unsafe { slot.tx.get_mut() };
    match tx.try_send(item) {
      Ok(()) => {}
      Err(TrySendError::Closed(_)) => return Err(ZmqError::ConnectionClosed),
      Err(TrySendError::Full(returned)) => {
        // Block here. If cancelled mid-await, Drop runs on the reservation.
        tx.send(returned).await.map_err(|_| ZmqError::ConnectionClosed)?;
      }
      Err(TrySendError::Sent(_)) => unreachable!(),
    }

    // Message is committed to the channel. Seal the reservation so Drop
    // does not roll it back; the consumer's pop() will release it instead.
    let prev = slot.queued_count.fetch_add(1, Ordering::AcqRel);
    reservation.commit();

    if prev == 0 {
      let mut spins = 0usize;
      loop {
        match self.ready_tx.try_send(Arc::clone(&slot)) {
          Ok(()) => break,
          Err(TrySendError::Full(_)) => {
            spins += 1;
            log_rpq_spin_deadlock!(spins, "send spinning on ready_tx", "Full");
            std::thread::yield_now();
          }
          Err(TrySendError::Closed(_)) => return Err(ZmqError::ConnectionClosed),
          Err(TrySendError::Sent(_)) => unreachable!(),
        }
      }
    }

    audit_slot(&slot, "send", || slot.queued_count.load(Ordering::Acquire));
    Ok(())
  }

  pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
    let slot = match self.slot.upgrade() {
      Some(s) => s,
      None => return Err(TrySendError::Closed(item)),
    };

    let mut reservation = SendReservation::new(Arc::clone(&slot));

    // If this returns an error, the reservation is dropped (rolled back).
    // SAFETY: this ReadyPipeSender is the pipe's single producer.
    unsafe { slot.tx.get_mut() }.try_send(item)?;

    let prev = slot.queued_count.fetch_add(1, Ordering::AcqRel);
    reservation.commit();

    if prev == 0 {
      // 0→1 transition: ready queue capacity must be >= max registered
      // pipes so this should never spin more than one iteration.
      let mut spins = 0usize;
      loop {
        match self.ready_tx.try_send(Arc::clone(&slot)) {
          Ok(()) => break,
          Err(TrySendError::Full(_)) => {
            spins += 1;
            log_rpq_spin_deadlock!(spins, "try_send spinning on ready_tx", "Full");
            std::thread::yield_now();
          }
          Err(TrySendError::Closed(_)) => break,
          Err(TrySendError::Sent(_)) => unreachable!(),
        }
      }
    }

    audit_slot(&slot, "try_send", || slot.queued_count.load(Ordering::Acquire));
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

    // SAFETY: this ReadyPipeSender is the pipe's single producer.
    let tx = unsafe { slot.tx.get_mut() };
    while let Some(item) = items.pop_front() {
      let weight = get_weight(&item);
      match tx.try_send(item) {
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
      slot
        .reserved_count
        .fetch_sub(n - sent_batches, Ordering::AcqRel);
    }

    // Guaranteed wakeup on 0→1 transition. ready_capacity >= max registered
    // pipes, so the spin almost never executes more than one iteration.
    if had_zero_transition {
      let mut spins = 0usize;
      loop {
        match self.ready_tx.try_send(Arc::clone(&slot)) {
          Ok(()) => break,
          Err(TrySendError::Full(_)) => {
            spins += 1;
            log_rpq_spin_deadlock!(spins, "try_send_batch spinning on ready_tx", "Full");
            std::thread::yield_now();
          }
          Err(TrySendError::Closed(_)) => break,
          Err(TrySendError::Sent(_)) => unreachable!(),
        }
      }
    }

    audit_slot(&slot, "try_send_batch", || {
      slot.queued_count.load(Ordering::Acquire)
    });
    total_weight
  }

  pub async fn send_batch_mut(&self, items: &mut Vec<T>) -> Result<usize, ZmqError> {
    let slot = self.slot.upgrade().ok_or(ZmqError::ConnectionClosed)?;
    let mut total_sent = 0;

    // SAFETY: this ReadyPipeSender is the pipe's single producer.
    let tx = unsafe { slot.tx.get_mut() };
    while !items.is_empty() {
      // 1. Drain synchronously into the channel until full.
      let sent_this_pass = match tx.try_send_batch_mut(items) {
        Ok(n) => n,
        Err(fibre::SendError::Closed) => return Err(ZmqError::ConnectionClosed),
        Err(fibre::SendError::Sent) => unreachable!(),
      };

      if sent_this_pass > 0 {
        total_sent += sent_this_pass;
        slot.reserved_count.fetch_add(sent_this_pass, Ordering::AcqRel);
        let prev = slot.queued_count.fetch_add(sent_this_pass, Ordering::AcqRel);

        if prev == 0 {
          let mut spins = 0usize;
          loop {
            match self.ready_tx.try_send(Arc::clone(&slot)) {
              Ok(()) => break,
              Err(TrySendError::Full(_)) => {
                spins += 1;
                log_rpq_spin_deadlock!(spins, "send_batch_mut spinning on ready_tx", "Full");
                std::thread::yield_now();
              }
              Err(TrySendError::Closed(_)) => return Err(ZmqError::ConnectionClosed),
              Err(TrySendError::Sent(_)) => unreachable!(),
            }
          }
        }
        audit_slot(&slot, "send_batch_mut_sync_pass", || {
          slot.queued_count.load(Ordering::Acquire)
        });
      }

      if items.is_empty() {
        break;
      }

      // 2. The channel is full. We must yield/wait.
      // Take exactly one item out of the vector to await on.
      let mut temp = vec![items.remove(0)];

      // Micro-guard: if the future is dropped while awaiting, or fails,
      // put the item back into `items` so nothing is lost.
      struct WaitGuard<'a, T> {
        items: &'a mut Vec<T>,
        temp: &'a mut Vec<T>,
      }
      impl<'a, T> Drop for WaitGuard<'a, T> {
        fn drop(&mut self) {
          if !self.temp.is_empty() {
            self.items.insert(0, self.temp.remove(0));
          }
        }
      }

      let guard = WaitGuard {
        items: &mut *items,
        temp: &mut temp,
      };

      // Await space for this single item.
      if tx.send_batch_mut(guard.temp).await.is_err() {
        return Err(ZmqError::ConnectionClosed);
      }

      // Successfully sent. The guard drops here with `temp` empty.
      drop(guard);

      total_sent += 1;
      slot.reserved_count.fetch_add(1, Ordering::AcqRel);
      let prev = slot.queued_count.fetch_add(1, Ordering::AcqRel);

      if prev == 0 {
        let mut spins = 0usize;
        loop {
          match self.ready_tx.try_send(Arc::clone(&slot)) {
            Ok(()) => break,
            Err(TrySendError::Full(_)) => {
              spins += 1;
              log_rpq_spin_deadlock!(spins, "send_batch_mut spinning on ready_tx", "Full");
              std::thread::yield_now();
            }
            Err(TrySendError::Closed(_)) => return Err(ZmqError::ConnectionClosed),
            Err(TrySendError::Sent(_)) => unreachable!(),
          }
        }
      }
      audit_slot(&slot, "send_batch_mut_async_pass", || {
        slot.queued_count.load(Ordering::Acquire)
      });
    }

    Ok(total_sent)
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
    trie: Arc<PrefixMatcher>,
  },
  DirectAddressed {
    sender: ReadyPipeSender<FrameBatch>,
  },
  /// Consumes inbound frames as PUB-side subscription commands (`[0x01|0x00] +
  /// topic`) on the session thread, updating `matcher` for `peer_idx` instead of
  /// queueing. Used by the PUB socket so it can filter on the publisher side.
  SubscriptionSink {
    peer_idx: u32,
    matcher: Arc<SubscriptionMatcher>,
  },
}

/// Applies a single inbound subscription frame body of the form
/// `[0x01|0x00] + topic` to `matcher` for `peer_idx`. `0x01` subscribes, `0x00`
/// unsubscribes; any other/empty body is ignored.
#[inline]
fn apply_subscription_frame(matcher: &SubscriptionMatcher, peer_idx: u32, body: &[u8]) {
  match body.first() {
    Some(0x01) => matcher.subscribe(peer_idx, &body[1..]),
    Some(0x00) => {
      matcher.unsubscribe(peer_idx, &body[1..]);
    }
    _ => {}
  }
}

/// Applies every frame of one logical message as a subscription command,
/// returning the number of frames consumed.
#[inline]
fn apply_subscription_batch(matcher: &SubscriptionMatcher, peer_idx: u32, batch: &FrameBatch) -> usize {
  for frame in batch.iter() {
    apply_subscription_frame(matcher, peer_idx, frame.data().unwrap_or(&[]));
  }
  batch.len()
}

/// Debug-only invariant: one send call = one complete logical message.
///
/// The ingress caches (the anonymous engine's frame cache and the addressed
/// engine's entry cache) reassemble messages by MORE flags, and any future
/// direct-to-pipe transport delivers each send as a standalone message — both
/// depend on a batch never ending mid-message. A batch whose last frame still
/// has MORE set means some egress path split a logical message across sends
/// (the failure mode behind the DEALER "expected empty delimiter" symptom).
/// Empty batches are tolerated (legacy empty-message edge).
#[inline(always)]
fn debug_assert_complete_message(batch: &FrameBatch, site: &str) {
  debug_assert!(
    batch.last().map_or(true, |m| !m.is_more()),
    "PipeMessageSender::{site}: FrameBatch ends with MORE set — logical message split across sends ({} frames)",
    batch.len(),
  );
  #[cfg(not(debug_assertions))]
  let _ = (batch, site);
}

impl PipeMessageSender {
  #[cfg(feature = "io-uring")]
  pub fn bind_uring_wakeup(&self, wakeup: UringWakeup) {
    match self {
      Self::DirectAnonymous(s) => s.bind_uring_wakeup(wakeup),
      Self::FilteredAnonymous { sender, .. } => sender.bind_uring_wakeup(wakeup),
      Self::DirectAddressed { sender } => sender.bind_uring_wakeup(wakeup),
      Self::SubscriptionSink { .. } => {}
    }
  }

  pub async fn send(&self, batch: FrameBatch) -> Result<(), ZmqError> {
    debug_assert_complete_message(&batch, "send");
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
      Self::SubscriptionSink { peer_idx, matcher } => {
        apply_subscription_batch(matcher, *peer_idx, &batch);
        Ok(())
      }
    }
  }

  pub async fn send_batch_mut(&self, items: &mut Vec<FrameBatch>) -> Result<usize, ZmqError> {
    #[cfg(debug_assertions)]
    for batch in items.iter() {
      debug_assert_complete_message(batch, "send_batch_mut");
    }
    match self {
      Self::DirectAnonymous(s) => s.send_batch_mut(items).await,
      Self::DirectAddressed { sender } => sender.send_batch_mut(items).await,
      Self::FilteredAnonymous { sender, trie } => {
        // In-place, zero-allocation filter of the vector before transmitting.
        items.retain(|batch| {
          let topic = batch.first().and_then(|m| m.data()).unwrap_or(&[]);
          trie.matches(topic)
        });

        if items.is_empty() {
          return Ok(0);
        }
        sender.send_batch_mut(items).await
      }
      Self::SubscriptionSink { peer_idx, matcher } => {
        let mut consumed = 0usize;
        for batch in items.drain(..) {
          consumed += apply_subscription_batch(matcher, *peer_idx, &batch);
        }
        Ok(consumed)
      }
    }
  }

  pub fn try_send_sync(&self, batch: FrameBatch) -> Result<(), TrySendError<FrameBatch>> {
    debug_assert_complete_message(&batch, "try_send_sync");
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
      Self::SubscriptionSink { peer_idx, matcher } => {
        apply_subscription_batch(matcher, *peer_idx, &batch);
        Ok(())
      }
    }
  }

  /// Synchronously drains as many `FrameBatch`es from `items` as possible,
  /// applying subscription filtering for `FilteredAnonymous` senders.
  ///
  /// Returns the total frame count consumed (sent + discarded). Backpressured
  /// items remain at the front of `items` in FIFO order.
  pub fn try_send_batch(&self, items: &mut VecDeque<FrameBatch>) -> usize {
    #[cfg(debug_assertions)]
    for batch in items.iter() {
      debug_assert_complete_message(batch, "try_send_batch");
    }
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

        // SAFETY: this sender is the pipe's single producer.
        let tx = unsafe { slot.tx.get_mut() };
        while let Some(item) = items.pop_front() {
          let topic: &[u8] = item.first().and_then(|m| m.data()).unwrap_or(&[]);
          if trie.matches(topic) {
            let frame_count = item.len();
            match tx.try_send(item) {
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
          slot
            .reserved_count
            .fetch_sub(match_count - sent_batches, Ordering::AcqRel);
        }

        if had_zero_transition {
          let mut spins = 0usize;
          loop {
            match sender.ready_tx.try_send(Arc::clone(&slot)) {
              Ok(()) => break,
              Err(TrySendError::Full(_)) => {
                spins += 1;
                log_rpq_spin_deadlock!(spins, "try_send_batch filtered spinning on ready_tx", "Full");
                std::thread::yield_now();
              }
              Err(TrySendError::Closed(_)) => break,
              Err(TrySendError::Sent(_)) => unreachable!(),
            }
          }
        }

        total_frames
      }

      Self::DirectAddressed { sender } => sender.try_send_batch(items, |b| b.len()),

      Self::SubscriptionSink { peer_idx, matcher } => {
        let mut consumed = 0usize;
        while let Some(batch) = items.pop_front() {
          consumed += apply_subscription_batch(matcher, *peer_idx, &batch);
        }
        consumed
      }
    }
  }

  pub fn queued_count(&self) -> usize {
    match self {
      Self::DirectAnonymous(s) => s.queued_count(),
      Self::FilteredAnonymous { sender, .. } => sender.queued_count(),
      Self::DirectAddressed { sender } => sender.queued_count(),
      Self::SubscriptionSink { .. } => 0,
    }
  }

  pub fn reserved_count(&self) -> usize {
    match self {
      Self::DirectAnonymous(s) => s.reserved_count(),
      Self::FilteredAnonymous { sender, .. } => sender.reserved_count(),
      Self::DirectAddressed { sender } => sender.reserved_count(),
      Self::SubscriptionSink { .. } => 0,
    }
  }

  pub fn len(&self) -> usize {
    match self {
      Self::DirectAnonymous(s) => s.len(),
      Self::FilteredAnonymous { sender, .. } => sender.len(),
      Self::DirectAddressed { sender } => sender.len(),
      Self::SubscriptionSink { .. } => 0,
    }
  }

  pub fn capacity(&self) -> usize {
    match self {
      Self::DirectAnonymous(s) => s.capacity(),
      Self::FilteredAnonymous { sender, .. } => sender.capacity(),
      Self::DirectAddressed { sender } => sender.capacity(),
      Self::SubscriptionSink { .. } => 0,
    }
  }

  pub fn is_congested(&self) -> bool {
    match self {
      Self::DirectAnonymous(s) => s.is_congested(),
      Self::FilteredAnonymous { sender, .. } => sender.is_congested(),
      Self::DirectAddressed { sender } => sender.is_congested(),
      Self::SubscriptionSink { .. } => false,
    }
  }

  pub fn is_drained(&self) -> bool {
    match self {
      Self::DirectAnonymous(s) => s.is_drained(),
      Self::FilteredAnonymous { sender, .. } => sender.is_drained(),
      Self::DirectAddressed { sender } => sender.is_drained(),
      Self::SubscriptionSink { .. } => true,
    }
  }
}

impl std::fmt::Debug for PipeMessageSender {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::DirectAnonymous(_) => write!(f, "PipeMessageSender::DirectAnonymous"),
      Self::FilteredAnonymous { .. } => write!(f, "PipeMessageSender::FilteredAnonymous"),
      Self::DirectAddressed { .. } => write!(f, "PipeMessageSender::DirectAddressed"),
      Self::SubscriptionSink { peer_idx, .. } => {
        write!(f, "PipeMessageSender::SubscriptionSink(peer_idx={peer_idx})")
      }
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
            // SAFETY: this thread is the test's sole consumer, so it is the
            // exclusive rx accessor.
            let rx_len = unsafe { slot.rx.get_mut() }.len();
            let has_items = rx_len > 0;
            // reserved_count covers both in-flight and committed messages so
            // a non-zero value means a wakeup signal is guaranteed to arrive.
            let reserved = slot.reserved_count.load(Ordering::Acquire);
            let has_ready_signal = !queue.ready_rx.is_empty();

            if has_items && reserved == 0 && !has_ready_signal {
              println!(
                "\n[LOST WAKEUP] pipe={} rx_len={} reserved={} queued={} ready_rx_len={}",
                pipe_id,
                rx_len,
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
    // SAFETY: producer and consumer tasks have both been joined; this thread
    // is the only remaining accessor.
    assert_eq!(unsafe { slot.rx.get_mut() }.len(), 0, "rx not fully drained");
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
  use crate::Msg;

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
    // SPSC contract: one producer at a time — tasks serialize through the Mutex.
    let sender = Arc::new(tokio::sync::Mutex::new(queue.register_pipe(1, 1, 0)));

    // Fill the pipe so every send blocks.
    sender.lock().await.send(0).await.unwrap();

    let pipes = queue.pipes.read();
    let slot = pipes.get(&1).unwrap().clone();
    drop(pipes);

    // Launch 1000 send futures and immediately cancel every one.
    let mut tasks = Vec::new();
    for i in 1..=1000 {
      let s = sender.clone();
      tasks.push(tokio::spawn(async move {
        let _ = timeout(Duration::from_millis(1), async {
          let _ = s.lock().await.send(i).await;
        })
        .await;
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
    // SPSC contract: one producer at a time — tasks serialize through the Mutex.
    let sender = Arc::new(tokio::sync::Mutex::new(queue.register_pipe(1, 4, 0)));

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
            let _ = timeout(Duration::from_micros(10), async {
              let _ = s.lock().await.send(seq).await;
            })
            .await;
          } else {
            let _ = s.lock().await.send(seq).await;
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

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_exact_rzmq_ready_pipe_queue_uaf_crash() {
    println!("\n--- STARTING DETERMINISTIC READY_PIPE_QUEUE CRASH TEST ---");

    let queue = Arc::new(ReadyPipeQueue::<FrameBatch>::new(10));

    // SPSC contract: one producer at a time per pipe — the chaos tasks
    // serialize access to each pipe's sender through its Mutex.
    let mut senders = Vec::new();
    for i in 0..4 {
      senders.push(Arc::new(tokio::sync::Mutex::new(queue.register_pipe(i, 1, 0))));
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    for t_id in 0..20 {
      let senders_clone = senders.clone();
      let stop_clone = stop.clone();

      handles.push(tokio::spawn(async move {
        let mut seq = t_id * 10000;
        let mut rng = u64::wrapping_mul(seq as u64, 0x9E37_79B9_7F4A_7C15);

        while !stop_clone.load(Ordering::Relaxed) {
          rng = rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
          let target_pipe = (rng % 4) as usize;
          let sender = &senders_clone[target_pipe];

          let mut batch = FrameBatch::new();
          batch.push(Msg::from_static(b"chaos-data"));

          let timeout_us = 10 + (rng % 150);
          let _ = timeout(Duration::from_micros(timeout_us), async {
            let _ = sender.lock().await.send(batch).await;
          })
          .await;

          seq += 1;
          tokio::task::yield_now().await;
        }
      }));
    }

    for t_id in 0..20 {
      let queue_clone = queue.clone();
      let stop_clone = stop.clone();

      handles.push(tokio::spawn(async move {
        let mut rng = u64::wrapping_mul((t_id + 100) as u64, 0x9E37_79B9_7F4A_7C15);

        while !stop_clone.load(Ordering::Relaxed) {
          rng = rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
          let timeout_us = 10 + (rng % 150);

          let _ = timeout(Duration::from_micros(timeout_us), queue_clone.pop()).await;
          tokio::task::yield_now().await;
        }
      }));
    }

    tokio::time::sleep(Duration::from_secs(10)).await;
    println!("[SYS] Stopping tasks...");
    stop.store(true, Ordering::SeqCst);

    for h in handles {
      let _ = h.await;
    }

    println!("--- REPRO COMPLETED SUCCESSFULLY (No Segfault occurred) ---");
  }
}
