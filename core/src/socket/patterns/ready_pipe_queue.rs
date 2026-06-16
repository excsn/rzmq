use std::collections::HashMap;
use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use parking_lot::RwLock;

use fibre::mpmc::{AsyncReceiver, AsyncSender, bounded_async};
use fibre::{RecvError, TryRecvError, TrySendError};

use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::socket::patterns::trie::SubscriptionTrie;

#[cfg(feature = "io-uring")]
use crate::io_uring_backend::ops::{WAKEUP_STATE_SIGNALED, WAKEUP_STATE_SLEEPING};

// ---------------------------------------------------------------------------
// io_uring direct-wakeup payload — lets a draining consumer signal the
// io_uring worker OS thread directly via eventfd, bypassing the SocketCore
// mailbox / ResumeConnection round-trip.
// ---------------------------------------------------------------------------

#[cfg(feature = "io-uring")]
#[derive(Clone)]
pub(crate) struct UringWakeup {
  pub event_fd: eventfd::EventFD,
  pub worker_asleep: Arc<std::sync::atomic::AtomicU8>,
}

// ---------------------------------------------------------------------------
// Per-pipe slot stored inside ReadyPipeQueue
// ---------------------------------------------------------------------------

struct PipeSlot<T: Send + 'static> {
  tx: AsyncSender<T>,
  rx: AsyncReceiver<T>,
  /// True while at least one activation signal is in flight for this pipe.
  /// Prevents duplicate signals from flooding the activation queue.
  is_activated: Arc<AtomicBool>,
  #[cfg(feature = "io-uring")]
  uring_wakeup: Arc<parking_lot::Mutex<Option<UringWakeup>>>,
}

impl<T: Send + 'static> PipeSlot<T> {
  pub fn len(&self) -> usize {
    self.rx.len()
  }

  pub fn capacity(&self) -> usize {
    self.rx.capacity().unwrap_or(usize::MAX)
  }

  /// Centralized check: is the pipe congested (HWM)?
  pub fn is_congested(&self) -> bool {
    self.len() >= self.capacity()
  }

  /// Centralized check: has the pipe drained below the LWM (50% capacity)?
  pub fn is_drained(&self) -> bool {
    self.len() < self.capacity() / 2
  }
}

// ---------------------------------------------------------------------------
// ReadyPipeQueue — consumer side
// ---------------------------------------------------------------------------

pub(crate) struct ReadyPipeQueue<T: Send + 'static> {
  pipes: Arc<RwLock<HashMap<usize, PipeSlot<T>>>>,
  activation_rx: AsyncReceiver<usize>,
  activation_tx: AsyncSender<usize>,
}

impl<T: Send + 'static> ReadyPipeQueue<T> {
  /// `activation_capacity` — maximum number of in-flight activation signals.
  /// With the `is_activated` flag, each pipe emits at most one signal per
  /// "burst", so capacity = expected peak concurrent active connections.
  pub fn new(activation_capacity: usize) -> Self {
    let (atx, arx) = bounded_async(activation_capacity.max(1));
    Self {
      pipes: Arc::new(RwLock::new(HashMap::new())),
      activation_rx: arx,
      activation_tx: atx,
    }
  }

  /// Register a new connection pipe and return its producer handle.
  ///
  /// Idempotent: if `pipe_id` is already registered, returns a new sender
  /// that shares the existing channel rather than overwriting it.
  pub fn register_pipe(&self, pipe_id: usize, capacity: usize) -> ReadyPipeSender<T> {
    let mut pipes = self.pipes.write();
    if let Some(slot) = pipes.get(&pipe_id) {
      return ReadyPipeSender {
        sender: slot.tx.clone(),
        activation_tx: self.activation_tx.clone(),
        pipe_id,
        is_activated: Arc::clone(&slot.is_activated),
        #[cfg(feature = "io-uring")]
        uring_wakeup: Arc::clone(&slot.uring_wakeup),
      };
    }
    let (tx, rx) = bounded_async(capacity.max(1));
    let is_activated = Arc::new(AtomicBool::new(false));
    #[cfg(feature = "io-uring")]
    let uring_wakeup = Arc::new(parking_lot::Mutex::new(None));
    pipes.insert(
      pipe_id,
      PipeSlot {
        tx: tx.clone(),
        rx,
        is_activated: Arc::clone(&is_activated),
        #[cfg(feature = "io-uring")]
        uring_wakeup: Arc::clone(&uring_wakeup),
      },
    );
    ReadyPipeSender {
      sender: tx,
      activation_tx: self.activation_tx.clone(),
      pipe_id,
      is_activated,
      #[cfg(feature = "io-uring")]
      uring_wakeup,
    }
  }

  /// Remove a connection pipe. Stale activation signals for this pipe are
  /// silently skipped inside `pop()`.
  pub fn deregister_pipe(&self, pipe_id: usize) {
    self.pipes.write().remove(&pipe_id);
  }

  /// Block until any pipe has a complete message ready, then return it with
  /// round-robin fairness across concurrent pipes.
  pub async fn pop(&self) -> Result<(usize, T), ZmqError> {
    loop {
      let pipe_id = match self.activation_rx.recv().await {
        Ok(id) => id,
        Err(RecvError::Disconnected) => {
          return Err(ZmqError::InvalidState("activation channel closed"));
        }
      };

      // Acquire a read-lock only for the duration of the try_recv; the
      // lock is released before we return so the next iteration can write.
      let result = {
        let pipes = self.pipes.read();
        match pipes.get(&pipe_id) {
          None => None, // pipe deregistered; stale activation — skip
          Some(slot) => match slot.rx.try_recv() {
            Ok(item) => {
              if slot.rx.is_empty() {
                // No more items: allow the next send to re-activate.
                slot.is_activated.store(false, Ordering::Release);
              } else {
                // More items remain: re-queue for round-robin.
                let _ = self.activation_tx.try_send(pipe_id);
              }

              #[cfg(feature = "io-uring")]
              {
                if slot.is_drained() {
                  if let Some(wakeup) = &*slot.uring_wakeup.lock() {
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
              }

              Some(item)
            }
            Err(TryRecvError::Empty) => {
              // Stale signal (rare race between dequeue and store).
              slot.is_activated.store(false, Ordering::Release);
              None
            }
            Err(TryRecvError::Disconnected) => None,
          },
        }
      };

      if let Some(item) = result {
        return Ok((pipe_id, item));
      }
    }
  }

  /// Non-blocking pop: returns `Some((pipe_id, item))` if a message is immediately
  /// available, or `None` if no pipe is currently active.
  pub fn try_pop(&self) -> Option<(usize, T)> {
    loop {
      let pipe_id = match self.activation_rx.try_recv() {
        Ok(id) => id,
        Err(_) => return None,
      };

      let pipes = self.pipes.read();
      match pipes.get(&pipe_id) {
        None => continue, // stale signal for deregistered pipe
        Some(slot) => match slot.rx.try_recv() {
          Ok(item) => {
            if slot.rx.is_empty() {
              slot.is_activated.store(false, Ordering::Release);
            } else {
              let _ = self.activation_tx.try_send(pipe_id);
            }

            #[cfg(feature = "io-uring")]
            {
              if slot.is_drained() {
                if let Some(wakeup) = &*slot.uring_wakeup.lock() {
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
            }

            return Some((pipe_id, item));
          }
          Err(TryRecvError::Empty) => {
            slot.is_activated.store(false, Ordering::Release);
            continue;
          }
          Err(TryRecvError::Disconnected) => continue,
        },
      }
    }
  }

  /// Close the queue on socket shutdown.
  ///
  /// Drops all per-pipe receivers so that any actor blocked in
  /// `ReadyPipeSender::send().await` gets an immediate error and can exit.
  /// Also closes the activation channel so pending `pop()` calls return an error.
  pub fn close(&self) {
    self.pipes.write().clear();
    self.activation_tx.close();
  }
}

// ---------------------------------------------------------------------------
// ReadyPipeSender — producer side (one per connection actor)
// ---------------------------------------------------------------------------

pub(crate) struct ReadyPipeSender<T: Send + 'static> {
  pub(crate) sender: AsyncSender<T>,
  activation_tx: AsyncSender<usize>,
  pipe_id: usize,
  is_activated: Arc<AtomicBool>,
  #[cfg(feature = "io-uring")]
  uring_wakeup: Arc<parking_lot::Mutex<Option<UringWakeup>>>,
}

impl<T: Send + 'static> ReadyPipeSender<T> {
  #[cfg(feature = "io-uring")]
  pub fn bind_uring_wakeup(&self, wakeup: UringWakeup) {
    *self.uring_wakeup.lock() = Some(wakeup);
  }

  /// Send one item. Blocks if the per-pipe queue is at capacity (rcvhwm),
  /// propagating backpressure to the TCP receive buffer of the connection.
  pub async fn send(&self, item: T) -> Result<(), ZmqError> {
    match self.sender.try_send(item) {
      Ok(()) => {}
      Err(TrySendError::Full(returned)) => {
        self
          .sender
          .send(returned)
          .await
          .map_err(|_| ZmqError::ConnectionClosed)?;
      }
      Err(TrySendError::Closed(_)) => return Err(ZmqError::ConnectionClosed),
      Err(TrySendError::Sent(_)) => unreachable!(),
    }

    // Signal the consumer only when transitioning from quiet to active.
    if !self.is_activated.swap(true, Ordering::AcqRel) {
      let _ = self.activation_tx.send(self.pipe_id).await;
    }

    Ok(())
  }

  /// Non-blocking send. Returns `Err(TrySendError::Full)` if the pipe queue is at capacity.
  /// Safe to call from a non-async context (e.g., the io_uring worker OS thread).
  pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
    self.sender.try_send(item)?;
    // Signal the consumer only when transitioning from quiet to active.
    if !self.is_activated.swap(true, Ordering::AcqRel) {
      if self.activation_tx.try_send(self.pipe_id).is_err() {
        // Activation channel full — reset flag so the next send can retry.
        self.is_activated.store(false, Ordering::Release);
      }
    }
    Ok(())
  }

  pub fn len(&self) -> usize {
    self.sender.len()
  }

  pub fn capacity(&self) -> usize {
    self.sender.capacity().unwrap_or(usize::MAX)
  }

  pub fn is_congested(&self) -> bool {
    self.len() >= self.capacity()
  }

  pub fn is_drained(&self) -> bool {
    self.len() < self.capacity() / 2
  }
}

// ---------------------------------------------------------------------------
// PipeMessageSender — enum unifying plain and subscription-filtered senders
// ---------------------------------------------------------------------------

/// The producer handle stored in a session actor for its ingress pipe.
///
/// `Direct` is used by all non-filtering sockets (PULL, DEALER, REQ, REP, ROUTER).
/// `Filtered` is used by SUB: the subscription trie is consulted inline before
/// forwarding so that non-matching messages are dropped at the producer, keeping
/// per-pipe HWM accounting limited to messages the application will actually see.
pub(crate) enum PipeMessageSender {
  Direct(ReadyPipeSender<FrameBatch>),
  Filtered {
    inner: ReadyPipeSender<FrameBatch>,
    trie: Arc<SubscriptionTrie>,
  },
}

impl PipeMessageSender {
  #[cfg(feature = "io-uring")]
  pub fn bind_uring_wakeup(&mut self, wakeup: UringWakeup) {
    match self {
      Self::Direct(s) => s.bind_uring_wakeup(wakeup),
      Self::Filtered { inner, .. } => inner.bind_uring_wakeup(wakeup),
    }
  }

  pub async fn send(&self, batch: FrameBatch) -> Result<(), ZmqError> {
    match self {
      Self::Direct(s) => s.send(batch).await,
      Self::Filtered { inner, trie } => {
        let topic: &[u8] = batch.first().and_then(|m| m.data()).unwrap_or(&[]);
        if trie.matches(topic) {
          inner.send(batch).await
        } else {
          Ok(()) // drop non-matching message; no HWM cost
        }
      }
    }
  }

  /// Non-blocking send, safe to call from the io_uring worker OS thread.
  /// Returns `Err(TrySendError::Full)` if the per-pipe queue is at capacity (HWM).
  pub fn try_send_sync(&self, batch: FrameBatch) -> Result<(), TrySendError<FrameBatch>> {
    match self {
      Self::Direct(s) => s.try_send(batch),
      Self::Filtered { inner, trie } => {
        let topic: &[u8] = batch.first().and_then(|m| m.data()).unwrap_or(&[]);
        if trie.matches(topic) {
          inner.try_send(batch)
        } else {
          Ok(()) // subscription filter: drop silently, no HWM cost
        }
      }
    }
  }

  pub fn len(&self) -> usize {
    match self {
      Self::Direct(s) => s.len(),
      Self::Filtered { inner, .. } => inner.len(),
    }
  }

  pub fn capacity(&self) -> usize {
    match self {
      Self::Direct(s) => s.capacity(),
      Self::Filtered { inner, .. } => inner.capacity(),
    }
  }

  pub fn is_congested(&self) -> bool {
    match self {
      Self::Direct(s) => s.is_congested(),
      Self::Filtered { inner, .. } => inner.is_congested(),
    }
  }

  pub fn is_drained(&self) -> bool {
    match self {
      Self::Direct(s) => s.is_drained(),
      Self::Filtered { inner, .. } => inner.is_drained(),
    }
  }
}

impl std::fmt::Debug for PipeMessageSender {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Direct(_) => write!(f, "PipeMessageSender::Direct"),
      Self::Filtered { .. } => write!(f, "PipeMessageSender::Filtered"),
    }
  }
}
