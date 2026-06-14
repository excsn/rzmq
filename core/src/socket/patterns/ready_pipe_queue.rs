use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use parking_lot::RwLock;

use fibre::mpmc::{bounded_async, AsyncReceiver, AsyncSender};
use fibre::{RecvError, TryRecvError, TrySendError};

use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::socket::patterns::trie::SubscriptionTrie;

// ---------------------------------------------------------------------------
// Per-pipe slot stored inside ReadyPipeQueue
// ---------------------------------------------------------------------------

struct PipeSlot<T: Send + 'static> {
    tx: AsyncSender<T>,
    rx: AsyncReceiver<T>,
    /// True while at least one activation signal is in flight for this pipe.
    /// Prevents duplicate signals from flooding the activation queue.
    is_activated: Arc<AtomicBool>,
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
    /// Idempotent: if `pipe_id` is already registered (e.g., called again by
    /// `event_processor` on `PeerIdentityEstablished`), returns a new sender
    /// that shares the existing channel rather than overwriting it.
    pub fn register_pipe(&self, pipe_id: usize, capacity: usize) -> ReadyPipeSender<T> {
        let mut pipes = self.pipes.write();
        if let Some(slot) = pipes.get(&pipe_id) {
            return ReadyPipeSender {
                sender: slot.tx.clone(),
                activation_tx: self.activation_tx.clone(),
                pipe_id,
                is_activated: Arc::clone(&slot.is_activated),
            };
        }
        let (tx, rx) = bounded_async(capacity.max(1));
        let is_activated = Arc::new(AtomicBool::new(false));
        pipes.insert(pipe_id, PipeSlot {
            tx: tx.clone(),
            rx,
            is_activated: Arc::clone(&is_activated),
        });
        ReadyPipeSender {
            sender: tx,
            activation_tx: self.activation_tx.clone(),
            pipe_id,
            is_activated,
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
                Err(RecvError::Disconnected) => return Err(ZmqError::InvalidState("activation channel closed")),
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
                                // try_send is safe here because we just freed one slot
                                // by consuming the activation signal above.
                                let _ = self.activation_tx.try_send(pipe_id);
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
    sender: AsyncSender<T>,
    activation_tx: AsyncSender<usize>,
    pipe_id: usize,
    is_activated: Arc<AtomicBool>,
}

impl<T: Send + 'static> ReadyPipeSender<T> {
    /// Send one item. Blocks if the per-pipe queue is at capacity (rcvhwm),
    /// propagating backpressure to the TCP receive buffer of the connection.
    pub async fn send(&self, item: T) -> Result<(), ZmqError> {
        match self.sender.try_send(item) {
            Ok(()) => {}
            Err(TrySendError::Full(returned)) => {
                self.sender
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
    pub async fn send(&self, batch: FrameBatch) -> Result<(), ZmqError> {
        match self {
            Self::Direct(s) => s.send(batch).await,
            Self::Filtered { inner, trie } => {
                let topic: &[u8] = batch
                    .first()
                    .and_then(|m| m.data())
                    .unwrap_or(&[]);
                if trie.matches(topic).await {
                    inner.send(batch).await
                } else {
                    Ok(()) // drop non-matching message; no HWM cost
                }
            }
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
