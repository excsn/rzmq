use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::patterns::pipe_credits::PipeCredits;
use crate::socket::patterns::sub_matcher::SubscriptionMatcher;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use xs_foundation::collections::map::VecMapU32;

thread_local! {
  /// Reusable per-send buffer of matched targets. Kept thread-local so the hot
  /// send path allocates nothing per message while remaining safe under
  /// concurrent sends from different threads. Never held across an `.await`.
  static SEND_TARGETS: RefCell<Vec<SendTarget>> = RefCell::new(Vec::new());
}

struct SendTarget {
  peer_idx: u32,
  conn: Arc<dyn ISocketConnection>,
  /// Whether the message goes to this peer: it holds a credit, or its pipe is
  /// uncredited and gets a plain `try_send`.
  send: bool,
}

#[derive(Default)]
pub(crate) struct SendOutcome {
  /// Peers that were closed or errored, by index, for removal by the caller.
  pub failed: Vec<(u32, ZmqError)>,
  /// At least one matched peer missed the message because its pipe was full.
  pub dropped: bool,
}

/// An all-or-nothing send found a matched peer with no free slot. Nothing was
/// sent; the caller gets the message back and waits on that peer's credits.
pub(crate) struct Blocked {
  pub frames: FrameBatch,
  pub credits: Arc<PipeCredits>,
}

/// One connected subscriber peer. Addressed by a small stable `peer_idx` (the
/// `VecMapU32` key) so the [`SubscriptionMatcher`] can reference peers cheaply.
///
/// `match_stamp` is a per-peer generation marker used to de-duplicate a peer
/// within a single send when it subscribed to several prefixes of the topic.
#[derive(Debug)]
struct PeerSlot {
  pipe_read_id: usize,
  uri: String,
  conn: Arc<dyn ISocketConnection>,
  match_stamp: AtomicU64,
}

#[derive(Debug, Default)]
struct DistributorInner {
  /// `peer_idx -> PeerSlot`.
  peers: VecMapU32<PeerSlot>,
  by_uri: HashMap<String, u32>,
  by_pipe: HashMap<usize, u32>,
  /// Recycled peer indices, reused before growing `next_idx`.
  free: Vec<u32>,
  next_idx: u32,
}

impl DistributorInner {
  fn alloc_idx(&mut self) -> u32 {
    if let Some(idx) = self.free.pop() {
      idx
    } else {
      let idx = self.next_idx;
      self.next_idx += 1;
      idx
    }
  }

  /// Removes the slot for `peer_idx`, clearing its maps and recycling the index.
  fn remove_idx(&mut self, peer_idx: u32) {
    if let Some(slot) = self.peers.remove(&peer_idx) {
      if let Some(credits) = slot.conn.pipe_credits() {
        credits.close();
      }
      self.by_uri.remove(&slot.uri);
      self.by_pipe.remove(&slot.pipe_read_id);
      self.free.push(peer_idx);
    }
  }
}

/// PUB-side peer registry and matched fan-out.
///
/// Peers are registered at pipe attach/detach and addressed by a stable
/// `peer_idx`. The per-message send paths ([`Distributor::send_matched`] and
/// [`Distributor::try_send_matched_all`]) walk a [`SubscriptionMatcher`] to find
/// the interested peers and enqueue the message only to those.
#[derive(Debug, Default)]
pub(crate) struct Distributor {
  inner: RwLock<DistributorInner>,
  /// Monotonic generation source for per-send de-duplication.
  send_gen: AtomicU64,
}

impl Distributor {
  pub fn new() -> Self {
    Self::default()
  }

  /// Registers (or returns the existing index for) a peer connection. Idempotent
  /// per URI. Returns the peer's stable `peer_idx`.
  pub fn add_peer(
    &self,
    pipe_read_id: usize,
    endpoint_uri: String,
    conn: Arc<dyn ISocketConnection>,
  ) -> u32 {
    let mut inner = self.inner.write();
    if let Some(&idx) = inner.by_uri.get(&endpoint_uri) {
      return idx;
    }
    let idx = inner.alloc_idx();
    inner.by_uri.insert(endpoint_uri.clone(), idx);
    inner.by_pipe.insert(pipe_read_id, idx);
    inner.peers.insert(
      idx,
      PeerSlot {
        pipe_read_id,
        uri: endpoint_uri,
        conn,
        match_stamp: AtomicU64::new(0),
      },
    );
    idx
  }

  /// Removes the peer attached on `pipe_read_id`, returning its freed `peer_idx`
  /// so the caller can purge it from the [`SubscriptionMatcher`].
  pub fn remove_peer_by_pipe(&self, pipe_read_id: usize) -> Option<u32> {
    let mut inner = self.inner.write();
    let idx = inner.by_pipe.get(&pipe_read_id).copied()?;
    inner.remove_idx(idx);
    Some(idx)
  }

  /// Removes a peer by index (used for send-path failure cleanup).
  pub fn remove_peer_by_idx(&self, peer_idx: u32) {
    self.inner.write().remove_idx(peer_idx);
  }

  /// Sends one logical ZMQ message to every peer whose subscription (held in
  /// `matcher`) is a prefix of the topic, the first frame's data. A peer with no
  /// free slot misses this message.
  pub fn send_matched(
    &self,
    zmtp_frames: FrameBatch,
    matcher: &SubscriptionMatcher,
    core_handle: usize,
  ) -> SendOutcome {
    if zmtp_frames.is_empty() {
      return SendOutcome::default();
    }
    let mut targets = self.collect_targets(&zmtp_frames, matcher);
    for target in targets.iter_mut() {
      target.send = target.conn.pipe_credits().map_or(true, |c| c.try_acquire());
    }
    let outcome = commit(&targets, zmtp_frames, core_handle);
    recycle_targets(targets);
    outcome
  }

  /// Sends one logical ZMQ message to every matched peer or to none of them.
  /// When a matched peer has no free slot, every credit taken so far is
  /// returned and the message comes back in [`Blocked`].
  pub fn try_send_matched_all(
    &self,
    zmtp_frames: FrameBatch,
    matcher: &SubscriptionMatcher,
    core_handle: usize,
  ) -> Result<SendOutcome, Blocked> {
    if zmtp_frames.is_empty() {
      return Ok(SendOutcome::default());
    }
    let mut targets = self.collect_targets(&zmtp_frames, matcher);
    for i in 0..targets.len() {
      let Some(credits) = targets[i].conn.pipe_credits() else {
        targets[i].send = true;
        continue;
      };
      if credits.try_acquire() {
        targets[i].send = true;
        continue;
      }
      if credits.is_closed() {
        targets[i].send = false;
        continue;
      }
      let blocked = Arc::clone(credits);
      for taken in &targets[..i] {
        if taken.send {
          if let Some(c) = taken.conn.pipe_credits() {
            c.release(1);
          }
        }
      }
      recycle_targets(targets);
      return Err(Blocked {
        frames: zmtp_frames,
        credits: blocked,
      });
    }
    let outcome = commit(&targets, zmtp_frames, core_handle);
    recycle_targets(targets);
    Ok(outcome)
  }

  /// Collects the deduplicated set of interested peers into the reusable
  /// thread-local buffer, holding only read locks.
  fn collect_targets(&self, zmtp_frames: &FrameBatch, matcher: &SubscriptionMatcher) -> Vec<SendTarget> {
    let generation = self.send_gen.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    let mut targets = SEND_TARGETS.with(|t| std::mem::take(&mut *t.borrow_mut()));
    targets.clear();
    let topic: &[u8] = zmtp_frames.first().and_then(|m| m.data()).unwrap_or(&[]);
    let inner = self.inner.read();
    if !inner.peers.is_empty() {
      matcher.for_each_match(topic, |idx| {
        if let Some(slot) = inner.peers.get(&idx) {
          // First visit this generation wins; later prefix matches are skipped.
          if slot.match_stamp.swap(generation, Ordering::Relaxed) != generation {
            targets.push(SendTarget {
              peer_idx: idx,
              conn: slot.conn.clone(),
              send: false,
            });
          }
        }
      });
    }
    targets
  }
}

/// Enqueues the message on every target marked `send`. The last such target
/// takes the original batch; earlier ones get clones (Bytes refcounts).
fn commit(targets: &[SendTarget], zmtp_frames: FrameBatch, core_handle: usize) -> SendOutcome {
  let mut outcome = SendOutcome::default();
  let Some(last) = targets.iter().rposition(|t| t.send) else {
    outcome.dropped = !targets.is_empty();
    return outcome;
  };
  let mut original = Some(zmtp_frames);

  for (i, target) in targets.iter().enumerate() {
    if !target.send {
      outcome.dropped = true;
      tracing::trace!(
        handle = core_handle,
        peer_idx = target.peer_idx,
        "PUB (Distributor) dropping message for peer at HWM"
      );
      continue;
    }
    let batch = if i == last {
      original.take().expect("original batch consumed early")
    } else {
      original.as_ref().expect("original batch present").clone()
    };

    match target.conn.try_send_multipart_owned_sync(batch) {
      Ok(()) => {}
      Err((_, ZmqError::ResourceLimitReached)) => {
        debug_assert!(
          target.conn.pipe_credits().is_none(),
          "a credited pipe was full"
        );
        if let Some(credits) = target.conn.pipe_credits() {
          credits.release(1);
        }
        outcome.dropped = true;
        tracing::trace!(
          handle = core_handle,
          peer_idx = target.peer_idx,
          "PUB (Distributor) dropping message for peer at HWM"
        );
      }
      Err((_, e @ ZmqError::ConnectionClosed)) => {
        tracing::debug!(handle = core_handle, peer_idx = target.peer_idx, "PUB (Distributor) peer disconnected during send");
        outcome.failed.push((target.peer_idx, e));
      }
      Err((_, e)) => {
        tracing::error!(handle = core_handle, peer_idx = target.peer_idx, error = %e, "PUB (Distributor) send encountered unexpected error");
        outcome.failed.push((target.peer_idx, e));
      }
    }
  }
  outcome
}

fn recycle_targets(mut targets: Vec<SendTarget>) {
  targets.clear();
  SEND_TARGETS.with(|t| *t.borrow_mut() = targets);
}
