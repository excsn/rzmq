use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::patterns::sub_matcher::SubscriptionMatcher;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use xs_foundation::collections::map::VecMapU32;

thread_local! {
  /// Reusable per-send buffer of matched `(peer_idx, connection)` targets. Kept
  /// thread-local so the hot send path allocates nothing per message (the vast
  /// majority of PUB sends reuse this) while remaining safe under concurrent
  /// sends from different threads. Only touched at synchronous points — never
  /// held across an `.await`.
  static SEND_TARGETS: RefCell<Vec<(u32, Arc<dyn ISocketConnection>)>> = RefCell::new(Vec::new());
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
      self.by_uri.remove(&slot.uri);
      self.by_pipe.remove(&slot.pipe_read_id);
      self.free.push(peer_idx);
    }
  }
}

/// PUB-side peer registry and matched fan-out.
///
/// Peers are registered at pipe attach/detach and addressed by a stable
/// `peer_idx`. The per-message send path ([`Distributor::send_matched_multipart`])
/// walks a [`SubscriptionMatcher`] to find the interested peers and enqueues the
/// message only to those — no broadcast, no per-message URI cloning.
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

  /// Sends one logical ZMQ message (one or more ZMTP frames) to every peer whose
  /// subscription (held in `matcher`) is a prefix of the message topic (the first
  /// frame's data).
  ///
  /// The synchronous `try_send_multipart_owned_sync` is the fast path; the boxed
  /// async send only runs for backpressured peers, preserving SNDTIMEO
  /// block/timeout semantics. HWM/timeout drops the message for that peer
  /// (standard PUB behavior); closed/errored peers are returned by index for
  /// removal by the caller.
  pub async fn send_matched_multipart(
    &self,
    zmtp_frames: FrameBatch,
    matcher: &SubscriptionMatcher,
    core_handle: usize,
  ) -> Result<(), Vec<(u32, ZmqError)>> {
    if zmtp_frames.is_empty() {
      return Ok(());
    }

    // Phase 1: collect the deduplicated set of interested peer connections into
    // the reusable thread-local buffer while holding only read locks; release
    // them before doing any (async) sends.
    let generation = self.send_gen.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    let mut targets = SEND_TARGETS.with(|t| std::mem::take(&mut *t.borrow_mut()));
    targets.clear();
    {
      let topic: &[u8] = zmtp_frames.first().and_then(|m| m.data()).unwrap_or(&[]);
      let inner = self.inner.read();
      if !inner.peers.is_empty() {
        matcher.for_each_match(topic, |idx| {
          if let Some(slot) = inner.peers.get(&idx) {
            // First visit this generation wins; later prefix matches are skipped.
            if slot.match_stamp.swap(generation, Ordering::Relaxed) != generation {
              targets.push((idx, slot.conn.clone()));
            }
          }
        });
      }
    }

    if targets.is_empty() {
      // Return the (empty) buffer to the thread-local for reuse.
      SEND_TARGETS.with(|t| *t.borrow_mut() = targets);
      return Ok(());
    }

    // Phase 2: fan out. The last target takes the original batch; earlier peers
    // get clones (Bytes refcounts). With a single subscriber this never clones.
    // `Vec::new()` here does not allocate until a peer actually errors.
    let mut failed: Vec<(u32, ZmqError)> = Vec::new();
    let last = targets.len() - 1;
    let mut original = Some(zmtp_frames);

    for (i, (idx, conn)) in targets.iter().enumerate() {
      let batch = if i == last {
        original.take().expect("original batch consumed early")
      } else {
        original.as_ref().expect("original batch present").clone()
      };

      match conn.try_send_multipart_owned_sync(batch) {
        Ok(()) => {}
        Err((returned, ZmqError::ResourceLimitReached)) => match conn.send_multipart_owned(returned).await {
          Ok(()) => {}
          Err((_, ZmqError::ResourceLimitReached)) | Err((_, ZmqError::Timeout)) => {
            tracing::trace!(
              handle = core_handle, peer_idx = *idx,
              "PUB (Distributor) dropping message due to HWM/Timeout for peer"
            );
          }
          Err((_, e @ ZmqError::ConnectionClosed)) => {
            tracing::debug!(handle = core_handle, peer_idx = *idx, "PUB (Distributor) peer disconnected during send");
            failed.push((*idx, e));
          }
          Err((_, e)) => {
            tracing::error!(handle = core_handle, peer_idx = *idx, error = %e, "PUB (Distributor) send encountered unexpected error");
            failed.push((*idx, e));
          }
        },
        Err((_, e @ ZmqError::ConnectionClosed)) => {
          tracing::debug!(handle = core_handle, peer_idx = *idx, "PUB (Distributor) peer disconnected during send");
          failed.push((*idx, e));
        }
        Err((_, e)) => {
          tracing::error!(handle = core_handle, peer_idx = *idx, error = %e, "PUB (Distributor) send encountered unexpected error");
          failed.push((*idx, e));
        }
      }
    }

    // Return the drained buffer for reuse (its capacity is retained). If the task
    // migrated across `.await`, this restores it to the resuming thread's slot,
    // which is still correct — just a one-off buffer move.
    targets.clear();
    SEND_TARGETS.with(|t| *t.borrow_mut() = targets);

    if failed.is_empty() {
      Ok(())
    } else {
      Err(failed)
    }
  }
}
