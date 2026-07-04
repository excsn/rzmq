//! Publisher-side subscription matcher.
//!
//! A radix (path-compressed) prefix trie held in a `Vec` arena behind a single
//! `parking_lot::RwLock`, mapping each subscribed topic prefix to the **set of
//! subscribed peers**. This is the PUB-side equivalent of libzmq's `mtrie_t`:
//! on `zmq_send`, the message topic is matched once to find every interested
//! peer, and the message is enqueued only to those peers.
//!
//! Design notes:
//! - **Reads (matching) dominate**, so the hot path ([`SubscriptionMatcher::for_each_match`])
//!   takes a single shared read lock and walks the arena by `u32` index — no
//!   per-node atomics, no per-node locks, no hashing. Path compression keeps the
//!   number of hops (and `memcmp`s) small.
//! - **Writes stay `O(topic-len)`** (in-place mutation under the write lock), so
//!   subscription churn does not trigger any whole-structure copy.
//! - Peers are referenced by a small stable `peer_idx: u32` allocated by the
//!   [`Distributor`](super::distributor::Distributor); the matcher never owns the
//!   connection, only the index.
//!
//! Matching semantics are ZMQ prefix matching: a subscription `S` matches a
//! message topic `T` iff `S` is a prefix of `T`. The empty subscription (`""`)
//! terminates at the root node, so it matches everything.

use parking_lot::RwLock;
use xs_foundation::collections::thin_map::ThinMapU32;

/// The set of peers subscribed at a node: `peer_idx -> refcount`. The refcount
/// mirrors libzmq's per-pipe subscription counting so N `subscribe` calls for
/// the same (peer, topic) require N `unsubscribe` calls to clear.
///
/// Backed by [`ThinMapU32`] (a tightly-packed, memory-efficient sorted map for
/// `u32` keys). It is wrapped in this newtype so we can opt back into
/// `Send`/`Sync`.
///
/// SAFETY: `ThinMapU32` is `!Send`/`!Sync` only because it stores a raw
/// `NonNull` (the conservative default for raw pointers). It otherwise owns its
/// heap allocation exclusively — `Drop` frees it, there is no shared ownership
/// or interior mutability, and every `&self` method (`get`, `iter`, `len`) is
/// read-only while all mutation goes through `&mut self`. That makes it
/// semantically equivalent to `Box<[KV<u32>]>`, which is `Send + Sync` when its
/// element type is; `u32` is. Concurrent access is additionally serialized by
/// the enclosing `RwLock` in [`SubscriptionMatcher`].
#[derive(Debug, Default)]
struct PeerSet(ThinMapU32<u32>);

unsafe impl Send for PeerSet {}
unsafe impl Sync for PeerSet {}

impl PeerSet {
  #[inline]
  fn add(&mut self, peer_idx: u32) {
    match self.0.get_mut(&peer_idx) {
      Some(rc) => *rc += 1,
      None => {
        self.0.insert(peer_idx, 1);
      }
    }
  }

  /// Decrements the refcount, removing the peer at zero. Returns `true` when the
  /// peer's subscription was fully removed.
  #[inline]
  fn remove_one(&mut self, peer_idx: u32) -> bool {
    match self.0.get_mut(&peer_idx) {
      Some(rc) if *rc > 1 => {
        *rc -= 1;
        false
      }
      Some(_) => {
        self.0.remove(&peer_idx);
        true
      }
      None => false,
    }
  }

  /// Removes the peer entirely regardless of refcount (used on detach).
  #[inline]
  fn remove_all(&mut self, peer_idx: u32) {
    self.0.remove(&peer_idx);
  }

  #[inline]
  fn for_each(&self, mut f: impl FnMut(u32)) {
    for kv in self.0.iter() {
      f(kv.key);
    }
  }
}

/// An outgoing, path-compressed edge. `label` is the full segment consumed to
/// reach `child` (always at least one byte); `label[0]` is the search key used
/// to keep a node's `children` sorted for binary search.
#[derive(Debug)]
struct Edge {
  label: Box<[u8]>,
  child: u32,
}

/// A node in the arena. `subscribers` holds the peers whose subscription
/// terminates exactly at this node's accumulated prefix.
#[derive(Debug, Default)]
struct Node {
  /// Sorted ascending by `Edge::label[0]`.
  children: Vec<Edge>,
  subscribers: PeerSet,
}

#[derive(Debug)]
struct Inner {
  /// `nodes[0]` is always the root (prefix `""`).
  nodes: Vec<Node>,
}

/// Thread-safe, prefix-matching subscription database keyed by peer index.
#[derive(Debug)]
pub(crate) struct SubscriptionMatcher {
  inner: RwLock<Inner>,
}

impl Default for SubscriptionMatcher {
  fn default() -> Self {
    Self::new()
  }
}

impl SubscriptionMatcher {
  pub fn new() -> Self {
    Self {
      inner: RwLock::new(Inner {
        nodes: vec![Node::default()],
      }),
    }
  }

  /// Records that `peer_idx` subscribed to `topic` (prefix). Idempotent per call
  /// via refcounting: N subscribes require N unsubscribes to fully clear.
  pub fn subscribe(&self, peer_idx: u32, topic: &[u8]) {
    let mut inner = self.inner.write();
    let mut node = 0usize;
    let mut rest = topic;

    loop {
      if rest.is_empty() {
        inner.nodes[node].subscribers.add(peer_idx);
        return;
      }

      match child_index(&inner.nodes[node].children, rest[0]) {
        Ok(ei) => {
          // Clone the label so we can mutate `nodes` without aliasing the edge.
          let label = inner.nodes[node].children[ei].label.clone();
          let cpl = common_prefix_len(&label, rest);

          if cpl == label.len() {
            // Whole edge consumed; descend.
            node = inner.nodes[node].children[ei].child as usize;
            rest = &rest[cpl..];
            continue;
          }

          // Partial match: split the edge at `cpl` by inserting an intermediate
          // node. The old child keeps `label[cpl..]`; the parent now points to
          // the intermediate via `label[..cpl]`.
          let old_child = inner.nodes[node].children[ei].child;
          let mid = alloc_node(&mut inner.nodes);
          inner.nodes[mid].children.push(Edge {
            label: Box::from(&label[cpl..]),
            child: old_child,
          });
          inner.nodes[node].children[ei] = Edge {
            label: Box::from(&label[..cpl]),
            child: mid as u32,
          };

          if cpl == rest.len() {
            // Subscription terminates at the split point.
            inner.nodes[mid].subscribers.add(peer_idx);
          } else {
            // Subscription diverges past the split; hang a new leaf off `mid`.
            let leaf = alloc_node(&mut inner.nodes);
            inner.nodes[leaf].subscribers.add(peer_idx);
            insert_child(
              &mut inner.nodes[mid].children,
              Edge {
                label: Box::from(&rest[cpl..]),
                child: leaf as u32,
              },
            );
          }
          return;
        }
        Err(pos) => {
          // No edge shares the first byte; add a fresh leaf holding all of `rest`.
          let leaf = alloc_node(&mut inner.nodes);
          inner.nodes[leaf].subscribers.add(peer_idx);
          inner.nodes[node].children.insert(
            pos,
            Edge {
              label: Box::from(rest),
              child: leaf as u32,
            },
          );
          return;
        }
      }
    }
  }

  /// Removes one subscription of `peer_idx` to the exact `topic`. Returns `true`
  /// if this was the peer's last subscription to that topic (refcount hit zero).
  /// No structural pruning is performed — empty nodes are retained (bounded by
  /// the set of distinct prefixes ever subscribed); see module notes.
  pub fn unsubscribe(&self, peer_idx: u32, topic: &[u8]) -> bool {
    let mut inner = self.inner.write();
    let mut node = 0usize;
    let mut rest = topic;

    loop {
      if rest.is_empty() {
        return inner.nodes[node].subscribers.remove_one(peer_idx);
      }
      match child_index(&inner.nodes[node].children, rest[0]) {
        Ok(ei) => {
          let edge = &inner.nodes[node].children[ei];
          let l = edge.label.len();
          if rest.len() >= l && rest[..l] == *edge.label {
            node = edge.child as usize;
            rest = &rest[l..];
          } else {
            return false; // diverges within a compressed edge — not subscribed
          }
        }
        Err(_) => return false,
      }
    }
  }

  /// Purges `peer_idx` from every subscription set. Called when a peer detaches,
  /// before its index may be recycled by the [`Distributor`]. `O(nodes)`, which
  /// is acceptable on the (rare) detach path.
  pub fn remove_peer(&self, peer_idx: u32) {
    let mut inner = self.inner.write();
    for node in inner.nodes.iter_mut() {
      node.subscribers.remove_all(peer_idx);
    }
  }

  /// Invokes `visit(peer_idx)` for every peer whose subscription is a prefix of
  /// `topic` (including the empty `""` subscription at the root). A peer that
  /// subscribed to several prefixes of `topic` is visited once **per matching
  /// prefix**; the caller is responsible for de-duplication.
  pub fn for_each_match(&self, topic: &[u8], mut visit: impl FnMut(u32)) {
    let inner = self.inner.read();
    let mut node = 0usize;
    let mut rest = topic;

    loop {
      // Every subscription terminating at `node` is a prefix of `topic`.
      inner.nodes[node].subscribers.for_each(&mut visit);
      if rest.is_empty() {
        return;
      }
      match child_index(&inner.nodes[node].children, rest[0]) {
        Ok(ei) => {
          let edge = &inner.nodes[node].children[ei];
          let l = edge.label.len();
          if rest.len() >= l && rest[..l] == *edge.label {
            node = edge.child as usize;
            rest = &rest[l..];
          } else {
            // The remaining topic diverges partway through a compressed edge, so
            // no subscription at or beyond it can be a prefix of `topic`.
            return;
          }
        }
        Err(_) => return,
      }
    }
  }
}

// --- Free helpers (kept private to this module) -----------------------------

/// Pushes a fresh node and returns its arena index. Callers must hold only
/// indices (not `&mut Node`) across this call, since the `Vec` may reallocate.
#[inline]
fn alloc_node(nodes: &mut Vec<Node>) -> usize {
  nodes.push(Node::default());
  nodes.len() - 1
}

/// Binary search a node's sorted children by their leading byte.
#[inline]
fn child_index(children: &[Edge], first_byte: u8) -> Result<usize, usize> {
  children.binary_search_by(|e| e.label[0].cmp(&first_byte))
}

/// Insert an edge keeping `children` sorted by leading byte. The caller
/// guarantees no existing edge shares `edge.label[0]`.
#[inline]
fn insert_child(children: &mut Vec<Edge>, edge: Edge) {
  let pos = child_index(children, edge.label[0]).unwrap_or_else(|p| p);
  children.insert(pos, edge);
}

#[inline]
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
  a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Collect the deduplicated set of matched peers for `topic`, sorted for
  /// stable comparison.
  fn matches(m: &SubscriptionMatcher, topic: &[u8]) -> Vec<u32> {
    let mut out = Vec::new();
    m.for_each_match(topic, |idx| out.push(idx));
    out.sort_unstable();
    out.dedup();
    out
  }

  #[test]
  fn empty_subscription_matches_everything() {
    let m = SubscriptionMatcher::new();
    m.subscribe(7, b"");
    assert_eq!(matches(&m, b"anything"), vec![7]);
    assert_eq!(matches(&m, b""), vec![7]);
  }

  #[test]
  fn prefix_match_semantics() {
    let m = SubscriptionMatcher::new();
    m.subscribe(1, b"news");
    // Prefix of a longer topic matches.
    assert_eq!(matches(&m, b"news/weather"), vec![1]);
    assert_eq!(matches(&m, b"news"), vec![1]);
    // A shorter topic than the subscription does NOT match.
    assert_eq!(matches(&m, b"new"), Vec::<u32>::new());
    // Divergent topic does not match.
    assert_eq!(matches(&m, b"sports"), Vec::<u32>::new());
  }

  #[test]
  fn selective_delivery_across_peers() {
    let m = SubscriptionMatcher::new();
    m.subscribe(10, b"sports");
    m.subscribe(20, b"news");
    assert_eq!(matches(&m, b"sports/nba"), vec![10]);
    assert_eq!(matches(&m, b"news/weather"), vec![20]);
    assert_eq!(matches(&m, b"other"), Vec::<u32>::new());
  }

  #[test]
  fn overlapping_prefixes_dedup_to_single_peer() {
    let m = SubscriptionMatcher::new();
    m.subscribe(5, b"a");
    m.subscribe(5, b"ab");
    // Peer 5 matches both "a" and "ab" for topic "abc"; for_each_match visits it
    // twice, callers dedup to one.
    let mut visits = Vec::new();
    m.for_each_match(b"abc", |idx| visits.push(idx));
    assert_eq!(visits, vec![5, 5], "expected one visit per matching prefix");
    assert_eq!(matches(&m, b"abc"), vec![5]);
  }

  #[test]
  fn edge_split_preserves_existing_subscriptions() {
    let m = SubscriptionMatcher::new();
    // Insert a long topic first, then a shorter shared prefix to force a split.
    m.subscribe(1, b"foobar");
    m.subscribe(2, b"foo");
    assert_eq!(matches(&m, b"foobar"), vec![1, 2]);
    assert_eq!(matches(&m, b"foobaz"), vec![2]);
    assert_eq!(matches(&m, b"foo"), vec![2]);
    // Divergent branch split.
    m.subscribe(3, b"food");
    assert_eq!(matches(&m, b"food"), vec![2, 3]);
    assert_eq!(matches(&m, b"foobar"), vec![1, 2]);
  }

  #[test]
  fn refcount_requires_matching_unsubscribes() {
    let m = SubscriptionMatcher::new();
    m.subscribe(9, b"news");
    m.subscribe(9, b"news");
    assert!(!m.unsubscribe(9, b"news"), "still one ref left");
    assert_eq!(matches(&m, b"news"), vec![9]);
    assert!(m.unsubscribe(9, b"news"), "last ref removed");
    assert_eq!(matches(&m, b"news"), Vec::<u32>::new());
    // Over-unsubscribe is a no-op returning false.
    assert!(!m.unsubscribe(9, b"news"));
  }

  #[test]
  fn remove_peer_purges_all_subscriptions() {
    let m = SubscriptionMatcher::new();
    m.subscribe(1, b"");
    m.subscribe(1, b"a/b/c");
    m.subscribe(2, b"a/b/c");
    m.remove_peer(1);
    assert_eq!(matches(&m, b"a/b/c/d"), vec![2]);
    assert_eq!(matches(&m, b"unrelated"), Vec::<u32>::new());
    // A recycled index must not inherit the removed peer's subscriptions.
    m.subscribe(1, b"fresh");
    assert_eq!(matches(&m, b"a/b/c"), vec![2]);
    assert_eq!(matches(&m, b"fresh/topic"), vec![1]);
  }
}
