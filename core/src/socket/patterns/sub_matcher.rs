//! Radix subscription matchers (publisher- and subscriber-side).
//!
//! Both are built on one shared, path-compressed radix trie held in a `Vec`
//! arena ([`Arena`]) behind a single `parking_lot::RwLock`. The arena is generic
//! over the per-node terminal payload `T`:
//!
//! - [`SubscriptionMatcher`] (PUB side, `T = PeerSet`) maps each subscribed topic
//!   prefix to the **set of subscribed peers** — libzmq's `mtrie_t` model. On
//!   send, the message topic is matched once to find every interested peer.
//! - [`PrefixMatcher`] (SUB side, `T = u32` refcount) answers the boolean
//!   "does any subscription match this topic?" — the per-message receive filter.
//!   It replaces the old per-byte `HashMap<u8, Arc<RwLock<..>>>` trie so the
//!   (interop-required) SUB-side filtering pass is as cheap as the PUB one.
//!
//! Design notes:
//! - **Reads (matching) dominate**, so the hot paths take a single shared read
//!   lock and walk the arena by `u32` index — no per-node atomics, no per-node
//!   locks, no hashing. Path compression keeps the number of hops (and `memcmp`s)
//!   small.
//! - **Writes stay `O(topic-len)`** (in-place mutation under the write lock), so
//!   subscription churn does not trigger any whole-structure copy.
//!
//! Matching semantics are ZMQ prefix matching: a subscription `S` matches a
//! message topic `T` iff `S` is a prefix of `T`. The empty subscription (`""`)
//! terminates at the root node, so it matches everything.

use parking_lot::RwLock;
use xs_foundation::collections::thin_map::ThinMapU32;

// ===========================================================================
// Generic path-compressed radix arena
// ===========================================================================

/// An outgoing, path-compressed edge. `label` is the full segment consumed to
/// reach `child` (always at least one byte); `label[0]` is the search key used
/// to keep a node's `children` sorted for binary search.
#[derive(Debug)]
struct Edge {
  label: Box<[u8]>,
  child: u32,
}

/// A node in the arena. `payload` holds whatever terminates exactly at this
/// node's accumulated prefix (a peer set, or a refcount).
#[derive(Debug, Default)]
struct Node<T> {
  /// Sorted ascending by `Edge::label[0]`.
  children: Vec<Edge>,
  payload: T,
}

/// Path-compressed radix trie over `[u8]` keys with a payload `T` at every node.
#[derive(Debug)]
struct Arena<T> {
  /// `nodes[0]` is always the root (prefix `""`).
  nodes: Vec<Node<T>>,
}

impl<T: Default> Arena<T> {
  fn new() -> Self {
    Self {
      nodes: vec![Node::default()],
    }
  }

  /// Inserts `topic`, creating nodes as needed (with edge splitting), and
  /// returns a mutable reference to the terminal node's payload.
  fn insert(&mut self, topic: &[u8]) -> &mut T {
    let node = self.insert_node(topic);
    &mut self.nodes[node].payload
  }

  /// Radix insert; returns the arena index of the terminal node for `topic`.
  fn insert_node(&mut self, topic: &[u8]) -> usize {
    let mut node = 0usize;
    let mut rest = topic;

    loop {
      if rest.is_empty() {
        return node;
      }

      match child_index(&self.nodes[node].children, rest[0]) {
        Ok(ei) => {
          // Clone the label so we can mutate `nodes` without aliasing the edge.
          let label = self.nodes[node].children[ei].label.clone();
          let cpl = common_prefix_len(&label, rest);

          if cpl == label.len() {
            // Whole edge consumed; descend.
            node = self.nodes[node].children[ei].child as usize;
            rest = &rest[cpl..];
            continue;
          }

          // Partial match: split the edge at `cpl` via an intermediate node. The
          // old child keeps `label[cpl..]`; the parent points to the intermediate
          // via `label[..cpl]`.
          let old_child = self.nodes[node].children[ei].child;
          let mid = alloc_node(&mut self.nodes);
          self.nodes[mid].children.push(Edge {
            label: Box::from(&label[cpl..]),
            child: old_child,
          });
          self.nodes[node].children[ei] = Edge {
            label: Box::from(&label[..cpl]),
            child: mid as u32,
          };

          if cpl == rest.len() {
            // Subscription terminates at the split point.
            return mid;
          } else {
            // Subscription diverges past the split; hang a new leaf off `mid`.
            let leaf = alloc_node(&mut self.nodes);
            insert_child(
              &mut self.nodes[mid].children,
              Edge {
                label: Box::from(&rest[cpl..]),
                child: leaf as u32,
              },
            );
            return leaf;
          }
        }
        Err(pos) => {
          // No edge shares the first byte; add a fresh leaf holding all of `rest`.
          let leaf = alloc_node(&mut self.nodes);
          self.nodes[node].children.insert(
            pos,
            Edge {
              label: Box::from(rest),
              child: leaf as u32,
            },
          );
          return leaf;
        }
      }
    }
  }

  /// Exact-match walk; returns the terminal node's payload for `topic` if that
  /// exact prefix exists in the trie.
  fn get_terminal_mut(&mut self, topic: &[u8]) -> Option<&mut T> {
    let node = self.find_terminal(topic)?;
    Some(&mut self.nodes[node].payload)
  }

  fn find_terminal(&self, topic: &[u8]) -> Option<usize> {
    let mut node = 0usize;
    let mut rest = topic;
    loop {
      if rest.is_empty() {
        return Some(node);
      }
      match child_index(&self.nodes[node].children, rest[0]) {
        Ok(ei) => {
          let edge = &self.nodes[node].children[ei];
          let l = edge.label.len();
          if rest.len() >= l && rest[..l] == *edge.label {
            node = edge.child as usize;
            rest = &rest[l..];
          } else {
            return None;
          }
        }
        Err(_) => return None,
      }
    }
  }

  /// Visits the payload of every node whose prefix is a prefix of `topic` (root
  /// first). `visit` returns `false` to stop the walk early.
  fn walk<F: FnMut(&T) -> bool>(&self, topic: &[u8], mut visit: F) {
    let mut node = 0usize;
    let mut rest = topic;
    loop {
      if !visit(&self.nodes[node].payload) {
        return;
      }
      if rest.is_empty() {
        return;
      }
      match child_index(&self.nodes[node].children, rest[0]) {
        Ok(ei) => {
          let edge = &self.nodes[node].children[ei];
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

  /// Mutable access to every node payload (used to purge a peer everywhere).
  fn payloads_mut(&mut self) -> impl Iterator<Item = &mut T> + '_ {
    self.nodes.iter_mut().map(|n| &mut n.payload)
  }

  /// Collects the accumulated prefix of every node whose payload satisfies
  /// `is_terminal`.
  fn collect_terminals<F: Fn(&T) -> bool>(&self, is_terminal: F) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut stack: Vec<(usize, Vec<u8>)> = vec![(0, Vec::new())];
    while let Some((node, prefix)) = stack.pop() {
      if is_terminal(&self.nodes[node].payload) {
        out.push(prefix.clone());
      }
      for edge in &self.nodes[node].children {
        let mut child_prefix = prefix.clone();
        child_prefix.extend_from_slice(&edge.label);
        stack.push((edge.child as usize, child_prefix));
      }
    }
    out
  }
}

// --- Free helpers -----------------------------------------------------------

/// Pushes a fresh node and returns its arena index. Callers must hold only
/// indices (not `&mut Node`) across this call, since the `Vec` may reallocate.
#[inline]
fn alloc_node<T: Default>(nodes: &mut Vec<Node<T>>) -> usize {
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

// ===========================================================================
// PeerSet payload (PUB side)
// ===========================================================================

/// The set of peers subscribed at a node: `peer_idx -> refcount`. The refcount
/// mirrors libzmq's per-pipe subscription counting so N `subscribe` calls for
/// the same (peer, topic) require N `unsubscribe` calls to clear.
///
/// Backed by [`ThinMapU32`] (a tightly-packed, memory-efficient sorted map for
/// `u32` keys), wrapped in this newtype so we can opt back into `Send`/`Sync`.
///
/// SAFETY: `ThinMapU32` is `!Send`/`!Sync` only because it stores a raw
/// `NonNull` (the conservative default for raw pointers). It otherwise owns its
/// heap allocation exclusively — `Drop` frees it, there is no shared ownership
/// or interior mutability, and every `&self` method (`get`, `iter`, `len`) is
/// read-only while all mutation goes through `&mut self`. That makes it
/// semantically equivalent to `Box<[KV<u32>]>`, which is `Send + Sync` when its
/// element type is; `u32` is. Concurrent access is additionally serialized by
/// the enclosing `RwLock`.
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

// ===========================================================================
// SubscriptionMatcher — publisher side (prefix -> set of peers)
// ===========================================================================

/// Thread-safe, prefix-matching subscription database keyed by peer index.
///
/// Peers are referenced by a small stable `peer_idx: u32` allocated by the
/// [`Distributor`](super::distributor::Distributor); the matcher never owns the
/// connection, only the index.
#[derive(Debug)]
pub(crate) struct SubscriptionMatcher {
  inner: RwLock<Arena<PeerSet>>,
}

impl Default for SubscriptionMatcher {
  fn default() -> Self {
    Self::new()
  }
}

impl SubscriptionMatcher {
  pub fn new() -> Self {
    Self {
      inner: RwLock::new(Arena::new()),
    }
  }

  /// Records that `peer_idx` subscribed to `topic` (prefix). Idempotent per call
  /// via refcounting: N subscribes require N unsubscribes to fully clear.
  pub fn subscribe(&self, peer_idx: u32, topic: &[u8]) {
    self.inner.write().insert(topic).add(peer_idx);
  }

  /// Removes one subscription of `peer_idx` to the exact `topic`. Returns `true`
  /// if this was the peer's last subscription to that topic.
  pub fn unsubscribe(&self, peer_idx: u32, topic: &[u8]) -> bool {
    match self.inner.write().get_terminal_mut(topic) {
      Some(set) => set.remove_one(peer_idx),
      None => false,
    }
  }

  /// Purges `peer_idx` from every subscription set (on detach, before its index
  /// may be recycled). `O(nodes)`, acceptable on the rare detach path.
  pub fn remove_peer(&self, peer_idx: u32) {
    let mut inner = self.inner.write();
    for payload in inner.payloads_mut() {
      payload.remove_all(peer_idx);
    }
  }

  /// Invokes `visit(peer_idx)` for every peer whose subscription is a prefix of
  /// `topic` (including the empty `""` subscription at the root). A peer that
  /// subscribed to several prefixes of `topic` is visited once **per matching
  /// prefix**; the caller de-duplicates.
  pub fn for_each_match(&self, topic: &[u8], mut visit: impl FnMut(u32)) {
    self.inner.read().walk(topic, |set| {
      set.for_each(&mut visit);
      true
    });
  }
}

// ===========================================================================
// PrefixMatcher — subscriber side (boolean "does any subscription match?")
// ===========================================================================

/// Thread-safe boolean prefix matcher for a single SUB socket's own
/// subscriptions. Same fast radix arena as [`SubscriptionMatcher`], but each
/// node stores a subscription refcount rather than a peer set.
///
/// Synchronous — safe to call `matches` from the io_uring worker OS thread
/// (`parking_lot::RwLock` does not park the Tokio runtime).
#[derive(Debug)]
pub(crate) struct PrefixMatcher {
  inner: RwLock<Arena<u32>>,
}

impl Default for PrefixMatcher {
  fn default() -> Self {
    Self::new()
  }
}

impl PrefixMatcher {
  pub fn new() -> Self {
    Self {
      inner: RwLock::new(Arena::new()),
    }
  }

  /// Adds a subscription topic (prefix). Increments the refcount if it exists.
  pub fn subscribe(&self, topic: &[u8]) {
    *self.inner.write().insert(topic) += 1;
  }

  /// Removes a subscription topic. Returns `true` if its refcount reached zero
  /// (so the SUB should forward an UNSUBSCRIBE upstream).
  pub fn unsubscribe(&self, topic: &[u8]) -> bool {
    match self.inner.write().get_terminal_mut(topic) {
      Some(count) if *count > 0 => {
        *count -= 1;
        *count == 0
      }
      _ => false,
    }
  }

  /// Returns whether any active subscription prefix matches `message_topic`.
  pub fn matches(&self, message_topic: &[u8]) -> bool {
    let mut found = false;
    self.inner.read().walk(message_topic, |&count| {
      if count > 0 {
        found = true;
        false // stop early on first matching prefix
      } else {
        true
      }
    });
    found
  }

  /// All currently active subscription topics (used to resync a new peer).
  pub fn get_all_topics(&self) -> Vec<Vec<u8>> {
    self.inner.read().collect_terminals(|&count| count > 0)
  }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod matcher_tests {
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
    assert_eq!(matches(&m, b"news/weather"), vec![1]);
    assert_eq!(matches(&m, b"news"), vec![1]);
    assert_eq!(matches(&m, b"new"), Vec::<u32>::new());
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
    let mut visits = Vec::new();
    m.for_each_match(b"abc", |idx| visits.push(idx));
    assert_eq!(visits, vec![5, 5], "expected one visit per matching prefix");
    assert_eq!(matches(&m, b"abc"), vec![5]);
  }

  #[test]
  fn edge_split_preserves_existing_subscriptions() {
    let m = SubscriptionMatcher::new();
    m.subscribe(1, b"foobar");
    m.subscribe(2, b"foo");
    assert_eq!(matches(&m, b"foobar"), vec![1, 2]);
    assert_eq!(matches(&m, b"foobaz"), vec![2]);
    assert_eq!(matches(&m, b"foo"), vec![2]);
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
    m.subscribe(1, b"fresh");
    assert_eq!(matches(&m, b"a/b/c"), vec![2]);
    assert_eq!(matches(&m, b"fresh/topic"), vec![1]);
  }
}

#[cfg(test)]
mod prefix_matcher_tests {
  use super::*;

  #[test]
  fn empty_subscription_matches_all() {
    let m = PrefixMatcher::new();
    m.subscribe(b"");
    assert!(m.matches(b"sports/football"));
    assert!(m.matches(b"news/weather"));
    assert!(m.matches(b""));
  }

  #[test]
  fn prefix_and_divergent() {
    let m = PrefixMatcher::new();
    m.subscribe(b"news");
    assert!(m.matches(b"news"));
    assert!(m.matches(b"news/weather"));
    assert!(!m.matches(b"new"));
    assert!(!m.matches(b"sports"));
  }

  #[test]
  fn overlapping_refcount_and_unsubscribe() {
    let m = PrefixMatcher::new();
    m.subscribe(b"news");
    m.subscribe(b"news");
    assert!(!m.unsubscribe(b"news"), "count still 1");
    assert!(m.matches(b"news"));
    assert!(m.unsubscribe(b"news"), "count reached zero");
    assert!(!m.matches(b"news"));
    assert!(!m.unsubscribe(b"news"));
  }

  #[test]
  fn get_all_topics_roundtrip() {
    let m = PrefixMatcher::new();
    m.subscribe(b"sports");
    m.subscribe(b"sports/football");
    m.subscribe(b"news/weather");
    let mut topics = m.get_all_topics();
    topics.sort();
    let mut expected = vec![
      b"news/weather".to_vec(),
      b"sports".to_vec(),
      b"sports/football".to_vec(),
    ];
    expected.sort();
    assert_eq!(topics, expected);
  }

  #[test]
  fn empty_subscription_appears_in_topics() {
    let m = PrefixMatcher::new();
    m.subscribe(b"");
    assert_eq!(m.get_all_topics(), vec![Vec::<u8>::new()]);
  }
}
