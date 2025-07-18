use async_recursion::async_recursion;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// A node in the subscription trie.
#[derive(Debug, Default)]
struct TrieNode {
  children: HashMap<u8, Arc<RwLock<TrieNode>>>,
  /// Count of subscriptions ending exactly at this node.
  count: AtomicUsize,
}

/// Manages topic subscriptions using a prefix trie for efficient matching.
#[derive(Debug, Default)]
pub(crate) struct SubscriptionTrie {
  root: Arc<RwLock<TrieNode>>,
}

impl SubscriptionTrie {
  pub fn new() -> Self {
    Self::default()
  }

  /// Retrieves all currently active subscription topics.
  /// Returns a Vec where each inner Vec<u8> is a subscribed topic prefix.
  pub async fn get_all_topics(&self) -> Vec<Vec<u8>> {
    let mut topics = Vec::new();
    // Start recursive traversal from the root.
    self
      .collect_topics_recursive(self.root.clone(), Vec::new(), &mut topics)
      .await;
    topics
  }

  /// Adds a subscription topic (prefix).
  /// Increments the count if the topic already exists.
  pub async fn subscribe(&self, topic: &[u8]) {
    let mut current_node_arc = self.root.clone();

    for &byte in topic {
      let mut current_node_w = current_node_arc.write().await; // Lock current node
      let next_node_arc = current_node_w
        .children
        .entry(byte)
        .or_insert_with(|| Arc::new(RwLock::new(TrieNode::default())))
        .clone();
      drop(current_node_w); // Release lock before moving to next node
      current_node_arc = next_node_arc;
    }

    // Reached the end node for the topic, increment its count
    let final_node_r = current_node_arc.read().await; // Read lock sufficient for fetch_add
    final_node_r.count.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(topic = ?String::from_utf8_lossy(topic), "Subscribed");
  }

  /// Removes a subscription topic (prefix).
  /// Decrements the count. Returns true if the topic existed and count reached zero.
  /// Note: This basic version doesn't prune empty nodes.
  pub async fn unsubscribe(&self, topic: &[u8]) -> bool {
    let mut current_node_arc = self.root.clone();
    let mut path = Vec::new(); // Store path for potential cleanup later if needed

    for &byte in topic {
      let next_node_option = {
        // Start new scope
        let current_node_r = current_node_arc.read().await; // Lock inside scope
        current_node_r.children.get(&byte).cloned() // Clone the Arc<RwLock<TrieNode>> if found
      }; // current_node_r guard is dropped here

      match next_node_option {
        Some(next_node_arc) => {
          path.push((current_node_arc.clone(), byte));
          current_node_arc = next_node_arc; // Reassignment happens *after* guard is dropped
        }
        None => {
          tracing::debug!(topic = ?String::from_utf8_lossy(topic), "Unsubscribe failed: Topic prefix not found");
          return false;
        }
      }
    }

    // Reached the end node
    let final_node_r = current_node_arc.read().await;
    let old_count = final_node_r.count.fetch_sub(1, Ordering::Relaxed); // Decrement count

    if old_count > 0 {
      tracing::debug!(topic = ?String::from_utf8_lossy(topic), new_count = old_count - 1, "Unsubscribed");
      // TODO: Implement pruning of the trie if old_count was 1 and node has no children?
      // Pruning requires write locks back up the path and careful handling of races.
      // Delay pruning implementation for simplicity for now.
      old_count == 1 // Return true if this was the last subscription for this exact topic
    } else {
      // Count was already zero somehow, restore it? Or log error?
      final_node_r.count.fetch_add(1, Ordering::Relaxed); // Restore count
      tracing::warn!(topic = ?String::from_utf8_lossy(topic), "Unsubscribe attempt on topic with zero count");
      false
    }
  }

  /// Checks if a given message topic matches *any* active subscription prefix.
  pub async fn matches(&self, message_topic: &[u8]) -> bool {
    let mut current_node_arc = self.root.clone();

    // Check for exact match at root (empty subscription "")
    {
      let root_r = current_node_arc.read().await;
      if root_r.count.load(Ordering::Relaxed) > 0 {
        return true; // Matches empty subscription
      }
    }

    // Traverse the trie based on the message topic bytes
    for &byte in message_topic {
      let (matched_prefix, next_node_option) = {
        // Start new scope
        let current_node_r = current_node_arc.read().await; // Lock inside scope
                                                            // Check if current node itself matches a prefix
        let prefix_match = current_node_r.count.load(Ordering::Relaxed) > 0;
        // Get the next node Arc if it exists
        let next_node = current_node_r.children.get(&byte).cloned();
        (prefix_match, next_node) // Return results
      }; // current_node_r guard is dropped here

      if matched_prefix {
        return true; // Matched a subscription prefix during traversal
      }

      match next_node_option {
        Some(next_node_arc) => {
          current_node_arc = next_node_arc; // Reassignment happens *after* guard is dropped
        }
        None => {
          return false; // No matching path further down
        }
      }
    }

    // Reached the end of the message topic, check if the final node is a subscription end
    let final_node_r = current_node_arc.read().await;
    final_node_r.count.load(Ordering::Relaxed) > 0
  }

  /// Recursive helper function to collect topics.
  #[async_recursion]
  async fn collect_topics_recursive(
    &self,
    node_arc: Arc<RwLock<TrieNode>>,
    current_prefix: Vec<u8>,
    all_topics: &mut Vec<Vec<u8>>,
  ) {
    let node_read_guard = node_arc.read().await; // Lock current node for reading

    // If this node marks the end of a subscription (count > 0), add its prefix.
    if node_read_guard.count.load(Ordering::Relaxed) > 0 {
      all_topics.push(current_prefix.clone());
    }

    // Create a list of children to visit *after* releasing the read lock.
    // Cloning the Arc<RwLock<TrieNode>> is cheap.
    let children_to_visit: Vec<(u8, Arc<RwLock<TrieNode>>)> = node_read_guard
      .children
      .iter()
      .map(|(byte, child_arc)| (*byte, child_arc.clone()))
      .collect();

    // Drop the read lock before recursing to avoid potential deadlocks if the
    // recursion somehow tried to acquire a write lock later (unlikely here, but good practice).
    drop(node_read_guard);

    // Recurse into children
    for (byte, child_node_arc) in children_to_visit {
      let mut next_prefix = current_prefix.clone();
      next_prefix.push(byte);
      self
        .collect_topics_recursive(child_node_arc, next_prefix, all_topics)
        .await;
    }
  }
}
