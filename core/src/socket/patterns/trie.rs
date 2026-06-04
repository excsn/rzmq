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

#[cfg(test)]
mod concurrent_trie_tests {
  use super::*;
  use std::sync::Arc;
  use std::time::Duration;

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_concurrent_subscribe_100k_unique_topics() {
    let trie = Arc::new(SubscriptionTrie::new());
    const NUM_TASKS: usize = 20;
    const TOPICS_PER_TASK: usize = 5_000;

    let mut handles = Vec::new();
    for task_id in 0..NUM_TASKS {
      let trie = trie.clone();
      handles.push(tokio::spawn(async move {
        for i in 0..TOPICS_PER_TASK {
          let topic = format!("topic/{}/{}", task_id, i);
          trie.subscribe(topic.as_bytes()).await;
        }
      }));
    }
    for h in handles {
      h.await.unwrap();
    }

    let all_topics = trie.get_all_topics().await;
    assert_eq!(
      all_topics.len(),
      NUM_TASKS * TOPICS_PER_TASK,
      "Expected {} subscriptions, got {}",
      NUM_TASKS * TOPICS_PER_TASK,
      all_topics.len()
    );
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn test_get_all_topics_under_write_contention() {
    let trie = Arc::new(SubscriptionTrie::new());

    // Pre-populate with some topics
    for i in 0..100 {
      trie.subscribe(format!("base/topic/{}", i).as_bytes()).await;
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();

    // 5 writer tasks: subscribe/unsubscribe continuously
    for task_id in 0..5usize {
      let trie = trie.clone();
      let stop = stop.clone();
      handles.push(tokio::spawn(async move {
        let mut i = 0u64;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
          let topic = format!("dynamic/{}/{}", task_id, i % 50);
          trie.subscribe(topic.as_bytes()).await;
          trie.unsubscribe(topic.as_bytes()).await;
          i += 1;
        }
      }));
    }

    // 5 reader tasks: get_all_topics() continuously
    for _ in 0..5usize {
      let trie = trie.clone();
      let stop = stop.clone();
      handles.push(tokio::spawn(async move {
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
          let topics = trie.get_all_topics().await;
          // At minimum, the 100 base topics are always present
          assert!(
            topics.len() >= 100,
            "get_all_topics() returned less than base topics: {}",
            topics.len()
          );
        }
      }));
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    for h in handles {
      h.await.unwrap();
    }
  }
}

#[cfg(test)]
mod additional_trie_tests {
  use super::*;

  #[tokio::test]
  async fn test_trie_empty_subscription_behavior() {
    let trie = SubscriptionTrie::new();
    trie.subscribe(b"").await;

    assert!(trie.matches(b"sports/football").await);
    assert!(trie.matches(b"news/weather").await);
  }

  #[tokio::test]
  async fn test_trie_overlapping_subscriptions() {
    let trie = SubscriptionTrie::new();
    let topic = b"news";

    trie.subscribe(topic).await;
    trie.subscribe(topic).await; // Double subscribe → count = 2

    // First unsubscribe: count 2 → 1, not the last reference
    let removed = trie.unsubscribe(topic).await;
    assert!(!removed, "Should not fully remove topic with count still at 1");
    assert!(trie.matches(topic).await, "Topic should still match");

    // Second unsubscribe: count 1 → 0, was the last reference
    let removed_final = trie.unsubscribe(topic).await;
    assert!(removed_final, "Topic should be fully removed on last unsubscribe");
    assert!(!trie.matches(topic).await, "Topic should no longer match");
  }

  #[tokio::test]
  async fn test_trie_recursive_topic_collection() {
    let trie = SubscriptionTrie::new();
    trie.subscribe(b"sports").await;
    trie.subscribe(b"sports/football").await;
    trie.subscribe(b"news/weather").await;

    let mut topics = trie.get_all_topics().await;
    topics.sort();

    let mut expected = vec![
      b"sports".to_vec(),
      b"sports/football".to_vec(),
      b"news/weather".to_vec(),
    ];
    expected.sort();

    assert_eq!(topics, expected);
  }
}
