use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

/// A node in the subscription trie.
#[derive(Debug, Default)]
struct TrieNode {
  children: HashMap<u8, Arc<RwLock<TrieNode>>>,
  /// Count of subscriptions ending exactly at this node.
  count: AtomicUsize,
}

/// Manages topic subscriptions using a prefix trie for efficient matching.
///
/// All methods are synchronous — `parking_lot::RwLock` is used so that the
/// io_uring worker OS thread can call `matches` without blocking the Tokio runtime.
#[derive(Debug, Default)]
pub(crate) struct SubscriptionTrie {
  root: Arc<RwLock<TrieNode>>,
}

impl SubscriptionTrie {
  pub fn new() -> Self {
    Self::default()
  }

  /// Retrieves all currently active subscription topics.
  pub fn get_all_topics(&self) -> Vec<Vec<u8>> {
    let mut topics = Vec::new();
    self.collect_topics_recursive(self.root.clone(), Vec::new(), &mut topics);
    topics
  }

  /// Adds a subscription topic (prefix). Increments count if already exists.
  pub fn subscribe(&self, topic: &[u8]) {
    let mut current_node_arc = self.root.clone();

    for &byte in topic {
      let next_node_arc = {
        let mut current_node_w = current_node_arc.write();
        current_node_w
          .children
          .entry(byte)
          .or_insert_with(|| Arc::new(RwLock::new(TrieNode::default())))
          .clone()
      };
      current_node_arc = next_node_arc;
    }

    let final_node_r = current_node_arc.read();
    final_node_r.count.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(topic = ?String::from_utf8_lossy(topic), "Subscribed");
  }

  /// Removes a subscription topic (prefix).
  /// Returns true if the topic existed and its count reached zero.
  pub fn unsubscribe(&self, topic: &[u8]) -> bool {
    let mut current_node_arc = self.root.clone();

    for &byte in topic {
      let next_node_option = {
        let current_node_r = current_node_arc.read();
        current_node_r.children.get(&byte).cloned()
      };

      match next_node_option {
        Some(next_node_arc) => {
          current_node_arc = next_node_arc;
        }
        None => {
          tracing::debug!(topic = ?String::from_utf8_lossy(topic), "Unsubscribe failed: Topic prefix not found");
          return false;
        }
      }
    }

    let final_node_r = current_node_arc.read();
    let old_count = final_node_r.count.fetch_sub(1, Ordering::Relaxed);

    if old_count > 0 {
      tracing::debug!(topic = ?String::from_utf8_lossy(topic), new_count = old_count - 1, "Unsubscribed");
      old_count == 1
    } else {
      final_node_r.count.fetch_add(1, Ordering::Relaxed);
      tracing::warn!(topic = ?String::from_utf8_lossy(topic), "Unsubscribe attempt on topic with zero count");
      false
    }
  }

  /// Checks if a given message topic matches any active subscription prefix.
  /// Sync — safe to call from the io_uring worker OS thread.
  pub fn matches(&self, message_topic: &[u8]) -> bool {
    let mut current_node_arc = self.root.clone();

    // Empty subscription "" matches everything.
    {
      let root_r = current_node_arc.read();
      if root_r.count.load(Ordering::Relaxed) > 0 {
        return true;
      }
    }

    for &byte in message_topic {
      let (matched_prefix, next_node_option) = {
        let current_node_r = current_node_arc.read();
        let prefix_match = current_node_r.count.load(Ordering::Relaxed) > 0;
        let next_node = current_node_r.children.get(&byte).cloned();
        (prefix_match, next_node)
      };

      if matched_prefix {
        return true;
      }

      match next_node_option {
        Some(next_node_arc) => {
          current_node_arc = next_node_arc;
        }
        None => return false,
      }
    }

    let final_node_r = current_node_arc.read();
    final_node_r.count.load(Ordering::Relaxed) > 0
  }

  fn collect_topics_recursive(
    &self,
    node_arc: Arc<RwLock<TrieNode>>,
    current_prefix: Vec<u8>,
    all_topics: &mut Vec<Vec<u8>>,
  ) {
    let children_to_visit: Vec<(u8, Arc<RwLock<TrieNode>>)> = {
      let node_r = node_arc.read();
      if node_r.count.load(Ordering::Relaxed) > 0 {
        all_topics.push(current_prefix.clone());
      }
      node_r.children.iter().map(|(&b, arc)| (b, arc.clone())).collect()
    };

    for (byte, child_arc) in children_to_visit {
      let mut next_prefix = current_prefix.clone();
      next_prefix.push(byte);
      self.collect_topics_recursive(child_arc, next_prefix, all_topics);
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
          trie.subscribe(topic.as_bytes());
        }
      }));
    }
    for h in handles {
      h.await.unwrap();
    }

    let all_topics = trie.get_all_topics();
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

    for i in 0..100 {
      trie.subscribe(format!("base/topic/{}", i).as_bytes());
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();

    for task_id in 0..5usize {
      let trie = trie.clone();
      let stop = stop.clone();
      handles.push(tokio::spawn(async move {
        let mut i = 0u64;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
          let topic = format!("dynamic/{}/{}", task_id, i % 50);
          trie.subscribe(topic.as_bytes());
          trie.unsubscribe(topic.as_bytes());
          i += 1;
        }
      }));
    }

    for _ in 0..5usize {
      let trie = trie.clone();
      let stop = stop.clone();
      handles.push(tokio::spawn(async move {
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
          let topics = trie.get_all_topics();
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
    trie.subscribe(b"");

    assert!(trie.matches(b"sports/football"));
    assert!(trie.matches(b"news/weather"));
  }

  #[tokio::test]
  async fn test_trie_overlapping_subscriptions() {
    let trie = SubscriptionTrie::new();
    let topic = b"news";

    trie.subscribe(topic);
    trie.subscribe(topic); // Double subscribe → count = 2

    let removed = trie.unsubscribe(topic);
    assert!(!removed, "Should not fully remove topic with count still at 1");
    assert!(trie.matches(topic), "Topic should still match");

    let removed_final = trie.unsubscribe(topic);
    assert!(removed_final, "Topic should be fully removed on last unsubscribe");
    assert!(!trie.matches(topic), "Topic should no longer match");
  }

  #[tokio::test]
  async fn test_trie_recursive_topic_collection() {
    let trie = SubscriptionTrie::new();
    trie.subscribe(b"sports");
    trie.subscribe(b"sports/football");
    trie.subscribe(b"news/weather");

    let mut topics = trie.get_all_topics();
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
