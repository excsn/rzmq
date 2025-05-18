// src/runtime/event_bus.rs
#![allow(dead_code)] // Allow unused elements during development

use super::system_events::SystemEvent; // Use the defined event type
use tokio::sync::broadcast::{self, error::SendError, Receiver, Sender};
use tracing; // <<< ADDED

// Define a reasonable capacity for the bus
const DEFAULT_EVENT_BUS_CAPACITY: usize = 256;

/// A self-contained event bus for broadcasting system-wide events.
/// Internally uses tokio::sync::broadcast.
#[derive(Debug, Clone)] // <<< MODIFIED [Added Clone] >>>
pub struct EventBus {
  sender: Sender<SystemEvent>,
}

impl EventBus {
  /// Creates a new EventBus with default capacity.
  pub fn new() -> Self {
    let (sender, _) = broadcast::channel(DEFAULT_EVENT_BUS_CAPACITY);
    tracing::debug!(capacity = DEFAULT_EVENT_BUS_CAPACITY, "Created new EventBus"); // <<< ADDED >>>
    Self { sender }
  }

  /// Creates a new EventBus with specific capacity.
  pub fn with_capacity(capacity: usize) -> Self {
    let (sender, _) = broadcast::channel(capacity.max(1)); // Ensure capacity >= 1
    tracing::debug!(capacity = capacity.max(1), "Created new EventBus with capacity"); // <<< ADDED >>>
    Self { sender }
  }

  /// Publishes an event onto the bus.
  ///
  /// Returns the number of active receivers the event was sent to,
  /// or an error if there are no receivers (which might indicate an issue).
  pub fn publish(&self, event: SystemEvent) -> Result<usize, SendError<SystemEvent>> {
    tracing::trace!(event = ?event, "Publishing event"); // <<< ADDED >>>
    self.sender.send(event)
  }

  /// Creates a new receiver handle to subscribe to events from the bus.
  ///
  /// Each receiver will see all events published *after* it subscribed.
  /// If a receiver lags, it might miss messages (see `tokio::sync::broadcast` docs).
  pub fn subscribe(&self) -> Receiver<SystemEvent> {
    tracing::trace!("Creating new event bus subscription");
    self.sender.subscribe()
  }

  /// Returns the number of active subscribers.
  pub fn subscriber_count(&self) -> usize {
    self.sender.receiver_count()
  }
}

// Implement Default trait for convenience
impl Default for EventBus {
  fn default() -> Self {
    Self::new()
  }
}
