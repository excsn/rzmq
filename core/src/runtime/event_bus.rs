use super::system_events::SystemEvent;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;

pub(crate) use tokio::sync::mpsc::error::TryRecvError;

/// Which events a subscriber receives. Every subscriber receives the
/// context-wide ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Subscriber {
  /// A `SocketCore`: its own closing, what its child actors report to it and
  /// inproc binding requests.
  Core(usize),
  /// A listener, connecter or session: its parent socket closing and stopping.
  ChildOf(usize),
}

type EventSender = mpsc::UnboundedSender<SystemEvent>;

#[derive(Default)]
struct Routes {
  next_id: u64,
  cores: HashMap<usize, (u64, EventSender)>,
  children: HashMap<usize, HashMap<u64, EventSender>>,
}

impl Routes {
  fn to_core(&self, handle: usize, event: &SystemEvent) -> usize {
    match self.cores.get(&handle) {
      Some((_, sender)) => usize::from(sender.send(event.clone()).is_ok()),
      None => 0,
    }
  }

  fn to_children_of(&self, parent: usize, event: &SystemEvent) -> usize {
    self.children.get(&parent).map_or(0, |senders| {
      senders
        .values()
        .filter(|sender| sender.send(event.clone()).is_ok())
        .count()
    })
  }

  fn to_all_cores(&self, event: &SystemEvent) -> usize {
    self
      .cores
      .values()
      .filter(|(_, sender)| sender.send(event.clone()).is_ok())
      .count()
  }

  fn to_everyone(&self, event: &SystemEvent) -> usize {
    self.to_all_cores(event)
      + self
        .children
        .keys()
        .map(|parent| self.to_children_of(*parent, event))
        .sum::<usize>()
  }

  fn subscriber_count(&self) -> usize {
    self.cores.len() + self.children.values().map(HashMap::len).sum::<usize>()
  }
}

/// Delivers each `SystemEvent` to the actors that act on it. Every subscriber
/// has its own unbounded queue, so an actor that is slow to poll delays its own
/// events and loses none.
pub(crate) struct EventBus {
  routes: Arc<Mutex<Routes>>,
}

impl EventBus {
  pub fn new() -> Self {
    Self {
      routes: Arc::new(Mutex::new(Routes::default())),
    }
  }

  /// Returns how many subscribers the event was queued for. One subscriber
  /// sees events in the order they were published.
  pub fn publish(&self, event: SystemEvent) -> usize {
    tracing::trace!(event = ?event, "Publishing event");
    let routes = self.routes.lock();
    match &event {
      SystemEvent::ContextTerminating | SystemEvent::ContextTerminationForced => routes.to_everyone(&event),
      SystemEvent::SocketClosing { socket_id } => {
        routes.to_core(*socket_id, &event) + routes.to_children_of(*socket_id, &event)
      }
      SystemEvent::ActorStarted { .. } => 0,
      SystemEvent::ActorStopping {
        handle_id, parent_id, ..
      } => {
        parent_id.map_or(0, |parent| routes.to_core(parent, &event)) + routes.to_children_of(*handle_id, &event)
      }
      SystemEvent::PeerIdentityEstablished { parent_core_id, .. }
      | SystemEvent::HandshakeFailed { parent_core_id, .. }
      | SystemEvent::ConnectionAttemptFailed { parent_core_id, .. } => routes.to_core(*parent_core_id, &event),
      #[cfg(feature = "inproc")]
      SystemEvent::InprocBindingRequest { .. } => routes.to_all_cores(&event),
    }
  }

  pub fn subscribe(&self, subscriber: Subscriber) -> EventReceiver {
    let (sender, receiver) = mpsc::unbounded_channel();
    let mut routes = self.routes.lock();
    routes.next_id += 1;
    let id = routes.next_id;
    match subscriber {
      Subscriber::Core(handle) => {
        routes.cores.insert(handle, (id, sender));
      }
      Subscriber::ChildOf(parent) => {
        routes.children.entry(parent).or_default().insert(id, sender);
      }
    }
    EventReceiver {
      routes: Arc::downgrade(&self.routes),
      subscriber,
      id,
      receiver,
    }
  }

  pub fn subscriber_count(&self) -> usize {
    self.routes.lock().subscriber_count()
  }
}

impl Default for EventBus {
  fn default() -> Self {
    Self::new()
  }
}

impl fmt::Debug for EventBus {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("EventBus")
      .field("subscribers", &self.subscriber_count())
      .finish()
  }
}

pub(crate) struct EventReceiver {
  routes: Weak<Mutex<Routes>>,
  subscriber: Subscriber,
  id: u64,
  receiver: mpsc::UnboundedReceiver<SystemEvent>,
}

impl EventReceiver {
  /// `None` once the bus is gone. Cancel safe.
  pub async fn recv(&mut self) -> Option<SystemEvent> {
    self.receiver.recv().await
  }

  pub fn try_recv(&mut self) -> Result<SystemEvent, TryRecvError> {
    self.receiver.try_recv()
  }
}

impl Drop for EventReceiver {
  fn drop(&mut self) {
    let Some(routes) = self.routes.upgrade() else {
      return;
    };
    let mut routes = routes.lock();
    match self.subscriber {
      Subscriber::Core(handle) => {
        if routes.cores.get(&handle).is_some_and(|(id, _)| *id == self.id) {
          routes.cores.remove(&handle);
        }
      }
      Subscriber::ChildOf(parent) => {
        if let Some(senders) = routes.children.get_mut(&parent) {
          senders.remove(&self.id);
          if senders.is_empty() {
            routes.children.remove(&parent);
          }
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::runtime::ActorType;

  fn stopping(handle_id: usize, parent_id: Option<usize>) -> SystemEvent {
    SystemEvent::ActorStopping {
      handle_id,
      actor_type: ActorType::Session,
      parent_id,
      endpoint_uri: None,
      error: None,
    }
  }

  fn drained(receiver: &mut EventReceiver) -> Vec<String> {
    let mut seen = Vec::new();
    while let Ok(event) = receiver.try_recv() {
      seen.push(format!("{event:?}"));
    }
    seen
  }

  #[test]
  fn a_child_report_reaches_its_core_only() {
    let bus = EventBus::new();
    let mut core = bus.subscribe(Subscriber::Core(1));
    let mut other_core = bus.subscribe(Subscriber::Core(2));
    let mut sibling = bus.subscribe(Subscriber::ChildOf(1));

    assert_eq!(bus.publish(stopping(10, Some(1))), 1);

    assert_eq!(drained(&mut core).len(), 1);
    assert!(drained(&mut other_core).is_empty());
    assert!(drained(&mut sibling).is_empty());
  }

  #[test]
  fn a_socket_closing_and_stopping_reach_its_children() {
    let bus = EventBus::new();
    let mut core = bus.subscribe(Subscriber::Core(1));
    let mut child = bus.subscribe(Subscriber::ChildOf(1));
    let mut other_child = bus.subscribe(Subscriber::ChildOf(2));

    assert_eq!(bus.publish(SystemEvent::SocketClosing { socket_id: 1 }), 2);
    assert_eq!(bus.publish(stopping(1, None)), 1);

    assert_eq!(drained(&mut core).len(), 1);
    assert_eq!(drained(&mut child).len(), 2);
    assert!(drained(&mut other_child).is_empty());
  }

  #[test]
  fn context_events_reach_every_subscriber() {
    let bus = EventBus::new();
    let mut core = bus.subscribe(Subscriber::Core(1));
    let mut child = bus.subscribe(Subscriber::ChildOf(1));

    assert_eq!(bus.publish(SystemEvent::ContextTerminating), 2);
    assert_eq!(bus.publish(SystemEvent::ContextTerminationForced), 2);

    assert_eq!(drained(&mut core).len(), 2);
    assert_eq!(drained(&mut child).len(), 2);
  }

  #[test]
  fn an_unpolled_subscriber_loses_no_event() {
    let bus = EventBus::new();
    let mut core = bus.subscribe(Subscriber::Core(1));

    for child in 0..100_000 {
      bus.publish(stopping(child, Some(1)));
    }

    assert_eq!(drained(&mut core).len(), 100_000);
  }

  #[test]
  fn a_dropped_receiver_leaves_the_routes() {
    let bus = EventBus::new();
    let core = bus.subscribe(Subscriber::Core(1));
    let child = bus.subscribe(Subscriber::ChildOf(1));
    assert_eq!(bus.subscriber_count(), 2);

    drop(child);
    drop(core);

    assert_eq!(bus.subscriber_count(), 0);
    assert_eq!(bus.publish(SystemEvent::ContextTerminating), 0);
  }

  #[test]
  fn a_receiver_ends_when_the_bus_is_gone() {
    let bus = EventBus::new();
    let mut core = bus.subscribe(Subscriber::Core(1));
    drop(bus);

    assert!(matches!(core.try_recv(), Err(TryRecvError::Disconnected)));
  }
}
