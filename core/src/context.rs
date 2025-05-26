use crate::error::ZmqError;
#[cfg(feature = "io-uring")]
use crate::runtime::uring_runtime;
use crate::runtime::{ActorType, EventBus, MailboxSender, SystemEvent, WaitGroup, DEFAULT_MAILBOX_CAPACITY};
use crate::socket::{Socket, SocketType}; // For creating and managing Sockets

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering}; // Renamed Ordering to avoid clash
use std::sync::Arc;

#[cfg(feature = "curve")]
use libsodium_rs as libsodium;
use tracing::warn; // For CURVE security initialization

use std::time::Duration;

/// Information stored in the inproc registry for a bound endpoint.
/// This is used by in-process connectors to find the binder.
#[derive(Debug, Clone)]
#[cfg(feature = "inproc")]
pub(crate) struct InprocBinding {
  /// The unique handle ID of the `SocketCore` actor that bound to this inproc name.
  /// Connectors will use this ID to target events or filter responses if needed,
  /// though primary communication is via `SystemEvent::InprocBindingRequest`.
  pub(crate) binder_core_id: usize,
}

/// Holds the internal state shared by multiple `Context` handles.
/// This struct is Arc'd to allow shared ownership.
#[derive(Debug)]
pub(crate) struct ContextInner {
  /// Source for generating the next available unique handle ID for sockets, pipes, etc.
  pub(crate) next_handle: Arc<std::sync::atomic::AtomicUsize>,
  /// Map of active socket handles to their command mailboxes (single command mailbox per socket).
  /// This allows the context (or other authorized entities) to potentially interact
  /// directly with a socket's command processing loop if absolutely necessary,
  /// though most inter-socket coordination is now event-driven.
  pub(crate) sockets: parking_lot::RwLock<HashMap<usize, MailboxSender>>,
  /// Registry for in-process bindings. Key is the inproc address name (e.g., "my-service").
  #[cfg(feature = "inproc")]
  pub(crate) inproc_registry: parking_lot::RwLock<HashMap<String, InprocBinding>>,

  // --- Shutdown Coordination ---
  /// Central event bus for broadcasting system-wide notifications (e.g., termination, actor lifecycle).
  event_bus: Arc<EventBus>,
  /// WaitGroup tracking all active actors spawned under this context.
  /// This is used by `Context::term()` to wait for all actors to complete shutdown.
  actor_wait_group: WaitGroup,
  /// Flag indicating if context-wide shutdown has been initiated.
  /// Used to prevent redundant shutdown operations and to signal actors.
  pub(crate) shutdown_initiated: AtomicBool,
  actor_mailbox_capacity: usize,
}

impl ContextInner {
  /// Creates new shared context state, including initializing libsodium (if CURVE enabled)
  /// and spawning the event listener task.
  fn new(actor_mailbox_capacity: usize) -> Self {
    #[cfg(feature = "curve")]
    {
      if let Err(_) = libsodium::ensure_init() {
        tracing::error!("Failed to initialize libsodium! CURVE security will not work.");
      } else {
        tracing::debug!("Libsodium initialized successfully.");
      }
    }

    let event_bus = Arc::new(EventBus::new());
    let actor_wait_group = WaitGroup::new();

    Self {
      next_handle: Arc::new(std::sync::atomic::AtomicUsize::new(1)), // Start handle IDs from 1.
      sockets: parking_lot::RwLock::new(HashMap::new()),
      #[cfg(feature = "inproc")]
      inproc_registry: parking_lot::RwLock::new(HashMap::new()),
      event_bus,
      actor_wait_group,
      shutdown_initiated: AtomicBool::new(false),
      actor_mailbox_capacity,
    }
  }

  pub(crate) fn get_actor_mailbox_capacity(&self) -> usize {
    self.actor_mailbox_capacity
  }

  /// Generates the next unique handle ID using an atomic counter.
  pub(crate) fn next_handle(&self) -> usize {
    self.next_handle.fetch_add(1, AtomicOrdering::Relaxed)
  }

  /// Registers a newly created socket actor's command mailbox.
  /// The WaitGroup increment for the socket actor happens *after* its task is spawned,
  /// via an `ActorStarted` event published by the spawner.
  pub(crate) fn register_socket(&self, handle: usize, command_sender: MailboxSender) {
    let mut sockets_w = self.sockets.write();
    sockets_w.insert(handle, command_sender);
    tracing::debug!(socket_handle = handle, "Socket command mailbox registered");
  }

  /// Unregisters a socket actor's command mailbox.
  /// The WaitGroup decrement for the socket actor happens when it publishes an
  /// `ActorStopping` event just before its task terminates.
  pub(crate) fn unregister_socket(&self, handle: usize) {
    let mut sockets_w = self.sockets.write();
    if sockets_w.remove(&handle).is_some() {
      tracing::debug!(socket_handle = handle, "Socket command mailbox unregistered");
    } else {
      tracing::warn!(
        socket_handle = handle,
        "Attempted to unregister non-existent socket handle"
      );
    }
  }

  /// Initiates shutdown for all actors managed by this context.
  /// This is done by publishing a `SystemEvent::ContextTerminating` event.
  /// Individual socket actors are responsible for reacting to this event and shutting down.
  pub(crate) async fn shutdown(&self) {
    if self
      .shutdown_initiated
      .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
      .is_ok()
    {
      tracing::info!("Context shutdown initiated.");
      // Publish the global termination event. All actors should listen for this.
      if let Err(e) = self.event_bus.publish(SystemEvent::ContextTerminating) {
        tracing::warn!(
          "Publishing ContextTerminating event failed (receivers={}): {}",
          self.event_bus.subscriber_count(),
          e
        );
      } else {
        tracing::debug!("Published ContextTerminating event via bus.");
      }
      // Direct Stop commands to individual sockets are removed.
      // Actors now rely on the ContextTerminating event or SocketClosing events from their parents.
    } else {
      tracing::debug!("Context shutdown already initiated.");
    }
  }

  /// Waits until all actors associated with the context have terminated.
  /// This relies on the `actor_wait_group` which is managed by the `event_listener_task`.
  pub(crate) async fn wait_for_termination(&self) {
    if !self.shutdown_initiated.load(AtomicOrdering::Acquire) {
      tracing::warn!("Context::term waiting but shutdown not initiated? Proceeding anyway.");
      // Consider initiating shutdown here if it's a valid recovery path: self.shutdown().await;
    }
    let initial_count = self.actor_wait_group.get_count();
    tracing::debug!(
      count = initial_count,
      "Context wait_for_termination starting wait on WG (includes listener task)..."
    );

    // Add a timeout to prevent indefinite blocking if an actor fails to stop correctly.
    let wait_timeout = Duration::from_secs(10); // Example: 10 second timeout.
    match tokio::time::timeout(wait_timeout, self.actor_wait_group.wait()).await {
      Ok(()) => {
        tracing::info!(
          initial_count,
          final_count = self.actor_wait_group.get_count(),
          "Context termination complete (WaitGroup reached zero)."
        );
      }
      Err(_) => {
        let final_count = self.actor_wait_group.get_count();
        tracing::error!(
            initial_count,
            final_count,
            timeout=?wait_timeout,
            "Context wait_for_termination timed out! {} actors may not have stopped correctly.",
            final_count // This count still includes the listener if it hasn't exited.
        );
        // Consider returning an error or panicking here for critical applications.
      }
    }
  }

  /// Registers an in-process binding. The `binder_core_id` identifies the `SocketCore`
  /// that is binding to this name, allowing it to filter `InprocBindingRequest` events.
  #[cfg(feature = "inproc")]
  pub(crate) fn register_inproc(&self, name: String, binder_core_id: usize) -> Result<(), ZmqError> {
    let mut registry = self.inproc_registry.write();
    if registry.contains_key(&name) {
      Err(ZmqError::AddrInUse(format!("inproc://{}", name)))
    } else {
      tracing::debug!(inproc_name = %name, binder_core_id = binder_core_id, "Registering inproc binding");
      registry.insert(name, InprocBinding { binder_core_id });
      Ok(())
    }
  }

  /// Unregisters an in-process binding.
  #[cfg(feature = "inproc")]
  pub(crate) fn unregister_inproc(&self, name: &str) {
    let mut registry = self.inproc_registry.write();
    if registry.remove(name).is_some() {
      tracing::debug!(inproc_name = %name, "Unregistered inproc binding");
    }
  }

  /// Looks up an in-process binding by name.
  #[cfg(feature = "inproc")]
  pub(crate) fn lookup_inproc(&self, name: &str) -> Option<InprocBinding> {
    self.inproc_registry.read().get(name).cloned()
  }

  /// Gets the command mailbox sender for a specific registered socket.
  pub(crate) fn get_socket_command_sender(&self, handle: usize) -> Option<MailboxSender> {
    self.sockets.read().get(&handle).cloned()
  }

  /// Provides access to the shared `EventBus` instance Arc.
  pub(crate) fn event_bus(&self) -> Arc<EventBus> {
    self.event_bus.clone()
  }
}

/// A handle to an rzmq context, managing sockets and shared resources.
/// `Context` handles are cloneable (`Arc`-based).
#[derive(Clone)]
pub struct Context {
  inner: Arc<ContextInner>,
}

impl Context {
  /// Creates a new, independent context.
  pub fn new() -> Result<Self, ZmqError> {
    Self::with_capacity(None)
  }

  /// Creates a new, independent context.
  ///
  /// # Arguments
  /// * `actor_mailbox_capacity`: Optionally, specify the bounded capacity for internal
  ///   actor command mailboxes. If `None`, `rzmq::runtime::DEFAULT_MAILBOX_CAPACITY` is used.
  ///   Minimum capacity is 1.
  pub fn with_capacity(actor_mailbox_capacity: Option<usize>) -> Result<Self, ZmqError> {
    let capacity = actor_mailbox_capacity
      .map(|c| c.max(1)) // Ensure capacity is at least 1
      .unwrap_or(DEFAULT_MAILBOX_CAPACITY);

    tracing::debug!(target_capacity = capacity, "Creating new rzmq Context");

    #[cfg(feature = "io-uring")]
    {
      tracing::debug!("io-uring feature enabled, ensuring UringRuntimeManager is started.");
      uring_runtime::ensure_uring_runtime_manager_started();
    }

    Ok(Self {
      inner: Arc::new(ContextInner::new(capacity)),
    })
  }

  /// Creates a socket of the specified type associated with this context.
  pub fn socket(&self, socket_type: SocketType) -> Result<Socket, ZmqError> {
    let handle = self.inner.next_handle();
    tracing::debug!(socket_type = ?socket_type, handle = handle, "Creating socket");

    // `create_socket_actor` now returns the ISocket logic and the single command_sender.
    let (socket_logic, command_sender) = crate::socket::create_socket_actor(handle, self.clone(), socket_type)?;

    // Register the socket's command mailbox with the context.
    // WG increment for the socket actor happens via an ActorStarted event published by create_socket_actor.
    let inner_clone = self.inner.clone();
    let cmd_sender_clone = command_sender.clone();
    tokio::spawn(async move {
      inner_clone.register_socket(handle, cmd_sender_clone);
    });

    // The public `Socket` handle now needs the command_sender to interact with its `SocketCore`.
    Ok(Socket::new(socket_logic, command_sender))
  }

  /// Initiates background shutdown of all sockets and actors created by this context.
  /// This publishes `ContextTerminating` on the event bus.
  pub async fn shutdown(&self) -> Result<(), ZmqError> {
    self.inner.shutdown().await;
    Ok(())
  }

  /// Shuts down all sockets and waits for their clean termination.
  /// This involves publishing `ContextTerminating` and then waiting on the context's `WaitGroup`.
  pub async fn term(&self) -> Result<(), ZmqError> {
    self.inner.shutdown().await; // Ensure shutdown is initiated.
    self.inner.wait_for_termination().await; // Wait using the WG.
    Ok(())
  }

  /// Internal helper to get the `Arc<ContextInner>`.
  /// Used by `SocketCore` and other internal components to access shared context state.
  pub(crate) fn inner(&self) -> &Arc<ContextInner> {
    &self.inner
  }

  // Helper for actors to get the event bus Arc easily from a Context handle.
  pub(crate) fn event_bus(&self) -> Arc<EventBus> {
    self.inner.event_bus() // Delegate to inner.
  }

  /// Publishes an `ActorStarted` event. This is typically called by the code
  /// that spawns a new actor task, immediately after successful spawning.
  pub(crate) fn publish_actor_started(&self, handle_id: usize, actor_type: ActorType, parent_id: Option<usize>) {
    let event = SystemEvent::ActorStarted {
      handle_id,
      actor_type,
      parent_id,
    };
    if let Err(e) = self.inner.event_bus().publish(event) {
      tracing::warn!(
        actor_handle = handle_id,
        ?actor_type,
        "Failed to publish ActorStarted event: {}",
        e
      );
    }

    let wg = &self.inner.actor_wait_group; // Borrow the WaitGroup
    wg.add(1); // Increment WaitGroup for the newly started actor.
  }

  /// Publishes an `ActorStopping` event. This is typically called by an actor task
  /// itself, just before it exits, to signal its termination.
  pub(crate) fn publish_actor_stopping(
    &self,
    handle_id: usize,
    actor_type: ActorType,
    endpoint_uri: Option<String>,
    error: Option<ZmqError>,
  ) {
    let error_msg_opt = error.map(|e| format!("{}", e));
    let event = SystemEvent::ActorStopping {
      handle_id,
      actor_type,
      endpoint_uri,
      error_msg: error_msg_opt,
    };

    // --- Attempt to publish the event (best effort) ---
    if let Err(e) = self.inner.event_bus().publish(event) {
      // Log failure, especially if during active shutdown phase.
      // Use eprintln for higher chance of visibility during shutdown/panic.
      // Check if already panicking to avoid making things worse.
      if !std::thread::panicking() {
        warn!(
          "WARN: Failed to publish ActorStopping event for handle {}: {} (receivers={})",
          handle_id,
          e,
          self.inner.event_bus().subscriber_count()
        );
      }
      // Note: Even if publish fails, we MUST decrement the WaitGroup below.
    } else {
      // Optional: Trace successful publish if needed, but can be noisy.
      // tracing::trace!(actor_handle = handle_id, ?actor_type, "Published ActorStopping event");
    }

    // --- Unconditionally decrement the WaitGroup ---
    // This is the crucial part to ensure termination completes even if the
    // event listener is gone or event publishing fails.
    let wg = &self.inner.actor_wait_group; // Borrow the WaitGroup
                                           // Ensure count doesn't go below zero before decrementing.
    if wg.get_count() > 0 {
      tracing::trace!(
        actor_handle = handle_id,
        ?actor_type,
        wg_prev = wg.get_count(),
        "Decrementing WaitGroup for stopping actor."
      );
      wg.done(); // Directly decrement the counter
    } else {
      // Log a warning if done() is called when count is already zero.
      // This indicates a potential logic error (e.g., double stop).
      if !std::thread::panicking() {
        warn!(
          "WARN: Attempted WaitGroup done() for handle {} ({:?}) but count was already zero!",
          handle_id, actor_type
        );
      }
      // Consider if panicking here is appropriate, maybe only in debug builds.
      // panic!("WaitGroup done() called with count zero for handle {} ({:?})", handle_id, actor_type);
    }
  }
}

impl fmt::Debug for Context {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // Provide a more informative Debug representation if useful,
    // e.g., number of active sockets, shutdown status.
    // For now, keep it simple.
    f.debug_struct("Context").finish_non_exhaustive()
  }
}

/// Creates a new library context. This is the main entry point for using rzmq.
pub fn context() -> Result<Context, ZmqError> {
  Context::new()
}
