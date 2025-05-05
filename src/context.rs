// src/context.rs

use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender}; // Use mailbox fn
use crate::socket::{self, ISocket, Socket, SocketType};

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(feature = "curve")]
use libsodium_rs as libsodium;
use tokio::sync::{Notify, RwLock}; // Use Tokio's RwLock for async access

/// Information stored in the inproc registry for a bound endpoint.
#[derive(Debug, Clone)]
#[cfg(feature = "inproc")]
pub(crate) struct InprocBinding {
  /// The mailbox of the SocketCore actor that bound to this name.
  /// The connector sends an `InprocConnectRequest` command here.
  pub(crate) binder_command_mailbox: MailboxSender,
  // Store socket type? Optional, for validation.
  // socket_type: crate::socket::SocketType,
}

/// Holds the internal state shared by multiple Context handles.
#[derive(Debug)] // Basic Debug for now
pub(crate) struct ContextInner {
  /// Next available unique handle ID for sockets, pipes, etc.
  pub(crate) next_handle: Arc<AtomicUsize>, // Use Atomic for simple counter

  /// Map of active socket handles to their command mailboxes.
  pub(crate) sockets: RwLock<HashMap<usize, MailboxSender>>, // Async RwLock

  /// Registry for in-process bindings. Key is the inproc address name.
  #[cfg(feature = "inproc")]
  pub(crate) inproc_registry: RwLock<HashMap<String, InprocBinding>>, // Feature-gated

  // --- Shutdown Coordination ---
  /// Used to notify tasks waiting in `Context::term()` when shutdown might be complete.
  pub(crate) shutdown_notify: Notify,
  /// Count of currently active sockets associated with this context.
  pub(crate) active_sockets: AtomicUsize,
  /// Flag indicating if shutdown has been initiated.
  pub(crate) shutdown_initiated: AtomicBool,
}

impl ContextInner {
  /// Creates new shared context state.
  fn new() -> Self {
    #[cfg(feature = "curve")]
    {
      // libsodium::init() returns Result, handle potential error
      if let Err(_) = libsodium::ensure_init() {
        // Log error, decide if this should be fatal for the context/app
        tracing::error!("Failed to initialize libsodium! CURVE security will not work.");
        // Consider returning an error:
        // return Err(ZmqError::Internal("Libsodium initialization failed".to_string()));
      } else {
        tracing::debug!("Libsodium initialized successfully.");
      }
    }
    Self {
      next_handle: Arc::new(AtomicUsize::new(1)),
      sockets: RwLock::new(HashMap::new()),
      #[cfg(feature = "inproc")]
      inproc_registry: RwLock::new(HashMap::new()),
      shutdown_notify: Notify::new(),
      active_sockets: AtomicUsize::new(0),
      shutdown_initiated: AtomicBool::new(false),
    }
  }

  /// Generates the next unique handle ID.
  pub(crate) fn next_handle(&self) -> usize {
    // Relaxed ordering is sufficient for a simple counter
    self.next_handle.fetch_add(1, Ordering::Relaxed)
  }

  /// Registers a newly created socket actor.
  pub(crate) async fn register_socket(&self, handle: usize, mailbox: MailboxSender) {
    let mut sockets_w = self.sockets.write().await;
    sockets_w.insert(handle, mailbox);
    self.active_sockets.fetch_add(1, Ordering::AcqRel);
    tracing::debug!(socket_handle = handle, "Socket registered");
  }

  /// Unregisters a socket actor, typically when it stops cleanly.
  pub(crate) async fn unregister_socket(&self, handle: usize) {
    let mut sockets_w = self.sockets.write().await;
    if sockets_w.remove(&handle).is_some() {
      let prev_count = self.active_sockets.fetch_sub(1, Ordering::AcqRel);
      tracing::debug!(socket_handle = handle, "Socket unregistered");
      // If this was the last socket AND shutdown was initiated, notify waiters.
      if prev_count == 1 && self.shutdown_initiated.load(Ordering::Acquire) {
        tracing::debug!("Last socket unregistered during shutdown, notifying term waiters.");
        self.shutdown_notify.notify_waiters();
      }
    } else {
      tracing::warn!(socket_handle = handle, "Attempted to unregister non-existent socket");
    }
  }

  /// Initiates shutdown for all registered sockets.
  pub(crate) async fn shutdown(&self) {
    if self
      .shutdown_initiated
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
    {
      tracing::info!("Context shutdown initiated.");
      let sockets_r = self.sockets.read().await;
      if sockets_r.is_empty() {
        tracing::debug!("No active sockets during shutdown initiation.");
        // If already empty when shutdown starts, notify immediately
        self.shutdown_notify.notify_waiters();
        return;
      }

      // Create a list of mailboxes to avoid holding read lock while sending
      let mailboxes_to_stop: Vec<_> = sockets_r.values().cloned().collect();
      drop(sockets_r); // Release read lock

      // Send Stop command to all sockets concurrently
      let stop_futures = mailboxes_to_stop.into_iter().map(|mb| async move {
        // Ignore send errors - socket might have already terminated
        let _ = mb.send(Command::Stop).await;
      });
      futures::future::join_all(stop_futures).await;
      tracing::debug!("Sent Stop command to all registered sockets.");
    } else {
      tracing::debug!("Context shutdown already initiated.");
    }
  }

  /// Waits until all sockets are unregistered after shutdown is initiated.
  pub(crate) async fn wait_for_termination(&self) {
    if !self.shutdown_initiated.load(Ordering::Acquire) {
      tracing::warn!("Context::term called before shutdown initiated. Call shutdown() first or just use term().");
      // If we didn't initiate shutdown, there's nothing to wait for in this logic.
      // Alternatively, call self.shutdown().await here? Decided against it for clarity.
      return;
    }

    loop {
      let count = self.active_sockets.load(Ordering::Acquire);
      if count == 0 {
        tracing::info!("Context termination complete (all sockets stopped).");
        break;
      }
      tracing::debug!(active_sockets = count, "Waiting for sockets to terminate...");
      // Wait for a notification OR a timeout to avoid infinite loop if something goes wrong
      tokio::select! {
          _ = self.shutdown_notify.notified() => {
              tracing::debug!("Received termination notification signal.");
              // Re-check count immediately after notification
              continue;
          }
          _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
               tracing::warn!(active_sockets = count, "Timeout while waiting for context termination, still checking...");
               // Log timeout and continue loop - allows recovery if notification was missed
          }
      }
    }
  }

  #[cfg(feature = "inproc")]
  pub(crate) async fn register_inproc(&self, name: String, binding_info: InprocBinding) -> Result<(), ZmqError> {
    let mut registry = self.inproc_registry.write().await;
    if registry.contains_key(&name) {
      Err(ZmqError::AddrInUse(format!("inproc://{}", name))) // Address already bound
    } else {
      tracing::debug!(inproc_name = %name, "Registering inproc binding");
      registry.insert(name, binding_info);
      Ok(())
    }
  }

  #[cfg(feature = "inproc")]
  pub(crate) async fn unregister_inproc(&self, name: &str) {
    let mut registry = self.inproc_registry.write().await;
    if registry.remove(name).is_some() {
      tracing::debug!(inproc_name = %name, "Unregistered inproc binding");
    }
  }

  #[cfg(feature = "inproc")]
  pub(crate) async fn lookup_inproc(&self, name: &str) -> Option<InprocBinding> {
    self.inproc_registry.read().await.get(name).cloned()
  }
  
  /// Gets the mailbox for a specific registered socket.
  pub(crate) async fn get_socket_mailbox(&self, handle: usize) -> Option<MailboxSender> {
    self.sockets.read().await.get(&handle).cloned()
  }
}

/// A handle to an rzmq context, managing sockets and shared resources.
/// Contexts are cloneable and thread-safe.
#[derive(Clone)] // Clone is cheap due to Arc
pub struct Context {
  inner: Arc<ContextInner>,
}
// <<< ADDED PUBLIC CONTEXT STRUCT END >>>

// <<< ADDED PUBLIC CONTEXT IMPL >>>
impl Context {
  /// Creates a new, independent context.
  pub fn new() -> Result<Self, ZmqError> {
    // Initialize tracing subscriber (optional, good practice)
    // setup_tracing(); // Define this helper function somewhere

    tracing::debug!("Creating new rzmq Context");
    Ok(Self {
      inner: Arc::new(ContextInner::new()),
    })
  }

  /// Creates a socket of the specified type associated with this context.
  pub fn socket(&self, socket_type: SocketType) -> Result<Socket, ZmqError> {
    let handle = self.inner.next_handle();
    tracing::debug!(socket_type = ?socket_type, handle = handle, "Creating socket");

    // --- Socket Actor Creation ---
    // This is complex: involves Arc::new_cyclic, creating SocketCore,
    // creating the specific ISocket impl (e.g., PubSocket), spawning the
    // SocketCore's run_loop task, and registering with ContextInner.
    // We'll defer the full implementation to SocketCore::create_and_spawn.

    // Placeholder for the actual socket creation logic:
    let (socket_handle, socket_mailbox) = crate::socket::create_socket_actor(handle, self.clone(), socket_type)?;
    // `create_socket_actor` needs to:
    // 1. Create SocketCore + ISocket impl (e.g., PubSocket) via Arc::new_cyclic
    // 2. Spawn SocketCore::run_command_loop task
    // 3. Return Arc<dyn ISocket> handle and the SocketCore's MailboxSender

    // Register the socket *after* it's successfully created and spawned
    let inner = self.inner.clone(); // Clone Arc for async block
    let mailbox = socket_mailbox.clone(); // Clone mailbox for async block
    tokio::spawn(async move {
      inner.register_socket(handle, mailbox).await;
    });

    // Wrap the ISocket Arc in the public Socket handle
    Ok(Socket::new(socket_handle))
  }

  /// Initiates background shutdown of all sockets created by this context.
  /// Returns immediately. Sockets will close gracefully.
  pub async fn shutdown(&self) -> Result<(), ZmqError> {
    self.inner.shutdown().await;
    Ok(())
  }

  /// Shuts down all sockets and waits for their clean termination.
  /// Consumes the Context handle. Use for final cleanup.
  /// Note: This awaits, so it should be called from an async context.
  pub async fn term(self) -> Result<(), ZmqError> {
    self.inner.shutdown().await;
    self.inner.wait_for_termination().await;
    Ok(())
  }

  // --- Internal Methods ---

  /// Internal helper to get the inner Arc. Used by SocketCore etc.
  pub(crate) fn inner(&self) -> &Arc<ContextInner> {
    &self.inner
  }
}
// <<< ADDED PUBLIC CONTEXT IMPL END >>>

// <<< ADDED DEBUG IMPL FOR CONTEXT >>>
impl fmt::Debug for Context {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // Avoid trying to display the full inner state, just identify it
    f.debug_struct("Context")
      // Optionally include a unique ID or pointer address if helpful
      // .field("inner_ptr", &Arc::as_ptr(&self.inner))
      .finish_non_exhaustive()
  }
}
// <<< ADDED DEBUG IMPL FOR CONTEXT END >>>

// <<< MODIFIED LIB.RS CONTEXT FUNCTION >>>
/// Creates a new library context.
pub fn context() -> Result<Context, ZmqError> {
  Context::new() // Now calls the actual implementation
}
// <<< MODIFIED LIB.RS CONTEXT FUNCTION END >>>
