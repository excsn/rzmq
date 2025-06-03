// core/src/io_uring_backend/worker/handler_manager.rs

#![cfg(feature = "io-uring")]

use crate::io_uring_backend::{
  buffer_manager::BufferRingManager,
  connection_handler::{
    HandlerIoOps, ProtocolHandlerFactory, UringConnectionHandler, UringWorkerInterface, WorkerIoConfig,
  },
  ops::ProtocolConfig,
  UserData, // Import the ProtocolConfig enum
};

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use tracing::{debug, error, info, trace, warn};

/// Metadata stored for active listener FDs.
#[derive(Debug, Clone)] // ProtocolConfig needs to be Clone
pub(crate) struct ListenerMetadata {
  pub(crate) factory_id_for_accepted_connections: String,
  pub(crate) protocol_config_for_accepted: ProtocolConfig, // Stores the config for this listener
}

pub(crate) struct HandlerManager {
  handlers: HashMap<RawFd, Box<dyn UringConnectionHandler + Send>>,
  factories: Arc<HashMap<String, Arc<dyn ProtocolHandlerFactory>>>, // Stores Arc<dyn Trait>
  worker_io_config: Arc<WorkerIoConfig>,                            // Passed to handlers via UringWorkerInterface
  listener_metadata: HashMap<RawFd, ListenerMetadata>,              // Keyed by listener FD
}

impl HandlerManager {
  pub fn new(factories_vec: Vec<Arc<dyn ProtocolHandlerFactory>>, worker_io_config: Arc<WorkerIoConfig>) -> Self {
    let mut factory_map = HashMap::new();
    for factory_arc in factories_vec {
      factory_map.insert(factory_arc.id().to_string(), factory_arc);
    }
    Self {
      handlers: HashMap::new(),
      factories: Arc::new(factory_map),
      worker_io_config,
      listener_metadata: HashMap::new(),
    }
  }

  /// Creates a new handler, adds it, calls `connection_ready`, and returns initial I/O operations.
  pub fn create_and_add_handler<'a>(
    &mut self,
    fd: RawFd,
    factory_id: &str,
    protocol_config: &ProtocolConfig, // Passed as a reference from caller
    is_server: bool,                  // Indicates if this handler is for server-side (accepted)
    buffer_manager_for_interface: Option<&'a BufferRingManager>,
    default_bgid_val_from_worker: Option<u16>,
    originating_op_ud_for_connection: UserData, // UD of Connect/RegisterExternalFd, or sentinel for accept
  ) -> Result<HandlerIoOps, String> {
    if self.handlers.contains_key(&fd) {
      let err_msg = format!(
        "HandlerManager: Handler for FD {} already exists. Cannot create new one with factory '{}'.",
        fd, factory_id
      );
      error!("{}", err_msg);
      return Err(err_msg);
    }

    let factory = self.factories.get(factory_id).ok_or_else(|| {
      format!(
        "HandlerManager: ProtocolHandlerFactory '{}' not found for FD {}.",
        factory_id, fd
      )
    })?;

    // The factory's create_handler now takes &ProtocolConfig
    let mut handler_box = factory.create_handler(
      fd,
      self.worker_io_config.clone(), // For UringWorkerInterface construction later
      protocol_config,               // Pass the reference to the enum variant
      is_server,
    )?; // Propagate error from factory creation

    info!(
      "HandlerManager: Created handler for FD {} using factory '{}'. Calling connection_ready...",
      fd, factory_id
    );

    let interface_for_ready = UringWorkerInterface::new(
      fd,
      &self.worker_io_config,
      buffer_manager_for_interface,
      default_bgid_val_from_worker,
      originating_op_ud_for_connection,
    );

    let initial_ops = handler_box.connection_ready(&interface_for_ready);
    self.handlers.insert(fd, handler_box);
    Ok(initial_ops)
  }

  pub fn get_mut(&mut self, fd: RawFd) -> Option<&mut Box<dyn UringConnectionHandler + Send>> {
    self.handlers.get_mut(&fd)
  }

  pub fn remove_handler(&mut self, fd: RawFd) -> Option<Box<dyn UringConnectionHandler + Send>> {
    debug!(
      "HandlerManager: Removing handler for FD {}. Also removing listener metadata if it was a listener.",
      fd
    );
    // If this FD was a listener, also remove its metadata.
    // It's okay if it wasn't a listener; remove will do nothing.
    self.listener_metadata.remove(&fd);
    self.handlers.remove(&fd)
  }

  #[allow(dead_code)] // May be useful
  pub fn contains_handler_for(&self, fd: RawFd) -> bool {
    self.handlers.contains_key(&fd)
  }

  /// Calls `prepare_sqes` on all managed handlers and collects their requested operations.
  pub fn prepare_all_handler_io_ops<'a>(
    &mut self,
    buffer_manager_for_interface: Option<&'a BufferRingManager>,
    default_bgid_val_from_worker: Option<u16>,
  ) -> Vec<(RawFd, HandlerIoOps)> {
    let mut all_ops = Vec::new();
    const PREPARE_SQES_SENTINEL_UD: UserData = 0; // Sentinel for general polling

    for (fd, handler) in self.handlers.iter_mut() {
      let interface = UringWorkerInterface::new(
        *fd,
        &self.worker_io_config,
        buffer_manager_for_interface,
        default_bgid_val_from_worker,
        PREPARE_SQES_SENTINEL_UD,
      );
      trace!("HandlerManager: Calling prepare_sqes for FD {}", fd);
      let handler_output = handler.prepare_sqes(&interface);
      if !handler_output.sqe_blueprints.is_empty() || handler_output.initiate_close_due_to_error {
        all_ops.push((*fd, handler_output));
      }
    }
    all_ops
  }

  /// Stores metadata for a listener FD, including the factory ID and config for accepted connections.
  pub fn add_listener_metadata(
    &mut self,
    listener_fd: RawFd,
    factory_id_for_accepted_connections: String,
    protocol_config_for_accepted: ProtocolConfig, // Store the actual ProtocolConfig
  ) {
    info!(
      "HandlerManager: Adding listener metadata for FD {}. Accepted conns will use factory '{}' with specific config.",
      listener_fd, factory_id_for_accepted_connections
    );
    self.listener_metadata.insert(
      listener_fd,
      ListenerMetadata {
        factory_id_for_accepted_connections,
        protocol_config_for_accepted,
      },
    );
  }

  /// Retrieves the stored metadata for a listener FD.
  /// This is used by `cqe_processor` when an `Accept` SQE completes.
  pub fn get_listener_metadata(&self, listener_fd: RawFd) -> Option<&ListenerMetadata> {
    self.listener_metadata.get(&listener_fd)
  }

  #[allow(dead_code)] // May be useful
  pub fn is_listener_fd(&self, fd: RawFd) -> bool {
    self.listener_metadata.contains_key(&fd)
  }

  /// Removes all handlers, calling `fd_has_been_closed` on each.
  /// Also clears all listener metadata.
  pub fn drain_all_handlers_calling_closed(&mut self) -> Vec<Box<dyn UringConnectionHandler + Send>> {
    info!("HandlerManager: Draining all handlers and calling fd_has_been_closed.");
    let mut drained_handlers = Vec::new();
    for (_fd, mut handler) in self.handlers.drain() {
      handler.fd_has_been_closed(); // Notify handler
      drained_handlers.push(handler);
    }
    self.listener_metadata.clear();
    drained_handlers
  }

  #[allow(dead_code)] // May be used in shutdown sequence
  pub(crate) fn iter_mut_for_shutdown(
    &mut self,
  ) -> impl Iterator<Item = (RawFd, &mut Box<dyn UringConnectionHandler + Send>)> {
    self.handlers.iter_mut().map(|(fd, handler)| (*fd, handler))
  }

  pub(crate) fn get_active_fds(&self) -> Vec<RawFd> {
    self.handlers.keys().copied().collect()
  }
}
