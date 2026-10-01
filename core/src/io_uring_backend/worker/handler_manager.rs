#![cfg(feature = "io-uring")]

use crate::io_uring_backend::{
  buffer_manager::BufferRingManager,
  connection_handler::{
    HandlerIoOps, ProtocolHandlerFactory, UringConnectionHandler, UringWorkerInterface,
    WorkerIoConfig,
  },
  ops::ProtocolConfig,
  UserData,
};
use crate::runtime::MailboxSyncSender;
use crate::socket::connection_iface::ISocketConnection;

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use tracing::{debug, error, info, trace};

pub(crate) struct HandlerManager {
  handlers: HashMap<RawFd, Box<dyn UringConnectionHandler + Send>>,
  factories: Arc<HashMap<String, Arc<dyn ProtocolHandlerFactory>>>,
}

impl HandlerManager {
  pub fn new(factories_vec: Vec<Arc<dyn ProtocolHandlerFactory>>) -> Self {
    let mut factory_map = HashMap::new();
    for factory_arc in factories_vec {
      factory_map.insert(factory_arc.id().to_string(), factory_arc);
    }
    Self {
      handlers: HashMap::new(),
      factories: Arc::new(factory_map),
    }
  }

  pub(crate) fn fill_active_fds(&self, dst: &mut Vec<RawFd>) {
    dst.clear();
    dst.extend(self.handlers.keys().copied());
  }

  /// Creates a new handler, adds it, calls `connection_ready`, and returns initial I/O operations.
  pub fn create_and_add_handler<'a>(
    &mut self,
    fd: RawFd,
    factory_id: &str,
    protocol_config: &ProtocolConfig,
    is_server: bool,
    socket_mailbox: MailboxSyncSender,
    endpoint_uri: String,
    target_endpoint_uri: String,
    connection_iface: Arc<dyn ISocketConnection>,
    buffer_manager_for_interface: Option<&'a BufferRingManager>,
    default_bgid_val_from_worker: Option<u16>,
    originating_op_ud_for_connection: UserData,
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

    let per_conn_config = Arc::new(WorkerIoConfig {
      socket_mailbox,
      endpoint_uri,
      target_endpoint_uri,
      connection_iface,
    });

    let mut handler_box =
      factory.create_handler(fd, per_conn_config.clone(), protocol_config, is_server)?;

    info!(
      "HandlerManager: Created handler for FD {} using factory '{}'. Calling connection_ready...",
      fd, factory_id
    );

    let interface_for_ready = UringWorkerInterface::new(
      fd,
      &per_conn_config,
      buffer_manager_for_interface,
      default_bgid_val_from_worker,
      originating_op_ud_for_connection,
      0,
      false,
      0, // egress_cap unused at connection_ready time (no egress yet)
    );

    let initial_ops = handler_box.connection_ready(&interface_for_ready);
    self.handlers.insert(fd, handler_box);
    Ok(initial_ops)
  }

  /// Adds a pre-built handler (bypasses the factory), calls `connection_ready`, and stores it.
  /// Used to inject a pre-built `UringByteHandler` directly, bypassing the factory.
  pub(crate) fn add_handler_directly(
    &mut self,
    fd: RawFd,
    mut handler: Box<dyn UringConnectionHandler + Send>,
    buffer_manager: Option<&BufferRingManager>,
    default_bgid: Option<u16>,
    originating_op_ud: UserData,
  ) -> Result<HandlerIoOps, String> {
    if self.handlers.contains_key(&fd) {
      return Err(format!(
        "HandlerManager: FD {} already registered, cannot add_handler_directly",
        fd
      ));
    }
    let config_clone = handler.io_config().clone();
    let interface = UringWorkerInterface::new(
      fd,
      &config_clone,
      buffer_manager,
      default_bgid,
      originating_op_ud,
      0,
      false,
      0, // egress_cap unused at connection_ready time (no egress yet)
    );
    let initial_ops = handler.connection_ready(&interface);
    self.handlers.insert(fd, handler);
    info!(
      "HandlerManager: Directly added handler for FD {} via add_handler_directly.",
      fd
    );
    Ok(initial_ops)
  }

  pub fn get_mut(&mut self, fd: RawFd) -> Option<&mut Box<dyn UringConnectionHandler + Send>> {
    self.handlers.get_mut(&fd)
  }

  pub fn remove_handler(&mut self, fd: RawFd) -> Option<Box<dyn UringConnectionHandler + Send>> {
    debug!("HandlerManager: Removing handler for FD {}.", fd);
    self.handlers.remove(&fd)
  }

  #[allow(dead_code)] // May be useful
  pub fn contains_handler_for(&self, fd: RawFd) -> bool {
    self.handlers.contains_key(&fd)
  }

  /// True if any handler has spillover bytes that can now flow into the inbound channel
  /// (i.e., the Tokio side drained the channel below capacity). Used in the pre-sleep
  /// double-check to prevent the worker from sleeping while there is drainable spillover.
  pub fn any_handler_has_inbound_data(&self) -> bool {
    self.handlers.values().any(|h| h.has_drainable_spillover())
  }

  /// Calls `prepare_sqes` on all managed handlers and collects their requested operations.
  pub fn prepare_all_handler_io_ops<'a>(
    &mut self,
    buffer_manager_for_interface: Option<&'a BufferRingManager>,
    default_bgid_val_from_worker: Option<u16>,
    egress_cap: usize,
    get_pending_egress: impl Fn(RawFd) -> usize,
  ) -> Vec<(RawFd, HandlerIoOps)> {
    let mut all_ops = Vec::new();
    const PREPARE_SQES_SENTINEL_UD: UserData = 0;

    for (fd, handler) in self.handlers.iter_mut() {
      let pending_egress = get_pending_egress(*fd);
      let io_config = handler.io_config().clone();
      let interface = UringWorkerInterface::new(
        *fd,
        &io_config,
        buffer_manager_for_interface,
        default_bgid_val_from_worker,
        PREPARE_SQES_SENTINEL_UD,
        pending_egress,
        false,
        egress_cap,
      );
      trace!("HandlerManager: Calling prepare_sqes for FD {}", fd);
      let handler_output = handler.prepare_sqes(&interface);
      if !handler_output.sqe_blueprints.is_empty() || handler_output.initiate_close_due_to_error {
        all_ops.push((*fd, handler_output));
      }
    }
    all_ops
  }

  #[allow(dead_code)] // May be used in shutdown sequence
  pub(crate) fn iter_mut_for_shutdown(
    &mut self,
  ) -> impl Iterator<Item = (RawFd, &mut Box<dyn UringConnectionHandler + Send>)> {
    self.handlers.iter_mut().map(|(fd, handler)| (*fd, handler))
  }
}
