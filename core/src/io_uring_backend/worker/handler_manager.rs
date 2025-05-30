// core/src/io_uring_backend/worker/handler_manager.rs

#![cfg(feature = "io-uring")]

use crate::io_uring_backend::{buffer_manager::BufferRingManager, connection_handler::{
    HandlerIoOps, ProtocolHandlerFactory, UringConnectionHandler, UringWorkerInterface, WorkerIoConfig         // The new return type from handler methods
}};

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Metadata stored for active listener FDs.
#[derive(Debug, Clone)]
struct ListenerMetadata {
    factory_id_for_accepted_connections: String,
}

pub(crate) struct HandlerManager {
    handlers: HashMap<RawFd, Box<dyn UringConnectionHandler + Send>>,
    factories: Arc<HashMap<String, Arc<dyn ProtocolHandlerFactory>>>,
    worker_io_config: Arc<WorkerIoConfig>,
    listener_metadata: HashMap<RawFd, ListenerMetadata>,
}

impl HandlerManager {
    pub fn new(
        factories_vec: Vec<Arc<dyn ProtocolHandlerFactory>>,
        worker_io_config: Arc<WorkerIoConfig>,
    ) -> Self {
        let mut factory_map = HashMap::new();
        for factory in factories_vec {
            factory_map.insert(factory.id().to_string(), factory);
        }
        Self {
            handlers: HashMap::new(),
            factories: Arc::new(factory_map),
            worker_io_config,
            listener_metadata: HashMap::new(),
        }
    }

    /// Creates a new handler, adds it, calls `connection_ready`, and returns initial I/O operations.
    ///
    /// The caller is responsible for processing the returned `HandlerIoOps.sqe_blueprints`.
    ///
    /// # Arguments
    /// * `fd`: The `RawFd` the new handler will manage.
    /// * `factory_id`: The ID of the `ProtocolHandlerFactory` to use.
    /// * `buffer_manager_for_interface`: Optional `BufferRingManager` reference for the interface.
    ///
    /// # Returns
    /// `Ok(HandlerIoOps)` containing initial blueprints if successfully created.
    /// `Err(String)` if factory not found or FD already managed.
    pub fn create_and_add_handler<'a>( // Lifetime 'a for the interface
        &mut self,
        fd: RawFd,
        factory_id: &str,
        // Pass components needed to construct the lean UringWorkerInterface
        buffer_manager_for_interface: Option<&'a BufferRingManager>,
        default_bgid_val_from_worker: Option<u16>, 
    ) -> Result<HandlerIoOps, String> {
        if self.handlers.contains_key(&fd) {
            let err_msg = format!("HandlerManager: Handler for FD {} already exists. Cannot create new one with factory '{}'.", fd, factory_id);
            error!("{}", err_msg);
            return Err(err_msg);
        }
        match self.factories.get(factory_id) {
            Some(factory) => {
                let mut handler_box = factory.create_handler(fd, self.worker_io_config.clone());
                info!("HandlerManager: Created handler for FD {} using factory '{}'. Calling connection_ready...", fd, factory_id);

                let interface_for_ready = UringWorkerInterface::new(
                    fd,
                    &self.worker_io_config,
                    buffer_manager_for_interface,
                    default_bgid_val_from_worker
                );
                
                let initial_ops = handler_box.connection_ready(&interface_for_ready);
                self.handlers.insert(fd, handler_box);
                Ok(initial_ops)
            }
            None => {
                let err_msg = format!("HandlerManager: ProtocolHandlerFactory '{}' not found for FD {}.", factory_id, fd);
                error!("{}", err_msg);
                Err(err_msg)
            }
        }
    }

    pub fn get_mut(&mut self, fd: RawFd) -> Option<&mut Box<dyn UringConnectionHandler + Send>> {
        self.handlers.get_mut(&fd)
    }

    pub fn remove_handler(&mut self, fd: RawFd) -> Option<Box<dyn UringConnectionHandler + Send>> {
        debug!("HandlerManager: Removing handler and listener metadata for FD {}", fd);
        self.listener_metadata.remove(&fd);
        self.handlers.remove(&fd)
    }
    
    pub fn contains_handler_for(&self, fd: RawFd) -> bool {
        self.handlers.contains_key(&fd)
    }

    /// Calls `prepare_sqes` on all managed handlers and collects their requested operations.
    ///
    /// The UringWorker's main loop is responsible for processing these blueprints and submitting them.
    ///
    /// # Arguments
    /// * `buffer_manager_for_interface`: Optional `BufferRingManager` for the interfaces.
    ///
    /// # Returns
    /// A vector of `(RawFd, HandlerIoOps)` for each active handler.
    pub fn prepare_all_handler_io_ops<'a>( // Lifetime 'a for the interface
        &mut self,
        buffer_manager_for_interface: Option<&'a BufferRingManager>,
        default_bgid_val_from_worker: Option<u16>, 
    ) -> Vec<(RawFd, HandlerIoOps)> {
        let mut all_ops = Vec::new();
        for (fd, handler) in self.handlers.iter_mut() {
            // Construct the lean interface for each handler
            let interface = UringWorkerInterface::new(
                *fd,
                &self.worker_io_config,
                buffer_manager_for_interface,
                default_bgid_val_from_worker
            );
            tracing::trace!("HandlerManager: Calling prepare_sqes for FD {}", fd);
            let handler_output = handler.prepare_sqes(&interface);
            if !handler_output.sqe_blueprints.is_empty() || handler_output.initiate_close_due_to_error {
                 all_ops.push((*fd, handler_output));
            }
        }
        all_ops
    }

    pub fn add_listener_metadata(&mut self, listener_fd: RawFd, factory_id_for_accepted_connections: String) {
        info!("HandlerManager: Adding listener metadata for FD {}, accepted conns will use factory '{}'", listener_fd, factory_id_for_accepted_connections);
        self.listener_metadata.insert(listener_fd, ListenerMetadata {
            factory_id_for_accepted_connections,
        });
    }

    pub fn get_listener_factory_id(&self, listener_fd: RawFd) -> Option<String> {
        self.listener_metadata.get(&listener_fd).map(|meta| meta.factory_id_for_accepted_connections.clone())
    }

    pub fn is_listener_fd(&self, fd: RawFd) -> bool {
        self.listener_metadata.contains_key(&fd)
    }

    pub fn drain_all_handlers_calling_closed(&mut self) -> Vec<Box<dyn UringConnectionHandler + Send>> {
        let mut drained_handlers = Vec::new();
        for (_fd, mut handler) in self.handlers.drain() {
            handler.fd_has_been_closed();
            drained_handlers.push(handler);
        }
        self.listener_metadata.clear();
        drained_handlers
    }

    #[allow(dead_code)] // May be used in shutdown sequence
    pub(crate) fn iter_mut_for_shutdown(&mut self) -> impl Iterator<Item = (RawFd, &mut Box<dyn UringConnectionHandler + Send>)> {
        self.handlers.iter_mut().map(|(fd, handler)| (*fd, handler))
    }

    #[allow(dead_code)]
    pub(crate) fn get_active_fds(&self) -> Vec<RawFd> {
        self.handlers.keys().copied().collect()
    }
}