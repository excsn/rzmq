// core/src/io_uring_backend/mod.rs

#![cfg(feature = "io-uring")]

// <<< MODIFIED START: Declare all submodules we'll create >>>
use crate::ZmqError; // Common error type from the crate root

pub mod buffer_manager;
pub mod connection_handler; // For the UringConnectionHandler trait and ZMTP impl
pub mod ops;
pub mod worker;
pub mod one_shot_sender; // Moved here
// <<< MODIFIED END >>>

// Re-export key types for easier access from the ZMTP engine adapter that will use this backend
pub use ops::{UringOpRequest, UringOpCompletion, UserData};
pub use connection_handler::{ProtocolHandlerFactory, WorkerIoConfig}; // For registering protocol handlers

// Helper type for results within this backend
pub(crate) type UringBackendResult<T> = Result<T, ZmqError>;