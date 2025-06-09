#![cfg(feature = "io-uring")]

use crate::ZmqError; // Common error type from the crate root

pub mod buffer_manager;
pub mod connection_handler;
pub mod ops;
pub mod send_buffer_pool;
pub mod signaling_op_sender;
pub mod worker;
pub mod zmtp_handler;

// Re-export key types for easier access from the ZMTP engine adapter that will use this backend
pub use connection_handler::{ProtocolHandlerFactory, WorkerIoConfig};
pub use ops::{UringOpCompletion, UringOpRequest, UserData}; // For registering protocol handlers

// Helper type for results within this backend
pub(crate) type UringBackendResult<T> = Result<T, ZmqError>;
