#![cfg(feature = "io-uring")]

pub mod buffer_manager;
pub mod byte_handler;
pub mod connection_handler;
pub mod ops;
pub mod provided_buffer_ring;
pub mod send_buffer_pool;
pub mod signaling_op_sender;
pub mod worker;
pub(crate) mod zmtp_handler;

pub use connection_handler::{ProtocolHandlerFactory, WorkerIoConfig};
pub use ops::{UringOpCompletion, UringOpRequest, UserData};


/// Minimum payload size (bytes) above which we attempt a zero-copy send.
/// Below this threshold the `ioctl`/registration overhead outweighs the copy cost.
pub(crate) const ZC_SEND_THRESHOLD: usize = 16834;