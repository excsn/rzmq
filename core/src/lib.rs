//! rzmq - A pure-Rust asynchronous ZeroMQ implementation using Tokio.
//!
//! This library aims to provide a compatible API with ZeroMQ patterns
//! while leveraging Rust's safety and Tokio's asynchronous capabilities.

// These modules encapsulate different aspects of the ZMQ implementation.

/// Defines the `Context`, which is the entry point for creating sockets.
pub mod context;
/// Defines custom error types used throughout the library.
pub mod error;
/// Contains types related to message representation (Msg, Blob, etc.).
pub mod message;
/// Implements ZMTP (ZeroMQ Message Transport Protocol) details like greetings and commands.
pub mod protocol;
/// Provides core asynchronous runtime primitives like mailboxes and events.
pub mod runtime;
/// Handles security mechanisms (NULL, PLAIN, NOISE_XX) and ZAP.
pub mod security;
/// Manages individual connection sessions, bridging sockets and engines.
pub mod sessionx;
/// Defines socket types, options, and the core socket actor logic.
pub mod socket;
/// Deals with network transport layers (TCP, IPC, Inproc).
pub mod transport;

pub(crate) mod throttle;

pub(crate) mod profiler;

#[cfg(feature = "io-uring")]
pub mod io_uring_backend;
#[cfg(feature = "io-uring")]
pub mod uring;

// Re-export core types for user convenience, making them accessible directly
// from the crate root (e.g., `rzmq::ZmqError`, `rzmq::Socket`).
pub use context::Context;
pub use error::ZmqError;
pub use message::{Blob, Metadata, Msg, MsgFlags};
pub use runtime::Command;
pub(crate) use runtime::{MailboxReceiver, MailboxSender};

pub(crate) use socket::core::CoreState;

// Socket and SocketType are fundamental for users.
pub use socket::types::{Socket, SocketType};
