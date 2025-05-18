// src/lib.rs

//! rzmq - A pure-Rust asynchronous ZeroMQ implementation using Tokio.
//!
//! This library aims to provide a compatible API with ZeroMQ patterns
//! while leveraging Rust's safety and Tokio's asynchronous capabilities.

// Declare modules that make up the library.
// These modules encapsulate different aspects of the ZMQ implementation.

/// Defines the `Context`, which is the entry point for creating sockets.
pub mod context;
/// Manages the ZMTP protocol engine and transport-specific I/O.
pub mod engine;
/// Defines custom error types used throughout the library.
pub mod error;
/// Contains types related to message representation (Msg, Blob, etc.).
pub mod message;
/// Implements ZMTP (ZeroMQ Message Transport Protocol) details like greetings and commands.
pub mod protocol;
/// Provides core asynchronous runtime primitives like mailboxes and events.
pub mod runtime;
/// Handles security mechanisms (NULL, PLAIN, CURVE) and ZAP.
pub mod security;
/// Manages individual connection sessions, bridging sockets and engines.
pub mod session;
/// Defines socket types, options, and the core socket actor logic.
pub mod socket;
/// Deals with network transport layers (TCP, IPC, Inproc).
pub mod transport;

/// A conditional main macro.
///
/// If the `io-uring` feature is enabled and the target is Linux,
/// this expands to `#[tokio_uring::main]`.
/// Otherwise, it expands to `#[tokio::main]`.
///
/// Apply this to your async main function instead of `#[tokio::main]`
/// when using the `io-uring` feature of `rzmq`.
///
/// # Example
/// ```ignore
/// #[rzmq::main]
/// async fn main() {
///   // Your application code
/// }
/// ```
#[cfg(feature = "io-uring")]
pub use rzmq_macros::main;

// Re-export core types for user convenience, making them accessible directly
// from the crate root (e.g., `rzmq::ZmqError`, `rzmq::Socket`).
pub use error::ZmqError;
pub use message::{Blob, Metadata, Msg, MsgFlags}; // Core message components.
pub use runtime::{Command, IPipeEvents}; // Command for actor communication, IPipeEvents for pipe interaction.
                                         // pub(crate) is used for types that are part of the public API surface for internal modules
                                         // but not necessarily for direct end-user consumption from the crate root.
                                         // However, MailboxSender/Receiver might be useful if users build custom actors interacting with rzmq.
                                         // For now, keep them internal. If a use case arises, they can be made pub.
pub use context::Context;
pub(crate) use runtime::{MailboxReceiver, MailboxSender}; // The main entry point for creating sockets.
                                                          // CoreState is internal to socket implementation.
pub(crate) use socket::core::CoreState;
// Socket and SocketType are fundamental for users.
pub use socket::types::{Socket, SocketType};
// Socket options might be re-exported later for easier access, e.g., `rzmq::SNDHWM`.
// pub use socket::options::*;

// --- Top-Level Library Information Functions ---

/// Major version number of the rzmq library.
const VERSION_MAJOR: i32 = 0;
/// Minor version number of the rzmq library.
const VERSION_MINOR: i32 = 1;
/// Patch version number of the rzmq library.
const VERSION_PATCH: i32 = 0;

/// Returns the library version as a tuple (major, minor, patch).
///
/// # Examples
///
/// ```
/// let (major, minor, patch) = rzmq::version();
/// println!("rzmq version: {}.{}.{}", major, minor, patch);
/// ```
pub fn version() -> (i32, i32, i32) {
  (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)
}

/// Returns the major version number of the library.
pub fn version_major() -> i32 {
  VERSION_MAJOR
}

/// Returns the minor version number of the library.
pub fn version_minor() -> i32 {
  VERSION_MINOR
}

/// Returns the patch version number of the library.
pub fn version_patch() -> i32 {
  VERSION_PATCH
}

// The `context()` function is now defined within the `context.rs` module
// and re-exported by `pub use context::Context;` and the `context()` free function there.
// So, no need for a separate placeholder here if `context.rs` provides it.
// If `context.rs` only provides `Context::new()`, then a top-level helper might be desired:
//
// /// Creates a new rzmq context.
// /// This is a convenience function equivalent to `Context::new()`.
// pub fn context() -> Result<Context, ZmqError> {
//     Context::new()
// }
// The current setup (Context::new() and context::context()) is fine.
