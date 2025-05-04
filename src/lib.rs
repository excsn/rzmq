//! rzmq - A pure-Rust asynchronous ZeroMQ implementation using Tokio.

// Declare modules we will create shortly
pub mod engine;
pub mod error;
pub mod message;
pub mod runtime;
pub mod context; // Declare Context module
pub mod protocol; // Declare Protocol module
pub mod security; // Declare Security module
pub mod session; // Declare Session module
pub mod socket; // Declare Socket module
pub mod transport; // Declare Transport module

// Re-export core types for user convenience
pub use error::ZmqError;
pub use message::{Blob, Metadata, Msg, MsgFlags}; // Assuming Blob for now
pub use runtime::{Command, IPipeEvents, MailboxReceiver, MailboxSender};
// pub use context::Context; // Add later
pub use socket::types::{Socket, SocketType};
// pub use socket::options::*; // Add later

// --- Top-Level Functions ---

const VERSION_MAJOR: i32 = 0;
const VERSION_MINOR: i32 = 1;
const VERSION_PATCH: i32 = 0;

/// Returns the library version as a tuple (major, minor, patch).
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

// Placeholder for context creation - will be implemented later
// use context::Context; // Uncomment when context exists
// pub fn context() -> Result<Context, ZmqError> {
//     Context::new()
// }