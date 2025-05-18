// src/transport/mod.rs

pub mod endpoint;
pub mod tcp; // Add tcp module
#[cfg(feature = "ipc")]
pub mod ipc;
#[cfg(feature = "inproc")]
pub mod inproc;