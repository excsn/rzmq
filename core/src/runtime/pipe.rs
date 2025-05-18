// src/runtime/pipe.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::command::Command;
use crate::runtime::mailbox::MailboxSender; // Use our defined type alias
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Trait implemented by actors (SocketCore, SessionBase) that consume from a Pipe.
/// Allows the Pipe to notify the consumer about events.
#[async_trait::async_trait] // Use async_trait if methods need to be async
pub trait IPipeEvents: Send + Sync {
  /// Called by the Pipe when a message is successfully written to an empty Pipe,
  /// signaling the consumer that data is ready to be read.
  async fn activate_read(&self, pipe_id: usize);

  /// Called by the Pipe when the *other* end (the writer) is closed or dropped.
  async fn pipe_closed_by_peer(&self, pipe_id: usize);

  // Add other event notifications if needed (e.g., HWM reached/cleared)
}
