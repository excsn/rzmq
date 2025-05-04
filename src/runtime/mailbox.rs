// src/runtime/mailbox.rs

//! Type aliases for actor communication channels based on `async-channel`.

use crate::runtime::command::Command; // Make sure Command is visible

/// The sending end of an actor's mailbox. Cloneable.
pub type MailboxSender = async_channel::Sender<Command>;

/// The receiving end of an actor's mailbox.
pub type MailboxReceiver = async_channel::Receiver<Command>;

/// Default capacity for bounded mailboxes.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 128; // Tunable

/// Creates a new bounded mailbox channel pair.
pub fn mailbox() -> (MailboxSender, MailboxReceiver) {
  async_channel::bounded(DEFAULT_MAILBOX_CAPACITY)
}
