//! Type aliases for actor communication channels.

use crate::runtime::command::Command;
use fibre::mpsc;

/// Default capacity for bounded mailboxes created by the `mailbox()` helper function.
/// This capacity applies to the single mailbox used by `SocketCore` and other simpler actors.
/// It can be tuned based on expected load and performance characteristics.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 512;

/// The async sending end of an actor's mailbox.
/// Used by Tokio tasks to send commands to `SocketCore`.
pub type MailboxSender = mpsc::BoundedAsyncSender<Command>;

/// The sync sending end of an actor's mailbox.
/// Used by OS threads (e.g. the io_uring worker) to send commands to `SocketCore` across the sync/async boundary.
pub type MailboxSyncSender = mpsc::BoundedSender<Command>;

/// The receiving end of an actor's mailbox.
/// Only one task should typically own and receive from a `MailboxReceiver` to process commands sequentially.
pub type MailboxReceiver = mpsc::BoundedAsyncReceiver<Command>;

/// Creates a new single bounded mailbox channel pair.
/// This is the standard way to create mailboxes for actors in this system, including `SocketCore`.
///
/// # Returns
/// A tuple containing the `MailboxSender` and `MailboxReceiver`.
pub fn mailbox(capacity: usize) -> (MailboxSender, MailboxReceiver) {
  mpsc::bounded_async(capacity.max(1))
}
