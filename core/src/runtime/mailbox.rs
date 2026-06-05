//! Type aliases for actor communication channels.

use crate::runtime::command::Command;
use fibre::mpmc;

/// Default capacity for bounded mailboxes created by the `mailbox()` helper function.
/// This capacity applies to the single mailbox used by `SocketCore` and other simpler actors.
/// It can be tuned based on expected load and performance characteristics.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 512;

/// The async sending end of an actor's mailbox.
/// Cloneable; used by Tokio tasks to send commands to `SocketCore`.
pub type MailboxSender = mpmc::AsyncSender<Command>;

/// The sync sending end of an actor's mailbox.
/// Cloneable; used by OS threads (e.g. the io_uring worker) to send commands to `SocketCore`
/// across the sync/async boundary. The `fibre` runtime activates the correct cross-thread
/// waker when this variant is used, ensuring the Tokio task wakes up.
pub type MailboxSyncSender = mpmc::Sender<Command>;

/// The receiving end of an actor's mailbox.
/// Only one task should typically own and receive from a `MailboxReceiver` to process commands sequentially.
pub type MailboxReceiver = mpmc::AsyncReceiver<Command>;

// Note: The `SocketMailbox` struct and its associated constants
// (DEFAULT_SYSTEM_MAILBOX_CAPACITY, DEFAULT_USER_MAILBOX_CAPACITY)
// have been removed as part of the refactor to use a single command mailbox for SocketCore
// and to move system-level notifications to the EventBus.

/// Creates a new single bounded mailbox channel pair.
/// This is the standard way to create mailboxes for actors in this system, including `SocketCore`.
///
/// # Returns
/// A tuple containing the `MailboxSender` and `MailboxReceiver`.
pub fn mailbox(capacity: usize) -> (MailboxSender, MailboxReceiver) {
  mpmc::bounded_async(capacity.max(1))
}
