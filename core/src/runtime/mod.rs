//! Core asynchronous primitives: Commands, Mailboxes, Pipes.

pub mod actor_drop_guard;
pub mod command;
pub mod event_bus;
pub mod latch;
pub mod mailbox;
pub mod system_events;
pub mod waitgroup;

pub use command::Command;
pub(crate) use mailbox::{mailbox, MailboxReceiver, MailboxSender};

// System Coordination
pub use event_bus::EventBus;
pub use system_events::{ActorType, SystemEvent};

// Sync Primitives
pub(crate) use actor_drop_guard::ActorDropGuard;
pub(crate) use latch::CountDownLatch;
pub(crate) use waitgroup::WaitGroup;
