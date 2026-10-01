//! Core asynchronous primitives: Commands, Mailboxes, Pipes.

pub mod actor_drop_guard;
pub mod command;
pub mod event_bus;
pub mod latch;
pub mod mailbox;
pub(crate) mod reusable_box;
pub(crate) use reusable_box::ReusableBoxFuture;
pub mod system_events;
pub mod waitgroup;

pub(crate) use command::Command;
pub(crate) use mailbox::{mailbox, MailboxReceiver, MailboxSender, MailboxSyncSender};

// System Coordination
pub(crate) use event_bus::EventBus;
pub use system_events::ActorType;
pub(crate) use system_events::SystemEvent;

// Sync Primitives
pub(crate) use actor_drop_guard::ActorDropGuard;
pub(crate) use waitgroup::WaitGroup;
