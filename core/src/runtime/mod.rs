// src/runtime/mod.rs

//! Core asynchronous primitives: Commands, Mailboxes, Pipes.

pub mod actor_drop_guard;
pub mod command;
pub mod event_bus;
#[cfg(feature = "io-uring")]
pub mod global_uring_state;
pub mod latch;
pub mod mailbox;
pub mod one_shot_sender;
pub mod pipe;
pub mod system_events;
pub mod waitgroup;

pub use command::Command;
pub(crate) use mailbox::{mailbox, MailboxReceiver, MailboxSender};
pub use pipe::IPipeEvents;

pub use mailbox::DEFAULT_MAILBOX_CAPACITY;
// System Coordination
pub use event_bus::EventBus;
pub use system_events::{ActorType, SystemEvent};

pub use command::EngineConnectionType;

// Sync Primitives
pub(crate) use latch::CountDownLatch;
pub(crate) use waitgroup::WaitGroup;
pub(crate) use one_shot_sender::OneShotSender;
pub(crate) use actor_drop_guard::ActorDropGuard;