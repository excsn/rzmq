// src/runtime/mod.rs

//! Core asynchronous primitives: Commands, Mailboxes, Pipes.

pub mod command;
pub mod mailbox;
pub mod pipe;

pub use command::Command;
pub use mailbox::{mailbox, MailboxReceiver, MailboxSender};
pub use pipe::IPipeEvents;
