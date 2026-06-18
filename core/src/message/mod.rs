//! Message-related types (`Msg`, `MsgFlags`, `Metadata`, `Blob`).

mod blob;
mod flags;
mod metadata;
mod msg;

pub use blob::Blob;
pub use flags::MsgFlags;
pub use metadata::Metadata;
pub use msg::Msg;

use smallvec::SmallVec;

/// A stack-allocated batch of message frames for a single logical ZMQ message.
/// 99% of ZMQ messages fit within 4 frames (Identity, Delimiter, Payload, …).
pub type FrameBatch = SmallVec<[Msg; 2]>;
