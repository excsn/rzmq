//! Message-related types (`Msg`, `MsgFlags`, `Metadata`, `Blob`).

mod blob;
mod flags;
mod metadata;
mod msg;

pub use blob::Blob;
pub use flags::MsgFlags;
pub use metadata::Metadata;
pub use msg::Msg;
