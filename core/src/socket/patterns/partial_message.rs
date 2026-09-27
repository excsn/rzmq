use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use crate::message::{FrameBatch, Msg};

/// The frames of a message sent one `send` call at a time, held until the
/// frame without MORE completes it, so the socket routes the message whole.
#[derive(Debug, Default)]
pub(crate) struct PartialMessage {
  open: AtomicBool,
  frames: Mutex<FrameBatch>,
}

impl PartialMessage {
  /// Adds a frame of a multipart message. Returns the complete message once
  /// `msg` ends it, `None` while it is still open.
  pub fn push(&self, msg: Msg) -> Option<FrameBatch> {
    let more = msg.is_more();
    let mut frames = self.frames.lock();
    frames.push(msg);
    self.open.store(more, Ordering::Release);
    if more { None } else { Some(std::mem::take(&mut *frames)) }
  }

  pub fn is_open(&self) -> bool {
    self.open.load(Ordering::Acquire)
  }
}
