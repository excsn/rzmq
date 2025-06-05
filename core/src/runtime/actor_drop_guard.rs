use tracing::debug;

use crate::runtime::{ActorType, SystemEvent}; // Adjust imports as needed
use crate::{Context, ZmqError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) struct ActorDropGuard {
  context: Context,
  handle_id: usize,
  actor_type: ActorType,
  endpoint_uri: Option<String>,
  // Use an Arc<AtomicBool> to signal normal exit, preventing double publish
  stopped_normally: bool,
  error: Option<ZmqError>,
}

impl ActorDropGuard {
  pub fn new(context: Context, handle_id: usize, actor_type: ActorType, endpoint_uri: Option<String>, parent_handle_id: Option<usize>) -> Self {

    context.publish_actor_started(handle_id, actor_type, parent_handle_id);
    Self {
      context,
      handle_id,
      actor_type,
      endpoint_uri,
      stopped_normally: false,
      error: None,
    }
  }

  pub fn set_error(&mut self, error: ZmqError) {
    self.error = Some(error);
  }

  pub fn waive(&mut self) {
    self.stopped_normally = true;
  }
}

impl Drop for ActorDropGuard {
  fn drop(&mut self) {
    // Only publish stop if the task didn't signal normal completion
    let error;
    if !self.stopped_normally {
      error = self.error.take().or_else(|| Some(ZmqError::Internal("Actor task cancelled/aborted".into())));

      // Log carefully here, as we are in drop context
      debug!(
        // Use eprintln or tracing::error! if subscriber setup handles panics
        "ActorDropGuard: Actor {} ({:?}) stopping abnormally (error: {}). Publishing stop.",
        self.handle_id, self.actor_type, error.as_ref().unwrap(),
      );
    } else {
      error = None;
    }

    // NOTE: publish_actor_stopping involves sending on a broadcast channel.
    // This is *usually* safe from drop if the receiver task is still running,
    // but it's not ideal. A truly robust solution might involve
    // a synchronous mechanism for the final WaitGroup decrement if possible,
    // but the event system is the current design.
    self.context.publish_actor_stopping(
      self.handle_id,
      self.actor_type,
      self.endpoint_uri.take(),
      error,
    );
  }
}
