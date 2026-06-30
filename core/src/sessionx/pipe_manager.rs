#![allow(dead_code)]

use crate::message::FrameBatch;
use fibre::{RecvError, TryRecvError};
use fibre::mpsc::BoundedAsyncReceiver;

// Import the state struct we defined earlier
use super::states::CorePipeManagerXState;

#[derive(Debug)]
pub(crate) struct CorePipeManagerX {
  pub(crate) state: CorePipeManagerXState,
}

impl CorePipeManagerX {
  pub(crate) fn new() -> Self {
    Self {
      state: CorePipeManagerXState::new(),
    }
  }

  /// Attaches the pipes and routing information received from SocketCore.
  pub(crate) fn attach(&mut self, rx_from_core: BoundedAsyncReceiver<FrameBatch>, core_pipe_read_id_for_incoming_routing: usize) {
    if self.state.is_attached {
      return;
    }
    self.state.rx_from_core = Some(rx_from_core);
    self.state.core_pipe_read_id_for_incoming_routing = Some(core_pipe_read_id_for_incoming_routing);
    self.state.is_attached = true;
  }

  /// Returns true if the pipes from SocketCore have been attached.
  pub(crate) fn is_attached(&self) -> bool {
    self.state.is_attached
  }

  /// Attempts to receive a message from SocketCore (i.e., an outgoing message
  /// to be sent over the network).
  /// This is an async method that will await if the channel is empty.
  pub(crate) async fn recv_from_core(&self) -> Result<FrameBatch, RecvError> {
    if let Some(ref rx) = self.state.rx_from_core {
      rx.recv().await
    } else {
      // This indicates a programming error: called before pipes are attached
      // or after they've been detached.
      Err(RecvError::Disconnected) // Or a more specific custom error
    }
  }

  /// Closes the sending ends of pipes and clears internal references.
  /// The receiving end (`rx_from_core`) is typically managed by its sender (`SocketCore`).
  /// This method ensures `CorePipeManagerX` stops trying to use the pipes.
  pub(crate) fn detach_and_clear_pipes(&mut self) {
    let _ = self.state.rx_from_core.take();
    self.state.core_pipe_read_id_for_incoming_routing = None;
    self.state.is_attached = false;
  }

  /// Non-blocking check for a queued outbound message batch from SocketCore.
  /// Returns `Err(TryRecvError::Empty)` when the pipe is empty, or
  /// `Err(TryRecvError::Disconnected)` when not attached or the channel is closed.
  pub(crate) fn try_recv_from_core(&self) -> Result<FrameBatch, TryRecvError> {
    if let Some(ref rx) = self.state.rx_from_core {
      rx.try_recv()
    } else {
      Err(TryRecvError::Disconnected)
    }
  }

  /// Non-blocking batch pull of queued outbound message batches from SocketCore.
  /// Appends up to `max` items to `out` in one channel pass and returns the count
  /// appended; `0` when the pipe is empty, closed, or not attached (closure is
  /// surfaced by the next blocking `recv_from_core`).
  pub(crate) fn len(&self) -> usize {
    self.state.rx_from_core.as_ref().map(|rx| rx.len()).unwrap_or(0)
  }

  pub(crate) fn try_recv_batch_from_core(&self, out: &mut Vec<FrameBatch>, max: usize) -> usize {
    if let Some(ref rx) = self.state.rx_from_core {
      rx.try_recv_batch_mut(out, max).unwrap_or(0)
    } else {
      0
    }
  }

  #[cfg(test)]
  pub(crate) fn rx_from_core_is_some(&self) -> bool {
    self.state.rx_from_core.is_some()
  }
}

#[cfg(test)]
mod additional_pipe_manager_tests {
  use super::*;

  #[tokio::test]
  async fn test_pipe_manager_attach_and_detach() {
    let mut manager = CorePipeManagerX::new();
    assert!(!manager.is_attached());

    let (tx, rx) = fibre::mpsc::bounded_async::<FrameBatch>(1);
    let _ = tx; // keep sender alive
    manager.attach(rx, 42);

    assert!(manager.is_attached());
    assert_eq!(manager.state.core_pipe_read_id_for_incoming_routing, Some(42));

    manager.detach_and_clear_pipes();
    assert!(!manager.is_attached());
    assert!(manager.state.rx_from_core.is_none());
  }

  #[tokio::test]
  async fn test_pipe_manager_read_on_detached_errors() {
    let manager = CorePipeManagerX::new();
    let res = manager.recv_from_core().await;
    assert!(res.is_err());
  }
}
