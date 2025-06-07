#![allow(dead_code)]

use crate::Msg;
use fibre::mpmc::{AsyncReceiver, RecvError};

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
  pub(crate) fn attach(&mut self, rx_from_core: AsyncReceiver<Msg>, core_pipe_read_id_for_incoming_routing: usize) {
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
  pub(crate) async fn recv_from_core(&self) -> Result<Msg, RecvError> {
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

  #[cfg(test)]
  pub(crate) fn rx_from_core_is_some(&self) -> bool {
    self.state.rx_from_core.is_some()
  }
}
