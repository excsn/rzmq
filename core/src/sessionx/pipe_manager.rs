// core/src/sessionx/pipe_manager.rs

#![allow(dead_code)] // Allow dead code for now

use crate::error::ZmqError;
use crate::runtime::command::Command;
use crate::runtime::MailboxSender as GenericMailboxSender;
use crate::Msg;
use async_channel::{Receiver as AsyncReceiver, RecvError, TrySendError};

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
  pub(crate) fn attach(
    &mut self,
    rx_from_core: AsyncReceiver<Msg>,
    parent_socket_core_mailbox: GenericMailboxSender,
    core_pipe_read_id_for_incoming_routing: usize,
  ) {
    if self.state.is_attached {
      // This should ideally be logged by the caller (SessionConnectionActorX)
      // as it indicates a logical error in the actor's state machine.
      // For now, CorePipeManagerX just ignores a re-attachment attempt.
      return;
    }
    self.state.rx_from_core = Some(rx_from_core);
    self.state.parent_socket_core_mailbox = Some(parent_socket_core_mailbox);
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
  pub(crate) async fn recv_from_core(&mut self) -> Result<Msg, RecvError> {
    if let Some(rx) = &self.state.rx_from_core {
      rx.recv().await
    } else {
      // This indicates a programming error: called before pipes are attached
      // or after they've been detached.
      Err(RecvError) // Or a more specific custom error
    }
  }

  /// Sends an incoming message (parsed from the network) to the parent SocketCore.
  /// This method attempts a non-blocking send (`try_send`) to avoid stalling
  /// the SessionConnectionActorX if SocketCore's mailbox is full.
  ///
  /// Returns:
  /// - `Ok(())` if the message was successfully sent (or accepted by SocketCore's mailbox).
  /// - `Err(ZmqError::ResourceLimitReached)` if SocketCore's mailbox is full.
  /// - `Err(ZmqError::ChannelClosed)` if SocketCore's mailbox is closed.
  /// - `Err(ZmqError::InvalidState)` if pipes are not attached.
  pub(crate) async fn send_msg_to_core(&self, msg: Msg, actor_handle: usize) -> Result<(), ZmqError> {
    let mailbox = self.state.parent_socket_core_mailbox.as_ref().ok_or_else(|| {
      tracing::error!(
        sca_handle = actor_handle,
        "CorePipeManagerX: Attempted to send_msg_to_core but parent_socket_core_mailbox is None."
      );
      ZmqError::InvalidState("CorePipeManagerX: Not attached to SocketCore mailbox".into())
    })?;

    let pipe_id = self.state.core_pipe_read_id_for_incoming_routing.ok_or_else(|| {
      tracing::error!(
        sca_handle = actor_handle,
        "CorePipeManagerX: Attempted to send_msg_to_core but core_pipe_read_id_for_incoming_routing is None."
      );
      ZmqError::InvalidState("CorePipeManagerX: Missing routing ID for SocketCore".into())
    })?;

    let command = Command::PipeMessageReceived { pipe_id, msg };

    match mailbox.try_send(command) {
      Ok(()) => Ok(()),
      Err(TrySendError::Full(_command_back)) => {
        tracing::warn!(
          sca_handle = actor_handle,
          core_pipe_id = pipe_id,
          "CorePipeManagerX: Failed to send PipeMessageReceived to SocketCore (mailbox full). Message dropped."
        );
        // SessionConnectionActorX will decide if this is fatal or if message can be dropped.
        // For now, this manager signals that the resource limit was hit.
        Err(ZmqError::ResourceLimitReached)
      }
      Err(TrySendError::Closed(_command_back)) => {
        tracing::error!(
          sca_handle = actor_handle,
          core_pipe_id = pipe_id,
          "CorePipeManagerX: Failed to send PipeMessageReceived to SocketCore (mailbox closed)."
        );
        Err(ZmqError::Internal("SocketCore command mailbox unexpectedly closed".into()))
      }
    }
  }

  /// Closes the sending ends of pipes and clears internal references.
  /// The receiving end (`rx_from_core`) is typically managed by its sender (`SocketCore`).
  /// This method ensures `CorePipeManagerX` stops trying to use the pipes.
  pub(crate) fn detach_and_clear_pipes(&mut self) {
    if let Some(mailbox) = self.state.parent_socket_core_mailbox.take() {
      // We don't explicitly close the mailbox from here, as it's owned by SocketCore.
      // We just stop referencing it.
      drop(mailbox);
    }
    if let Some(rx) = self.state.rx_from_core.take() {
      // The receiver will be closed when dropped if the sender (SocketCore) is also dropped or closes its end.
      // We can explicitly close it to signal the sender if it's still trying to send,
      // but usually, the sender dictates the closure.
      rx.close(); // Make it explicit that we are done with this receiver.
    }
    self.state.core_pipe_read_id_for_incoming_routing = None;
    self.state.is_attached = false;
  }

  #[cfg(test)]
  pub(crate) fn rx_from_core_is_some(&self) -> bool {
    self.state.rx_from_core.is_some()
  }
}
