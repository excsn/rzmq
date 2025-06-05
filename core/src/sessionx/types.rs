#![allow(dead_code)] // Allow dead code for now as we build incrementally

use crate::error::ZmqError;
use crate::Blob;

/// Phases for the SessionConnectionActorX lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionPhaseX {
  /// Actor just started, stream might be present but no pipes from SocketCore yet.
  Initial,
  /// ZMTP handshake (Greeting, Security, Ready) is in progress.
  HandshakeInProgress,
  /// Handshake complete, but waiting for AttachPipesAndRoutingInfo from SocketCore.
  WaitingForPipes,
  /// Handshake done, pipes attached. Normal data transfer phase.
  Operational,
  /// Graceful shutdown of the ZMTP stream and connection initiated.
  ShuttingDownStream,
  /// Actor is finalizing and loop should exit.
  Terminating,
}

/// Progress of the ZMTP handshake, returned by `ZmtpProtocolHandlerX::advance_handshake`.
#[derive(Debug, Clone)] // Clone needed if we store it temporarily
pub(crate) enum ZmtpHandshakeProgressX {
  /// Handshake is ongoing, more steps needed.
  InProgress,
  /// Security mechanism has established a peer identity.
  IdentityReady(Blob),
  /// Entire ZMTP handshake (Greeting, Security, Ready) is complete.
  HandshakeComplete,
  /// A ZMTP-level error occurred during handshake that might be recoverable by retrying a step,
  /// or might need to be escalated by the SessionConnectionActorX.
  ProtocolError(String),
  /// An unrecoverable error occurred.
  FatalError(ZmqError),
}

/// Internal sub-phases for ZmtpProtocolHandlerX's handshake state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandshakeSubPhaseX {
  GreetingExchange,
  SecurityHandshake,
  ReadyExchange,
  Done,
}

// <<< ADDED [Module for SessionConnectionActorX related command variants] >>>
/// Commands specific to the SessionConnectionActorX, typically sent by SocketCore.
#[derive(Debug)]
pub(crate) enum ScaCommand {
  /// Provides the SessionConnectionActorX with the necessary channels and IDs
  /// to communicate back and forth with the SocketCore.
  AttachPipesAndRoutingInfo {
    /// Channel to receive outgoing application messages from SocketCore.
    rx_from_core: async_channel::Receiver<crate::Msg>,
    /// Mailbox of the parent SocketCore to send incoming messages (as Commands) to.
    parent_socket_core_mailbox: crate::runtime::MailboxSender,
    /// The ID that SocketCore uses to identify this connection for messages
    /// flowing from this actor *to* SocketCore.
    core_pipe_read_id_for_incoming_routing: usize,
  },
  /// Command to initiate a graceful shutdown of the connection.
  Stop,
}
