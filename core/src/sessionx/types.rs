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