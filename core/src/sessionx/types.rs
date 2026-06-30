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

