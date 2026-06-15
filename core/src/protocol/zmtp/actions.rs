use bytes::Bytes;
use std::time::Duration;
use crate::message::FrameBatch;
use crate::{Blob, ZmqError};

/// Instructions for the I/O driver (Tokio task or io_uring worker) to execute on the network socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetAction {
    /// Write these exact bytes to the network.
    /// If `zc_eligible` is true, the driver should attempt to use a zero-copy send (like `IORING_OP_SEND_ZC`).
    Send {
        data: Bytes,
        zc_eligible: bool,
    },

    /// Enable (true) or Disable (false) TCP_CORK on the underlying file descriptor.
    SetCork(bool),

    /// Close the connection. If `delay` is present, the driver must allow
    /// the socket to flush for the specified duration before performing the actual close.
    ScheduleClose(Option<Duration>),
}

/// Instructions for the I/O driver to communicate back up to the application layer (SocketCore).
#[derive(Debug, Clone)]
pub enum AppAction {
    /// The ZMTP handshake (Greeting + Security + Ready) is fully complete.
    HandshakeComplete {
        peer_identity: Option<Blob>,
        peer_socket_type: Option<String>,
    },

    /// A complete, decrypted, and verified logical message is ready for delivery.
    DeliverMessage(FrameBatch),

    /// A protocol, security, or timeout error occurred. The driver must shut down the connection.
    PeerError(ZmqError),
}

/// The unified container returned by state transitions in the protocol engine.
#[derive(Debug, Default, Clone)]
pub struct EngineOutput {
    pub net_actions: Vec<NetAction>,
    pub app_actions: Vec<AppAction>,
}

impl EngineOutput {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_net(mut self, action: NetAction) -> Self {
        self.net_actions.push(action);
        self
    }

    pub fn with_app(mut self, action: AppAction) -> Self {
        self.app_actions.push(action);
        self
    }
}
