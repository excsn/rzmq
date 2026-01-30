use crate::error::ZmqError;
use crate::message::Msg;
use crate::socket::MonitorSender;
#[cfg(feature = "io-uring")]
use crate::Blob;

#[cfg(feature = "io-uring")]
use std::os::unix::io::RawFd;

use fibre::mpmc::{AsyncReceiver, AsyncSender};
use fibre::oneshot;

/// Defines messages exchanged between actors (Sockets, Sessions, Engines, etc.).
/// These are primarily for direct, targeted communication, often expecting a reply,
/// or for high-frequency data flow (like pipe messages).
/// Broader system notifications and lifecycle events are handled by `SystemEvent` on the `EventBus`.
#[derive(Debug)]
#[allow(dead_code)]
pub enum Command {
  // --- User Requests (from API Handle -> SocketCore's single command mailbox) ---
  /// Command to bind the socket to a local endpoint.
  UserBind {
    endpoint: String,                                // The endpoint string to bind to.
    reply_tx: oneshot::Sender<Result<(), ZmqError>>, // Channel to send the bind result back.
  },
  /// Command to connect the socket to a remote endpoint.
  UserConnect {
    endpoint: String,                                // The endpoint string to connect to.
    reply_tx: oneshot::Sender<Result<(), ZmqError>>, // Channel to send the connect result back.
  },
  /// Command to disconnect from a specific endpoint.
  UserDisconnect {
    endpoint: String,                                // The endpoint string to disconnect from.
    reply_tx: oneshot::Sender<Result<(), ZmqError>>, // Channel to send the disconnect result back.
  },
  /// Command to unbind from a specific endpoint.
  UserUnbind {
    endpoint: String,                                // The endpoint string to unbind from.
    reply_tx: oneshot::Sender<Result<(), ZmqError>>, // Channel to send the unbind result back.
  },
  /// Command to send a message.
  UserSend {
    msg: Msg, // The message to send.
              // No reply_tx here for PUSH/PUB simplicity; errors handled by options (SNDTIMEO) or pattern.
  },
  /// Command to receive a message.
  UserRecv {
    reply_tx: oneshot::Sender<Result<Msg, ZmqError>>, // Channel to send the received message or error back.
  },
  /// Command to set a socket option.
  UserSetOpt {
    option: i32,                                     // The integer ID of the option to set.
    value: Vec<u8>,                                  // The new value for the option, as raw bytes.
    reply_tx: oneshot::Sender<Result<(), ZmqError>>, // Channel to send the set option result back.
  },
  /// Command to get a socket option's value.
  UserGetOpt {
    option: i32,                                          // The integer ID of the option to get.
    reply_tx: oneshot::Sender<Result<Vec<u8>, ZmqError>>, // Channel to send the option value or error back.
  },
  /// Command to register a monitor channel for socket events.
  UserMonitor {
    monitor_tx: MonitorSender, // The sender end of the channel where monitor events will be sent.
    reply_tx: oneshot::Sender<Result<(), ZmqError>>, // Confirms registration.
  },
  /// Command to initiate the closing sequence for the socket.
  UserClose {
    // Reply confirms that the close process has been initiated, not necessarily completed.
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },

  // --- Lifecycle ---
  /// Universal signal to gracefully shut down an actor task.
  /// Can be sent directly to an actor's mailbox if needed, bypassing the event bus
  /// for very targeted shutdown scenarios (though event bus is preferred for general lifecycle).
  Stop,

  // --- Pipe Management (PipeReaderTask -> SocketCore, direct commands for performance) ---
  /// Sent from PipeReaderTask -> SocketCore when a message arrives from a session's data pipe.
  PipeMessageReceived {
    /// The ID of the pipe (from SocketCore's perspective, its read pipe ID) that received the message.
    pipe_id: usize,
    msg: Msg, // The message received from the pipe.
  },
  /// Sent from PipeReaderTask -> SocketCore when the session closes its *sending* end of the data pipe.
  PipeClosedByPeer {
    /// The ID of the pipe (from SocketCore's perspective, its read pipe ID) that was closed.
    pipe_id: usize,
  },

  // --- SocketCore -> Session (Direct command for initial pipe setup) ---
  /// Sent from SocketCore -> Session to provide its ends of the inter-actor data pipe.
  AttachPipe {
    /// The channel receiver for the Session to read messages from the SocketCore.
    rx_from_core: AsyncReceiver<Msg>,
    /// The channel sender for the Session to send messages to the SocketCore.
    tx_to_core: AsyncSender<Msg>,
    /// The ID the Session uses to read from its pipe (SocketCore writes to this ID).
    pipe_read_id: usize,
    /// The ID the Session uses to write to its pipe (SocketCore reads from this ID).
    pipe_write_id: usize,
  },

  /// Sent from SocketCore -> SessionConnectionActorX to provide its pipes and routing info.
  ScaInitializePipes {
    /// The unique handle of the target SessionConnectionActorX.
    sca_handle_id: usize,
    /// Channel for SCA to receive Msgs (outgoing data) from SocketCore.
    rx_from_core: AsyncReceiver<Vec<Msg>>,
    /// The ID the SCA should use in the `pipe_id` field when calling `ISocket::handle_pipe_event`.
    core_pipe_read_id_for_incoming_routing: usize,
  },
  #[cfg(feature = "io-uring")]
  UringFdMessage { fd: RawFd, msg: Msg },
  #[cfg(feature = "io-uring")]
  UringFdError { fd: RawFd, error: ZmqError },
  #[cfg(feature = "io-uring")]
  UringFdHandshakeComplete {
    // Used by uring::global_state processor to inform SocketCore
    fd: RawFd,
    peer_identity: Option<Blob>,
  },
}

impl Command {
  /// Returns a string representation of the command variant's name. Useful for logging.
  pub fn variant_name(&self) -> &'static str {
    match self {
      Command::UserBind { .. } => "UserBind",
      Command::UserConnect { .. } => "UserConnect",
      Command::UserDisconnect { .. } => "UserDisconnect",
      Command::UserUnbind { .. } => "UserUnbind",
      Command::UserSend { .. } => "UserSend",
      Command::UserRecv { .. } => "UserRecv",
      Command::UserSetOpt { .. } => "UserSetOpt",
      Command::UserGetOpt { .. } => "UserGetOpt",
      Command::UserMonitor { .. } => "UserMonitor",
      Command::UserClose { .. } => "UserClose",
      Command::Stop => "Stop",
      Command::PipeMessageReceived { .. } => "PipeMessageReceived",
      Command::PipeClosedByPeer { .. } => "PipeClosedByPeer",
      Command::AttachPipe { .. } => "AttachPipe",
      Command::ScaInitializePipes { .. } => "ScaInitializePipes",
      #[cfg(feature = "io-uring")]
      Command::UringFdMessage { .. } => "UringFdMessage",
      #[cfg(feature = "io-uring")]
      Command::UringFdError { .. } => "UringFdError",
      #[cfg(feature = "io-uring")]
      Command::UringFdHandshakeComplete { .. } => "UringFdHandshakeComplete",
    }
  }
}
