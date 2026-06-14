use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::runtime::system_events::ConnectionInteractionModel;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::socket::MonitorSender;
#[cfg(feature = "io-uring")]
use crate::Blob;

use std::os::unix::io::RawFd;
use std::sync::Arc;

use fibre::mpmc::{AsyncReceiver, AsyncSender};
use fibre::mpsc::BoundedAsyncReceiver;
use fibre::oneshot;
use tokio::task::Id as TaskId;

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
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  /// Command to connect the socket to a remote endpoint.
  UserConnect {
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  /// Command to disconnect from a specific endpoint.
  UserDisconnect {
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  /// Command to unbind from a specific endpoint.
  UserUnbind {
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  /// Command to send a message.
  UserSend {
    msg: Msg,
  },
  /// Command to receive a message.
  UserRecv {
    reply_tx: oneshot::Sender<Result<Msg, ZmqError>>,
  },
  /// Command to set a socket option.
  UserSetOpt {
    option: i32,
    value: Vec<u8>,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  /// Command to get a socket option's value.
  UserGetOpt {
    option: i32,
    reply_tx: oneshot::Sender<Result<Vec<u8>, ZmqError>>,
  },
  /// Command to register a monitor channel for socket events.
  UserMonitor {
    monitor_tx: MonitorSender,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  /// Command to initiate the closing sequence for the socket.
  UserClose {
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },

  // --- Lifecycle ---
  Stop,

  // --- Pipe Management ---
  PipeMessageReceived {
    pipe_id: usize,
    msg: Msg,
  },
  PipeMessageBatchReceived {
    pipe_id: usize,
    msgs: FrameBatch,
  },
  PipeClosedByPeer {
    pipe_id: usize,
  },

  // --- SocketCore -> Session ---
  AttachPipe {
    rx_from_core: AsyncReceiver<Msg>,
    tx_to_core: AsyncSender<Msg>,
    pipe_read_id: usize,
    pipe_write_id: usize,
  },

  ScaInitializePipes {
    sca_handle_id: usize,
    rx_from_core: AsyncReceiver<FrameBatch>,
    core_pipe_read_id_for_incoming_routing: usize,
    incoming_pipe_sender: Option<PipeMessageSender>,
  },

  /// Sent from a TCP/IPC connecter or acceptor directly to SocketCore for ViaSca connections.
  NewConnectionEstablished {
    endpoint_uri: String,
    target_endpoint_uri: String,
    connection_iface: Option<Arc<dyn ISocketConnection>>,
    interaction_model: ConnectionInteractionModel,
    managing_actor_task_id: Option<TaskId>,
  },

  // --- io_uring path: unified, URI-keyed commands ---

  /// Sent from the UringWorker to SocketCore when an io_uring connection's ZMTP handshake
  /// completes. Carries the URI, peer identity, and connection interface atomically, replacing
  /// the old two-step NewConnectionEstablished + UringFdHandshakeComplete flow.
  #[cfg(feature = "io-uring")]
  UringConnectionEstablished {
    endpoint_uri: String,
    target_endpoint_uri: String,
    connection_iface: Arc<dyn ISocketConnection>,
    peer_identity: Option<Blob>,
    /// Dedicated per-connection inbound data channel. Taken once by command_processor
    /// to spawn the UringPipeReader task, bypassing the control mailbox for data delivery.
    inbound_data_rx: Option<BoundedAsyncReceiver<FrameBatch>>,
    /// Raw file descriptor for this connection. Passed to UringPipeReader so it can send
    /// `ResumeConnection { fd }` when the inbound channel drains below the LWM.
    fd: RawFd,
  },

  /// Fatal error or connection closure from an io_uring connection, keyed by URI.
  #[cfg(feature = "io-uring")]
  UringFdError { endpoint_uri: String, error: ZmqError },
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
      Command::PipeMessageBatchReceived { .. } => "PipeMessageBatchReceived",
      Command::PipeClosedByPeer { .. } => "PipeClosedByPeer",
      Command::AttachPipe { .. } => "AttachPipe",
      Command::ScaInitializePipes { .. } => "ScaInitializePipes",
      Command::NewConnectionEstablished { .. } => "NewConnectionEstablished",
      #[cfg(feature = "io-uring")]
      Command::UringConnectionEstablished { .. } => "UringConnectionEstablished",
      #[cfg(feature = "io-uring")]
      Command::UringFdError { .. } => "UringFdError",
    }
  }
}
