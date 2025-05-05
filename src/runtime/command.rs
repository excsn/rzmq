// src/runtime/command.rs

use crate::message::Msg;
use crate::socket::MonitorSender;
use crate::{error::ZmqError, Blob};

use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use tokio::sync::oneshot; // Using tokio's oneshot for replies

/// Type alias for the mailbox sender used within Commands.
pub type MailboxSender = async_channel::Sender<Command>;

/// Defines messages exchanged between actors (Sockets, Sessions, Engines, etc.).
#[derive(Debug)] // Auto-derive Debug for now
#[allow(dead_code)] // Allow unused variants during development
pub enum Command {
  // --- User Requests (from API Handle -> SocketCore) ---
  UserBind {
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  UserConnect {
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  UserDisconnect {
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  UserUnbind {
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  UserSend {
    msg: Msg,
    // Reply might be needed for backpressure / errors? TBD
    // reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  UserRecv {
    reply_tx: oneshot::Sender<Result<Msg, ZmqError>>,
  },
  UserSetOpt {
    option: i32,
    value: Vec<u8>, // Pass owned value
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },
  UserGetOpt {
    option: i32,
    reply_tx: oneshot::Sender<Result<Vec<u8>, ZmqError>>,
  },
  UserMonitor {
    monitor_tx: MonitorSender,                       // Sender end of the monitor channel
    reply_tx: oneshot::Sender<Result<(), ZmqError>>, // To confirm setup
  },
  UserClose {
    // reply_tx confirms shutdown initiated, not completed
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  },

  // --- Lifecycle ---
  /// Universal signal to gracefully shut down an actor task.
  Stop,
  /// Sent from child to parent confirming clean shutdown.
  CleanupComplete {
    handle: usize,
    endpoint_uri: Option<String>,
  }, // Identify child by handle
  /// Sent from child to parent reporting a fatal error.
  ReportError {
    handle: usize,
    endpoint_uri: String, // Make URI mandatory for error reporting
    error: ZmqError,
  },

  // --- Connection Management (Listener/Connecter -> SocketCore) ---
  ConnSuccess {
    /// The specific endpoint URI representing the established connection (e.g., `tcp://peer_ip:peer_port`).
    endpoint: String, // Identify which bind/connect succeeded
    /// The original target endpoint URI requested by the user (e.g., `tcp://hostname:port`).
    /// Needed for automatic reconnection attempts.
    target_endpoint_uri: String,
    // Send mailbox for the *Session* actor created for this connection
    session_mailbox: MailboxSender,
    // Optional Session handle if needed for tracking
    session_handle: Option<usize>,
    // Optional Session task handle if needed
    session_task_handle: Option<tokio::task::JoinHandle<()>>,
  },
  ConnFailed {
    endpoint: String,
    error: ZmqError,
  },
  ListenerStopped {
    handle: usize,
    endpoint_uri: String,
  },
  ConnecterStopped {
    handle: usize,
    endpoint_uri: String,
  },

  /// Sent from Session -> SocketCore upon clean stop
  SessionStopped {
    handle: usize,
    // Include URI to identify which session stopped
    endpoint_uri: String,
  },
  // --- Session <-> Engine Interaction ---
  /// Sent from Listener/Connecter/Socket -> Session to provide Engine details
  Attach {
    engine_mailbox: MailboxSender,
    // Optional handle/task_handle for the Engine itself
    engine_handle: Option<usize>,
    engine_task_handle: Option<tokio::task::JoinHandle<()>>,
  },
  /// Sent from Session/Pipe -> Engine to send a message
  SessionPushCmd {
    msg: Msg,
  },
  /// Sent from Engine -> Session carrying a received message
  EnginePushCmd {
    msg: Msg,
  }, // Distinguish direction? Maybe reuse SessionPushCmd? TBD
  // Placeholder if Engine needs to request something from Session:
  // SessionPullRequestCmd { reply_tx: oneshot::Sender<Result<SomeData, ZmqError>> },
  /// Sent from Engine -> Session when ZMTP handshake is complete
  EngineReady {
    peer_identity: Option<Blob>,
  },
  /// Sent from Engine -> Session upon fatal error
  EngineError {
    error: ZmqError,
  },
  /// Sent from Engine -> Session upon clean stop
  EngineStopped,
  // --- ZAP Related (Examples) ---
  /// Sent from Engine -> Session to initiate ZAP check
  RequestZapAuth {
    // ZAP Request Frames (Mechanism, Credentials, etc.)
    // TBD based on ZAP implementation details
  },
  /// Sent from Session -> Engine with ZAP reply from authenticator
  ProcessZapReply {
    // ZAP Reply Frames (Status Code, UserID, Metadata)
    // TBD
  },

  // --- Pipe Management (Pipe -> Owner) ---
  /// Sent from PipeReaderTask -> SocketCore when a message arrives from a session pipe.
  PipeMessageReceived {
    pipe_id: usize,
    msg: Msg,
  },
  /// Sent from PipeReaderTask -> SocketCore when the session closes its *sending* end.
  PipeClosedByPeer {
    pipe_id: usize,
  },

  /// Sent from SocketCore -> Session to provide its pipe channel ends.
  AttachPipe {
    // Session needs receiver FROM Core and sender TO Core
    rx_from_core: AsyncReceiver<Msg>,
    tx_to_core: AsyncSender<Msg>,
    // Pass IDs for potential future use/logging in Session
    pipe_read_id: usize,  // ID Session uses to read (Core writes to this)
    pipe_write_id: usize, // ID Session uses to write (Core reads from this)
  },
  /// Sent from Connector's SocketCore -> Binder's SocketCore via Context registry.
  #[cfg(feature = "inproc")]
  InprocConnectRequest {
    connector_uri: String,                           // Identify the connector
    connector_pipe_tx: AsyncSender<Msg>,             // Connector's SENDING end (Binder writes here)
    connector_pipe_rx: AsyncReceiver<Msg>,           // Connector's RECEIVING end (Binder reads from here)
    connector_pipe_write_id: usize,                  // ID Binder uses to write to connector
    connector_pipe_read_id: usize,                   // ID Binder uses to read from connector
    reply_tx: oneshot::Sender<Result<(), ZmqError>>, // Confirm connection accepted/rejected
  },
  /// Sent from Connector's SocketCore -> Binder's SocketCore when connector closes pipe.
  /// Needed because Binder holds the other ends of the channels directly.
  #[cfg(feature = "inproc")]
  InprocPipeClosed {
    // Identify which pipe closed (e.g., the ID Binder uses to read from connector)
    pipe_read_id: usize,
  },
}

impl Command {
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
      Command::CleanupComplete { .. } => "CleanupComplete",
      Command::ReportError { .. } => "ReportError",
      Command::ConnSuccess { .. } => "ConnSuccess",
      Command::ConnFailed { .. } => "ConnFailed",
      Command::ListenerStopped { .. } => "ListenerStopped",
      Command::ConnecterStopped { .. } => "ConnecterStopped",
      Command::SessionStopped { .. } => "SessionStopped",
      Command::Attach { .. } => "Attach",
      Command::SessionPushCmd { .. } => "SessionPushCmd",
      Command::EnginePushCmd { .. } => "EnginePushCmd",
      Command::EngineReady { .. } => "EngineReady",
      Command::EngineError { .. } => "EngineError",
      Command::EngineStopped => "EngineStopped",
      Command::RequestZapAuth { .. } => "RequestZapAuth",
      Command::ProcessZapReply { .. } => "ProcessZapReply",
      Command::PipeMessageReceived { .. } => "PipeMessageReceived",
      Command::PipeClosedByPeer { .. } => "PipeClosedByPeer",
      Command::AttachPipe { .. } => "AttachPipe",
      #[cfg(feature = "inproc")]
      Command::InprocConnectRequest { .. } => "InprocConnectRequest",
      #[cfg(feature = "inproc")]
      Command::InprocPipeClosed { .. } => "InprocPipeClosed",
      // _ => "Unknown/Other", // Keep wildcard or ensure all are listed
    }
  }
}

// Implement Display trait for better logging if needed later
// impl fmt::Display for Command { ... }

