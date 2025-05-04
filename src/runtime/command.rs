// src/runtime/command.rs

use crate::error::ZmqError;
use crate::message::Msg;

use std::fmt;

use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use tokio::sync::oneshot; // Using tokio's oneshot for replies

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
  }, // Identify child by handle (TBD)
  /// Sent from child to parent reporting a fatal error.
  ReportError {
    handle: usize,
    error: ZmqError,
  },

  // --- Connection Management (Listener/Connecter -> SocketCore) ---
  ConnSuccess {
    endpoint: String, // Identify which bind/connect succeeded
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
  EngineReady,
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
}

/// Type alias for the mailbox sender used within Commands.
pub type MailboxSender = async_channel::Sender<Command>;

// Implement Display trait for better logging if needed later
// impl fmt::Display for Command { ... }
