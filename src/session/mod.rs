// src/session/mod.rs

pub mod base; // Contains SessionBase implementation
// <<< ADDED MODULE DECLARATIONS END >>>

// <<< ADDED EXPORTS >>>
pub(crate) use base::SessionBase;
// <<< ADDED EXPORTS END >>>

// <<< ADDED ISESSION TRAIT >>>
use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::MailboxSender; // Mailbox of the Engine
use async_trait::async_trait;
use tokio::task::JoinHandle;

/// Defines the behavior of a Session actor.
#[async_trait]
pub trait ISession: Send + Sync + 'static {
  /// Attaches the corresponding Engine actor to this Session.
  /// Provides the Engine's mailbox for communication.
  async fn attach_engine(
    &self,
    engine_mailbox: MailboxSender,
    engine_task_handle: Option<JoinHandle<()>>,
  ) -> Result<(), ZmqError>;

  /// Pushes a message received from the Socket down towards the Engine.
  /// The Session might perform actions before forwarding (e.g., add ZAP context).
  async fn push_msg_to_engine(&self, msg: Msg) -> Result<(), ZmqError>;

  // --- ZAP Interaction Callbacks (called by Engine) ---

  /// Handles an authentication request from the Engine (e.g., PLAIN/CURVE).
  /// The Session will use its ZapClient to communicate with the authenticator.
  async fn handle_zap_request(&self, request_frames: Vec<Vec<u8>>) -> Result<(), ZmqError>;

  // --- Engine Status Callbacks (called by Engine) ---

  /// Called by the Engine when the ZMTP handshake (including security) is complete.
  fn engine_ready(&self);

  /// Called by the Engine when a fatal, non-recoverable error occurs.
  /// The Session should typically report this up to the SocketCore and initiate shutdown.
  fn engine_error(&self, error: ZmqError);

  /// Called by the Engine when it has cleanly shut down (e.g., after receiving Stop).
  fn engine_stopped(&self);
}