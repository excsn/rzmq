// src/engine/zmtp_ipc.rs

// <<< ADDED ZmtpIpcEngine (Feature-gated) >>>
#![cfg(feature = "ipc")] // Only compile this file if ipc feature is enabled

use crate::runtime::{mailbox, MailboxSender};
use crate::socket::options::{SocketOptions, ZmtpEngineConfig};
use std::sync::Arc;
use tokio::net::UnixStream; // Use UnixStream
use tokio::task::JoinHandle;

use super::core::ZmtpEngineCore;

/// Creates and spawns the ZMTP Engine actor task for an IPC stream.
/// Returns the MailboxSender for the spawned engine task and its JoinHandle.
pub(crate) fn create_and_spawn_ipc_engine(
  handle: usize,
  session_mailbox: MailboxSender,
  stream: UnixStream, // Takes UnixStream
  options: Arc<SocketOptions>,
  is_server: bool,
) -> (MailboxSender, JoinHandle<()>) {
  let (tx, rx) = mailbox();

  let config = ZmtpEngineConfig {
    routing_id: options.routing_id.clone(),
    socket_type_name: options.socket_type_name.clone(),
    // Add heartbeat options
    heartbeat_ivl: options.heartbeat_ivl,
    heartbeat_timeout: options.heartbeat_timeout,
    #[cfg(feature = "io-uring")]
    use_send_zerocopy: false, // N/A for UnixStream? Check tokio-uring
    #[cfg(feature = "io-uring")]
    use_recv_multishot: false, // N/A for UnixStream? Check tokio-uring
  };

  // Create the generic core state with UnixStream as the type
  let core = ZmtpEngineCore::<UnixStream>::new(handle, session_mailbox, rx, stream, config, is_server);
  // Spawn the generic core loop
  let task_handle = tokio::spawn(core.run_loop());
  // Return the mailbox *for the core task* and the task handle
  (tx, task_handle)
}
