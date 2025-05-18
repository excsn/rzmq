// src/engine/zmtp_ipc.rs

// <<< MODIFIED START [Feature gate and Imports for Event Bus integration] >>>
#![cfg(feature = "ipc")] // Only compile this file if ipc feature is enabled

use crate::runtime::{mailbox, ActorType, Command, MailboxSender}; // Added Context, ActorType
use crate::socket::options::{SocketOptions, ZmtpEngineConfig};
use crate::Context;
use std::sync::Arc;
use tokio::net::UnixStream; // Use UnixStream
use tokio::task::JoinHandle;

use super::core::ZmtpEngineCore;
// <<< MODIFIED END >>>

/// Creates and spawns the ZMTP Engine actor task for an IPC stream.
/// Returns the MailboxSender for the spawned engine task and its JoinHandle.
// <<< MODIFIED START [Signature accepts Context and parent_id] >>>
pub(crate) fn create_and_spawn_ipc_engine(
  handle: usize, // Engine's own handle ID
  session_mailbox: MailboxSender,
  stream: UnixStream, // Takes UnixStream
  options: Arc<SocketOptions>,
  is_server: bool,
  context: &Context, // Accept Context reference
  parent_id: usize,  // ID of the parent Session actor
) -> (MailboxSender, JoinHandle<()>) {
  // <<< MODIFIED END >>>

  let (tx, rx) = mailbox(); // Mailbox for the engine actor

  let config = ZmtpEngineConfig {
    routing_id: options.routing_id.clone(),
    socket_type_name: options.socket_type_name.clone(),
    heartbeat_ivl: options.heartbeat_ivl,
    heartbeat_timeout: options.heartbeat_timeout,
    // io_uring specific options - usually less relevant for IPC
    #[cfg(feature = "io-uring")]
    use_send_zerocopy: false, // N/A for UnixStream? Check tokio-uring docs if needed
    #[cfg(feature = "io-uring")]
    use_recv_multishot: false, // N/A for UnixStream? Check tokio-uring docs if needed
  };

  // <<< MODIFIED START [Pass Context to ZmtpEngineCore::new] >>>
  eprintln!("[DEBUG ENGINE {} PRE-NEW] Creating ZmtpEngineCore, is_server={}", handle, is_server);
  let core = ZmtpEngineCore::<UnixStream>::new(
    handle,
    session_mailbox,
    rx, // Pass the receiving end of the engine's mailbox
    stream,
    config,
    is_server,
    context.clone(), // Pass context clone to the engine core
  );
  // <<< MODIFIED END >>>
  eprintln!("[DEBUG ENGINE {} POST-NEW] ZmtpEngineCore created. Spawning run_loop...", handle);

  // Spawn the generic core loop task
  let task_handle = tokio::spawn(core.run_loop()); // run_loop consumes core
  eprintln!("[DEBUG ENGINE {} POST-SPAWN] ZmtpEngineCore run_loop spawned.", handle);

  // Note: ActorStarted event is published by the caller (e.g., Listener/Connecter)
  // immediately after this function returns successfully and the task is known to be spawned.

  // Return the mailbox sender *for the engine task* and the task handle
  (tx, task_handle)
}
