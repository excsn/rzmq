// src/engine/zmtp_ipc.rs

#![cfg(feature = "ipc")] // Only compile this file if ipc feature is enabled

use crate::runtime::{mailbox, MailboxSender};
use crate::socket::options::{SocketOptions, ZmtpEngineConfig};
use crate::Context;
use std::sync::Arc;
use tokio::net::UnixStream;
use tokio::task::JoinHandle;

use super::core::ZmtpEngineCoreStd;

/// Creates and spawns the ZMTP Engine actor task for an IPC stream.
/// Returns the MailboxSender for the spawned engine task and its JoinHandle.
pub(crate) fn create_and_spawn_ipc_engine(
  handle: usize, // Engine's own handle ID
  session_mailbox: MailboxSender,
  stream: UnixStream, // Takes UnixStream
  options: Arc<SocketOptions>,
  is_server: bool,
  context: &Context, // Accept Context reference
  _parent_id: usize, // ID of the parent Session actor
) -> (MailboxSender, JoinHandle<()>) {
  let capacity = context.inner().get_actor_mailbox_capacity();
  let (tx, rx) = mailbox(capacity);

  let config = ZmtpEngineConfig {
    routing_id: options.routing_id.clone(),
    socket_type_name: options.socket_type_name.clone(),
    heartbeat_ivl: options.heartbeat_ivl,
    heartbeat_timeout: options.heartbeat_timeout,
    use_send_zerocopy: false,
    use_recv_multishot: false,
    use_cork: false,
    #[cfg(feature = "io-uring")]
    recv_multishot_buffer_count: options.io_uring_recv_buffer_count,
    #[cfg(feature = "io-uring")]
    recv_multishot_buffer_capacity: options.io_uring_recv_buffer_size,
  };

  eprintln!(
    "[DEBUG ENGINE {} PRE-NEW] Creating ZmtpEngineCore, is_server={}",
    handle, is_server
  );
  let core = ZmtpEngineCoreStd::<UnixStream>::new(
    handle,
    session_mailbox,
    rx, // Pass the receiving end of the engine's mailbox
    stream,
    config,
    is_server,
    context.clone(), // Pass context clone to the engine core
  );

  eprintln!(
    "[DEBUG ENGINE {} POST-NEW] ZmtpEngineCore created. Spawning run_loop...",
    handle
  );

  // Spawn the generic core loop task
  let task_handle = tokio::spawn(core.run_loop()); // run_loop consumes core
  eprintln!("[DEBUG ENGINE {} POST-SPAWN] ZmtpEngineCore run_loop spawned.", handle);

  // Note: ActorStarted event is published by the caller (e.g., Listener/Connecter)
  // immediately after this function returns successfully and the task is known to be spawned.

  // Return the mailbox sender *for the engine task* and the task handle
  (tx, task_handle)
}
