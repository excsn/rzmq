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
  handle: usize,
  session_mailbox: MailboxSender,
  stream: UnixStream,
  options: Arc<SocketOptions>,
  is_server: bool,
  context: &Context,
  parent_id: usize, // ID of the parent Session actor
) -> (MailboxSender, JoinHandle<()>) {
  let capacity = context.inner().get_actor_mailbox_capacity();
  let (tx, rx) = mailbox(capacity);

  let mut config = ZmtpEngineConfig::from(&*options);
  config.use_send_zerocopy = false;
  config.use_recv_multishot = false;
  config.use_cork = false;

  let core = ZmtpEngineCoreStd::<UnixStream>::new(
    handle,
    session_mailbox,
    rx, // Pass the receiving end of the engine's mailbox
    stream,
    config,
    is_server,
    context.clone(), // Pass context clone to the engine core
  );

  // Spawn the generic core loop task
  let task_handle = tokio::spawn(core.run_loop(parent_id)); // run_loop consumes core

  // Note: ActorStarted event is published by the caller (e.g., Listener/Connecter)
  // immediately after this function returns successfully and the task is known to be spawned.

  // Return the mailbox sender *for the engine task* and the task handle
  (tx, task_handle)
}
