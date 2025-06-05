use crate::runtime::{mailbox, MailboxSender};
use crate::socket::options::{SocketOptions, ZmtpEngineConfig};
use crate::Context;

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use super::core::ZmtpEngineCoreStd;

/// Creates and spawns the ZMTP Engine actor task for a TCP stream.
/// Returns the MailboxSender for the spawned engine task and its JoinHandle.
pub(crate) fn create_and_spawn_tcp_engine(
  handle: usize, // Engine's own handle ID
  session_mailbox: MailboxSender,
  stream: TcpStream,
  options: Arc<SocketOptions>,
  is_server: bool,
  context: &Context, // Accept Context reference
  parent_id: usize, // ID of the parent Session actor
) -> (MailboxSender, JoinHandle<()>) {
  let capacity = context.inner().get_actor_mailbox_capacity();
  let (tx, rx) = mailbox(capacity);

  let config = ZmtpEngineConfig::from(&*options);

  // Create the generic core state with TcpStream as the type
  let core = ZmtpEngineCoreStd::<TcpStream>::new(
    handle,
    session_mailbox,
    rx, // Pass the receiving end of the engine's mailbox
    stream,
    config,
    is_server,
    context.clone(), // Pass context clone to the engine core
  );

  let task_handle = tokio::spawn(core.run_loop(parent_id));

  // Note: ActorStarted event is published by the caller (e.g., Listener/Connecter)
  // immediately after this function returns successfully and the task is known to be spawned.

  // Return the mailbox sender *for the engine task* and the task handle
  (tx, task_handle)
}
