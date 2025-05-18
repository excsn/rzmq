// src/engine/zmtp_tcp.rs

use crate::runtime::{mailbox, MailboxSender};
use crate::socket::options::{SocketOptions, ZmtpEngineConfig};
use crate::Context;

use std::sync::Arc;

#[cfg(not(feature = "io-uring"))]
use tokio::net::TcpStream as CurrentTcpStream;
use tokio::task::JoinHandle;
#[cfg(feature = "io-uring")]
use tokio_uring::net::TcpStream as CurrentTcpStream;

use super::core::ZmtpEngineCore;

/// Creates and spawns the ZMTP Engine actor task for a TCP stream.
/// Returns the MailboxSender for the spawned engine task and its JoinHandle.
pub(crate) fn create_and_spawn_tcp_engine(
  handle: usize, // Engine's own handle ID
  session_mailbox: MailboxSender,
  stream: CurrentTcpStream,
  options: Arc<SocketOptions>,
  is_server: bool,
  context: &Context, // Accept Context reference
  _parent_id: usize, // ID of the parent Session actor
) -> (MailboxSender, JoinHandle<()>) {
  let capacity = context.inner().get_actor_mailbox_capacity();
  let (tx, rx) = mailbox(capacity);

  let config = ZmtpEngineConfig {
    // Copy relevant fields from socket options
    routing_id: options.routing_id.clone(),
    socket_type_name: options.socket_type_name.clone(),
    heartbeat_ivl: options.heartbeat_ivl,
    heartbeat_timeout: options.heartbeat_timeout,
    // io_uring options need checking based on feature flag
    use_send_zerocopy: options.io_uring_send_zerocopy,
    use_recv_multishot: options.io_uring_recv_multishot,
    use_cork: options.tcp_cork,
  };

  // Create the generic core state with TcpStream as the type
  let core = ZmtpEngineCore::<CurrentTcpStream>::new(
    handle,
    session_mailbox,
    rx, // Pass the receiving end of the engine's mailbox
    stream,
    config,
    is_server,
    context.clone(), // Pass context clone to the engine core
  );

  // Spawn the generic core loop task
  let task_handle = tokio::spawn(core.run_loop()); // run_loop consumes core

  // Note: ActorStarted event is published by the caller (e.g., Listener/Connecter)
  // immediately after this function returns successfully and the task is known to be spawned.

  // Return the mailbox sender *for the engine task* and the task handle
  (tx, task_handle)
}
