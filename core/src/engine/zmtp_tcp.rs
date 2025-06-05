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

  let config = ZmtpEngineConfig {
    // Copy relevant fields from socket options
    routing_id: options.routing_id.clone(),
    socket_type_name: options.socket_type_name.clone(),
    heartbeat_ivl: options.heartbeat_ivl,
    heartbeat_timeout: options.heartbeat_timeout,
    handshake_timeout: options.handshake_ivl,
    // io_uring options need checking based on feature flag
    use_send_zerocopy: options.io_uring.send_zerocopy,
    use_recv_multishot: options.io_uring.recv_multishot,
    use_cork: options.tcp_cork,
    #[cfg(feature = "noise_xx")]
    use_noise_xx: options.noise_xx_options.enabled,
    #[cfg(feature = "noise_xx")]
    noise_xx_local_sk_bytes_for_engine: options.noise_xx_options.static_secret_key_bytes,
    #[cfg(feature = "noise_xx")]
    noise_xx_remote_pk_bytes_for_engine: options.noise_xx_options.remote_static_public_key_bytes,
    use_plain: options.plain_options.enabled,
    plain_username_for_engine: options.plain_options.username.clone(),
    plain_password_for_engine: options.plain_options.password.clone(),
  };

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
