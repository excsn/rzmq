pub(crate) mod command_loop;
pub(crate) mod command_processor;
pub(crate) mod event_processor;
pub(crate) mod pipe_manager;
pub(crate) mod shutdown;
pub(crate) mod state;

use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{mailbox, MailboxSender};
use crate::socket::options::SocketOptions;
use crate::socket::types::SocketType;
use crate::socket::ISocket;
pub(crate) use state::{EndpointInfo, EndpointType};
pub(crate) use state::CoreState;
use state::ShutdownCoordinator;

use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, RwLock as TokioRwLock};


#[derive(Debug)]
pub struct SocketCore {
  pub(crate) handle: usize,
  pub(crate) context: Context,
  pub(crate) command_sender: MailboxSender,
  pub(crate) core_state: parking_lot::RwLock<CoreState>,
  // Weak reference to the ISocket pattern logic to avoid Arc cycles.
  pub(crate) socket_logic: TokioRwLock<Option<std::sync::Weak<dyn ISocket>>>,
  // Manages the multi-stage shutdown process. Needs Tokio's Mutex as its methods are async.
  pub(crate) shutdown_coordinator: TokioMutex<ShutdownCoordinator>,
}

impl SocketCore {
  pub(crate) fn create_and_spawn(
    handle: usize,
    context: Context,
    socket_type: SocketType,
    mut initial_options: SocketOptions,
  ) -> Result<(Arc<dyn ISocket>, MailboxSender), ZmqError> {
    let capacity = context.inner().get_actor_mailbox_capacity();
    let (cmd_tx, cmd_rx) = mailbox(capacity);
    initial_options.socket_type_name = format!("{:?}", socket_type).to_uppercase();

    let core_state_instance_new = CoreState::new(handle, socket_type, initial_options);

    let socket_core_arc = Arc::new(SocketCore {
      handle,
      context: context.clone(),
      command_sender: cmd_tx.clone(),
      core_state: parking_lot::RwLock::new(core_state_instance_new),
      socket_logic: TokioRwLock::new(None),
      shutdown_coordinator: TokioMutex::new(ShutdownCoordinator::default()),
    });

    let socket_logic_arc_impl: Arc<dyn ISocket> = match socket_type {
      SocketType::Pub => Arc::new(crate::socket::pub_socket::PubSocket::new(socket_core_arc.clone())),
      SocketType::Sub => Arc::new(crate::socket::sub_socket::SubSocket::new(socket_core_arc.clone())),
      SocketType::Req => Arc::new(crate::socket::req_socket::ReqSocket::new(socket_core_arc.clone())),
      SocketType::Rep => Arc::new(crate::socket::rep_socket::RepSocket::new(socket_core_arc.clone())),
      SocketType::Dealer => Arc::new(crate::socket::dealer_socket::DealerSocket::new(socket_core_arc.clone())),
      SocketType::Router => Arc::new(crate::socket::router_socket::RouterSocket::new(socket_core_arc.clone())),
      SocketType::Push => Arc::new(crate::socket::push_socket::PushSocket::new(socket_core_arc.clone())),
      SocketType::Pull => Arc::new(crate::socket::pull_socket::PullSocket::new(socket_core_arc.clone())),
    };

    {
      // Scope for TokioRwLock write guard
      let mut socket_logic_weak_ref_guard = socket_core_arc.socket_logic.try_write().map_err(|_| {
        ZmqError::Internal(
          "SocketCore: TokioRwLock fair lock poisoned or unavailable during ISocket weak ref init (try_write)".into(),
        )
      })?;
      *socket_logic_weak_ref_guard = Some(Arc::downgrade(&socket_logic_arc_impl));
    }

    let system_event_receiver = context.event_bus().subscribe();
    let core_arc_for_task = socket_core_arc.clone();
    let socket_logic_for_task = socket_logic_arc_impl.clone(); // Pass strong Arc to task

    // Spawn the main command loop for this SocketCore actor
    // run_command_loop is now in core/src/socket/core/command_loop.rs
    let _core_task_join_handle = tokio::spawn(command_loop::run_command_loop(
      core_arc_for_task,
      socket_logic_for_task,
      cmd_rx,
      system_event_receiver,
    ));

    Ok((socket_logic_arc_impl, cmd_tx))
  }

  // Publicly accessible (crate-level) methods for SocketCore
  pub(crate) fn command_sender(&self) -> MailboxSender {
    self.command_sender.clone()
  }

  pub(crate) async fn get_socket_logic(&self) -> Option<Arc<dyn ISocket>> {
    self
      .socket_logic
      .read()
      .await
      .as_ref()
      .and_then(|weak_ref| weak_ref.upgrade())
  }

  pub(crate) async fn is_running(&self) -> bool {
    // Assuming ShutdownPhase is in shutdown.rs or re-exported by state.rs
    self.shutdown_coordinator.lock().await.state == state::ShutdownPhase::Running
  }
}
