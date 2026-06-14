#![cfg(feature = "inproc")]

use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{Command, SystemEvent, mailbox, system_events::ConnectionInteractionModel};
use crate::sessionx::actor::SessionConnectionActorX;
use crate::sessionx::states::ActorConfigX;
use crate::socket::core::SocketCore;
use crate::socket::events::SocketEvent;
use crate::socket::options::ZmtpEngineConfig;
use crate::transport::inproc_stream::InprocStream;

use fibre::oneshot;
use std::sync::{Arc, Mutex};

pub(crate) async fn bind_inproc(name: String, core_arc: Arc<SocketCore>) -> Result<(), ZmqError> {
  tracing::debug!(binder_core_handle = core_arc.handle, inproc_name = %name, "Attempting to bind inproc endpoint");
  core_arc
    .context
    .inner()
    .register_inproc(name.clone(), core_arc.handle)?;

  let mut binder_core_state = core_arc.core_state.write();
  binder_core_state.bound_inproc_names.insert(name);
  tracing::info!(binder_core_handle = core_arc.handle, inproc_name = %binder_core_state.bound_inproc_names.iter().last().unwrap(), "Inproc endpoint bound successfully");
  Ok(())
}

pub(crate) async fn connect_inproc(
  name: String,
  core_arc: Arc<SocketCore>,
  reply_tx_user: oneshot::Sender<Result<(), ZmqError>>,
) {
  let connector_core_handle = core_arc.handle;
  let connector_uri_str = format!("inproc://{}", name);
  tracing::debug!(connector_core_handle, inproc_name = %name, "Attempting inproc connect via SCAX");

  // Verify the binder exists
  let _binder_info = match core_arc.context.inner().lookup_inproc(&name) {
    Some(info) => info,
    None => {
      let err_msg = format!("Inproc endpoint '{}' not bound or not found", name);
      tracing::warn!(connector_core_handle, inproc_name = %name, "{}", err_msg);
      let zmq_err = ZmqError::ConnectionRefused(err_msg.clone());
      let monitor_tx_opt = core_arc.core_state.read().get_monitor_sender_clone();
      if let Some(monitor) = monitor_tx_opt {
        let _ = monitor
          .send(SocketEvent::ConnectFailed {
            endpoint: connector_uri_str.clone(),
            error_msg: err_msg,
          })
          .await;
      }
      let _ = reply_tx_user.send(Err(zmq_err));
      return;
    }
  };

  let pipe_hwm = {
    let s = core_arc.core_state.read();
    s.options.rcvhwm.max(s.options.sndhwm).max(1)
  };

  let socket_logic = match core_arc.get_socket_logic().await {
    Some(logic) => logic,
    None => {
      tracing::error!(connector_core_handle, inproc_name = %name, "connect_inproc: ISocket logic unavailable. Aborting.");
      let err = ZmqError::Internal("ISocket logic unavailable for inproc connector".into());
      let _ = reply_tx_user.send(Err(err.clone()));
      let _ = core_arc
        .context
        .event_bus()
        .publish(SystemEvent::ConnectionAttemptFailed {
          parent_core_id: connector_core_handle,
          target_endpoint_uri: connector_uri_str,
          error: err,
        });
      return;
    }
  };

  // Create the in-memory duplex pipe; binder gets one half, connector the other.
  let pipe_hwm = {
    let s = core_arc.core_state.read();
    match s.socket_type {
      crate::socket::SocketType::Push | crate::socket::SocketType::Pub => s.options.sndhwm,
      crate::socket::SocketType::Pull | crate::socket::SocketType::Sub => s.options.rcvhwm,
      _ => s.options.rcvhwm.max(s.options.sndhwm),
    }
    .max(1)
  };

  let buf_size = pipe_hwm.saturating_mul(8192).clamp(8192, 4 * 1024 * 1024);
  let (connector_stream_end, binder_stream_end) = tokio::io::duplex(buf_size);

  // Spawn connector-side SCAX
  let sca_handle_id = core_arc.context.inner().next_handle();
  // Each connection gets a unique key so multiple connectors to the same inproc name
  // don't collide in the endpoints map.
  let connection_specific_uri = format!("inproc://{}#{}", name, sca_handle_id);
  let engine_conf = Arc::new(ZmtpEngineConfig::from(&*core_arc.core_state.read().options));
  let actor_conf = ActorConfigX {
    context: core_arc.context.clone(),
    monitor_tx: core_arc.core_state.read().get_monitor_sender_clone(),
    logical_target_endpoint_uri: connector_uri_str.clone(),
    connected_endpoint_uri: connection_specific_uri.clone(),
    is_server_role: false,
  };
  let capacity = core_arc.context.inner().get_actor_mailbox_capacity();
  let (command_sender_for_sca, command_receiver_for_sca) = mailbox(capacity);
  let sca_task_handle = SessionConnectionActorX::create_and_spawn(
    sca_handle_id,
    connector_core_handle,
    InprocStream::new(connector_stream_end),
    actor_conf,
    engine_conf,
    command_receiver_for_sca,
    socket_logic.clone(),
    None,
  );

  // Publish InprocBindingRequest so the binder's SocketCore spawns its own SCAX.
  // connector_uri carries the unique URI so the binder registers a distinct endpoint key.
  let (reply_tx, reply_rx) = oneshot::oneshot();
  let request_event = SystemEvent::InprocBindingRequest {
    target_inproc_name: name.clone(),
    connector_uri: connection_specific_uri.clone(),
    binder_stream_end: Arc::new(Mutex::new(Some(binder_stream_end))),
    reply_tx,
  };

  if core_arc.context.event_bus().publish(request_event).is_err() {
    tracing::error!(connector_core_handle, inproc_name = %name, "Failed to publish InprocBindingRequest to EventBus.");
    let _ = reply_tx_user.send(Err(ZmqError::Internal(
      "Event bus publish failed for inproc connect request".into(),
    )));
    return;
  }

  match reply_rx.recv().await {
    Ok(Ok(())) => {
      tracing::info!(connector_core_handle, inproc_name = %name, "Inproc connection accepted by binder; notifying connector socket core.");

      let cmd = Command::NewConnectionEstablished {
        endpoint_uri: connection_specific_uri.clone(),
        target_endpoint_uri: connector_uri_str.clone(),
        connection_iface: None, // SocketCore creates ScaConnectionIface
        interaction_model: ConnectionInteractionModel::ViaSca {
          sca_mailbox: command_sender_for_sca,
          sca_handle_id,
        },
        managing_actor_task_id: Some(sca_task_handle.id()),
      };
      if socket_logic.mailbox().send(cmd).await.is_err() {
        tracing::error!(connector_core_handle, inproc_name = %name, "Failed to send NewConnectionEstablished to connector socket core.");
        let _ = reply_tx_user.send(Err(ZmqError::Internal(
          "connector socket core closed".into(),
        )));
        return;
      }

      let monitor_tx = core_arc.core_state.read().get_monitor_sender_clone();
      if let Some(monitor) = monitor_tx {
        let _ = monitor
          .send(SocketEvent::Connected {
            endpoint: connector_uri_str.clone(),
            peer_addr: format!("inproc-binder-for-{}", name),
          })
          .await;
      }

      let _ = reply_tx_user.send(Ok(()));
    }
    Ok(Err(e)) => {
      tracing::warn!(connector_core_handle, inproc_name = %name, "Inproc connection rejected by binder: {}", e);
      let monitor_tx = core_arc.core_state.read().get_monitor_sender_clone();
      if let Some(monitor) = monitor_tx {
        let _ = monitor
          .send(SocketEvent::ConnectFailed {
            endpoint: connector_uri_str,
            error_msg: e.to_string(),
          })
          .await;
      }
      let _ = reply_tx_user.send(Err(e));
    }
    Err(_) => {
      let err_msg = format!("Binder for inproc endpoint '{}' disappeared", name);
      tracing::error!(connector_core_handle, inproc_name = %name, "{}", err_msg);
      let monitor_tx = core_arc.core_state.read().get_monitor_sender_clone();
      if let Some(monitor) = monitor_tx {
        let _ = monitor
          .send(SocketEvent::ConnectFailed {
            endpoint: connector_uri_str,
            error_msg: err_msg.clone(),
          })
          .await;
      }
      let _ = reply_tx_user.send(Err(ZmqError::Internal(err_msg)));
    }
  }
}

pub(crate) async fn unbind_inproc(name: &str, context: &Context) {
  tracing::debug!(inproc_name = %name, "Unbinding inproc endpoint from context registry");
  context.inner().unregister_inproc(name);
}

/// Fallback called when `handle_user_disconnect` cannot find the endpoint in the map.
/// For SCAX-backed connections the standard path via `ScaConnectionIface::close_connection`
/// handles all teardown; this path only fires when the connection was never fully established.
pub(crate) async fn disconnect_inproc(
  endpoint_uri: &str,
  core_arc: Arc<SocketCore>,
) -> Result<(), ZmqError> {
  tracing::debug!(
    connector_core_handle = core_arc.handle,
    %endpoint_uri,
    "disconnect_inproc: endpoint not in endpoints map; nothing to clean up"
  );
  Ok(())
}
