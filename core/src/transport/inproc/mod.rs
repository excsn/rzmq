pub(crate) mod connection;
pub(crate) mod handshake;
pub(crate) mod types;

pub(crate) use connection::DirectInprocConnection;
pub(crate) use types::{InprocHandshakeRequest, InprocHandshakeResponse};

use crate::context::Context;
use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::runtime::{Command, SystemEvent, system_events::ConnectionInteractionModel};
use crate::socket::core::SocketCore;
use crate::socket::events::SocketEvent;

use fibre::mpmc::bounded_async;
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
  tracing::debug!(connector_core_handle, inproc_name = %name, "Attempting direct inproc connect");

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

  let connection_handle_id = core_arc.context.inner().next_handle();
  let connection_specific_uri = format!("inproc://{}#{}", name, connection_handle_id);

  let (connector_socket_type, connector_identity, rcvhwm) = {
    let s = core_arc.core_state.read();
    (s.socket_type, s.options.routing_id.clone(), s.options.rcvhwm.max(1))
  };

  // Channel on which the connector receives frames from the binder.
  let (tx_to_connector, rx_for_connector) = bounded_async::<FrameBatch>(rcvhwm);

  let (reply_tx, reply_rx) = oneshot::oneshot();
  let request = InprocHandshakeRequest {
    connector_id: connector_core_handle,
    connector_socket_type,
    connector_identity,
    connector_rx_sender: tx_to_connector,
    reply_tx,
  };

  let request_event = SystemEvent::InprocBindingRequest {
    target_inproc_name: name.clone(),
    connector_uri: connection_specific_uri.clone(),
    handshake_request: Arc::new(Mutex::new(Some(request))),
  };

  if core_arc.context.event_bus().publish(request_event).is_err() {
    tracing::error!(connector_core_handle, inproc_name = %name, "Failed to publish InprocBindingRequest to EventBus.");
    let _ = reply_tx_user.send(Err(ZmqError::Internal(
      "Event bus publish failed for inproc connect request".into(),
    )));
    return;
  }

  match reply_rx.recv().await {
    Ok(Ok(response)) => {
      tracing::info!(connector_core_handle, inproc_name = %name, "Inproc connection accepted by binder.");

      // Validate the binder's socket type is compatible with ours.
      if let Err(e) = handshake::validate_socket_compatibility(connector_socket_type, response.binder_socket_type) {
        tracing::warn!(connector_core_handle, inproc_name = %name, "Inproc socket type mismatch: {}", e);
        let monitor_tx = core_arc.core_state.read().get_monitor_sender_clone();
        if let Some(monitor) = monitor_tx {
          let _ = monitor.send(SocketEvent::ConnectFailed {
            endpoint: connector_uri_str,
            error_msg: e.to_string(),
          }).await;
        }
        let _ = reply_tx_user.send(Err(e));
        return;
      }

      let peer_identity = response.binder_identity.clone();
      let direct_conn = DirectInprocConnection {
        connection_id: connection_handle_id,
        target_endpoint_uri: connection_specific_uri.clone(),
        peer_queue_sender: response.binder_rx_sender,
      };

      let cmd = Command::NewConnectionEstablished {
        endpoint_uri: connection_specific_uri.clone(),
        target_endpoint_uri: connector_uri_str.clone(),
        connection_iface: Some(Arc::new(direct_conn)),
        interaction_model: ConnectionInteractionModel::ViaDirectInproc {
          local_rx: Arc::new(Mutex::new(Some(rx_for_connector))),
          peer_identity,
        },
        managing_actor_task_id: None,
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

