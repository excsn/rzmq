// core/src/transport/inproc.rs

#![cfg(feature = "inproc")]

use crate::context::Context;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{SystemEvent};
use crate::socket::core::pipe_manager::run_pipe_reader_task;
use crate::socket::core::{EndpointInfo, EndpointType, SocketCore};
use crate::socket::SocketEvent;

use fibre::mpmc::{bounded_async};
use std::sync::Arc;
use fibre::oneshot;

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
  tracing::debug!(connector_core_handle = connector_core_handle, inproc_name = %name, "Attempting inproc connect via event");

  let _binder_info = match core_arc.context.inner().lookup_inproc(&name) {
    Some(info) => info,
    None => {
      let err_msg = format!("Inproc endpoint '{}' not bound or not found", name);
      tracing::warn!(connector_core_handle = connector_core_handle, inproc_name = %name, "{}", err_msg);
      let zmq_err = ZmqError::ConnectionRefused(err_msg.clone());
      let event = SocketEvent::ConnectFailed {
        endpoint: connector_uri_str.clone(),
        error_msg: err_msg,
      };

      // <<< MODIFIED [Get monitor_tx from core_arc.core_state] >>>
      let monitor_tx_opt = core_arc.core_state.read().get_monitor_sender_clone();
      if let Some(monitor) = monitor_tx_opt {
        // <<< MODIFIED END >>>
        let _ = monitor.send(event).await;
      }
      let _ = reply_tx_user.send(Err(zmq_err));
      return;
    }
  };

  let pipe_hwm = {
    let connector_core_state = core_arc.core_state.read();
    connector_core_state
      .options
      .rcvhwm
      .max(connector_core_state.options.sndhwm)
      .max(1)
  };

  let socket_logic = match core_arc.get_socket_logic().await {
    Some(logic) => logic,
    None => {
      tracing::error!(
        connector_core_handle = connector_core_handle,
        inproc_name = %name,
        "connect_inproc: Failed to get ISocket logic for PipeReaderTask spawning. Aborting connect."
      );
      let err = ZmqError::Internal("ISocket logic unavailable for inproc connector's PipeReaderTask".into());
      let _ = reply_tx_user.send(Err(err.clone()));
      let event_failed = SystemEvent::ConnectionAttemptFailed {
        parent_core_id: connector_core_handle,
        target_endpoint_uri: format!("inproc://{}", name),
        error_msg: err.to_string(),
      };
      let _ = core_arc.context.event_bus().publish(event_failed);
      return;
    }
  };

  let pipe_id_connector_writes_to_binder = core_arc.context.inner().next_handle();
  let pipe_id_connector_reads_from_binder = pipe_id_connector_writes_to_binder + 1;

  let (tx_connector_to_binder, rx_binder_from_connector) = bounded_async::<Msg>(pipe_hwm);
  let (tx_binder_to_connector, rx_connector_from_binder) = bounded_async::<Msg>(pipe_hwm);

  core_arc
    .core_state
    .write()
    .pipes_tx
    .insert(pipe_id_connector_writes_to_binder, tx_connector_to_binder.clone());

  let connector_context_clone = core_arc.context.clone();
  let pipe_reader_task_join_handle = tokio::spawn(run_pipe_reader_task(
    connector_context_clone,
    connector_core_handle,
    socket_logic,
    pipe_id_connector_reads_from_binder,
    rx_connector_from_binder.clone(),
  ));

  core_arc
    .core_state
    .write()
    .pipe_reader_task_handles
    .insert(pipe_id_connector_reads_from_binder, pipe_reader_task_join_handle);

  let inproc_endpoint_entry_handle_id = core_arc.context.inner().next_handle();

  let connector_socket_options = core_arc.core_state.read().options.clone();
  let connector_monitor_tx = core_arc.core_state.read().get_monitor_sender_clone();
  let inproc_conn_iface = Arc::new(crate::socket::connection_iface::InprocConnection::new(
    inproc_endpoint_entry_handle_id,
    pipe_id_connector_writes_to_binder,
    pipe_id_connector_reads_from_binder,
    format!("inproc://{}", name),
    core_arc.context.clone(),
    tx_connector_to_binder.clone(),
    connector_monitor_tx.clone(), // Pass it here
    connector_socket_options,
  ));
  // <<< MODIFIED END >>>

  let endpoint_info = EndpointInfo {
    mailbox: core_arc.command_sender(),
    task_handle: None,
    endpoint_type: EndpointType::Session,
    endpoint_uri: connector_uri_str.clone(),
    pipe_ids: Some((pipe_id_connector_writes_to_binder, pipe_id_connector_reads_from_binder)),
    handle_id: inproc_endpoint_entry_handle_id,
    target_endpoint_uri: Some(connector_uri_str.clone()),
    is_outbound_connection: true,
    connection_iface: inproc_conn_iface,
  };
  {
    // Scope for write lock
    let mut core_s_write = core_arc.core_state.write();
    core_s_write.endpoints.insert(connector_uri_str.clone(), endpoint_info);

    // Populate the reverse map for the connector's CoreState
    core_s_write
      .pipe_read_id_to_endpoint_uri
      .insert(pipe_id_connector_reads_from_binder, connector_uri_str.clone());
  }

  let monitor_tx_for_event = connector_monitor_tx;

  let (reply_tx, reply_rx_internal_binder) = oneshot::oneshot();
  let request_event = SystemEvent::InprocBindingRequest {
    target_inproc_name: name.clone(),
    connector_uri: connector_uri_str.clone(),
    binder_pipe_rx_from_connector: rx_binder_from_connector,
    binder_pipe_tx_to_connector: tx_binder_to_connector,
    connector_pipe_write_id: pipe_id_connector_writes_to_binder,
    connector_pipe_read_id: pipe_id_connector_reads_from_binder,
    reply_tx: reply_tx,
  };

  tracing::debug!(connector_core_handle = connector_core_handle, inproc_name = %name, "Publishing InprocBindingRequest event");
  if core_arc.context.event_bus().publish(request_event).is_err() {
    tracing::error!(connector_core_handle = connector_core_handle, inproc_name = %name, "Failed to publish InprocBindingRequest event to EventBus.");
    let err = ZmqError::Internal("Event bus publish failed for inproc connect request".into());
    {
      let mut cs = core_arc.core_state.write();
      cs.endpoints.remove(&connector_uri_str);
      cs.remove_pipe_state(pipe_id_connector_writes_to_binder, pipe_id_connector_reads_from_binder);
    }
    let _ = reply_tx_user.send(Err(err));
    return;
  }

  match reply_rx_internal_binder.recv().await {
    Ok(Ok(())) => {
      tracing::info!(connector_core_handle = connector_core_handle, inproc_name = %name, "Inproc connection established successfully via event bus");

      if let Some(monitor) = monitor_tx_for_event {
        let peer_addr_synthetic = format!("inproc-binder-for-{}", name);
        let event = SocketEvent::Connected {
          endpoint: connector_uri_str.clone(),
          peer_addr: peer_addr_synthetic,
        };
        let _ = monitor.send(event).await;
      }

      if let Some(socket_logic_impl) = core_arc.get_socket_logic().await {
        socket_logic_impl
          .pipe_attached(
            pipe_id_connector_reads_from_binder,
            pipe_id_connector_writes_to_binder,
            None,
          )
          .await;
      } else {
        tracing::error!(
          connector_core_handle = connector_core_handle,
          "Inproc connect: Connector ISocket logic unavailable for pipe_attached notification!"
        );
      }
      let _ = reply_tx_user.send(Ok(()));
    }
    Ok(Err(e)) => {
      tracing::warn!(connector_core_handle = connector_core_handle, inproc_name = %name, "Inproc connection rejected by binder: {}", e);
      {
        let mut cs = core_arc.core_state.write();
        cs.endpoints.remove(&connector_uri_str);
        cs.remove_pipe_state(pipe_id_connector_writes_to_binder, pipe_id_connector_reads_from_binder);
      }
      if let Some(monitor) = monitor_tx_for_event {
        let event = SocketEvent::ConnectFailed {
          endpoint: connector_uri_str.clone(),
          error_msg: format!("{}", e),
        };
        let _ = monitor.send(event).await;
      }
      let _ = reply_tx_user.send(Err(e));
    }
    Err(_) => {
      let error_msg = format!("Binder for inproc endpoint '{}' failed to reply or disappeared", name);
      tracing::error!(connector_core_handle = connector_core_handle, inproc_name = %name, "{}", error_msg);
      let zmq_err = ZmqError::Internal(error_msg.clone());
      {
        let mut cs = core_arc.core_state.write();
        cs.endpoints.remove(&connector_uri_str);
        cs.remove_pipe_state(pipe_id_connector_writes_to_binder, pipe_id_connector_reads_from_binder);
      }
      if let Some(monitor) = monitor_tx_for_event {
        let event = SocketEvent::ConnectFailed {
          endpoint: connector_uri_str.clone(),
          error_msg,
        };
        let _ = monitor.send(event).await;
      }
      let _ = reply_tx_user.send(Err(zmq_err));
    }
  }
}

// ... (unbind_inproc, disconnect_inproc remain the same)
pub(crate) async fn unbind_inproc(name: &str, context: &Context) {
  tracing::debug!(inproc_name = %name, "Unbinding inproc endpoint from context registry");
  context.inner().unregister_inproc(name);
}

pub(crate) async fn disconnect_inproc(endpoint_uri: &str, core_arc: Arc<SocketCore>) -> Result<(), ZmqError> {
  let connector_core_handle = core_arc.handle;
  let inproc_name_to_notify = endpoint_uri.strip_prefix("inproc://").unwrap_or("").to_string();
  if inproc_name_to_notify.is_empty() {
    tracing::warn!(connector_core_handle = connector_core_handle, %endpoint_uri, "Invalid inproc URI for disconnect");
    return Err(ZmqError::InvalidEndpoint(endpoint_uri.to_string()));
  }
  tracing::debug!(connector_core_handle = connector_core_handle, %endpoint_uri, "Disconnecting inproc endpoint via event");

  let removed_endpoint_info = match core_arc.core_state.write().endpoints.remove(endpoint_uri) {
    Some(info) => info,
    None => {
      tracing::warn!(connector_core_handle = connector_core_handle, %endpoint_uri, "Endpoint not found for inproc disconnect (already removed or connect failed?).");
      return Ok(());
    }
  };

  let (pipe_id_connector_writes, pipe_id_connector_reads) = match removed_endpoint_info.pipe_ids {
    Some(ids) => ids,
    None => {
      core_arc
        .core_state
        .write()
        .endpoints
        .insert(endpoint_uri.to_string(), removed_endpoint_info);
      tracing::error!(connector_core_handle = connector_core_handle, %endpoint_uri, "Inproc disconnect failed: Missing pipe IDs for stored endpoint info.");
      return Err(ZmqError::Internal(
        "Missing pipe IDs for stored inproc endpoint during disconnect".into(),
      ));
    }
  };

  let close_notification_event = SystemEvent::InprocPipePeerClosed {
    target_inproc_name: inproc_name_to_notify.clone(),
    closed_by_connector_pipe_read_id: pipe_id_connector_reads,
  };

  tracing::debug!(
    connector_core_handle = connector_core_handle,
    %endpoint_uri,
    target_binder_name = %inproc_name_to_notify,
    connector_read_pipe_id_closed = pipe_id_connector_reads,
    "Publishing InprocPipePeerClosed event to notify binder"
  );
  if core_arc.context.event_bus().publish(close_notification_event).is_err() {
    tracing::warn!(connector_core_handle = connector_core_handle, %endpoint_uri, "Failed to publish InprocPipePeerClosed event (binder likely gone or event bus issue)");
  }

  let pipes_were_removed = core_arc
    .core_state
    .write()
    .remove_pipe_state(pipe_id_connector_writes, pipe_id_connector_reads);
  let monitor_tx_for_event = core_arc.core_state.read().get_monitor_sender_clone();
  if let Some(handle) = removed_endpoint_info.task_handle {
    handle.abort();
  }

  if pipes_were_removed {
    if let Some(socket_logic_impl) = core_arc.get_socket_logic().await {
      socket_logic_impl.pipe_detached(pipe_id_connector_reads).await;
      tracing::debug!(connector_core_handle = connector_core_handle, %endpoint_uri, "Notified ISocket of inproc pipe detachment");
    } else {
      tracing::error!(
        connector_core_handle = connector_core_handle,
        "Inproc disconnect: ISocket logic unavailable for pipe_detached notification!"
      );
    }
  } else {
    tracing::warn!(connector_core_handle = connector_core_handle, %endpoint_uri, "Inproc disconnect: Local pipe state was already removed or inconsistent.");
  }

  // <<< MODIFIED [Use the already cloned monitor_tx_for_event] >>>
  if let Some(monitor) = monitor_tx_for_event {
    // <<< MODIFIED END >>>
    let event = SocketEvent::Disconnected {
      endpoint: endpoint_uri.to_string(),
    };
    if monitor.send(event).await.is_err() {
      tracing::warn!(
        socket_handle = connector_core_handle,
        "Failed to send Disconnected monitor event for inproc connection"
      );
    }
  } else {
    // Optional: log if no monitor was configured when expected
    // tracing::trace!(socket_handle = connector_core_handle, "No monitor configured, skipping Disconnected event for inproc.");
  }
  tracing::info!(connector_core_handle = connector_core_handle, %endpoint_uri, "Inproc connection disconnected successfully");
  Ok(())
}
