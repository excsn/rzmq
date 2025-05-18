// src/transport/inproc.rs

#![cfg(feature = "inproc")] // Only compile this file if the "inproc" feature is enabled.

use crate::context::Context; // For accessing the inproc registry and event bus.
                             // use crate::context::InprocBinding; // Not directly used here, but ContextInner uses it.
use crate::error::ZmqError;
use crate::message::Msg; // For the data type exchanged over inproc pipes.
use crate::runtime::{Command, OneShotSender, SystemEvent}; // Command for reconstructing InprocConnectRequest if needed by SocketCore.
use crate::socket::core::{EndpointInfo, EndpointType, SocketCore}; // For managing endpoint state.
use crate::socket::SocketEvent; // For emitting monitor events.

use async_channel::bounded; // For creating the bounded channels that form the inproc pipe.
use std::sync::Arc;
use tokio::sync::oneshot; // For reply channels in InprocBindingRequest and user connect.

/// Handles binding an in-process endpoint.
/// This involves registering the binder's `SocketCore` handle ID with the global `ContextInner`
/// so that connectors can find it by name.
///
/// # Arguments
/// * `name` - The logical name for the inproc endpoint (e.g., "my-service").
/// * `core_arc` - An `Arc` to the `SocketCore` of the socket that is binding.
pub(crate) async fn bind_inproc(
  name: String,
  core_arc: Arc<SocketCore>, // Binder's SocketCore.
) -> Result<(), ZmqError> {
  tracing::debug!(binder_core_handle = core_arc.handle, inproc_name = %name, "Attempting to bind inproc endpoint");
  // Register with the context's inproc registry, storing the binder's core handle ID.
  // This ID allows the binder SocketCore to identify InprocBindingRequest events intended for it.
  core_arc
    .context
    .inner()
    .register_inproc(name.clone(), core_arc.handle) // Pass binder's own handle ID.
    .await?;

  // Also, track locally within the binder's CoreState that it has bound this name.
  // This can be useful for cleanup or diagnostics.
  let mut binder_core_state = core_arc.core_state.lock().await;
  binder_core_state.bound_inproc_names.insert(name);
  tracing::info!(binder_core_handle = core_arc.handle, inproc_name = %binder_core_state.bound_inproc_names.iter().last().unwrap(), "Inproc endpoint bound successfully");
  Ok(())
}

/// Handles connecting to an in-process endpoint.
/// This involves:
/// 1. Looking up the binder's core ID in the `ContextInner`'s inproc registry.
/// 2. Creating a pair of asynchronous channels (pipes) for communication.
/// 3. Storing the connector's ends of the pipes and spawning a `PipeReaderTask`.
/// 4. Publishing an `SystemEvent::InprocBindingRequest` to the event bus.
///    This event contains the binder's target name and the connector's pipe ends.
/// 5. The binder `SocketCore` (listening on the event bus) will pick up this event,
///    set up its side of the pipes, and reply via the `oneshot::Sender` included in the event.
/// 6. This function awaits the binder's reply to confirm connection success or failure.
///
/// # Arguments
/// * `name` - The logical name of the inproc endpoint to connect to.
/// * `core_arc` - An `Arc` to the `SocketCore` of the socket that is connecting.
/// * `reply_tx_user` - The `oneshot::Sender` to send the final connection result (Ok/Err) back to the user API call.
pub(crate) async fn connect_inproc(
  name: String,                                         // Name of the inproc endpoint to connect to.
  core_arc: Arc<SocketCore>,                            // Connector's SocketCore.
  reply_tx_user: oneshot::Sender<Result<(), ZmqError>>, // User's reply channel for connect result.
) {
  let connector_core_handle = core_arc.handle;
  let connector_uri_str = format!("inproc://{}", name); // Full URI string for this connection.
  tracing::debug!(connector_core_handle = connector_core_handle, inproc_name = %name, "Attempting inproc connect via event");

  // 1. Lookup binder information (specifically, binder_core_id) in the context registry.
  //    The InprocBinding struct now only contains `binder_core_id`.
  let _binder_info = match core_arc.context.inner().lookup_inproc(&name).await {
    Some(info) => info, // `info` is `InprocBinding { binder_core_id }`.
    None => {
      // Binder not found in the registry.
      let err_msg = format!("Inproc endpoint '{}' not bound or not found", name);
      tracing::warn!(connector_core_handle = connector_core_handle, inproc_name = %name, "{}", err_msg);
      let zmq_err = ZmqError::ConnectionRefused(err_msg.clone());
      // Emit a ConnectFailed monitor event if monitoring is enabled.
      let event = SocketEvent::ConnectFailed {
        endpoint: connector_uri_str.clone(),
        error_msg: err_msg,
      };
      if let Some(monitor) = core_arc.core_state.lock().await.get_monitor_sender_clone() {
        let _ = monitor.send(event).await; // Best effort.
      }
      let _ = reply_tx_user.send(Err(zmq_err)); // Send error back to user.
      return;
    }
  };
  // Note: `binder_info.binder_core_id` is available here if needed for targeted events,
  // but `InprocBindingRequest` is published with `target_inproc_name`, and the binder filters by that.

  // 2. Create the pair of asynchronous channels (pipes) for bi-directional communication.
  let mut connector_core_state = core_arc.core_state.lock().await;
  // Determine HWM for the pipes based on socket options (max of SNDHWM/RCVHWM, min 1).
  let pipe_hwm = connector_core_state
    .options
    .rcvhwm
    .max(connector_core_state.options.sndhwm)
    .max(1);
  // Generate unique IDs for the connector's perspective of the pipes.
  let pipe_id_connector_writes_to_binder = core_arc.context.inner().next_handle();
  let pipe_id_connector_reads_from_binder = pipe_id_connector_writes_to_binder + 1; // Ensure uniqueness.

  // Pipe 1: Connector -> Binder
  let (tx_connector_to_binder, rx_binder_from_connector) = bounded::<Msg>(pipe_hwm);
  // Pipe 2: Binder -> Connector
  let (tx_binder_to_connector, rx_connector_from_binder) = bounded::<Msg>(pipe_hwm);
  drop(connector_core_state);

  // Store connector's ends and spawn reader
  let mut connector_core_state_again = core_arc.core_state.lock().await;
  // Connector sends on this pipe
  connector_core_state_again
    .pipes_tx
    .insert(pipe_id_connector_writes_to_binder, tx_connector_to_binder.clone()); // Store clone

  let connector_context_clone = core_arc.context.clone();
  let connector_command_sender_clone = core_arc.command_sender();
  // Connector receives on this pipe
  let pipe_reader_task_join_handle = tokio::spawn(SocketCore::run_pipe_reader_task(
    connector_context_clone,
    connector_core_handle,
    connector_command_sender_clone,
    pipe_id_connector_reads_from_binder,
    rx_connector_from_binder.clone(), // Pass clone to reader task
  ));
  connector_core_state_again
    .pipe_reader_task_handles
    .insert(pipe_id_connector_reads_from_binder, pipe_reader_task_join_handle);

  // Store EndpointInfo for the connector
  let inproc_endpoint_entry_handle_id = core_arc.context.inner().next_handle();
  let endpoint_info = EndpointInfo {
    mailbox: core_arc.command_sender(),
    task_handle: tokio::spawn(async {}), // Dummy JoinHandle
    endpoint_type: EndpointType::Session,
    endpoint_uri: connector_uri_str.clone(),
    pipe_ids: Some((pipe_id_connector_writes_to_binder, pipe_id_connector_reads_from_binder)),
    handle_id: inproc_endpoint_entry_handle_id,
    target_endpoint_uri: Some(connector_uri_str.clone()),
  };
  connector_core_state_again
    .endpoints
    .insert(connector_uri_str.clone(), endpoint_info);
  let monitor_tx_for_event = connector_core_state_again.get_monitor_sender_clone();
  drop(connector_core_state_again);

  // Publish event with CORRECT ends for the Binder
  let (reply_tx_internal_binder, reply_rx_internal_binder) = oneshot::channel();
  let request_event = SystemEvent::InprocBindingRequest {
    target_inproc_name: name.clone(),
    connector_uri: connector_uri_str.clone(),
    // Give binder the ends it needs to use
    binder_pipe_rx_from_connector: rx_binder_from_connector, // Binder reads on this
    binder_pipe_tx_to_connector: tx_binder_to_connector,     // Binder sends on this
    // Connector IDs remain the same
    connector_pipe_write_id: pipe_id_connector_writes_to_binder,
    connector_pipe_read_id: pipe_id_connector_reads_from_binder,
    reply_tx: OneShotSender::new(reply_tx_internal_binder),
  };

  tracing::debug!(connector_core_handle = connector_core_handle, inproc_name = %name, "Publishing InprocBindingRequest event");
  if core_arc.context.event_bus().publish(request_event).is_err() {
    // Failed to publish the event (e.g., event bus listener issue). This is a fatal error for connect.
    tracing::error!(connector_core_handle = connector_core_handle, inproc_name = %name, "Failed to publish InprocBindingRequest event to EventBus.");
    let err = ZmqError::Internal("Event bus publish failed for inproc connect request".into());
    // Attempt to clean up pipes stored in connector's CoreState.
    let mut cs = core_arc.core_state.lock().await;
    cs.endpoints.remove(&connector_uri_str);
    cs.remove_pipe_state(pipe_id_connector_writes_to_binder, pipe_id_connector_reads_from_binder);
    drop(cs);
    let _ = reply_tx_user.send(Err(err));
    return;
  }

  // 6. Wait for the Binder's reply via the `oneshot::Receiver` from the event.
  match reply_rx_internal_binder.await {
    Ok(Ok(())) => {
      // Connection accepted by the binder.
      tracing::info!(connector_core_handle = connector_core_handle, inproc_name = %name, "Inproc connection established successfully via event bus");

      // Emit "Connected" monitor event.
      if let Some(monitor) = monitor_tx_for_event {
        let peer_addr_synthetic = format!("inproc-binder-for-{}", name); // Synthetic address.
        let event = SocketEvent::Connected {
          endpoint: connector_uri_str.clone(),
          peer_addr: peer_addr_synthetic,
        };
        let _ = monitor.send(event).await; // Best effort.
      }

      // Notify the connector's `ISocket` logic that the pipe is attached.
      if let Some(socket_logic_impl) = core_arc.get_socket_logic().await {
        socket_logic_impl
          .pipe_attached(
            pipe_id_connector_reads_from_binder, // Connector reads from this pipe ID.
            pipe_id_connector_writes_to_binder,  // Connector writes to this pipe ID.
            None,                                // No ZMTP identity for inproc pipes.
          )
          .await;
      } else {
        tracing::error!(
          connector_core_handle = connector_core_handle,
          "Inproc connect: Connector ISocket logic unavailable for pipe_attached notification!"
        );
      }
      // Send success back to the user API call.
      let _ = reply_tx_user.send(Ok(()));
    }
    Ok(Err(e)) => {
      // Connection explicitly rejected by the binder.
      tracing::warn!(connector_core_handle = connector_core_handle, inproc_name = %name, "Inproc connection rejected by binder: {}", e);
      // Cleanup pipes and endpoint info stored in connector's CoreState.
      let mut cs = core_arc.core_state.lock().await;
      cs.endpoints.remove(&connector_uri_str);
      cs.remove_pipe_state(pipe_id_connector_writes_to_binder, pipe_id_connector_reads_from_binder);
      drop(cs);
      // Emit ConnectFailed monitor event.
      if let Some(monitor) = monitor_tx_for_event {
        let event = SocketEvent::ConnectFailed {
          endpoint: connector_uri_str.clone(),
          error_msg: format!("{}", e),
        };
        let _ = monitor.send(event).await;
      }
      let _ = reply_tx_user.send(Err(e)); // Forward binder's error to user.
    }
    Err(_) => {
      // Binder's reply oneshot channel was dropped (e.g., binder task panicked or didn't reply).
      let error_msg = format!("Binder for inproc endpoint '{}' failed to reply or disappeared", name);
      tracing::error!(connector_core_handle = connector_core_handle, inproc_name = %name, "{}", error_msg);
      let zmq_err = ZmqError::Internal(error_msg.clone());
      // Cleanup pipes and endpoint info.
      let mut cs = core_arc.core_state.lock().await;
      cs.endpoints.remove(&connector_uri_str);
      cs.remove_pipe_state(pipe_id_connector_writes_to_binder, pipe_id_connector_reads_from_binder);
      drop(cs);
      // Emit ConnectFailed monitor event.
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

/// Handles unbinding an in-process endpoint.
/// This primarily involves unregistering the name from the `ContextInner`'s inproc registry.
/// Note: This does *not* automatically tear down existing connections to this inproc endpoint.
/// Connected peers must be explicitly closed or disconnected.
pub(crate) async fn unbind_inproc(name: &str, context: &Context) {
  tracing::debug!(inproc_name = %name, "Unbinding inproc endpoint from context registry");
  context.inner().unregister_inproc(name).await;
}

/// Handles disconnecting an in-process endpoint from the connector's side.
/// This involves:
/// 1. Removing the connection state (pipes, endpoint info) from the connector's `SocketCore`.
/// 2. Publishing an `SystemEvent::InprocPipePeerClosed` event to notify the binder `SocketCore`
///    that this connector's side of the pipe is closing, so the binder can clean up its ends.
/// 3. Notifying the connector's `ISocket` logic about the pipe detachment.
///
/// # Arguments
/// * `endpoint_uri` - The full inproc URI (e.g., "inproc://my-service") being disconnected.
/// * `core_arc` - An `Arc` to the `SocketCore` of the connector socket.
pub(crate) async fn disconnect_inproc(
  endpoint_uri: &str,
  core_arc: Arc<SocketCore>, // Connector's SocketCore.
) -> Result<(), ZmqError> {
  let connector_core_handle = core_arc.handle;
  let inproc_name_to_notify = endpoint_uri.strip_prefix("inproc://").unwrap_or("").to_string();
  if inproc_name_to_notify.is_empty() {
    tracing::warn!(connector_core_handle = connector_core_handle, %endpoint_uri, "Invalid inproc URI for disconnect");
    return Err(ZmqError::InvalidEndpoint(endpoint_uri.to_string()));
  }
  tracing::debug!(connector_core_handle = connector_core_handle, %endpoint_uri, "Disconnecting inproc endpoint via event");

  // 1. Find and remove the EndpointInfo for this inproc connection from the connector's state.
  let mut connector_core_state = core_arc.core_state.lock().await;
  let removed_endpoint_info = match connector_core_state.endpoints.remove(endpoint_uri) {
    Some(info) => info,
    None => {
      // Endpoint not found, perhaps already disconnected or never fully connected.
      tracing::warn!(connector_core_handle = connector_core_handle, %endpoint_uri, "Endpoint not found for inproc disconnect (already removed or connect failed?).");
      return Ok(()); // Disconnect is idempotent.
    }
  };

  // 2. Extract pipe IDs from the removed info.
  let (pipe_id_connector_writes, pipe_id_connector_reads) = match removed_endpoint_info.pipe_ids {
    Some(ids) => ids,
    None => {
      // Should not happen if endpoint_info was valid. Restore and error.
      connector_core_state
        .endpoints
        .insert(endpoint_uri.to_string(), removed_endpoint_info);
      drop(connector_core_state);
      tracing::error!(connector_core_handle = connector_core_handle, %endpoint_uri, "Inproc disconnect failed: Missing pipe IDs for stored endpoint info.");
      return Err(ZmqError::Internal(
        "Missing pipe IDs for stored inproc endpoint during disconnect".into(),
      ));
    }
  };

  // 3. Publish an `InprocPipePeerClosed` event to notify the binder.
  //    The event carries the pipe ID from the connector's perspective of *its read pipe*,
  //    as this is the pipe the binder *writes to*.
  let close_notification_event = SystemEvent::InprocPipePeerClosed {
    target_inproc_name: inproc_name_to_notify.clone(), // The logical name of the binder.
    closed_by_connector_pipe_read_id: pipe_id_connector_reads, // Connector's read ID (Binder's write ID).
  };

  tracing::debug!(
    connector_core_handle = connector_core_handle,
    %endpoint_uri,
    target_binder_name = %inproc_name_to_notify,
    connector_read_pipe_id_closed = pipe_id_connector_reads,
    "Publishing InprocPipePeerClosed event to notify binder"
  );
  if core_arc.context.event_bus().publish(close_notification_event).is_err() {
    // If publishing fails, the binder might not clean up its side immediately.
    // However, the connector proceeds with its local cleanup.
    tracing::warn!(connector_core_handle = connector_core_handle, %endpoint_uri, "Failed to publish InprocPipePeerClosed event (binder likely gone or event bus issue)");
  }

  // 4. Clean up local state (pipes, reader task) for the connector.
  let pipes_were_removed = connector_core_state.remove_pipe_state(pipe_id_connector_writes, pipe_id_connector_reads);
  let monitor_tx_for_event = connector_core_state.get_monitor_sender_clone();
  // EndpointInfo for the dummy task_handle associated with inproc is implicitly cleaned by `endpoints.remove`.
  // `removed_endpoint_info.task_handle.abort()` is not strictly necessary if it's a dummy.
  removed_endpoint_info.task_handle.abort(); // Abort the dummy task handle just in case.
  drop(connector_core_state); // Release lock.

  // 5. Notify the connector's `ISocket` logic about the pipe detachment.
  if pipes_were_removed {
    // Only notify if pipes were actually present and removed.
    if let Some(socket_logic_impl) = core_arc.get_socket_logic().await {
      socket_logic_impl.pipe_detached(pipe_id_connector_reads).await; // Notify using connector's read pipe ID.
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

  // 6. Emit a "Disconnected" monitor event.
  println!("DISCONNECTED EVENT");
  if let Some(monitor) = monitor_tx_for_event {
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

  println!("OOPS DISCONNECTED EVENT");
  }
  tracing::info!(connector_core_handle = connector_core_handle, %endpoint_uri, "Inproc connection disconnected successfully");
  Ok(())
}
