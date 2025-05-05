// src/transport/inproc.rs

#![cfg(feature = "inproc")]

use std::sync::Arc;

use crate::context::Context;
use crate::context::InprocBinding;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{mailbox, Command, MailboxSender}; // Command mailbox
use crate::socket::core::{EndpointInfo, EndpointType, SocketCore}; // Use the real binding info
use async_channel::{bounded, Receiver as AsyncReceiver, Sender as AsyncSender};
use tokio::sync::oneshot;

/// Handles binding an inproc endpoint. Registers with the context.
pub(crate) async fn bind_inproc(
  name: String,              // The unique name (e.g., "my_service")
  core_arc: Arc<SocketCore>, // Need access to binder's info
) -> Result<(), ZmqError> {
  let binding_info = InprocBinding {
    binder_command_mailbox: core_arc.mailbox_sender().clone(),
    // socket_type: core_arc.core_state.lock().await.socket_type, // Optionally store type
  };
  // Register via context's inner state
  core_arc.context.inner().register_inproc(name.clone(), binding_info).await?;

  #[cfg(feature = "inproc")]
  {
    let mut state = core_arc.core_state.lock().await;
    state.bound_inproc_names.insert(name);
  }

  Ok(())
}

/// Handles connecting to an inproc endpoint. Looks up binder and sends request.
pub(crate) async fn connect_inproc(
  name: String,                                    // Name to connect to
  core_arc: Arc<SocketCore>,                       // Connector's core
  reply_tx: oneshot::Sender<Result<(), ZmqError>>, // User's reply channel for connect result
) {
  let connector_handle = core_arc.handle;
  let connector_uri = format!("inproc://{}", name);
  tracing::debug!(handle = connector_handle, name = %name, "Attempting inproc connect");

  // 1. Lookup binder in context registry
  let binder_info = match core_arc.context.inner().lookup_inproc(&name).await {
    Some(info) => info,
    None => {
      tracing::warn!(handle = connector_handle, name = %name, "Inproc lookup failed: Name not bound");
      // Send error back to user who called connect()
      let _ = reply_tx.send(Err(ZmqError::ConnectionRefused(connector_uri))); // Or AddrNotAvailable?
      return;
    }
  };

  // 2. Create the Pipe Channels for direct connection
  // TODO: Use HWM options from connector's core state?
  let mut connector_state = core_arc.core_state.lock().await;
  let hwm = connector_state
    .options
    .rcvhwm
    .max(connector_state.options.sndhwm)
    .max(1000);
  let pipe_id_connector_write = core_arc.context.inner().next_handle(); // Connector -> Binder chan
  let pipe_id_connector_read = pipe_id_connector_write + 1; // Binder -> Connector chan

  let (tx_connector_to_binder, rx_connector_to_binder) = bounded::<Msg>(hwm);
  let (tx_binder_to_connector, rx_binder_to_connector) = bounded::<Msg>(hwm);

  // 3. Store Connector's ends and spawn reader task
  connector_state
    .pipes_tx
    .insert(pipe_id_connector_write, tx_connector_to_binder.clone());
  let reader_task = tokio::spawn(SocketCore::run_pipe_reader_task(
    connector_handle,
    core_arc.mailbox_sender().clone(),
    pipe_id_connector_read,
    rx_binder_to_connector.clone(), // Pass receiver connector reads from
  ));
  connector_state
    .pipe_reader_task_handles
    .insert(pipe_id_connector_read, reader_task);

  // 4. Store connection info in Connector's endpoints map?
  // This is tricky because there's no Session actor mailbox.
  // Maybe store binder's mailbox? Or just the pipe IDs?
  // Let's store pipe IDs for now. EndpointInfo needs adapting or separate map.
  let info = EndpointInfo {
    mailbox: binder_info.binder_command_mailbox.clone(), // Store binder's cmd mailbox
    task_handle: tokio::spawn(async {}),                 // Dummy task handle? No good way to track binder task.
    endpoint_type: EndpointType::Session,                // Treat as Session conceptually?
    endpoint_uri: connector_uri.clone(),
    pipe_ids: Some((pipe_id_connector_write, pipe_id_connector_read)),
  };
  connector_state.endpoints.insert(connector_uri.clone(), info);
  // Release connector state lock
  drop(connector_state);

  // 5. Send connection request command to Binder's mailbox
  let (connect_reply_tx, connect_reply_rx) = oneshot::channel();
  let request_cmd = Command::InprocConnectRequest {
    connector_uri: connector_uri.clone(), // Identify self
    // Pass the ends that the *Binder* will use:
    connector_pipe_tx: tx_binder_to_connector, // Binder WRITES here (Connector READS from rx_binder_to_connector)
    connector_pipe_rx: rx_connector_to_binder, // Binder READS here (Connector WRITES to tx_connector_to_binder)
    connector_pipe_write_id: pipe_id_connector_read, // ID Binder uses to write (Connector reads this ID)
    connector_pipe_read_id: pipe_id_connector_write, // ID Binder uses to read (Connector writes this ID)
    reply_tx: connect_reply_tx,                // Binder replies here to confirm acceptance
  };

  tracing::debug!(handle = connector_handle, name = %name, "Sending InprocConnectRequest to binder");
  if binder_info.binder_command_mailbox.send(request_cmd).await.is_err() {
    tracing::error!(handle = connector_handle, name = %name, "Failed to send InprocConnectRequest (binder likely gone)");
    // Cleanup pipe state we just added
    let _ = reply_tx.send(Err(ZmqError::ConnectionRefused("Binder disappeared".into())));
    // TODO: Call cleanup helper using pipe IDs
    return;
  }

  // 6. Wait for Binder's reply
  match connect_reply_rx.await {
    Ok(Ok(())) => {
      // Connection accepted by binder!
      tracing::info!(handle = connector_handle, name = %name, "Inproc connection established");
      // Notify ISocket logic about pipe attachment
      if let Some(socket_logic_strong) = core_arc.get_socket_logic().await {
        socket_logic_strong
          .pipe_attached(
            pipe_id_connector_read,  // Connector reads this ID (receives from binder)
            pipe_id_connector_write, // Connector writes this ID (sends to binder)
            None,                    // No ZMTP identity
          )
          .await;
      } else {
        tracing::error!(
          handle = connector_handle,
          "Inproc connect: Connector socket logic unavailable!"
        );
        // This is bad, connection established but pattern logic unaware.
        // Maybe send error back via reply_tx? Or try cleanup?
        // For now, proceed but log error. User might get unexpected behavior.
      }
      // Send success back to user
      let _ = reply_tx.send(Ok(()));
    }
    Ok(Err(e)) => {
      // Connection rejected by binder
      tracing::warn!(handle = connector_handle, name = %name, "Inproc connection rejected by binder: {}", e);

      let _ = reply_tx.send(Err(e));
      // Cleanup pipe state added earlier in connector
      let mut connector_state = core_arc.core_state.lock().await;
      connector_state.endpoints.remove(&connector_uri); // Remove endpoint info
      connector_state.remove_pipe_state(pipe_id_connector_write, pipe_id_connector_read);
      // Clean up pipes/reader
    }
    Err(_) => {
      // Binder task died without replying
      tracing::error!(handle = connector_handle, name = %name, "Binder died before replying to InprocConnectRequest");

      let _ = reply_tx.send(Err(ZmqError::Internal("Binder died during inproc connect".into())));
      // Cleanup pipe state added earlier in connector
      let mut connector_state = core_arc.core_state.lock().await;
      connector_state.endpoints.remove(&connector_uri); // Remove endpoint info
      connector_state.remove_pipe_state(pipe_id_connector_write, pipe_id_connector_read);
      // Clean up pipes/reader
    }
  }
}

/// Handles unbinding an inproc endpoint. Unregisters from context.
pub(crate) async fn unbind_inproc(name: &str, context: &Context) {
  context.inner().unregister_inproc(name).await;
}

/// Handles disconnecting an inproc endpoint. Sends notification to binder.
pub(crate) async fn disconnect_inproc(
  endpoint_uri: &str, // e.g., "inproc://my_service"
  core_arc: Arc<SocketCore>,
) -> Result<(), ZmqError> {
  let handle = core_arc.handle; // Get handle for logging
  tracing::debug!(handle = handle, %endpoint_uri, "Disconnecting inproc endpoint");
  let mut state = core_arc.core_state.lock().await;

  // 1. Find and remove the EndpointInfo for this connection
  let removed_info = match state.endpoints.remove(endpoint_uri) {
    Some(info) => info,
    None => {
      
      tracing::debug!(handle = handle, %endpoint_uri, "Endpoint not found for inproc disconnect (already removed?)");
      return Ok(());
    }
  };

  // 2. Check if it was a valid inproc session with pipe IDs
  let (write_pipe_id, read_pipe_id) = match removed_info.pipe_ids {
    Some(ids) => ids,
    None => {
      state.endpoints.insert(endpoint_uri.to_string(), removed_info); // Put it back
      return Err(ZmqError::Internal("Missing pipe IDs for stored inproc endpoint".into()));
    }
  };

  // 3. Send close notification to the Binder
  // The command needs the ID the *binder* uses to read from *us* (the connector).
  // This corresponds to the connector's *write* pipe ID.
  let close_cmd = Command::InprocPipeClosed {
    pipe_read_id: write_pipe_id, // Correct: Binder reads from connector's write pipe
  };

  // Send to binder's command mailbox stored in EndpointInfo
  tracing::debug!(handle = handle, %endpoint_uri, "Sending InprocPipeClosed to binder");
  if removed_info.mailbox.send(close_cmd).await.is_err() {
    tracing::debug!(handle = handle, %endpoint_uri, "Binder mailbox closed while sending InprocPipeClosed (binder likely already gone)");
  }

  // 4. Clean up local state (pipes, reader task) for the connector
  let pipes_removed = state.remove_pipe_state(write_pipe_id, read_pipe_id);
  drop(state); // Release lock

  // 5. Notify the connector's ISocket logic
  if pipes_removed {
    if let Some(socket_logic) = core_arc.get_socket_logic().await {
      socket_logic.pipe_detached(read_pipe_id).await; // Notify using connector's read pipe ID
      tracing::debug!(handle = handle, %endpoint_uri, "Notified ISocket of inproc pipe detachment");
    } else {
      tracing::error!(
        handle = handle,
        "Inproc disconnect: Socket logic unavailable for detach notify"
      );
    }
  } else {
    tracing::warn!(handle = handle, %endpoint_uri, "Inproc disconnect: Local pipe state was already removed?");
  }

  Ok(())
}
