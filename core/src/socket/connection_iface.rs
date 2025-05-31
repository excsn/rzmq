// core/src/socket/connection_iface.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::SystemEvent;
use crate::runtime::{command::Command, mailbox::MailboxSender as SessionMailboxSender}; // Mailbox for SessionBase
#[cfg(feature = "io-uring")]
use crate::io_uring_backend::ops::UringOpRequest;
#[cfg(feature = "io-uring")]
use crate::runtime::global_uring_state; // To get worker op_tx
#[cfg(feature = "io-uring")]
use crate::io_uring_backend::one_shot_sender::OneShotSender as WorkerOneShotSender;
use crate::Context; // For UringOpRequest reply_tx
#[cfg(feature = "io-uring")]
use tokio::sync::oneshot as tokio_oneshot; use std::any::Any;
// For creating reply channels
#[cfg(feature = "io-uring")]
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Duration; // For Arc<Msg> in SendDataViaHandler
use async_trait::async_trait;
use std::fmt;

#[async_trait]
pub(crate) trait ISocketConnection: Send + Sync + fmt::Debug {
    /// Sends an application message downstream towards the network.
    async fn send_message(&self, msg: Msg) -> Result<(), ZmqError>;

    /// Initiates a graceful close of this specific connection.
    /// The implementation should ensure underlying resources are eventually released.
    async fn close_connection(&self) -> Result<(), ZmqError>;

    /// Returns a unique identifier for this connection instance.
    /// For session-based connections, this might be the Session's handle ID.
    /// For UringWorker-managed connections, this might be the RawFd.
    /// This ID is primarily for SocketCore to map its internal state.
    fn get_connection_id(&self) -> usize;

    // Optional: Add a method to check if the connection is likely still active,
    // though "active" can be hard to define without actual I/O.
    // async fn is_active(&self) -> bool;

    fn as_any(&self) -> &dyn Any;
}

// --- Dummy Implementation for Listeners/Placeholders ---
#[derive(Debug, Clone)]
pub(crate) struct DummyConnection; // Made pub(crate)

#[async_trait]
impl ISocketConnection for DummyConnection {
    async fn send_message(&self, _msg: Msg) -> Result<(), ZmqError> {
        Err(ZmqError::UnsupportedFeature("DummyConnection cannot send".into()))
    }
    async fn close_connection(&self) -> Result<(), ZmqError> {
        Ok(()) // NOP, as listeners are closed by stopping their actor.
    }
    fn get_connection_id(&self) -> usize {
        // Listeners might not have a "connection ID" in the same sense.
        // Return a sentinel value or make this Option<usize>. For now, 0.
        // This ID is mostly used for sessions/FDs by ISocket patterns.
        0
    }
    fn as_any(&self) -> &dyn Any { self }
}

// --- Implementation for Session-based connections ---

#[derive(Debug, Clone)] // MailboxSender is Clone
pub(crate) struct SessionConnection {
    session_mailbox: SessionMailboxSender, // MailboxSender<Command> to the SessionBase
    connection_id: usize, // Typically the SessionBase actor's handle_id
    // This is the TX end of the pipe that SocketCore writes to, and SessionBase reads from.
    pipe_to_session_tx: async_channel::Sender<Msg>, 
}

impl SessionConnection {
    pub(crate) fn new(
        session_mailbox: SessionMailboxSender,
        connection_id: usize,
        pipe_to_session_tx: async_channel::Sender<Msg>,
    ) -> Self {
        Self { session_mailbox, connection_id, pipe_to_session_tx }
    }
}

#[async_trait]
impl ISocketConnection for SessionConnection {
    async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
        // For session-based, SocketCore sends data messages via the dedicated pipe.
        self.pipe_to_session_tx.send(msg).await.map_err(|e| {
            tracing::warn!(conn_id = self.connection_id, "Failed to send message to Session pipe: {}", e);
            ZmqError::Internal("Session data pipe closed or send error".into())
        })
    }

    async fn close_connection(&self) -> Result<(), ZmqError> {
        // Tell the SessionBase actor to stop.
        // SessionBase, upon stopping, will ensure its engine is stopped.
        if self.session_mailbox.send(Command::Stop).await.is_err() {
            tracing::warn!(conn_id = self.connection_id, "Failed to send Stop command to Session mailbox (already closed?). Connection might not clean up fully via this path.");
            // Return Ok, as the goal is to close, and if mailbox is closed, session is likely gone.
            // Alternatively, could return an error if strict acknowledgement of stop initiation is needed.
        }
        Ok(())
    }

    fn get_connection_id(&self) -> usize {
        self.connection_id
    }

    fn as_any(&self) -> &dyn Any { self }
}

// --- Implementation for UringWorker-managed connections ---

#[cfg(feature = "io-uring")]
#[derive(Debug, Clone)] // KanalSender is Clone, RawFd is Copy
pub(crate) struct UringFdConnection {
    fd: RawFd,
    // Worker op_tx is cloned from global_uring_state when this is created.
    worker_op_tx: kanal::Sender<UringOpRequest>,
}

#[cfg(feature = "io-uring")]
impl UringFdConnection {
    pub(crate) fn new(fd: RawFd) -> Self {
        Self {
            fd,
            worker_op_tx: global_uring_state::get_global_uring_worker_op_tx(),
        }
    }
}

#[cfg(feature = "io-uring")]
#[async_trait]
impl ISocketConnection for UringFdConnection {
    async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
        let (reply_tx, reply_rx) = tokio_oneshot::channel(); // For SendDataViaHandlerAck or error
        let req = UringOpRequest::SendDataViaHandler {
            user_data: 0, // UserData for SendDataViaHandler might not be critical for caller if just ack
            fd: self.fd,
            app_data: Arc::new(msg), // Wrap Msg in Arc for the Any trait object
            reply_tx: WorkerOneShotSender::new(reply_tx),
        };

        self.worker_op_tx.as_async().send(req).await.map_err(|e| {
            tracing::error!(fd = self.fd, "Failed to send SendDataViaHandler request to UringWorker: {}", e);
            ZmqError::Internal(format!("UringWorker op channel error for send: {}", e))
        })?;

        // Wait for acknowledgement from the worker that it accepted the send request.
        // This doesn't mean the data is sent on the wire, just that the worker queued it.
        match tokio::time::timeout(Duration::from_secs(5), reply_rx).await { // Add a timeout
            Ok(Ok(Ok(_completion))) => { // Expected: SendDataViaHandlerAck
                // TODO: Check if _completion is actually SendDataViaHandlerAck if more specific success needed.
                Ok(())
            }
            Ok(Ok(Err(e))) => {
                tracing::warn!(fd = self.fd, "UringWorker reported error for SendDataViaHandler: {}", e);
                Err(e)
            }
            Ok(Err(oneshot_err)) => { // oneshot channel error (e.g., worker panicked)
                tracing::error!(fd = self.fd, "OneShot channel error waiting for SendDataViaHandler ack: {}", oneshot_err);
                Err(ZmqError::Internal("UringWorker reply channel error for send".into()))
            }
            Err(_timeout_elapsed) => {
                tracing::error!(fd = self.fd, "Timeout waiting for SendDataViaHandler ack from UringWorker.");
                Err(ZmqError::Timeout)
            }
        }
    }

    async fn close_connection(&self) -> Result<(), ZmqError> {
        let (reply_tx, reply_rx) = tokio_oneshot::channel();
        let req = UringOpRequest::ShutdownConnectionHandler {
            user_data: 0, // Similarly, UserData might not be critical for caller here
            fd: self.fd,
            reply_tx: WorkerOneShotSender::new(reply_tx),
        };

        self.worker_op_tx.as_async().send(req).await.map_err(|e| {
            tracing::error!(fd = self.fd, "Failed to send ShutdownConnectionHandler request to UringWorker: {}", e);
            ZmqError::Internal(format!("UringWorker op channel error for close: {}", e))
        })?;

        // Wait for acknowledgement from the worker.
        match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
            Ok(Ok(Ok(_completion))) => { // Expected: ShutdownConnectionHandlerComplete
                Ok(())
            }
            Ok(Ok(Err(e))) => {
                tracing::warn!(fd = self.fd, "UringWorker reported error for ShutdownConnectionHandler: {}", e);
                Err(e)
            }
            Ok(Err(oneshot_err)) => {
                tracing::error!(fd = self.fd, "OneShot channel error waiting for ShutdownConnectionHandler ack: {}", oneshot_err);
                 Err(ZmqError::Internal("UringWorker reply channel error for close".into()))
            }
            Err(_timeout_elapsed) => {
                tracing::error!(fd = self.fd, "Timeout waiting for ShutdownConnectionHandler ack from UringWorker.");
                Err(ZmqError::Timeout)
            }
        }
    }

    fn get_connection_id(&self) -> usize {
        self.fd as usize // Use RawFd directly as the ID
    }

    fn as_any(&self) -> &dyn Any { self }
}

/// Represents one side of an in-process connection from the perspective of ISocketConnection.
/// It needs to know how to send to its peer and how to signal closure.
#[derive(Debug)] // Clone might be tricky if it holds unique resources for closure signaling.
                 // Let's make it non-Clone for now and Arc it if needed.
pub(crate) struct InprocConnection {
    // ID of this specific inproc connection instance (e.g., the EndpointInfo.handle_id)
    connection_id: usize,
    // The pipe_write_id this SocketCore uses to send messages to its inproc peer.
    // This is used to look up the actual async_channel::Sender in self.core_state.pipes_tx.
    local_pipe_write_id_to_peer: usize,
    // The pipe_read_id this SocketCore uses to identify messages FROM its inproc peer.
    // This is needed to signal closure to the peer.
    local_pipe_read_id_from_peer: usize,
    // The logical name of the peer this connection is to (e.g., "my-service" if this is a connector).
    // Or the URI of the connector if this is the binder side.
    peer_inproc_name_or_uri: String,
    // Arc to the SocketCore that owns this InprocConnection instance.
    // Needed to access core_state.pipes_tx for sending and context for event bus.
    // Using Weak to avoid cycles if InprocConnection is also stored in an Arc inside SocketCore's EndpointInfo.
    // However, EndpointInfo.connection_iface IS an Arc<dyn ISocketConnection>.
    // Let's use Arc for now and be mindful. If SocketCore holds Arc<InprocConnection> and
    // InprocConnection holds Arc<SocketCore>, that's a cycle.
    // A better approach might be for InprocConnection to hold a clone of the
    // SocketCore's command_sender and context for event bus.
    // Let's try with Context and the local pipe_write_id.
    // For sending, the ISocket pattern logic will get this sender from CoreState.
    // For closing, this connection needs to publish an event.
    context: Context,
    // The specific async_channel::Sender<Msg> for sending data to the peer.
    // This is cloned and stored here when the InprocConnection is created.
    data_tx_to_peer: async_channel::Sender<Msg>,
}

impl InprocConnection {
    pub(crate) fn new(
        connection_id: usize,
        local_pipe_write_id_to_peer: usize,
        local_pipe_read_id_from_peer: usize,
        peer_inproc_name_or_uri: String, // If self is connector, this is "inproc://name". If self is binder, this is connector_uri.
        context: Context,
        data_tx_to_peer: async_channel::Sender<Msg>,
    ) -> Self {
        Self {
            connection_id,
            local_pipe_write_id_to_peer,
            local_pipe_read_id_from_peer,
            peer_inproc_name_or_uri,
            context,
            data_tx_to_peer,
        }
    }
}

#[async_trait]
impl ISocketConnection for InprocConnection {
    async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
        // For inproc, send_message directly uses the data_tx_to_peer channel.
        // The ISocket pattern logic (e.g. RouterSocket) would have already selected this
        // InprocConnection instance and called send_message on it.
        self.data_tx_to_peer.send(msg).await.map_err(|e| {
            tracing::warn!(
                conn_id = self.connection_id,
                peer = %self.peer_inproc_name_or_uri,
                "InprocConnection send_message failed: {}", e
            );
            // If send fails, it means the other side's PipeReaderTask (or the channel itself) is gone.
            ZmqError::ConnectionClosed
        })
    }

    async fn close_connection(&self) -> Result<(), ZmqError> {
        // Closing an inproc connection means:
        // 1. Close our local sender pipe (data_tx_to_peer.close()).
        // 2. Abort our local PipeReaderTask (done by SocketCore when EndpointInfo is removed and remove_pipe_state is called).
        // 3. Notify the peer that we've closed our side via InprocPipePeerClosed event.

        tracing::debug!(
            conn_id = self.connection_id,
            peer = %self.peer_inproc_name_or_uri,
            local_read_pipe_id_being_closed = self.local_pipe_read_id_from_peer,
            "InprocConnection::close_connection called."
        );

        // Close the data sender. The receiver on the other side will get an Err.
        self.data_tx_to_peer.close();

        // Determine the target_inproc_name for the event.
        // If self.peer_inproc_name_or_uri is "inproc://service-name", then "service-name" is the target.
        // If self.peer_inproc_name_or_uri is a connector_uri (when self is the binder),
        // then the target name is embedded in how the binder identified this connection.
        // For simplicity, let's assume peer_inproc_name_or_uri is "inproc://<name>" when called from connector.
        // When called from binder, it needs to know the *logical name* of the service it represents.
        // This detail needs to be passed into InprocConnection or inferred.

        // For now, let's assume this `close_connection` is primarily called by the connector side.
        // The binder side is often cleaned up reactively.
        let target_name_for_event = self.peer_inproc_name_or_uri
            .strip_prefix("inproc://")
            .unwrap_or(&self.peer_inproc_name_or_uri) // Fallback, though might be incorrect for binder
            .to_string();

        let event = SystemEvent::InprocPipePeerClosed {
            target_inproc_name: target_name_for_event, // The logical name of the *other* side's inproc binding.
            // The ID of the pipe that *this closing side reads from*.
            // The peer uses this to identify which of its *write pipes* has a now-closed reader.
            closed_by_connector_pipe_read_id: self.local_pipe_read_id_from_peer,
        };

        if self.context.event_bus().publish(event).is_err() {
            tracing::warn!(
                conn_id = self.connection_id,
                peer = %self.peer_inproc_name_or_uri,
                "Failed to publish InprocPipePeerClosed event."
            );
            // Not a fatal error for the closing side itself.
        }
        Ok(())
    }

    fn get_connection_id(&self) -> usize {
        self.connection_id
    }

    fn as_any(&self) -> &dyn Any { self }
}

/// Placeholder/Minimal ISocketConnection for the Binder's side of an inproc connection.
/// The binder manages its pipes directly via CoreState.
#[derive(Debug, Clone)]
pub(crate) struct InprocBinderSideConnection {
    pub(crate) _connection_id: usize, // Store some identifier, e.g., from EndpointInfo.handle_id
}

#[async_trait]
impl ISocketConnection for InprocBinderSideConnection {
    async fn send_message(&self, _msg: Msg) -> Result<(), ZmqError> {
        // Binder sends via its CoreState.pipes_tx, not this interface for data.
        tracing::warn!("InprocBinderSideConnection::send_message called, should not happen for data path.");
        Err(ZmqError::UnsupportedFeature("send_message not applicable for InprocBinderSideConnection direct use".into()))
    }

    async fn close_connection(&self) -> Result<(), ZmqError> {
        // The actual closing of pipes is handled by SocketCore when the EndpointInfo
        // for this inproc connection is removed (e.g., via InprocPipePeerClosed event
        // or binder shutdown).
        tracing::debug!(conn_id = self._connection_id, "InprocBinderSideConnection::close_connection called (NOP).");
        Ok(())
    }

    fn get_connection_id(&self) -> usize {
        self._connection_id
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
}