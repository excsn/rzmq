#![allow(dead_code)]

use crate::{error::ZmqError, Blob};
use crate::message::Msg;
use crate::runtime::mailbox::MailboxSender as SessionCommandMailboxSender;
use crate::socket::connection_iface::ISocketConnection;
use super::OneShotSender;

use std::fmt;
#[cfg(feature = "io-uring")]
use std::os::unix::io::RawFd;
use std::sync::Arc;

use tokio::task::Id as TaskId;

/// Type identifier for different actors in the system.
/// Used in ActorStarted and ActorStopping events to categorize actors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorType {
  /// The main actor managing a socket's state and children (e.g., Listeners, Sessions).
  SocketCore,
  /// The command loop actor for a Listener (e.g., TCP or IPC listener).
  Listener,
  /// The task dedicated to accepting new connections for a Listener.
  AcceptLoop,
  /// The actor managing a ZMTP session over an established connection.
  Session,
  /// The actor handling the ZMTP protocol details and I/O for a specific session.
  Engine,
  /// The task dedicated to establishing an outgoing connection (e.g., TCP or IPC connector).
  Connecter,
  /// The task dedicated to reading messages from an inter-actor pipe (e.g., SocketCore reading from Session).
  PipeReader,
  /// The dedicated task within ContextInner managing the WaitGroup via events from the EventBus.
  ContextListener,
}

/// Events broadcast system-wide or within a socket's actor tree via the EventBus.
/// These events are used for coordination and lifecycle management.
#[derive(Clone)]
pub enum SystemEvent {
  /// Indicates the entire context is terminating. All actors should react by shutting down.
  /// Published by `ContextInner::shutdown`.
  ContextTerminating,

  /// Indicates a specific socket (identified by `socket_id`) is closing.
  /// Its child actors (Listeners, Sessions, etc.) should react by shutting down.
  /// Published by `SocketCore` when its shutdown is initiated.
  SocketClosing {
    /// The unique handle ID of the `SocketCore` that is closing.
    socket_id: usize,
  },

  /// Published by the spawner of an actor *after* the actor task is successfully launched.
  /// This event is primarily used by the `ContextListener` to increment the `WaitGroup` count,
  /// ensuring proper tracking of active actors for context termination.
  ActorStarted {
    /// The unique handle ID assigned to the newly started actor task.
    handle_id: usize,
    /// The type of the actor task (e.g., Session, Engine).
    actor_type: ActorType,
    /// The handle ID of the parent actor that spawned this one, if applicable.
    /// `None` for top-level actors like `SocketCore` or `ContextListener`.
    parent_id: Option<usize>,
  },

  /// Published by an actor task itself just before it terminates (either cleanly or due to an error).
  /// This event is primarily used by the `ContextListener` to decrement the `WaitGroup` count.
  ActorStopping {
    /// The unique handle ID of the actor task that is stopping.
    handle_id: usize,
    /// The type of the actor task that is stopping.
    actor_type: ActorType,
    /// Optional URI associated with the actor, e.g., for a Session or Listener.
    endpoint_uri: Option<String>,
    /// Optional error message string if the actor stopped due to an error.
    error: Option<ZmqError>, 
  },

  /// Published by a Listener's accept loop or a Connecter task when a new network
  /// connection is fully established and its associated Session actor is ready.
  /// The parent `SocketCore` (identified by `parent_core_id`) listens for this event.
  NewConnectionEstablished {
    /// The handle ID of the parent `SocketCore` that owns the Listener/Connecter.
    parent_core_id: usize,
    /// The actual network endpoint URI of the established connection (e.g., `tcp://<peer_ip>:<peer_port>`).
    endpoint_uri: String,
    /// The original target endpoint URI requested by the user for outgoing connections.
    /// For listeners, this is usually the same as `endpoint_uri`.
    target_endpoint_uri: String,
    /// The actual interface SocketCore uses to send messages and close the connection.
    connection_iface: Arc<dyn ISocketConnection>,
    /// Describes the management model (Session actor or Uring FD).
    /// SocketCore uses this to know *how* to expect incoming messages.
    interaction_model: ConnectionInteractionModel,
    /// A unique identifier for the spawned Session task (e.g., derived from `JoinHandle::id()`).
    /// Used for tracking if needed, as `JoinHandle` itself is not `Clone`.
    managing_actor_task_id: Option<TaskId>,
  },
  
  /// Published by a `SessionBase` actor after its `ZmtpEngineCore` completes the handshake
  /// and establishes the peer's ZMTP identity.
  /// The parent `SocketCore` listens for this event to update its pattern logic (e.g., ROUTER map).
  PeerIdentityEstablished {
    /// The handle ID of the parent `SocketCore` this session belongs to.
    parent_core_id: usize,
    /// The pipe ID from the `SocketCore`'s perspective (Core's read ID for this session's pipe).
    /// This is the `pipe_write_id` given to the Session in `Command::AttachPipe`.
    connection_identifier: usize,
    /// The ZMTP identity of the peer, if established.
    /// This comes from `ZmtpEngineConfig::routing_id` of the peer, sent in its READY command,
    /// or potentially from a security mechanism.
    peer_identity: Option<Blob>,
  },

  /// Published by a Connecter task when a connection attempt fails definitively
  /// (e.g., after retries or due to a non-recoverable error).
  /// The parent `SocketCore` (identified by `parent_core_id`) listens for this event.
  ConnectionAttemptFailed {
    /// The handle ID of the parent `SocketCore` that owns the Connecter.
    parent_core_id: usize,
    /// The target endpoint URI that the connection attempt was made to.
    target_endpoint_uri: String,
    /// A string representation of the error that caused the connection failure.
    error_msg: String,
  },

  /// Published by an `inproc` connector's `SocketCore` to request a connection
  /// from an `inproc` binder `SocketCore`. The binder listens for events
  /// matching its `target_inproc_name`.
  InprocBindingRequest {
    /// The logical name of the inproc endpoint to connect to (e.g., "my-service").
    target_inproc_name: String,
    /// The URI of the connector socket, for logging or identification purposes.
    connector_uri: String,
    /// The channel sender the Binder uses to send messages TO the Connector.
    binder_pipe_tx_to_connector: async_channel::Sender<Msg>,
    /// The channel receiver the Binder uses to get messages FROM the Connector.
    binder_pipe_rx_from_connector: async_channel::Receiver<Msg>,
    /// The ID the connector uses to write messages to the binder.
    connector_pipe_write_id: usize,
    /// The ID the connector uses to read messages from the binder.
    connector_pipe_read_id: usize,
    /// A oneshot sender for the binder to reply with `Ok(())` if the connection
    /// is accepted, or `Err(ZmqError)` if rejected.
    /// Note: `ZmqError` is used here as `oneshot::Sender` itself doesn't require the payload to be `Clone`.
    reply_tx: OneShotSender,
  },

  /// Published by an `inproc` connector's `SocketCore` when it closes its side
  /// of an established inproc connection. This notifies the binder `SocketCore`
  /// (identified by `target_inproc_name`) to clean up its corresponding pipe ends.
  InprocPipePeerClosed {
    /// The logical name of the inproc binder being notified.
    target_inproc_name: String,
    /// The pipe ID from the perspective of the *closing connector's read pipe*.
    /// The binder uses this to identify which of its write pipes (to the connector)
    /// should be closed and cleaned up.
    closed_by_connector_pipe_read_id: usize,
  },
}

impl fmt::Debug for SystemEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemEvent::ContextTerminating => write!(f, "ContextTerminating"),
            SystemEvent::SocketClosing { socket_id } => f
                .debug_struct("SocketClosing")
                .field("socket_id", socket_id)
                .finish(),
            SystemEvent::ActorStarted { handle_id, actor_type, parent_id } => f
                .debug_struct("ActorStarted")
                .field("handle_id", handle_id)
                .field("actor_type", actor_type)
                .field("parent_id", parent_id)
                .finish(),
            SystemEvent::ActorStopping { handle_id, actor_type, endpoint_uri, error } => f
                .debug_struct("ActorStopping")
                .field("handle_id", handle_id)
                .field("actor_type", actor_type)
                .field("endpoint_uri", endpoint_uri)
                .field("error", error)
                .finish(),
            SystemEvent::NewConnectionEstablished {
                parent_core_id,
                endpoint_uri,
                target_endpoint_uri,
                connection_iface, // Will use ISocketConnection's Debug impl
                interaction_model,
                managing_actor_task_id,
            } => f
                .debug_struct("NewConnectionEstablished")
                .field("parent_core_id", parent_core_id)
                .field("endpoint_uri", endpoint_uri)
                .field("target_endpoint_uri", target_endpoint_uri)
                .field("connection_iface", connection_iface)
                .field("interaction_model", interaction_model)
                .field("managing_actor_task_id", managing_actor_task_id)
                .finish(),
            SystemEvent::PeerIdentityEstablished {
                parent_core_id,
                connection_identifier,
                peer_identity,
            } => f
                .debug_struct("PeerIdentityEstablished")
                .field("parent_core_id", parent_core_id)
                .field("connection_identifier", connection_identifier)
                .field("peer_identity", peer_identity)
                .finish(),
            SystemEvent::ConnectionAttemptFailed { parent_core_id, target_endpoint_uri, error_msg } => f
                .debug_struct("ConnectionAttemptFailed")
                .field("parent_core_id", parent_core_id)
                .field("target_endpoint_uri", target_endpoint_uri)
                .field("error_msg", error_msg)
                .finish(),
            SystemEvent::InprocBindingRequest { /* ... fields ... */ .. } => {
                // Simplified debug for brevity or implement fully
                f.debug_struct("InprocBindingRequest").finish_non_exhaustive()
            }
            SystemEvent::InprocPipePeerClosed { /* ... fields ... */ .. } => {
                f.debug_struct("InprocPipePeerClosed").finish_non_exhaustive()
            }
        }
    }
}

// This enum describes how SocketCore interacts with an established connection.
#[derive(Clone)] // ISocketConnection is Arc'd, RawFd is Copy, MailboxSender is Clone
pub enum ConnectionInteractionModel { // Renamed from ConnectionDetailsForSocketCore for clarity
    /// Connection is managed via a standard SessionBase actor (and its ZmtpEngineCoreStd).
    ViaSessionActor { 
        // SocketCore sends commands (like Stop) to SessionBase via this mailbox.
        // Data messages from SocketCore to SessionBase go via a dedicated pipe,
        // set up by SocketCore with Command::AttachPipe.
        session_actor_mailbox: SessionCommandMailboxSender,
    },
    /// Connection is managed directly by the UringWorker using a RawFd.
    #[cfg(feature = "io-uring")]
    ViaUringFd { 
        fd: RawFd,
        // SocketCore will get the UringWorker's op_tx from Context to send data/commands.
    },
    #[cfg(not(feature = "io-uring"))]
    ViaUringFd { _fd_placeholder: () }, // Ensure struct is valid if feature disabled
}

impl fmt::Debug for ConnectionInteractionModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionInteractionModel::ViaSessionActor { session_actor_mailbox } => f
                .debug_struct("ViaSessionActor")
                .field("session_actor_mailbox_closed", &session_actor_mailbox.is_closed())
                .finish(),
            #[cfg(feature = "io-uring")]
            ConnectionInteractionModel::ViaUringFd { fd } => {
                f.debug_struct("ViaUringFd").field("fd", fd).finish()
            }
            #[cfg(not(feature = "io-uring"))]
            ConnectionInteractionModel::ViaUringFd { _fd_placeholder } => {
                f.debug_struct("ViaUringFd").field("_fd_placeholder", &()).finish()
            }
        }
    }
}