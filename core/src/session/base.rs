// src/session/base.rs

use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{mailbox, ActorDropGuard, ActorType, Command, EngineConnectionType, MailboxReceiver, MailboxSender, SystemEvent};
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::{Blob, Msg};

use fibre::mpmc::{AsyncReceiver, AsyncSender};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub(crate) struct SessionBase {
  handle: usize,
  connected_endpoint_uri: String, // The actual URI of the connection (e.g., resolved peer or unique conn URI)
  logical_target_endpoint_uri: String, // The URI the user connect()ed or bind()ed to, for monitoring
  engine_connection: Option<EngineConnectionType>,
  engine_task_handle: Option<JoinHandle<()>>,
  mailbox_receiver: MailboxReceiver,
  rx_from_core: Option<AsyncReceiver<Msg>>,
  tx_to_core: Option<AsyncSender<Msg>>,
  pipe_read_id: Option<usize>,
  pipe_write_id: Option<usize>,
  monitor_tx: Option<MonitorSender>,
  pipe_attached: bool,
  engine_ready: bool,
  context: Context,
  parent_socket_id: usize,
}

impl SessionBase {
  pub(crate) fn create_and_spawn(
    handle: usize,
    connected_endpoint_uri: String,
    logical_target_endpoint_uri: String, // The URI the user specified for connect/bind
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> (MailboxSender, JoinHandle<()>) {

    let capacity = context.inner().get_actor_mailbox_capacity();
    let (tx, rx) = mailbox(capacity);
    let session = SessionBase {
      handle,
      connected_endpoint_uri: connected_endpoint_uri.clone(),
      logical_target_endpoint_uri: logical_target_endpoint_uri.clone(),
      engine_connection: None,
      engine_task_handle: None,
      mailbox_receiver: rx,
      rx_from_core: None,
      tx_to_core: None,
      pipe_read_id: None,
      pipe_write_id: None,
      monitor_tx,
      pipe_attached: false,
      engine_ready: false,
      context: context.clone(),
      parent_socket_id,
    };

    let endpoint_uri_for_log = logical_target_endpoint_uri;

    tracing::debug!(session_handle = handle, uri = %endpoint_uri_for_log, "Spawning SessionBase task");
    // `session` is moved into `run_loop` here.
    let task_handle = tokio::spawn(session.run_loop(parent_socket_id));

    // Log after spawning, using the cloned/argument value.
    tracing::debug!(session_handle = handle, uri = %endpoint_uri_for_log, "SessionBase task spawned. Publishing ActorStarted event.");

    (tx, task_handle)
  }

  async fn send_monitor_event(&self, event: SocketEvent) {
    if let Some(ref tx) = self.monitor_tx {
      if tx.send(event).await.is_err() {
        tracing::warn!(
          session_handle = self.handle,
          uri = %self.connected_endpoint_uri,
          "Monitor channel closed while sending session event (likely dropped by user)"
        );
      }
    }
  }

  async fn try_publish_peer_identity_established_if_ready(
    &mut self,
    session_handle: usize,
    logical_endpoint_uri_for_log_only: &str,
    pending_peer_identity: &mut Option<Blob>,
  ) {
    // Check general readiness flags and if specific data is available (and not yet consumed for publication)
    if self.engine_ready &&
       self.pipe_attached && // Identity is present and not yet taken for publishing
       self.pipe_write_id.is_some()
    {
      // Take the identity, consuming it. This ensures we only publish once for this identity setup.
      let identity_to_publish = pending_peer_identity.take(); // Safe due to .is_some()
      let core_pipe_read_id_for_event = self.pipe_write_id.unwrap(); // Safe due to .is_some()

      let event = SystemEvent::PeerIdentityEstablished {
        parent_core_id: self.parent_socket_id,
        connection_identifier: core_pipe_read_id_for_event, // This is Session's pipe_write_id
        peer_identity: identity_to_publish,                 // This is the Blob itself
      };

      if self.context.event_bus().publish(event).is_err() {
        tracing::warn!(
          handle = session_handle,
          uri = %logical_endpoint_uri_for_log_only,
          "Failed to publish PeerIdentityEstablished event after conditions met. Identity was consumed."
        );
        // Note: If publish fails, pending_peer_identity is now None.
        // Further EngineReady events would be needed to set a new pending_peer_identity.
      } else {
        tracing::trace!(
          handle = session_handle,
          uri = %logical_endpoint_uri_for_log_only,
          "Published PeerIdentityEstablished event successfully. Identity consumed."
        );
      }

      self
        .send_monitor_event(SocketEvent::HandshakeSucceeded {
          endpoint: self.connected_endpoint_uri.to_string(),
        })
        .await;
    }
  }

  async fn run_loop(mut self, parent_socket_id: usize) {
    let session_handle = self.handle;
    let endpoint_uri_for_monitor_event = self.logical_target_endpoint_uri.clone(); // Clone for logging and event publishing.
    let event_bus = self.context.event_bus();
    let mut system_event_rx = event_bus.subscribe();

    tracing::info!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "SessionBase actor started main loop. Initial engine_ready={}, pipe_attached={}", self.engine_ready, self.pipe_attached);


    let mut actor_drop_guard = ActorDropGuard::new(self.context.clone(), session_handle, ActorType::Session, Some(endpoint_uri_for_monitor_event.clone()), Some(parent_socket_id));

    let mut _engine_stopped_cleanly = false; // Renamed to avoid warning if not used extensively
    let mut error_to_report_on_stop: Option<ZmqError> = None;
    let mut received_shutdown_signal = false;
    let mut peer_identity_from_engine: Option<Blob> = None;

    let _loop_result: Result<(), ()> = async {
      loop {
        let should_read_core_pipe = self.pipe_attached && self.engine_ready && self.rx_from_core.is_some();
        let mut core_pipe_receiver_ref = self.rx_from_core.as_mut();
        tracing::debug!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session loop iteration. engine_ready={}, pipe_attached={}, shutdown_signal={}", self.engine_ready, self.pipe_attached, received_shutdown_signal);

        tokio::select! {

          biased;

          event_result = system_event_rx.recv(), if !received_shutdown_signal => {
            match event_result {
              Ok(SystemEvent::ContextTerminating) => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session received ContextTerminating event, stopping engine.");
                if !received_shutdown_signal {
                  self.signal_engine_stop().await;
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                }
              }
              Ok(SystemEvent::SocketClosing{ socket_id }) if socket_id == self.parent_socket_id => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, parent_id = self.parent_socket_id, "Session received SocketClosing for parent, stopping engine.");
                if !received_shutdown_signal {
                  self.signal_engine_stop().await;
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                }
              }
              Ok(_) => { /* Ignore other system events. */ }
              Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, skipped = n, "System event bus lagged for Session!");
                if !received_shutdown_signal {
                  self.signal_engine_stop().await;
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                  error_to_report_on_stop = Some(ZmqError::Internal("Session event bus lagged".into()));
                }
                break;
              }
              Err(broadcast::error::RecvError::Closed) => {
                tracing::error!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "System event bus closed unexpectedly for Session!");
                if !received_shutdown_signal {
                  self.signal_engine_stop().await;
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                  error_to_report_on_stop = Some(ZmqError::Internal("Session event bus closed".into()));
                }
                break;
              }
            }
          }

          // IMPORTANT: We are prioritizing THIS outgoing arm to keep a moving and robust server. THIS SHOULD NEVER BE BELOW THE INCOMING ARM!!!!
          msg_result = async { core_pipe_receiver_ref.as_mut().unwrap().recv().await }, if should_read_core_pipe && !received_shutdown_signal => {
            tracing::debug!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session polled rx_from_core branch.");
            match msg_result {
              Ok(msg) => {
                if let Err(e) = self.send_msg_to_engine(msg).await {
                    tracing::error!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Failed to send message to engine (error: {:?}), stopping session.", e);
                    if !received_shutdown_signal {
                        self.engine_connection = None;
                        if let Some(h) = self.engine_task_handle.take() { h.abort(); } // Abort std engine task
                        received_shutdown_signal = true;
                        if error_to_report_on_stop.is_none() {
                            error_to_report_on_stop = Some(ZmqError::Internal("Session failed to send to engine".into()));
                        }
                    }
                    break;
                }
              }
              Err(_) => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Pipe from SocketCore closed, stopping session.");
                if !received_shutdown_signal {
                  self.signal_engine_stop().await;
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                  if error_to_report_on_stop.is_none() {
                    error_to_report_on_stop = Some(ZmqError::ConnectionClosed);
                  }
                }
                break;
              }
            }
          }

          cmd_result = self.mailbox_receiver.recv() => {
            let command = match cmd_result {
              Ok(cmd) => cmd,
              Err(_) => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session command mailbox closed, initiating shutdown.");
                if !received_shutdown_signal {
                  self.signal_engine_stop().await;
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                  if error_to_report_on_stop.is_none() {
                    error_to_report_on_stop = Some(ZmqError::Internal("Session command mailbox closed by peer".into()));
                  }
                }
                break;
              }
            };

            tracing::trace!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, command = ?command.variant_name(), "Session received command");

            match command {
              Command::Attach { connection, engine_handle: _, engine_task_handle } => {
                tracing::debug!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session received Attach (Engine)");
                if self.engine_connection.is_some() {
                  tracing::warn!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session received Attach but engine already attached! Aborting new engine.");
                  if let Some(h) = engine_task_handle { h.abort(); }
                  continue;
                }
                self.engine_connection = Some(connection);
                if matches!(self.engine_connection, Some(EngineConnectionType::Standard { .. })) {
                  self.engine_task_handle = engine_task_handle;
                } else {
                  self.engine_task_handle = None;
                }
              }
              Command::AttachPipe { rx_from_core, tx_to_core, pipe_read_id, pipe_write_id } => {
                tracing::debug!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, pipe_read_id, pipe_write_id, "Session received AttachPipe");
                if self.pipe_attached {
                  tracing::warn!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session received AttachPipe but pipe already attached!");
                  continue;
                }
                self.rx_from_core = Some(rx_from_core);
                self.tx_to_core = Some(tx_to_core);
                self.pipe_read_id = Some(pipe_read_id);
                self.pipe_write_id = Some(pipe_write_id);
                self.pipe_attached = true;

                self.try_publish_peer_identity_established_if_ready(session_handle, &endpoint_uri_for_monitor_event, &mut peer_identity_from_engine).await;
              }
              Command::Stop => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session received Stop command");
                if !received_shutdown_signal {
                  self.signal_engine_stop().await;
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                }
                println!("THOUSAND YEARS OF WAR");
                break;
              }
              Command::EnginePushCmd { msg } => {
                if self.engine_ready {
                  tracing::trace!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, msg_size = msg.size(), "Session forwarding push from Engine to Core Pipe");
                  if let Some(ref tx) = self.tx_to_core {

                    if tx.send(msg).await.is_err() {
                      tracing::error!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Error sending message to core pipe. Stopping session.");
                      if !received_shutdown_signal {
                        self.signal_engine_stop().await;
                        self.engine_task_handle = None;
                        received_shutdown_signal = true;
                        error_to_report_on_stop = Some(ZmqError::Internal("Session failed to send to core pipe".into()));
                      }
                      break;
                    }
                  } else {
                    tracing::error!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Core pipe sender (tx_to_core) is None when receiving EnginePushCmd! State inconsistency.");
                    if !received_shutdown_signal {
                  self.signal_engine_stop().await;
                      self.engine_task_handle = None;
                      received_shutdown_signal = true;
                      error_to_report_on_stop = Some(ZmqError::Internal("Session missing core pipe sender".into()));
                    }
                    break;
                  }
                } else {
                  tracing::warn!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session received EnginePushCmd before EngineReady, dropping message.");
                }
              }
              Command::EngineReady { peer_identity: received_identity } => {
                tracing::debug!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, peer_id = ?received_identity, "Session received EngineReady");
                self.engine_ready = true;

                peer_identity_from_engine = received_identity.clone();

                self.try_publish_peer_identity_established_if_ready(session_handle, &endpoint_uri_for_monitor_event, &mut peer_identity_from_engine).await;
              }
              Command::EngineStopped => {
                tracing::debug!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session received EngineStopped signal");

                _engine_stopped_cleanly = true;
                self.engine_ready = false;
                self.engine_connection = None;

                if received_shutdown_signal {
                  tracing::info!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Engine stopped cleanly after session shutdown signal. Session will now stop.");
                  break;
                } else {
                  tracing::warn!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Engine stopped unexpectedly. Stopping session.");
                  if error_to_report_on_stop.is_none() {
                    error_to_report_on_stop = Some(ZmqError::Internal("Engine stopped unexpectedly".into()));
                  }
                  received_shutdown_signal = true;
                  break;
                }
              }
              Command::EngineError{ error } => {
                tracing::error!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, error = %error, "Session received EngineError");
                self.engine_ready = false;
                let error_msg_for_monitor = format!("{}", error);
                self.send_monitor_event(SocketEvent::HandshakeFailed {
                  endpoint: self.connected_endpoint_uri.clone(), error_msg: error_msg_for_monitor,
                }).await;
                if error_to_report_on_stop.is_none() {
                  error_to_report_on_stop = Some(error);
                }
                if !received_shutdown_signal {
                  if let Some(engine_handle) = self.engine_task_handle.take() { engine_handle.abort(); }
                  self.engine_connection = None;
                  received_shutdown_signal = true;
                }
                break;
              }
              cmd => tracing::warn!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session received unhandled command via Mailbox: {:?}", cmd.variant_name()),
            }
          }
        } // end select!
      } // end loop
      Ok(())
    }.await;

    tracing::info!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "SessionBase actor stopping, performing cleanup");

    if let Some(engine_handle) = self.engine_task_handle.take() {
      tracing::debug!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Session ensuring Engine task cleanup...");
      match tokio::time::timeout(Duration::from_millis(200), engine_handle).await {
        // Increased timeout
        Ok(Ok(())) => {
          tracing::debug!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Engine task joined cleanly during session cleanup.")
        }
        Ok(Err(e)) => {
          tracing::error!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Error joining engine task (it panicked): {:?}", e);
          if error_to_report_on_stop.is_none() {
            error_to_report_on_stop = Some(ZmqError::Internal(format!("Engine task panicked: {:?}", e)));
          }
        }
        Err(_) => {
          tracing::warn!(handle = session_handle, uri = %endpoint_uri_for_monitor_event, "Timeout joining engine task during session cleanup. Engine might be stuck.");
          if error_to_report_on_stop.is_none() {
            error_to_report_on_stop = Some(ZmqError::Internal("Timeout joining engine task".into()));
          }
        }
      }
    }

    if let Some(err) = error_to_report_on_stop {
      actor_drop_guard.set_error(err);
    } else {
      actor_drop_guard.waive();
    }
    tracing::info!(handle = session_handle, uri = %self.connected_endpoint_uri, "SessionBase actor fully stopped.");
  }

  async fn signal_engine_stop(&mut self) {
    match self.engine_connection.take() {
      // take() clears self.engine_connection
      Some(EngineConnectionType::Standard { engine_mailbox }) => {
        tracing::debug!(handle = self.handle, "Session signaling Stop to Standard Engine.");
        let _ = engine_mailbox.send(Command::Stop).await;
      }
      None => {
        tracing::trace!(
          handle = self.handle,
          "Session signal_engine_stop: No engine connection to signal."
        );
      }
    }
  }

  async fn send_msg_to_engine(&self, msg: Msg) -> Result<(), ZmqError> {
    match &self.engine_connection {
      Some(EngineConnectionType::Standard { engine_mailbox }) => engine_mailbox
        .send(Command::SessionPushCmd { msg })
        .await
        .map_err(|_| ZmqError::Internal("Std Engine mailbox closed for SessionPushCmd".into())),
      None => {
        tracing::warn!(
          handle = self.handle,
          "send_msg_to_engine: No engine connection, dropping message."
        );
        Err(ZmqError::InvalidState("No engine connected".into()))
      }
    }
  }
}
