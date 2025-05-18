// src/session/base.rs

use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{self, mailbox, ActorType, Command, EventBus, MailboxReceiver, MailboxSender, SystemEvent};
use crate::session::ISession;
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::{Blob, Msg};

use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub(crate) struct SessionBase {
  handle: usize,
  endpoint_uri: String,
  engine_mailbox: Option<MailboxSender>,
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
    endpoint_uri: String,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> (MailboxSender, JoinHandle<()>) {
    let actor_type = ActorType::Session;
    let (tx, rx) = mailbox();
    let session = SessionBase {
      handle,
      // <<< MODIFIED START [Clone endpoint_uri for logging before move] >>>
      endpoint_uri: endpoint_uri.clone(), // Clone here if original endpoint_uri is needed later
      // <<< MODIFIED END >>>
      engine_mailbox: None,
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

    // <<< MODIFIED START [Access session.endpoint_uri before it's moved] >>>
    // If you need endpoint_uri for logging here, use the one passed in or clone from session
    // For the tracing macro, we can use the `endpoint_uri` argument directly,
    // or clone `session.endpoint_uri` before the move if we specifically want the one from the struct.
    // Let's use the `endpoint_uri` argument for clarity here.
    let endpoint_uri_for_log = endpoint_uri; // Use the argument passed to the function

    tracing::debug!(session_handle = handle, uri = %endpoint_uri_for_log, "Spawning SessionBase task");
    // `session` is moved into `run_loop` here.
    let task_handle = tokio::spawn(session.run_loop());

    // Log after spawning, using the cloned/argument value.
    tracing::debug!(session_handle = handle, uri = %endpoint_uri_for_log, "SessionBase task spawned. Publishing ActorStarted event.");
    // <<< MODIFIED END >>>

    context.publish_actor_started(handle, actor_type, Some(parent_socket_id));

    (tx, task_handle)
  }

  async fn send_monitor_event(&self, event: SocketEvent) {
    if let Some(ref tx) = self.monitor_tx {
      if tx.send(event).await.is_err() {
        tracing::warn!(
          session_handle = self.handle,
          uri = %self.endpoint_uri,
          "Monitor channel closed while sending session event (likely dropped by user)"
        );
      }
    }
  }

  async fn run_loop(mut self) {
    let session_handle = self.handle;
    let actor_type = ActorType::Session;
    let endpoint_uri_clone = self.endpoint_uri.clone(); // Clone for logging and event publishing.
    let event_bus = self.context.event_bus();
    let mut system_event_rx = event_bus.subscribe();

    tracing::info!(handle = session_handle, uri = %endpoint_uri_clone, "SessionBase actor started main loop");

    let mut _engine_stopped_cleanly = false; // Renamed to avoid warning if not used extensively
    let mut error_to_report_on_stop: Option<ZmqError> = None;
    let mut received_shutdown_signal = false;
    let mut peer_identity_from_engine: Option<Blob> = None;

    let _loop_result: Result<(), ()> = async {
      loop {
        let should_read_core_pipe = self.pipe_attached && self.engine_ready && self.rx_from_core.is_some();
        let core_pipe_receiver_ref = self.rx_from_core.as_ref();

        tokio::select! {
          biased;

          event_result = system_event_rx.recv(), if !received_shutdown_signal => {
            match event_result {
              Ok(SystemEvent::ContextTerminating) => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_clone, "Session received ContextTerminating event, stopping engine.");
                if !received_shutdown_signal {
                  if let Some(engine_mb) = self.engine_mailbox.take() {
                    let _ = engine_mb.send(Command::Stop).await;
                  }
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                }
              }
              Ok(SystemEvent::SocketClosing{ socket_id }) if socket_id == self.parent_socket_id => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_clone, parent_id = self.parent_socket_id, "Session received SocketClosing for parent, stopping engine.");
                if !received_shutdown_signal {
                  if let Some(engine_mb) = self.engine_mailbox.take() {
                    let _ = engine_mb.send(Command::Stop).await;
                  }
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                }
              }
              Ok(_) => { /* Ignore other system events. */ }
              Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(handle = session_handle, uri = %endpoint_uri_clone, skipped = n, "System event bus lagged for Session!");
                if !received_shutdown_signal {
                  if let Some(engine_mb) = self.engine_mailbox.take() { let _ = engine_mb.send(Command::Stop).await; }
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                  error_to_report_on_stop = Some(ZmqError::Internal("Session event bus lagged".into()));
                }
                break;
              }
              Err(broadcast::error::RecvError::Closed) => {
                tracing::error!(handle = session_handle, uri = %endpoint_uri_clone, "System event bus closed unexpectedly for Session!");
                if !received_shutdown_signal {
                  if let Some(engine_mb) = self.engine_mailbox.take() { let _ = engine_mb.send(Command::Stop).await; }
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                  error_to_report_on_stop = Some(ZmqError::Internal("Session event bus closed".into()));
                }
                break;
              }
            }
          }

          cmd_result = self.mailbox_receiver.recv() => {
            let command = match cmd_result {
              Ok(cmd) => cmd,
              Err(_) => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_clone, "Session command mailbox closed, initiating shutdown.");
                if !received_shutdown_signal {
                  if let Some(engine_mb) = self.engine_mailbox.take() {
                    let _ = engine_mb.send(Command::Stop).await;
                  }
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                  if error_to_report_on_stop.is_none() {
                    error_to_report_on_stop = Some(ZmqError::Internal("Session command mailbox closed by peer".into()));
                  }
                }
                break;
              }
            };

            tracing::trace!(handle = session_handle, uri = %endpoint_uri_clone, command = ?command.variant_name(), "Session received command");

            match command {
              Command::Attach { engine_mailbox, engine_handle: _, engine_task_handle } => {
                tracing::debug!(handle = session_handle, uri = %endpoint_uri_clone, "Session received Attach (Engine)");
                if self.engine_mailbox.is_some() {
                  tracing::warn!(handle = session_handle, uri = %endpoint_uri_clone, "Session received Attach but engine already attached! Aborting new engine.");
                  if let Some(h) = engine_task_handle { h.abort(); }
                  continue;
                }
                self.engine_mailbox = Some(engine_mailbox);
                self.engine_task_handle = engine_task_handle;
              }
              Command::AttachPipe { rx_from_core, tx_to_core, pipe_read_id, pipe_write_id } => {
                tracing::debug!(handle = session_handle, uri = %endpoint_uri_clone, pipe_read_id, pipe_write_id, "Session received AttachPipe");
                if self.pipe_attached {
                  tracing::warn!(handle = session_handle, uri = %endpoint_uri_clone, "Session received AttachPipe but pipe already attached!");
                  continue;
                }
                self.rx_from_core = Some(rx_from_core);
                self.tx_to_core = Some(tx_to_core);
                self.pipe_read_id = Some(pipe_read_id);
                self.pipe_write_id = Some(pipe_write_id);
                self.pipe_attached = true;
              }
              Command::Stop => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_clone, "Session received Stop command");
                if !received_shutdown_signal {
                  if let Some(engine_mb) = self.engine_mailbox.take() {
                    tracing::debug!(handle = session_handle, uri = %endpoint_uri_clone, "Session sending Stop to Engine");
                    let _ = engine_mb.send(Command::Stop).await;
                  }
                  self.engine_task_handle = None;
                  received_shutdown_signal = true;
                }
              }
              Command::EnginePushCmd { msg } => {
                if self.engine_ready {
                  tracing::trace!(handle = session_handle, uri = %endpoint_uri_clone, msg_size = msg.size(), "Session forwarding push from Engine to Core Pipe");
                  if let Some(ref tx) = self.tx_to_core {
                    if tx.send(msg).await.is_err() {
                      tracing::error!(handle = session_handle, uri = %endpoint_uri_clone, "Error sending message to core pipe. Stopping session.");
                      if !received_shutdown_signal {
                        if let Some(engine_mb) = self.engine_mailbox.take() { let _ = engine_mb.send(Command::Stop).await; }
                        self.engine_task_handle = None;
                        received_shutdown_signal = true;
                        error_to_report_on_stop = Some(ZmqError::Internal("Session failed to send to core pipe".into()));
                      }
                      break;
                    }
                  } else {
                    tracing::error!(handle = session_handle, uri = %endpoint_uri_clone, "Core pipe sender (tx_to_core) is None when receiving EnginePushCmd! State inconsistency.");
                    if !received_shutdown_signal {
                      if let Some(engine_mb) = self.engine_mailbox.take() { let _ = engine_mb.send(Command::Stop).await; }
                      self.engine_task_handle = None;
                      received_shutdown_signal = true;
                      error_to_report_on_stop = Some(ZmqError::Internal("Session missing core pipe sender".into()));
                    }
                    break;
                  }
                } else {
                  tracing::warn!(handle = session_handle, uri = %endpoint_uri_clone, "Session received EnginePushCmd before EngineReady, dropping message.");
                }
              }
              Command::EngineReady { peer_identity: received_identity } => {
                tracing::debug!(handle = session_handle, uri = %endpoint_uri_clone, peer_id = ?received_identity, "Session received EngineReady");
                self.engine_ready = true;
                peer_identity_from_engine = received_identity.clone();

                if let Some(core_reads_from_this_pipe_id) = self.pipe_write_id { // Session's write_id is Core's read_id
                  let identity_event = SystemEvent::PeerIdentityEstablished {
                    parent_core_id: self.parent_socket_id,
                    core_pipe_read_id: core_reads_from_this_pipe_id,
                    peer_identity: received_identity, // This is Option<Blob>
                    session_handle_id: session_handle,
                  };
                  if let Err(e) = self.context.event_bus().publish(identity_event) {
                    tracing::warn!(
                      handle = session_handle,
                      uri = %endpoint_uri_clone,
                      "Failed to publish PeerIdentityEstablished event: {}", e
                    );
                  } else {
                     tracing::trace!(handle = session_handle, uri = %endpoint_uri_clone, "Published PeerIdentityEstablished event.");
                  }
                } else {
                  tracing::error!(handle = session_handle, uri = %endpoint_uri_clone, "Cannot publish PeerIdentityEstablished: Session's pipe_write_id is None.");
                }

                self.send_monitor_event(SocketEvent::HandshakeSucceeded { endpoint: endpoint_uri_clone.clone() }).await;
              }
              Command::EngineStopped => {
                tracing::debug!(handle = session_handle, uri = %endpoint_uri_clone, "Session received EngineStopped signal");
                _engine_stopped_cleanly = true;
                self.engine_ready = false;
                self.engine_mailbox = None;
                if received_shutdown_signal {
                  tracing::info!(handle = session_handle, uri = %endpoint_uri_clone, "Engine stopped cleanly after session shutdown signal. Session will now stop.");
                  break;
                } else {
                  tracing::warn!(handle = session_handle, uri = %endpoint_uri_clone, "Engine stopped unexpectedly. Stopping session.");
                  if error_to_report_on_stop.is_none() {
                    error_to_report_on_stop = Some(ZmqError::Internal("Engine stopped unexpectedly".into()));
                  }
                  received_shutdown_signal = true;
                  break;
                }
              }
              Command::EngineError{ error } => {
                tracing::error!(handle = session_handle, uri = %endpoint_uri_clone, error = %error, "Session received EngineError");
                self.engine_ready = false;
                let error_msg_for_monitor = format!("{}", error);
                self.send_monitor_event(SocketEvent::HandshakeFailed {
                  endpoint: endpoint_uri_clone.clone(), error_msg: error_msg_for_monitor,
                }).await;
                if error_to_report_on_stop.is_none() {
                  error_to_report_on_stop = Some(error);
                }
                if !received_shutdown_signal {
                  if let Some(engine_handle) = self.engine_task_handle.take() { engine_handle.abort(); }
                  self.engine_mailbox = None;
                  received_shutdown_signal = true;
                }
                break;
              }
              cmd => tracing::warn!(handle = session_handle, uri = %endpoint_uri_clone, "Session received unhandled command via Mailbox: {:?}", cmd.variant_name()),
            }
          }

          msg_result = async { core_pipe_receiver_ref.unwrap().recv().await }, if should_read_core_pipe && !received_shutdown_signal => {
            match msg_result {
              Ok(msg) => {
                if let Some(ref engine_mb) = self.engine_mailbox {
                  if engine_mb.send(Command::SessionPushCmd { msg }).await.is_err() {
                    tracing::error!(handle = session_handle, uri = %endpoint_uri_clone, "Failed to send message to engine, stopping session.");
                    if !received_shutdown_signal {
                      self.engine_mailbox = None;
                      if let Some(h) = self.engine_task_handle.take() { h.abort(); }
                      received_shutdown_signal = true;
                      if error_to_report_on_stop.is_none() {
                        error_to_report_on_stop = Some(ZmqError::Internal("Session failed to send to engine".into()));
                      }
                    }
                    break;
                  }
                } else {
                  tracing::warn!(handle = session_handle, uri = %endpoint_uri_clone, "Engine mailbox gone when message received from core pipe, dropping message. Initiating shutdown.");
                  if !received_shutdown_signal {
                    self.engine_task_handle = None;
                    received_shutdown_signal = true;
                    if error_to_report_on_stop.is_none() {
                        error_to_report_on_stop = Some(ZmqError::Internal("Engine mailbox missing during core pipe read".into()));
                    }
                  }
                  break;
                }
              }
              Err(_) => {
                tracing::info!(handle = session_handle, uri = %endpoint_uri_clone, "Pipe from SocketCore closed, stopping session.");
                if !received_shutdown_signal {
                  if let Some(engine_mb) = self.engine_mailbox.take() { let _ = engine_mb.send(Command::Stop).await; }
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
        } // end select!
      } // end loop
      Ok(())
    }.await;

    tracing::info!(handle = session_handle, uri = %endpoint_uri_clone, "SessionBase actor stopping, performing cleanup");

    if let Some(engine_handle) = self.engine_task_handle.take() {
      tracing::debug!(handle = session_handle, uri = %endpoint_uri_clone, "Session ensuring Engine task cleanup...");
      match tokio::time::timeout(Duration::from_millis(200), engine_handle).await {
        // Increased timeout
        Ok(Ok(())) => {
          tracing::debug!(handle = session_handle, uri = %endpoint_uri_clone, "Engine task joined cleanly during session cleanup.")
        }
        Ok(Err(e)) => {
          tracing::error!(handle = session_handle, uri = %endpoint_uri_clone, "Error joining engine task (it panicked): {:?}", e);
          if error_to_report_on_stop.is_none() {
            error_to_report_on_stop = Some(ZmqError::Internal(format!("Engine task panicked: {:?}", e)));
          }
        }
        Err(_) => {
          tracing::warn!(handle = session_handle, uri = %endpoint_uri_clone, "Timeout joining engine task during session cleanup. Engine might be stuck.");
          if error_to_report_on_stop.is_none() {
            error_to_report_on_stop = Some(ZmqError::Internal("Timeout joining engine task".into()));
          }
        }
      }
    }

    self.context.publish_actor_stopping(
      session_handle,
      actor_type,
      Some(endpoint_uri_clone),
      error_to_report_on_stop,
    );
    tracing::info!(handle = session_handle, uri = %self.endpoint_uri, "SessionBase actor fully stopped.");
  }
}
