// src/session/base.rs

use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::ISession;
use crate::socket::events::{MonitorSender, SocketEvent}; // Monitor imports
use crate::{Blob, Msg};

use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration; // Added for timeout join
use tokio::task::JoinHandle;

#[derive(Debug)]
pub(crate) struct SessionBase {
  handle: usize,
  endpoint_uri: String,                       // Store the URI for reporting/events
  socket_mailbox: MailboxSender,              // Mailbox TO SocketCore
  engine_mailbox: Option<MailboxSender>,      // Mailbox TO Engine
  engine_task_handle: Option<JoinHandle<()>>, // Handle TO Engine Task
  mailbox_receiver: MailboxReceiver,          // Mailbox FROM SocketCore/Engine
  rx_from_core: Option<AsyncReceiver<Msg>>,   // READ messages FROM SocketCore
  tx_to_core: Option<AsyncSender<Msg>>,       // WRITE messages TO SocketCore
  pipe_read_id: Option<usize>,                // ID Core uses to write TO session
  pipe_write_id: Option<usize>,               // ID Core uses to read FROM session
  monitor_tx: Option<MonitorSender>,          // Store optional monitor sender
  // --- State Flags ---
  pipe_attached: bool,
  engine_ready: bool, // Set true only after EngineReady received
}

impl SessionBase {
  /// Creates and spawns the SessionBase actor task.
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    socket_mailbox: MailboxSender,
    monitor_tx: Option<MonitorSender>, // Accept monitor sender
  ) -> (MailboxSender, JoinHandle<()>) {
    let (tx, rx) = mailbox();
    let session = SessionBase {
      handle,
      endpoint_uri,
      socket_mailbox,
      engine_mailbox: None,
      engine_task_handle: None,
      mailbox_receiver: rx,
      rx_from_core: None,
      tx_to_core: None,
      pipe_read_id: None,
      pipe_write_id: None,
      monitor_tx, // Store monitor_tx
      pipe_attached: false,
      engine_ready: false,
    };
    let task_handle = tokio::spawn(session.run_loop());
    (tx, task_handle)
  }

  /// Helper to emit monitor events if monitoring is enabled.
  async fn send_monitor_event(&self, event: SocketEvent) {
    if let Some(ref tx) = self.monitor_tx {
      if tx.send(event).await.is_err() {
        tracing::warn!(session_handle = self.handle, uri = %self.endpoint_uri,
                           "Monitor channel closed while sending session event");
      }
    }
  }

  /// Main actor loop for the session.
  async fn run_loop(mut self) {
    let session_handle = self.handle;
    let endpoint_uri_clone = self.endpoint_uri.clone();
    tracing::info!(handle = session_handle, uri=%endpoint_uri_clone, "SessionBase actor started");

    let mut engine_stopped_cleanly = false;
    let mut error_reported_to_core = false;
    let mut peer_identity: Option<Blob> = None;

    loop {
      let should_read_core_pipe = self.pipe_attached && self.engine_ready && self.rx_from_core.is_some();
      let core_pipe_receiver_ref = self.rx_from_core.as_ref();

      tokio::select! {
           biased; // Prioritize commands

           // --- Receive Commands from Core/Engine Mailbox ---
           cmd_result = self.mailbox_receiver.recv() => {
                 let command = match cmd_result {
                     Ok(cmd) => cmd,
                     Err(_) => {
                         tracing::info!(handle = session_handle, uri=%endpoint_uri_clone, "Session mailbox closed, stopping.");
                         if let Some(engine_mb) = self.engine_mailbox.take() {
                             let _ = engine_mb.send(Command::Stop).await;
                         }
                         break; // Exit loop
                     }
                 };

                 tracing::trace!(handle=session_handle, uri=%endpoint_uri_clone, ?command, "Session received command");

                 match command {
                    // Engine attached by Listener/Connecter
                    Command::Attach { engine_mailbox, engine_task_handle: handle_opt, .. } => {
                        tracing::debug!(handle = session_handle, uri=%endpoint_uri_clone, "Session received Attach (Engine)");
                        if self.engine_mailbox.is_some() {
                            tracing::warn!(handle=session_handle, uri=%endpoint_uri_clone, "Session received Attach but engine already attached!");
                            continue;
                        }
                        self.engine_mailbox = Some(engine_mailbox);
                        self.engine_task_handle = handle_opt;
                    }
                    // Pipe attached by SocketCore after ConnSuccess
                    Command::AttachPipe { rx_from_core, tx_to_core, pipe_read_id, pipe_write_id } => {
                        tracing::debug!(handle=session_handle, uri=%endpoint_uri_clone, pipe_read_id, pipe_write_id, "Session received AttachPipe");
                        if self.pipe_attached {
                            tracing::warn!(handle=session_handle, uri=%endpoint_uri_clone, "Session received AttachPipe but pipe already attached!");
                            continue;
                        }
                        self.rx_from_core = Some(rx_from_core);
                        self.tx_to_core = Some(tx_to_core);
                        self.pipe_read_id = Some(pipe_read_id);
                        self.pipe_write_id = Some(pipe_write_id);
                        self.pipe_attached = true;
                    }
                    // Stop command from SocketCore
                    Command::Stop => {
                        tracing::info!(handle = session_handle, uri=%endpoint_uri_clone, "Session received Stop command");
                        if let Some(engine_mb) = self.engine_mailbox.take() {
                            tracing::debug!(handle = session_handle, uri=%endpoint_uri_clone, "Session sending Stop to Engine");
                            let _ = engine_mb.send(Command::Stop).await;
                        }
                        if let Some(engine_handle) = self.engine_task_handle.take() {
                            tracing::debug!(handle = session_handle, uri=%endpoint_uri_clone, "Session awaiting Engine task completion");
                             match engine_handle.await {
                                 Ok(()) => tracing::debug!(handle=session_handle, uri=%endpoint_uri_clone, "Engine task joined successfully"),
                                 Err(e) => tracing::error!(handle=session_handle, uri=%endpoint_uri_clone, "Error joining engine task: {:?}", e),
                             }
                        }
                        break;
                    }
                    // Message FROM Engine -> TO SocketCore Pipe
                    Command::EnginePushCmd { msg } => {
                        if self.engine_ready {
                            tracing::trace!(handle = session_handle, uri=%endpoint_uri_clone, msg_size=msg.size(), "Session forwarding push from Engine to Core Pipe");
                            if let Some(ref tx) = self.tx_to_core {
                                if let Err(e) = tx.send(msg).await {
                                    tracing::error!(handle=session_handle, uri=%endpoint_uri_clone, "Error sending message to core pipe: {:?}. Stopping session.", e);
                                    if let Some(engine_mb) = self.engine_mailbox.take() { let _ = engine_mb.send(Command::Stop).await; }
                                    break;
                                }
                            } else {
                                 tracing::error!(handle=session_handle, uri=%endpoint_uri_clone, "Core pipe sender (tx_to_core) is None when receiving EnginePushCmd!");
                                 if let Some(engine_mb) = self.engine_mailbox.take() { let _ = engine_mb.send(Command::Stop).await; }
                                 break;
                            }
                        } else {
                            tracing::warn!(handle=session_handle, uri=%endpoint_uri_clone, "Session received EnginePushCmd before EngineReady, dropping message.");
                        }
                    }
                    // Engine Status Reports
                    Command::EngineReady { peer_identity: received_identity } => {
                        tracing::debug!(handle = session_handle, uri=%endpoint_uri_clone, peer_id=?received_identity, "Session received EngineReady");
                        self.engine_ready = true;
                        peer_identity = received_identity;

                        // Emit HandshakeSucceeded event
                        self.send_monitor_event(SocketEvent::HandshakeSucceeded {
                            endpoint: endpoint_uri_clone.clone()
                        }).await;
                    }
                    Command::EngineStopped => {
                        tracing::debug!(handle = session_handle, uri=%endpoint_uri_clone, "Session received EngineStopped signal");
                        engine_stopped_cleanly = true;
                        self.engine_ready = false;
                        self.engine_mailbox = None;

                        tracing::info!(handle = session_handle, uri=%endpoint_uri_clone, "Engine stopped cleanly, stopping session.");
                        // Optionally await engine handle briefly if needed, though it should be stopped.
                        if let Some(engine_handle) = self.engine_task_handle.take() {
                          match engine_handle.await {
                            Ok(()) => tracing::debug!(handle=session_handle, uri=%endpoint_uri_clone, "Engine task joined successfully after EngineStopped signal."),
                            Err(e) => tracing::error!(handle=session_handle, uri=%endpoint_uri_clone, "Error joining engine task after EngineStopped signal: {:?}", e),
                          }
                        }
                        break;
                    }
                    Command::EngineError{ error } => {
                        tracing::error!(handle = session_handle, uri=%endpoint_uri_clone, error=%error, "Session received EngineError");
                        self.engine_ready = false;
                        let error_msg = format!("{}", error);

                        // Emit HandshakeFailed event
                        self.send_monitor_event(SocketEvent::HandshakeFailed {
                            endpoint: endpoint_uri_clone.clone(),
                            error_msg,
                        }).await;

                        // Report error up to SocketCore
                        let report_cmd = Command::ReportError {
                            handle: session_handle,
                            endpoint_uri: endpoint_uri_clone.clone(),
                            error,
                        };
                        if self.socket_mailbox.send(report_cmd).await.is_err() {
                            tracing::warn!(handle=session_handle, uri=%endpoint_uri_clone, "Failed to send ReportError to SocketCore (mailbox closed).");
                        }
                        error_reported_to_core = true;

                        if let Some(engine_handle) = self.engine_task_handle.take() { engine_handle.abort(); }
                        self.engine_mailbox = None;
                        break; // Exit loop
                    }
                    // Commands *from* SocketCore should arrive via Pipe read
                    Command::SessionPushCmd { .. } => {
                        tracing::error!(handle=session_handle, uri=%endpoint_uri_clone, "Session received SessionPushCmd via Mailbox - Should arrive via Pipe read!");
                    }
                    // ZAP/Other Commands
                    Command::RequestZapAuth { .. } => {
                        tracing::warn!(handle = session_handle, uri=%endpoint_uri_clone, "ZAP authentication requested but not yet implemented in SessionBase.");
                        // TODO: Implement ZAP client logic here or delegate
                    }
                     Command::ProcessZapReply { .. } => {
                         tracing::warn!(handle = session_handle, uri=%endpoint_uri_clone, "Session received ProcessZapReply but ZAP not implemented.");
                    }
                    _ => tracing::warn!(handle = session_handle, uri=%endpoint_uri_clone, "Session received unhandled command via Mailbox: {:?}", command),
                 } // end match command
           } // end cmd_result = mailbox_receiver.recv() arm


            // --- Receive Messages From SocketCore (via Pipe) ---
             msg_result = async { core_pipe_receiver_ref.unwrap().recv().await }, if should_read_core_pipe => {
               match msg_result {
                  Ok(msg) => {
                    tracing::trace!(handle = session_handle, uri=%endpoint_uri_clone, msg_size=msg.size(), "Session received push from Core Pipe");
                    if self.engine_ready {
                      if let Some(ref engine_mb) = self.engine_mailbox {
                        if let Err(e) = engine_mb.send(Command::SessionPushCmd { msg }).await {
                            tracing::error!(handle=session_handle, uri=%endpoint_uri_clone, "Failed to send msg to engine: {:?}, stopping.", e);
                            let report_cmd = Command::ReportError {
                                handle: session_handle, endpoint_uri: endpoint_uri_clone.clone(),
                                error: ZmqError::Internal(format!("Engine send failed: {:?}", e)),
                            };
                            if self.socket_mailbox.send(report_cmd).await.is_err() { /* Warn */ }
                            error_reported_to_core = true;
                            self.engine_mailbox = None;
                            if let Some(h) = self.engine_task_handle.take() { h.abort(); }
                            break; // Stop session
                        }
                      } else {
                           tracing::warn!(handle=session_handle, uri=%endpoint_uri_clone, "Engine mailbox gone when message received from core pipe, dropping message.");
                      }
                    } else {
                      tracing::warn!(handle=session_handle, uri=%endpoint_uri_clone, "Engine not ready when message received from core pipe, dropping message.");
                    }
                  } // end Ok(msg)
                  Err(_) => { // Assuming RecvError means closed channel
                    tracing::info!(handle=session_handle, uri=%endpoint_uri_clone, "Pipe from SocketCore closed, stopping session.");
                     if let Some(engine_mb) = self.engine_mailbox.take() {
                        let _ = engine_mb.send(Command::Stop).await;
                     }
                    break; // Exit loop
                  }
              } // end match msg_result
           } // end msg_result = core_pipe_receiver.recv() arm

      } // end select!
    } // end loop <--- END loop

    // --- Cleanup ---
    tracing::info!(handle = session_handle, uri=%endpoint_uri_clone, "SessionBase actor stopping");

    // Notify SocketCore that this session has stopped.
    if !self.socket_mailbox.is_closed() {
      // Use SessionStopped command
      let stopped_cmd = Command::SessionStopped {
        handle: session_handle,
        endpoint_uri: endpoint_uri_clone.clone(),
      };
      if let Err(e) = self.socket_mailbox.send(stopped_cmd).await {
        tracing::warn!(handle = session_handle, uri=%endpoint_uri_clone, "Failed to send SessionStopped to SocketCore: {:?}", e);
      } else {
        tracing::debug!(handle=session_handle, uri=%endpoint_uri_clone, "Sent SessionStopped to SocketCore.");
      }
    } else {
      tracing::debug!(handle=session_handle, uri=%endpoint_uri_clone, "Skipping SessionStopped notification as socket core mailbox is closed.");
    }

    // Ensure engine task is joined if handle still exists
    if let Some(engine_handle) = self.engine_task_handle.take() {
      tracing::debug!(handle=session_handle, uri=%endpoint_uri_clone, "Session ensuring Engine task cleanup...");
      match tokio::time::timeout(Duration::from_millis(100), engine_handle).await {
        // Short timeout
        Ok(Ok(())) => {
          tracing::debug!(handle=session_handle, uri=%endpoint_uri_clone, "Engine task joined cleanly during session cleanup.")
        }
        Ok(Err(e)) => {
          tracing::error!(handle=session_handle, uri=%endpoint_uri_clone, "Error joining engine task during session cleanup: {:?}", e)
        }
        Err(_) => {
          tracing::warn!(handle=session_handle, uri=%endpoint_uri_clone, "Timeout joining engine task during session cleanup.")
        }
      }
    }
  } // end run_loop
}

// --- Dummy ISession Trait Implementation ---
#[async_trait]
impl ISession for SessionBase {
  async fn attach_engine(
    &self,
    _engine_mailbox: MailboxSender,
    _engine_task_handle: Option<JoinHandle<()>>,
  ) -> Result<(), ZmqError> {
    unimplemented!("Dummy ISession method called")
  }
  async fn push_msg_to_engine(&self, _msg: crate::message::Msg) -> Result<(), ZmqError> {
    unimplemented!("Dummy ISession method called")
  }
  async fn handle_zap_request(&self, _request_frames: Vec<Vec<u8>>) -> Result<(), ZmqError> {
    unimplemented!("Dummy ISession method called")
  }
  fn engine_ready(&self) {
    unimplemented!("Dummy ISession method called")
  }
  fn engine_error(&self, _error: ZmqError) {
    unimplemented!("Dummy ISession method called")
  }
  fn engine_stopped(&self) {
    unimplemented!("Dummy ISession method called")
  }
}
