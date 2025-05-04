// src/session/base.rs

use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::ISession;
use crate::Msg;

use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use async_trait::async_trait;
use futures::lock::Mutex;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Debug)] // Basic Debug
pub(crate) struct SessionBase {
  handle: usize,
  endpoint_uri: String,          // Store the URI for reporting SessionStopped
  socket_mailbox: MailboxSender, // Mailbox TO SocketCore
  engine_mailbox: Option<MailboxSender>, // Mailbox TO Engine
  engine_task_handle: Option<JoinHandle<()>>, // Handle TO Engine Task
  mailbox_receiver: MailboxReceiver, // Mailbox FROM SocketCore/Engine
  rx_from_core: Option<AsyncReceiver<Msg>>, // READ messages FROM SocketCore
  tx_to_core: Option<AsyncSender<Msg>>,     // WRITE messages TO SocketCore
  // Store IDs received from AttachPipe
  pipe_read_id: Option<usize>,
  pipe_write_id: Option<usize>,
  // --- State Flags ---
  pipe_attached: bool,
  engine_ready: bool,
}

impl SessionBase {
  /// Creates and spawns the SessionBase actor task.
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    // pipe_id: usize, // Pass ID if needed for reporting later
    socket_mailbox: MailboxSender,
  ) -> (MailboxSender, JoinHandle<()>) {
    let (tx, rx) = mailbox();
    let session = SessionBase {
      handle,
      endpoint_uri,
      socket_mailbox,
      engine_mailbox: None, // Engine attached later
      engine_task_handle: None,
      mailbox_receiver: rx,
      rx_from_core: None,
      tx_to_core: None,
      pipe_read_id: None,
      pipe_write_id: None,
      pipe_attached: false, // Pipe not attached yet
      engine_ready: false, // Engine not ready yet
    };
    let task_handle = tokio::spawn(session.run_loop());
    (tx, task_handle) // Return mailbox sender for *this* actor and its task handle
  }

  async fn run_loop(mut self) {
    tracing::info!(
      handle = self.handle,
      "SessionBase actor started (Placeholder)"
    );
    let mut engine_stopped_cleanly = false;
    let mut engine_ready = false; // Track if engine handshake complete

    loop {
      let should_read_core_pipe = self.pipe_attached && self.engine_ready && self.rx_from_core.is_some();
      let core_pipe_receiver = self.rx_from_core.as_ref(); // Get ref if Some
      tokio::select! {
           biased; // Prioritize commands
           cmd_result = self.mailbox_receiver.recv() => {
                 let command = match cmd_result {
                     Ok(cmd) => cmd,
                     Err(_) => {
                         tracing::info!(handle = self.handle, uri=%self.endpoint_uri, "Session mailbox closed, stopping.");
                         // Send Stop to engine if still connected
                         if let Some(engine_mb) = self.engine_mailbox.take() {
                             let _ = engine_mb.send(Command::Stop).await;
                             // Don't await here, just signal
                         }
                         break; // Exit loop
                     }
                 };

                 tracing::trace!(handle=self.handle, ?command, "Session received command");
                 match command {
                  Command::Attach { engine_mailbox, engine_task_handle: handle_opt, .. } => {
                      tracing::debug!(handle = self.handle, "Session received Attach");
                      if self.engine_mailbox.is_some() {
                        tracing::warn!(handle=self.handle, "Session received Attach but engine already attached!");
                        // Optionally stop the old engine? Or ignore?
                        continue;
                      }
                      self.engine_mailbox = Some(engine_mailbox);
                      self.engine_task_handle = handle_opt;
                      // TODO: Initiate ZMTP handshake via engine?
                      // let _ = self.engine_mailbox.as_ref().unwrap().send(Command::StartHandshake?).await;
                  }
                  Command::AttachPipe { rx_from_core, tx_to_core, pipe_read_id, pipe_write_id } => {
                    tracing::debug!(handle=self.handle, pipe_read_id, pipe_write_id, "Session received AttachPipe");
                    if self.pipe_attached {
                        tracing::warn!(handle=self.handle, "Session received AttachPipe but pipe already attached!");
                        continue;
                    }
                    self.rx_from_core = Some(rx_from_core);
                    self.tx_to_core = Some(tx_to_core);
                    self.pipe_read_id = Some(pipe_read_id);
                    self.pipe_write_id = Some(pipe_write_id);
                    self.pipe_attached = true;
                    // TODO: Now maybe initiate engine handshake if engine is attached?
                  }
                  Command::Stop => {
                      tracing::info!(handle = self.handle, "Session received Stop");
                      // 1. Stop Engine
                      if let Some(engine_mb) = self.engine_mailbox.take() {
                          tracing::debug!(handle = self.handle, "Session sending Stop to Engine");
                          let _ = engine_mb.send(Command::Stop).await;
                          // 2. Await Engine completion
                          if let Some(engine_handle) = self.engine_task_handle.take() {
                              tracing::debug!(handle = self.handle, "Session awaiting Engine task completion");
                              // Use spawn_blocking if join could block significantly? Unlikely for well-behaved actors.
                              if let Err(e) = engine_handle.await {
                                  tracing::error!(handle=self.handle, "Error joining engine task: {:?}", e);
                                  // Engine might have panicked
                              } else {
                                    tracing::debug!(handle=self.handle, "Engine task joined successfully");
                                    // Note: EngineStopped command might arrive *before* join completes
                              }
                          }
                      }
                      break; // Exit session loop after stopping engine
                  }
                  // Message FROM Engine -> TO SocketCore
                  Command::EnginePushCmd { msg } => {
                    if engine_ready { // Only forward data if engine handshake is complete
                      tracing::trace!(handle = self.handle, msg_size=msg.size(), "Session forwarding push from Engine to Core");
                      // Write message TO SocketCore via channel
                      if let Err(e) = self.tx_to_core.as_ref().unwrap().send(msg).await {
                          tracing::error!(handle=self.handle, "Error sending message to core: {:?}, stopping session.", e);
                          if let Some(engine_mb) = self.engine_mailbox.take() { let _ = engine_mb.send(Command::Stop).await; }
                          break; // Core pipe closed, stop
                      }
                    } else {
                        tracing::warn!(handle=self.handle, "Session received EnginePushCmd before EngineReady, dropping message.");
                    }
                  }
                  // Engine Status Reports
                  Command::EngineReady => {
                    tracing::debug!(handle = self.handle, "Session received EngineReady");
                    engine_ready = true;
                    // Maybe signal SocketCore that connection is usable? Or ConnSuccess implies this?
                  }
                  Command::EngineStopped => {

                    tracing::debug!(handle = self.handle, "Session received EngineStopped signal");
                    engine_stopped_cleanly = true;
                    engine_ready = false; // Engine stopped, not ready anymore
                    // If session isn't already stopping, maybe break loop?
                    // Or rely on socket core/pipe closure to trigger stop? Let's rely on that for now.
                  }
                  Command::EngineError{ error } => {
                    tracing::error!(handle = self.handle, error=%error, "Session received EngineError");
                    engine_ready = false;
                    let _ = self.socket_mailbox.send(Command::ReportError { handle: self.handle, error }).await;
                    if let Some(engine_handle) = self.engine_task_handle.take() { engine_handle.abort(); }
                    break; // Exit loop
                  }
                  // --- ZAP/Other Commands ---
                  // Command::RequestZapAuth { .. } => { /* TODO: Handle */ }
                  // Command::ProcessZapReply { .. } => { /* TODO: Handle */ }

                  // Commands *from* SocketCore (other than Stop)
                  Command::SessionPushCmd { .. } => {
                        tracing::error!(handle=self.handle, "Session received SessionPushCmd - Should arrive via Pipe read!");
                        // This indicates a logic error if received via mailbox
                  }
                  _ => tracing::warn!(handle = self.handle, "Session received unhandled command: {:?}", command),
               }
           } // end cmd_result = recv() arm

            // --- Receive Messages From SocketCore (via Pipe) ---
             // Need to lock reader to check or read
             // Option 1: Try read non-blockingly inside select! (requires try_read_message)
             // Option 2: Lock outside select and use read_message().await (might block command processing)
             // Option 3: Spawn separate task just for reading pipe? Adds complexity.
             // Let's try Option 1 for now.

             // Try reading from the pipe WITHOUT blocking the command loop indefinitely
             msg_result = async { core_pipe_receiver.unwrap().recv().await }, if should_read_core_pipe => {
               match msg_result {
                  Ok(msg) => {
                    tracing::trace!(handle = self.handle, msg_size=msg.size(), "Session received push from Socket pipe");
                    /// Check engine_mailbox again in case it stopped concurrently
                    if let Some(ref engine_mb) = self.engine_mailbox {
                      // Send SessionPushCmd to Engine
                      if let Err(e) = engine_mb.send(Command::SessionPushCmd { msg }).await {
                        tracing::error!("Failed to send msg to engine: {:?}, stopping.", e);
                        self.engine_mailbox = None; // Engine is gone
                        if let Some(h) = self.engine_task_handle.take() { h.abort(); }
                        break; // Stop session
                      }
                    } else {
                      tracing::warn!(handle=self.handle, "Engine not attached/ready when message received from core, dropping message.");
                      // Should ideally not happen if engine_ready flag is checked
                    }
                  }
                  Err(e) => {
                    // Channel closed by SocketCore
                    tracing::info!(handle=self.handle, "Channel from SocketCore closed, stopping session.");
                    if let Some(engine_mb) = self.engine_mailbox.take() { let _ = engine_mb.send(Command::Stop).await; }
                    break; // Exit loop
                  }
              } // end match msg_result
           } // end msg_result =  recv() arm

      } // end select!
    } // end loop <--- END loop

    // --- Cleanup ---
    tracing::info!(handle = self.handle, uri=%self.endpoint_uri, "SessionBase actor stopping");
    // Only report SessionStopped if we weren't stopped due to an engine error that was already reported
    // And maybe check if socket_mailbox itself is closed?
    if !engine_stopped_cleanly && self.engine_mailbox.is_none() {
      // Infer error if engine_mailbox was taken/aborted
      // Don't send SessionStopped if ReportError was likely sent
      tracing::debug!(
        handle = self.handle,
        "Skipping SessionStopped notification due to likely prior error report."
      );
    } else if self.socket_mailbox.is_closed() {
      tracing::debug!(
        handle = self.handle,
        "Skipping SessionStopped notification as socket core mailbox is closed."
      );
    } else {
      // Send SessionStopped for clean shutdown or unexpected closure
      let stopped_cmd = Command::SessionStopped {
        handle: self.handle,
        endpoint_uri: self.endpoint_uri.clone(),
        // pipe_id: self.pipe_id, // Need to get relevant pipe ID back from Core/EndpointInfo?
      };
      if let Err(e) = self.socket_mailbox.send(stopped_cmd).await {
        tracing::warn!(
          handle = self.handle,
          "Failed to send SessionStopped to SocketCore: {:?}",
          e
        );
      }
    }
  } // end run_loop
}

// Dummy ISession impl needed for type checking potentially later
#[async_trait]
impl ISession for SessionBase {
  async fn attach_engine(
    &self,
    _engine_mailbox: MailboxSender,
    _engine_task_handle: Option<JoinHandle<()>>,
  ) -> Result<(), ZmqError> {
    // Send Attach command to self? No, this is usually called *on* self by transport actor.
    // This trait might be better implemented directly on the MailboxSender<Command> for the session actor.
    // For now, assume this updates internal state (which it can't directly)
    tracing::warn!("Dummy ISession attach_engine called");
    Ok(())
  }
  async fn push_msg_to_engine(&self, _msg: crate::message::Msg) -> Result<(), ZmqError> {
    // Send SessionPushCmd command to self? Or directly to engine if mailbox known?
    tracing::warn!("Dummy ISession push_msg_to_engine called");
    Ok(())
  }
  async fn handle_zap_request(&self, _request_frames: Vec<Vec<u8>>) -> Result<(), ZmqError> {
    tracing::warn!("Dummy ISession handle_zap_request called");
    Ok(())
  }
  fn engine_ready(&self) {
    tracing::warn!("Dummy ISession engine_ready called");
  }
  fn engine_error(&self, error: ZmqError) {
    tracing::warn!("Dummy ISession engine_error called: {}", error);
  }
  fn engine_stopped(&self) {
    tracing::warn!("Dummy ISession engine_stopped called");
  }
}
