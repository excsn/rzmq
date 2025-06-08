// core/src/sessionx/actor.rs

#![allow(dead_code, unused_variables, unused_mut)]

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{ActorDropGuard, ActorType, Command, SystemEvent};
use crate::socket::options::ZmtpEngineConfig;
use crate::socket::ISocket;
use crate::throttle::{AdaptiveThrottle, types::AdaptiveThrottleConfig};
use crate::transport::ZmtpStdStream;
use crate::{Blob, MailboxReceiver};

use futures::FutureExt;
use std::fmt::Debug;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd; // For AsRawFd bound if S needs it for cork_info init
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::{yield_now, JoinHandle};
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};

use super::pipe_manager::CorePipeManagerX;
use super::protocol_handler::ZmtpProtocolHandlerX;
use super::states::ActorConfigX;
use super::types::{ConnectionPhaseX, ZmtpHandshakeProgressX};

// SessionConnectionActorX might not need to be Debug itself if not printed directly.
// If it is, S would need to be Debug. Let's assume for now it doesn't need direct Debug.
pub(crate) struct SessionConnectionActorX<S: ZmtpStdStream> {
  handle: usize,
  current_phase: ConnectionPhaseX,
  parent_socket_id: usize,
  actor_config: ActorConfigX,

  zmtp_handler: ZmtpProtocolHandlerX<S>,
  core_pipe_manager: CorePipeManagerX,

  command_mailbox_receiver: MailboxReceiver,
  system_event_receiver: broadcast::Receiver<SystemEvent>,

  pending_peer_identity_from_handshake: Option<Blob>,
  error_for_drop_guard: Option<ZmqError>,

  ping_check_timer: Option<tokio::time::Interval>,

  handshake_deadline: Option<TokioInstant>,
  socket_logic: Arc<dyn ISocket>,
}

// Add AsRawFd bound here if ZmtpProtocolHandlerX::new needs it for cork setup.
// This depends on how try_create_cork_info and ZPHX::new are finalized for S.
// If ZPHX::new takes Option<RawFd>, then SCA doesn't need this bound directly for that.
// However, if ZPHX::new takes &S and calls as_raw_fd(), then SCA::new needs S: AsRawFd.
// Let's assume for now that ZPHX::new is passed what it needs regarding FD for cork.
impl<S> SessionConnectionActorX<S>
where
  S: ZmtpStdStream + Debug + Unpin + Send + 'static, // Keep Send + 'static for tokio::spawn
{
  pub(crate) fn create_and_spawn(
    handle: usize,
    parent_socket_id: usize,
    stream: S, // Consumes the stream
    actor_config: ActorConfigX,
    engine_config: Arc<ZmtpEngineConfig>,
    command_mailbox_receiver: MailboxReceiver,
    socket_logic: Arc<dyn ISocket>,
  ) -> JoinHandle<()> {
    // Returns JoinHandle for the SCA task

    let zmtp_handler = ZmtpProtocolHandlerX::new(
      stream,
      engine_config.clone(), // ZmtpProtocolHandlerX clones Arc
      actor_config.is_server_role,
      handle, // Pass actor handle for logging within ZmtpProtocolHandlerX
    );

    let mut ping_check_timer = None;
    if let Some(ivl) = engine_config.heartbeat_ivl {
      if !ivl.is_zero() {
        // Start timer such that the first tick is after `ivl` from now.
        let mut timer = tokio::time::interval_at(TokioInstant::now() + ivl, ivl);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ping_check_timer = Some(timer);
      }
    }

    // Overall handshake timeout
    let handshake_deadline = engine_config
      .handshake_timeout
      .map(|d| TokioInstant::now() + d);

    let system_event_receiver = actor_config.context.event_bus().subscribe();

    let actor = Self {
      handle,
      current_phase: ConnectionPhaseX::HandshakeInProgress, // Start handshake immediately
      parent_socket_id,
      actor_config,
      zmtp_handler,
      core_pipe_manager: CorePipeManagerX::new(),
      command_mailbox_receiver,
      system_event_receiver,
      pending_peer_identity_from_handshake: None,
      error_for_drop_guard: None,
      ping_check_timer,
      handshake_deadline,
      socket_logic,
    };

    let task_handle = tokio::spawn(actor.run_loop());
    task_handle
  }

  async fn run_loop(mut self) {
    tracing::info!(
      sca_handle = self.handle,
      uri = %self.actor_config.connected_endpoint_uri,
      role = if self.actor_config.is_server_role { "Server" } else { "Client" },
      "SessionConnectionActorX started."
    );

    let mut actor_drop_guard = ActorDropGuard::new(
      self.actor_config.context.clone(),
      self.handle,
      ActorType::Session, // Consider ActorType::SessionConnectionActorX
      Some(self.actor_config.connected_endpoint_uri.clone()),
      Some(self.parent_socket_id),
    );

    let adaptive_throttle = {
      let mut config = AdaptiveThrottleConfig::default();
      config.credit_per_message = 5;
      config.healthy_balance_width = 1024000;
      config.max_imbalance = 65536;
      config.yield_after_n_consecutive = 256;
      // config.curve_factor = 2.0;
      AdaptiveThrottle::new(config)
    };

    // --- Main Loop ---
    while self.current_phase != ConnectionPhaseX::Terminating {
      let mut pong_timeout_future = futures::future::pending().left_future();
      if self.current_phase == ConnectionPhaseX::Operational
        && self.zmtp_handler.heartbeat_state.waiting_for_pong
      {
        if let Some(deadline_std) = self.zmtp_handler.heartbeat_state.get_pong_deadline() {
          // Convert std::time::Instant to tokio::time::Instant
          let deadline_tokio = TokioInstant::from_std(deadline_std);
          pong_timeout_future = tokio::time::sleep_until(deadline_tokio).right_future();
        }
      }

      let mut overall_handshake_timeout_future = futures::future::pending().left_future();
      if self.current_phase == ConnectionPhaseX::HandshakeInProgress {
        if let Some(deadline) = self.handshake_deadline {
          overall_handshake_timeout_future = tokio::time::sleep_until(deadline).right_future();
        }
      }

      tokio::select! {
        biased;

        // --- ARM 1: Control Commands (AttachPipes, Stop) ---
        maybe_cmd = self.command_mailbox_receiver.recv(), if !matches!(self.current_phase, ConnectionPhaseX::Terminating) => {
          match maybe_cmd {
            Ok(command) => self.process_command(command).await,
            Err(_) => {
              tracing::info!(sca_handle = self.handle, "SCA Command mailbox closed.");
              self.transition_to_shutdown_stream(None).await;
            }
          }
        }

        // --- ARM 2: System Events (ContextTerminating, SocketClosing for parent) ---
        maybe_event = self.system_event_receiver.recv(), if !matches!(self.current_phase, ConnectionPhaseX::Terminating) => {
          match maybe_event {
            Ok(event) => self.process_system_event(event).await,
            Err(broadcast::error::RecvError::Lagged(n)) => {
              self.set_fatal_error(ZmqError::Internal(format!("System event lagged by {}", n))).await;
            }
            Err(broadcast::error::RecvError::Closed) => {
              self.set_fatal_error(ZmqError::Internal("System event channel closed".into())).await;
            }
          }
        }

        // --- ARM 3: Overall Handshake Timeout ---
        _ = overall_handshake_timeout_future, if self.current_phase == ConnectionPhaseX::HandshakeInProgress && self.handshake_deadline.is_some() => {
            tracing::warn!(sca_handle = self.handle, uri = %self.actor_config.connected_endpoint_uri, "Overall ZMTP handshake timed out.");
            self.set_fatal_error(ZmqError::Timeout).await; // Specific handshake timeout error if desired
        }

        // --- ARM 4: ZMTP Handshake Progression ---
        _ = async {}, if self.current_phase == ConnectionPhaseX::HandshakeInProgress => {
          self.handle_handshake_progression().await;
        }

        // --- ARM 5: Ping Check Timer ---
        _ = async { self.ping_check_timer.as_mut().map_or(futures::future::pending().left_future(), |t| t.tick().right_future()).await },
            if self.current_phase == ConnectionPhaseX::Operational && self.ping_check_timer.is_some() => {
            if let Err(e) = self.zmtp_handler.maybe_send_ping().await {
                self.set_fatal_error(e).await;
            }
        }

        // --- ARM 6: Pong Timeout ---
        _ = pong_timeout_future,
          if self.current_phase == ConnectionPhaseX::Operational && self.zmtp_handler.heartbeat_state.waiting_for_pong => {
          if self.zmtp_handler.has_pong_timed_out() {
            self.set_fatal_error(ZmqError::Timeout).await;
          }
        }

        // --- ARM 6: Incoming Data (from Network to SocketCore) ---
        maybe_parsed_frame = async { self.zmtp_handler.read_and_parse_data_frame().await },
          if self.current_phase == ConnectionPhaseX::Operational && self.zmtp_handler.stream.is_some() => {
          match maybe_parsed_frame {
            Ok(Some(msg)) => {

              let throttle_guard = adaptive_throttle.begin_work(crate::throttle::Direction::Ingress);

              self.handle_incoming_from_network(msg).await;
              
              if throttle_guard.should_throttle() {
                yield_now().await;
              }

            },
            Ok(None) => { /* Not enough data yet */ }
            Err(ZmqError::ConnectionClosed) => {
              tracing::info!(sca_handle = self.handle, "Peer closed connection (data phase).");
              self.transition_to_shutdown_stream(Some(ZmqError::ConnectionClosed)).await;
            }
            Err(e) => self.set_fatal_error(e).await,
          }
        }

        // --- ARM 5: Outgoing Data (from SocketCore to Network) ---
        maybe_msgs_from_core = async { self.core_pipe_manager.recv_from_core().await },
          if self.current_phase == ConnectionPhaseX::Operational && self.core_pipe_manager.is_attached() => {
          match maybe_msgs_from_core {
            Ok(msgs) => {
              let throttle_guard = adaptive_throttle.begin_work(crate::throttle::Direction::Egress);

              if let Err(e) = self.zmtp_handler.write_data_msgs(msgs).await {
                self.set_fatal_error(e).await;
              }

              if throttle_guard.should_throttle() {
                yield_now().await;
              }
            },
            Err(_) => {
              tracing::info!(sca_handle = self.handle, "Pipe from SocketCore closed.");
              self.transition_to_shutdown_stream(Some(ZmqError::ConnectionClosed)).await; // Or Internal error
            }
          }
        }

        // --- ARM 9: Shutdown Step ---
         _ = async {}, if self.current_phase == ConnectionPhaseX::ShuttingDownStream => {
            self.perform_graceful_shutdown().await;
        }
      }
    }

    tracing::info!(
      sca_handle = self.handle,
      "SCA loop finished. Final phase: {:?}.",
      self.current_phase
    );
    
    // Post-loop cleanup (already initiated if error/stop occurred, this is a final catch-all)
    if self.current_phase != ConnectionPhaseX::Terminating {
      // If loop exited for other reasons
      self.perform_final_cleanup_actions().await;
    }

    if let Some(err) = self.error_for_drop_guard.take() {
      actor_drop_guard.set_error(err);
    } else {
      actor_drop_guard.waive();
    }
    tracing::info!(
      sca_handle = self.handle,
      "SCA {} fully stopped.",
      self.actor_config.connected_endpoint_uri
    );
  }

  // --- Helper Methods for select! Arms & State Transitions ---

  async fn process_command(&mut self, command: Command) {
    tracing::debug!(
      sca_handle = self.handle,
      "Processing ScaCommand: {:?}",
      command
    );
    match command {
      Command::ScaInitializePipes {
        sca_handle_id, // Used to verify command is for this actor instance
        rx_from_core,
        core_pipe_read_id_for_incoming_routing,
      } => {
        if self.handle != sca_handle_id {
          tracing::warn!(
            sca_handle = self.handle,
            expected_id = sca_handle_id,
            "Received ScaInitializePipes for wrong actor. Ignoring."
          );
          // Important: Drop rx_from_core to prevent resource leak if it's not used
          drop(rx_from_core);
          return;
        }
        if self.core_pipe_manager.is_attached() {
          tracing::warn!(
            sca_handle = self.handle,
            "Already attached. Ignoring ScaInitializePipes."
          );
          drop(rx_from_core); // Still drop the provided channels
          return;
        }
        
        self
          .core_pipe_manager
          .attach(rx_from_core, core_pipe_read_id_for_incoming_routing);
        tracing::info!(
          sca_handle = self.handle,
          core_read_id = core_pipe_read_id_for_incoming_routing,
          "Core pipes attached via ScaInitializePipes."
        );

        if self.current_phase == ConnectionPhaseX::WaitingForPipes {
          self.check_and_transition_to_operational().await;
        } else if self.current_phase == ConnectionPhaseX::HandshakeInProgress
          && self.zmtp_handler.is_handshake_complete()
        {
          self.current_phase = ConnectionPhaseX::WaitingForPipes;
          self.check_and_transition_to_operational().await;
        }
      }
      Command::Stop => {
        tracing::info!(sca_handle = self.handle, "Received Stop command.");
        self.transition_to_shutdown_stream(None).await;
      }
      unexpected_cmd => {
        tracing::warn!(
          sca_handle = self.handle,
          "Received unexpected Command variant: {:?}",
          unexpected_cmd.variant_name()
        );
      }
    }
  }

  async fn process_system_event(&mut self, event: SystemEvent) {
    match event {
      SystemEvent::ContextTerminating => {
        tracing::info!(sca_handle = self.handle, "Received ContextTerminating.");
        self
          .transition_to_shutdown_stream(Some(ZmqError::Internal("Context terminating".into())))
          .await;
      }
      SystemEvent::SocketClosing { socket_id } if socket_id == self.parent_socket_id => {
        tracing::info!(
          sca_handle = self.handle,
          "Parent SocketCore ({}) closing.",
          socket_id
        );
        self
          .transition_to_shutdown_stream(Some(ZmqError::Internal("Parent socket closing".into())))
          .await;
      }
      _ => {}
    }
  }

  async fn handle_handshake_progression(&mut self) {
    if self.current_phase != ConnectionPhaseX::HandshakeInProgress {
      return;
    }

    match self.zmtp_handler.advance_handshake().await {
      Ok(ZmtpHandshakeProgressX::InProgress) => {}
      Ok(ZmtpHandshakeProgressX::IdentityReady(blob)) => {
        self.pending_peer_identity_from_handshake = Some(blob);
        // Continue handshake if not yet complete overall
        if !self.zmtp_handler.is_handshake_complete() {
          return;
        }
        // If complete, fall through to HandshakeComplete logic
      }
      Ok(ZmtpHandshakeProgressX::HandshakeComplete) => {
        // If IdentityReady was also emitted in the same step, pending_peer_identity_from_handshake is set.
      }
      Ok(ZmtpHandshakeProgressX::ProtocolError(s)) => {
        self.set_fatal_error(ZmqError::ProtocolViolation(s)).await;
        return; // Error handled, no further progression this tick
      }
      Ok(ZmtpHandshakeProgressX::FatalError(e)) | Err(e) => {
        self.set_fatal_error(e).await;
        return;
      }
    }

    // Check if overall handshake is now complete
    if self.zmtp_handler.is_handshake_complete() {
      tracing::info!(
        sca_handle = self.handle,
        "Handshake complete via ZmtpProtocolHandlerX."
      );
      self.current_phase = ConnectionPhaseX::WaitingForPipes;
      self.handshake_deadline = None; // Handshake done, clear deadline
      self.check_and_transition_to_operational().await;
    }
  }

  async fn check_and_transition_to_operational(&mut self) {
    if self.current_phase == ConnectionPhaseX::WaitingForPipes &&
       self.zmtp_handler.is_handshake_complete() && // Should always be true if in WaitingForPipes
       self.core_pipe_manager.is_attached()
    {
      tracing::info!(
        sca_handle = self.handle,
        "Transitioning to Operational phase."
      );
      self.current_phase = ConnectionPhaseX::Operational;

      // Determine final peer identity: one from handshake (security or READY) takes precedence.
      let final_peer_identity = self.pending_peer_identity_from_handshake.take();

      if let Some(routing_id) = self
        .core_pipe_manager
        .state
        .core_pipe_read_id_for_incoming_routing
      {
        let event = SystemEvent::PeerIdentityEstablished {
          parent_core_id: self.parent_socket_id,
          connection_identifier: routing_id,
          peer_identity: final_peer_identity,
        };
        if self
          .actor_config
          .context
          .event_bus()
          .publish(event)
          .is_err()
        {
          tracing::warn!(
            sca_handle = self.handle,
            "Failed to publish PeerIdentityEstablished event."
          );
        } else {
          tracing::debug!(
            sca_handle = self.handle,
            "Published PeerIdentityEstablished event."
          );
        }
      } else {
        tracing::error!(
          sca_handle = self.handle,
          "CRITICAL: Cannot publish PeerIdentityEstablished: core_pipe_read_id not set after pipes attached."
        );
        // This is a logic error if pipes are attached but no routing ID.
        self
          .set_fatal_error(ZmqError::Internal(
            "Pipe routing ID missing post-attach".into(),
          ))
          .await;
        return;
      }

      if let Some(monitor) = &self.actor_config.monitor_tx {
        let _ = monitor.try_send(crate::socket::SocketEvent::HandshakeSucceeded {
          endpoint: self.actor_config.connected_endpoint_uri.clone(),
        });
      }
    }
  }

  async fn handle_incoming_from_network(&mut self, msg: Msg) {
    // Get the pipe_id needed for ISocket::handle_pipe_event
    let pipe_read_id = self
      .core_pipe_manager
      .state
      .core_pipe_read_id_for_incoming_routing
      .expect("Cannot handle incoming message without core_pipe_read_id");

    if msg.is_command() {
      // Process PING/PONG
      match self.zmtp_handler.process_incoming_data_command_frame(&msg) {
        Ok(Some(pong_reply)) => {
          if let Err(e) = self.zmtp_handler.write_data_msg(pong_reply, true).await {
            self.set_fatal_error(e).await;
          }
        }
        Ok(None) => { /* PONG received and handled */ }
        Err(e) => self.set_fatal_error(e).await,
      }
    } else {
      // Data message. Send DIRECTLY to ISocket pattern logic.
      let command_for_isocket = Command::PipeMessageReceived {
        pipe_id: pipe_read_id,
        msg,
      };
      if let Err(e) = self
        .socket_logic
        .handle_pipe_event(pipe_read_id, command_for_isocket)
        .await
      {
        tracing::error!(sca_handle = self.handle, pipe_id = pipe_read_id, error = %e, "Fatal error from ISocket::handle_pipe_event. Shutting down connection.");
        self.set_fatal_error(e).await;
      }
    }
  }

  async fn set_fatal_error(&mut self, error: ZmqError) {
    tracing::error!(sca_handle = self.handle, uri = %self.actor_config.connected_endpoint_uri, err = %error, "SCA fatal error.");
    if self.error_for_drop_guard.is_none() {
      self.error_for_drop_guard = Some(error);
    }
    self.transition_to_shutdown_stream(None).await; // Error is already set
  }

  async fn transition_to_shutdown_stream(&mut self, accompanying_error: Option<ZmqError>) {
    if self.current_phase != ConnectionPhaseX::ShuttingDownStream
      && self.current_phase != ConnectionPhaseX::Terminating
    {
      tracing::info!(
        sca_handle = self.handle,
        "Transitioning to ShuttingDownStream phase."
      );
      if let Some(err) = accompanying_error {
        if self.error_for_drop_guard.is_none() {
          self.error_for_drop_guard = Some(err);
        }
      }
      self.current_phase = ConnectionPhaseX::ShuttingDownStream;
    }
  }

  async fn perform_graceful_shutdown(&mut self) {
    // This method is called when self.current_phase is ShuttingDownStream
    tracing::info!(
      sca_handle = self.handle,
      "SCA performing graceful shutdown of stream."
    );
    if let Err(e) = self.zmtp_handler.initiate_stream_shutdown().await {
      tracing::warn!(
        sca_handle = self.handle,
        "Error during stream shutdown: {}.",
        e
      );
      if self.error_for_drop_guard.is_none() {
        self.error_for_drop_guard = Some(e);
      }
    }
    self.perform_final_cleanup_actions().await; // Includes setting phase to Terminating
  }

  async fn perform_final_cleanup_actions(&mut self) {
    // Ensure stream is none if not already handled by initiate_stream_shutdown
    if self.zmtp_handler.stream.is_some() {
      let _ = self.zmtp_handler.initiate_stream_shutdown().await; // Best effort
    }
    self.core_pipe_manager.detach_and_clear_pipes();
    self.current_phase = ConnectionPhaseX::Terminating;
    tracing::debug!(
      sca_handle = self.handle,
      "SCA final cleanup actions complete."
    );
  }
}
