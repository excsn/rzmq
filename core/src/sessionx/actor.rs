#![allow(dead_code, unused_variables, unused_mut)]

use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::runtime::{ActorDropGuard, ActorType, Command, SystemEvent};
use crate::sessionx::ingress_future::IngressDriver;
use crate::sessionx::regulator::SessionRegulator;
use crate::socket::ISocket;
use crate::socket::events::SocketEvent;
use crate::socket::options::ZmtpEngineConfig;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::throttle::AdaptiveThrottle;
use crate::transport::{ZmtpStdStream, ZmtpWriteHalf};
use crate::{Blob, MailboxReceiver};

use super::message_processor::ZmqMessageProcessor;

use futures::FutureExt;
use std::fmt::Debug;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, broadcast};
use tokio::task::{JoinHandle, yield_now};
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};

use super::egress_buffer::EgressBuffer;
use super::egress_future::EgressDriver;
use super::pipe_manager::CorePipeManagerX;
use super::protocol_handler::ZmtpProtocolHandlerX;
use super::states::ActorConfigX;
use super::types::{ConnectionPhaseX, ZmtpHandshakeProgressX};

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
  session_regulator: SessionRegulator,
  _connection_permit: Option<OwnedSemaphorePermit>,
  incoming_pipe_sender: Option<PipeMessageSender>,
  is_currently_congested: bool,
}

impl<S> SessionConnectionActorX<S>
where
  S: ZmtpStdStream + Debug + Unpin + Send + 'static,
{
  pub(crate) fn create_and_spawn(
    handle: usize,
    parent_socket_id: usize,
    stream: S,
    actor_config: ActorConfigX,
    engine_config: Arc<ZmtpEngineConfig>,
    command_mailbox_receiver: MailboxReceiver,
    socket_logic: Arc<dyn ISocket>,
    connection_permit: Option<OwnedSemaphorePermit>,
  ) -> JoinHandle<()> {

    let zmtp_handler = ZmtpProtocolHandlerX::new(
      stream,
      engine_config.clone(),
      actor_config.is_server_role,
      handle,
    );

    let mut ping_check_timer = None;
    if let Some(ivl) = engine_config.heartbeat_ivl {
      if !ivl.is_zero() {
        let mut timer = tokio::time::interval_at(TokioInstant::now() + ivl, ivl);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ping_check_timer = Some(timer);
      }
    }

    let handshake_deadline = engine_config
      .handshake_timeout
      .map(|d| TokioInstant::now() + d);

    let system_event_receiver = actor_config.context.event_bus().subscribe();

    let rcv_timeout = engine_config
      .rcvtimeo
      .unwrap_or(std::time::Duration::from_secs(30));
    let regulator_min_lifespan = if rcv_timeout < std::time::Duration::from_millis(1000) {
      std::time::Duration::from_millis(1000)
    } else {
      std::time::Duration::from_millis(1000)
    };

    let actor = Self {
      handle,
      current_phase: ConnectionPhaseX::HandshakeInProgress,
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
      session_regulator: SessionRegulator::new(regulator_min_lifespan),
      _connection_permit: connection_permit,
      incoming_pipe_sender: None,
      is_currently_congested: false,
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
      ActorType::Session,
      Some(self.actor_config.connected_endpoint_uri.clone()),
      Some(self.parent_socket_id),
    );

    let adaptive_throttle = {
      let mut config = self.zmtp_handler.config.throttle_config.clone();
      if self.actor_config.is_server_role {
        config.priority = crate::throttle::types::Priority::Egress;
      } else {
        config.priority = crate::throttle::types::Priority::Ingress;
      }
      AdaptiveThrottle::new(config)
    };

    let mut outgoing_batch: Vec<FrameBatch> =
      Vec::with_capacity(self.zmtp_handler.config.sndbatch_count);

    let mut egress_buffer = EgressBuffer::new();

    // Local FIFO queue decouples network read from application pipe send to prevent deadlock.
    let mut ingress_buffer: std::collections::VecDeque<FrameBatch> =
      std::collections::VecDeque::new();

    // ── HANDSHAKE LOOP ────────────────────────────────────────────────────────
    'handshake: while matches!(
      self.current_phase,
      ConnectionPhaseX::HandshakeInProgress | ConnectionPhaseX::WaitingForPipes
    ) {
      if self.current_phase == ConnectionPhaseX::ShuttingDownStream {
        self.perform_graceful_shutdown().await;
        break 'handshake;
      }

      let mut overall_handshake_timeout_future = futures::future::pending().left_future();
      if let Some(deadline) = self.handshake_deadline {
        overall_handshake_timeout_future = tokio::time::sleep_until(deadline).right_future();
      }

      tokio::select! {
        biased;

        maybe_cmd = self.command_mailbox_receiver.recv() => {
          match maybe_cmd {
            Ok(command) => self.process_command(command).await,
            Err(_) => {
              tracing::info!(sca_handle = self.handle, "SCA Command mailbox closed.");
              self.transition_to_shutdown_stream(None).await;
            }
          }
        }

        maybe_event = self.system_event_receiver.recv() => {
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

        _ = overall_handshake_timeout_future, if self.handshake_deadline.is_some() => {
          tracing::warn!(sca_handle = self.handle, uri = %self.actor_config.connected_endpoint_uri, "Overall ZMTP handshake timed out.");
          self.set_fatal_error(ZmqError::Timeout).await;
        }

        handshake_res = self.zmtp_handler.advance_handshake(),
            if self.current_phase == ConnectionPhaseX::HandshakeInProgress => {
          match handshake_res {
            Ok(p) => self.handle_handshake_progression(p).await,
            Err(e) => self.set_fatal_error(e).await,
          }
        }
      }
    }

    // ── OPERATIONAL LOOP ──────────────────────────────────────────────────────
    if self.current_phase == ConnectionPhaseX::Operational {
      let mut message_processor = ZmqMessageProcessor::new();

      let mut read_half = self.zmtp_handler.read_half.take()
        .expect("read_half must be present at operational start");
      let mut write_half = self.zmtp_handler.write_half.take()
        .expect("write_half must be present at operational start");

      let use_owned_write = write_half.supports_owned_write();
      let sndhwm = self.zmtp_handler.config.sndhwm.max(1);
      let sndbatch_count = self.zmtp_handler.config.sndbatch_count;
      let mut pending_vectored: std::collections::VecDeque<Vec<bytes::Bytes>> =
        std::collections::VecDeque::new();

      let mut core_carryover: std::collections::VecDeque<FrameBatch> =
        std::collections::VecDeque::new();
      let mut core_batch_scratch: Vec<FrameBatch> = Vec::new();

      'operational: while self.current_phase == ConnectionPhaseX::Operational
      {
        if !core_carryover.is_empty()
          && self.current_phase == ConnectionPhaseX::Operational
          && if use_owned_write {
            pending_vectored.is_empty()
          } else {
            egress_buffer.pending_messages() < sndhwm
          }
        {
          outgoing_batch.clear();
          let wire_size = |msgs: &FrameBatch| msgs.iter().map(|m| m.size() + 9).sum::<usize>();
          let hwm_budget = sndhwm.saturating_sub(egress_buffer.pending_messages()).max(1);
          let max_count = self.zmtp_handler.config.sndbatch_count.min(hwm_budget);
          let max_bytes = self.zmtp_handler.config.sndbatch_bytes;
          let mut total_bytes = 0usize;

          while outgoing_batch.len() < max_count && total_bytes < max_bytes {
            match core_carryover.pop_front() {
              Some(msgs) => {
                total_bytes += wire_size(&msgs);
                outgoing_batch.push(msgs);
              }
              None => break,
            }
          }
          if core_carryover.is_empty()
            && outgoing_batch.len() < max_count
            && total_bytes < max_bytes
          {
            core_batch_scratch.clear();
            let _ = self
              .core_pipe_manager
              .try_recv_batch_from_core(&mut core_batch_scratch, max_count - outgoing_batch.len());
            for msgs in core_batch_scratch.drain(..) {
              if outgoing_batch.len() < max_count && total_bytes < max_bytes {
                total_bytes += wire_size(&msgs);
                outgoing_batch.push(msgs);
              } else {
                core_carryover.push_back(msgs);
              }
            }
          }

          let throttle_guard = adaptive_throttle.begin_work_bulk(
            crate::throttle::Direction::Egress,
            outgoing_batch.len() as u32,
          );

          if use_owned_write {
            match self.zmtp_handler.frame_outgoing_batch_vectored(&outgoing_batch) {
              Ok(bufs) => pending_vectored.push_back(bufs),
              Err(e) => self.set_fatal_error(e).await,
            }
          } else {
            match self.zmtp_handler.frame_outgoing_batch(&outgoing_batch) {
              Ok(bytes) => {
                egress_buffer.push(bytes, outgoing_batch.len());
                if !self.is_currently_congested && egress_buffer.pending_messages() >= sndhwm {
                  self.is_currently_congested = true;
                  if let Some(ref tx) = self.actor_config.monitor_tx {
                    let ep = clean_endpoint_uri(&self.actor_config.logical_target_endpoint_uri);
                    let _ = tx.try_send(SocketEvent::ConnectionCongested { endpoint: ep });
                  }
                }
              }
              Err(e) => self.set_fatal_error(e).await,
            }
          }

          if throttle_guard.should_throttle() {
            yield_now().await;
          }
        }

        let mut pong_timeout_future = futures::future::pending().left_future();
        if self.zmtp_handler.heartbeat_state.waiting_for_pong {
          if let Some(deadline_std) = self.zmtp_handler.heartbeat_state.get_pong_deadline() {
            let deadline_tokio = TokioInstant::from_std(deadline_std);
            pong_timeout_future = tokio::time::sleep_until(deadline_tokio).right_future();
          }
        }

        tokio::select! {
          biased;

          maybe_cmd = self.command_mailbox_receiver.recv() => {
            match maybe_cmd {
              Ok(command) => self.process_command(command).await,
              Err(_) => {
                tracing::info!(sca_handle = self.handle, "SCA Command mailbox closed.");
                self.transition_to_shutdown_stream(None).await;
              }
            }
          }

          maybe_event = self.system_event_receiver.recv() => {
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

          _ = async { self.ping_check_timer.as_mut().map_or(futures::future::pending().left_future(), |t| t.tick().right_future()).await },
              if self.ping_check_timer.is_some() => {
            match self.zmtp_handler.maybe_build_ping() {
              Ok(Some(bytes)) => egress_buffer.push_priority(bytes),
              Ok(None) => {}
              Err(e) => self.set_fatal_error(e).await,
            }
          }

          _ = pong_timeout_future, if self.zmtp_handler.heartbeat_state.waiting_for_pong => {
            if self.zmtp_handler.has_pong_timed_out() {
              self.set_fatal_error(ZmqError::Timeout).await;
            }
          }

          // Drain ingress buffer: decouples network read from application pipe send.
          drain_res = IngressDriver::new(self.incoming_pipe_sender.as_ref(), &mut ingress_buffer),
              if !ingress_buffer.is_empty() => {
            match drain_res {
              Ok(batch_len) => {
                if self.incoming_pipe_sender.is_some() {
                  let weight = batch_len as u32;
                  let throttle_guard = adaptive_throttle
                    .begin_work_bulk(crate::throttle::Direction::Ingress, weight);
                  if throttle_guard.should_throttle() {
                    yield_now().await;
                  }
                }
              }
              Err(e) => {
                tracing::debug!(sca_handle = self.handle, error = %e, "Ingress pipe closed; shutting down.");
                self.transition_to_shutdown_stream(Some(e)).await;
              }
            }
          }

          // Ingress network read: gated on empty buffer to propagate TCP backpressure.
          ingress_res = message_processor.read_and_process(&mut read_half, &mut self.zmtp_handler),
              if ingress_buffer.is_empty() => {
            match ingress_res {
              Ok(complete_batches) => {
                for frame_batch in complete_batches {
                  let is_command_batch = frame_batch.first().map_or(false, |m| m.is_command());
                  if is_command_batch {
                    for msg in &frame_batch {
                      match self.zmtp_handler.process_incoming_data_command_frame(msg) {
                        Ok(Some(pong)) => {
                          let mut pong_fb = FrameBatch::new();
                          pong_fb.push(pong);
                          match self.zmtp_handler.frame_outgoing_msgs(pong_fb) {
                            Ok(bytes) => egress_buffer.push_priority(bytes),
                            Err(e) => { self.set_fatal_error(e).await; }
                          }
                        }
                        Ok(None) => {}
                        Err(e) => { self.set_fatal_error(e).await; }
                      }
                    }
                  } else {
                    ingress_buffer.push_back(frame_batch);
                  }
                }
              }
              Err(ZmqError::ConnectionClosed) => {
                tracing::info!(sca_handle = self.handle, "Peer closed connection.");
                self.transition_to_shutdown_stream(Some(ZmqError::ConnectionClosed)).await;
              }
              Err(e) => self.set_fatal_error(e).await,
            }
          }

          // Unified write arm.
          write_res = async {
            if !egress_buffer.is_empty() {
              let r = EgressDriver::new(&mut write_half, &mut egress_buffer, sndbatch_count).await;
              r?;
              Ok::<bool, ZmqError>(true)
            } else {
              let bufs = pending_vectored.front().cloned().unwrap_or_default();
              write_half.write_owned(bufs).await
                .map_err(|e| ZmqError::from_io_endpoint(e, "egress write"))?;
              Ok(false)
            }
          }, if !egress_buffer.is_empty()
              || (use_owned_write && pending_vectored.front().is_some()) => {
            match write_res {
              Ok(true) => {
                self.zmtp_handler.heartbeat_state.record_activity();
                if self.is_currently_congested && egress_buffer.pending_messages() < sndhwm {
                  self.is_currently_congested = false;
                  if let Some(ref tx) = self.actor_config.monitor_tx {
                    let ep = clean_endpoint_uri(&self.actor_config.logical_target_endpoint_uri);
                    let _ = tx.try_send(SocketEvent::ConnectionUncongested { endpoint: ep });
                  }
                }
              }
              Ok(false) => {
                pending_vectored.pop_front();
                self.zmtp_handler.heartbeat_state.record_activity();
              }
              Err(e) => {
                self.set_fatal_error(e).await;
              }
            }
          }

          // Outgoing from SocketCore.
          maybe_msgs_from_core = async { self.core_pipe_manager.recv_from_core().await },
              if self.current_phase == ConnectionPhaseX::Operational
                && self.core_pipe_manager.is_attached()
                && core_carryover.is_empty()
                && if use_owned_write {
                  pending_vectored.is_empty()
                } else {
                  egress_buffer.pending_messages() < sndhwm
                } => {
            match maybe_msgs_from_core {
              Ok(first_msgs) => {
                outgoing_batch.clear();
                let wire_size = |msgs: &FrameBatch| msgs.iter().map(|m| m.size() + 9).sum::<usize>();
                let mut total_bytes = wire_size(&first_msgs);
                outgoing_batch.push(first_msgs);

                let hwm_budget = sndhwm.saturating_sub(egress_buffer.pending_messages());
                let max_count = self.zmtp_handler.config.sndbatch_count.min(hwm_budget.max(1));
                let max_bytes = self.zmtp_handler.config.sndbatch_bytes;

                while outgoing_batch.len() < max_count && total_bytes < max_bytes {
                  core_batch_scratch.clear();
                  let drained = self
                    .core_pipe_manager
                    .try_recv_batch_from_core(&mut core_batch_scratch, max_count - outgoing_batch.len());
                  if drained == 0 {
                    break;
                  }
                  for msgs in core_batch_scratch.drain(..) {
                    if outgoing_batch.len() < max_count && total_bytes < max_bytes {
                      total_bytes += wire_size(&msgs);
                      outgoing_batch.push(msgs);
                    } else {
                      core_carryover.push_back(msgs);
                    }
                  }
                }

                let throttle_guard = adaptive_throttle.begin_work_bulk(
                  crate::throttle::Direction::Egress,
                  outgoing_batch.len() as u32,
                );

                if use_owned_write {
                  match self.zmtp_handler.frame_outgoing_batch_vectored(&outgoing_batch) {
                    Ok(bufs) => pending_vectored.push_back(bufs),
                    Err(e) => self.set_fatal_error(e).await,
                  }
                } else {
                  match self.zmtp_handler.frame_outgoing_batch(&outgoing_batch) {
                    Ok(bytes) => {
                      egress_buffer.push(bytes, outgoing_batch.len());
                      if !self.is_currently_congested && egress_buffer.pending_messages() >= sndhwm {
                        self.is_currently_congested = true;
                        if let Some(ref tx) = self.actor_config.monitor_tx {
                          let ep = clean_endpoint_uri(&self.actor_config.logical_target_endpoint_uri);
                          let _ = tx.try_send(SocketEvent::ConnectionCongested { endpoint: ep });
                        }
                      }
                    }
                    Err(e) => self.set_fatal_error(e).await,
                  }
                }

                if throttle_guard.should_throttle() {
                  yield_now().await;
                }
              }
              Err(_) => {
                tracing::info!(sca_handle = self.handle, "Pipe from SocketCore closed.");
                self.transition_to_shutdown_stream(Some(ZmqError::ConnectionClosed)).await;
              }
            }
          }

        }
      }

      self.zmtp_handler.read_half = Some(read_half);
      self.zmtp_handler.write_half = Some(write_half);
    }

    if self.current_phase == ConnectionPhaseX::ShuttingDownStream {
      self.perform_graceful_shutdown().await;
    }

    tracing::info!(
      sca_handle = self.handle,
      "SCA loop finished. Final phase: {:?}.",
      self.current_phase
    );

    if self.current_phase != ConnectionPhaseX::Terminating {
      self.perform_final_cleanup_actions().await;
    }

    let had_error = self.error_for_drop_guard.is_some();

    if let Some(err) = self.error_for_drop_guard.take() {
      actor_drop_guard.set_error(err);
    } else {
      actor_drop_guard.waive();
    }

    drop(actor_drop_guard);

    if had_error {
      self.session_regulator.enforce_min_lifespan().await;
    }

    tracing::info!(
      sca_handle = self.handle,
      "SCA {} fully stopped.",
      self.actor_config.connected_endpoint_uri
    );
  }

  // --- Helper Methods ---

  async fn process_command(&mut self, command: Command) {
    tracing::debug!(
      sca_handle = self.handle,
      "Processing ScaCommand: {:?}",
      command
    );
    match command {
      Command::ScaInitializePipes {
        sca_handle_id,
        rx_from_core,
        core_pipe_read_id_for_incoming_routing,
        incoming_pipe_sender,
      } => {
        if self.handle != sca_handle_id {
          tracing::warn!(
            sca_handle = self.handle,
            expected_id = sca_handle_id,
            "Received ScaInitializePipes for wrong actor. Ignoring."
          );
          drop(rx_from_core);
          return;
        }
        if self.core_pipe_manager.is_attached() {
          tracing::warn!(
            sca_handle = self.handle,
            "Already attached. Ignoring ScaInitializePipes."
          );
          drop(rx_from_core);
          return;
        }

        if matches!(
          self.current_phase,
          ConnectionPhaseX::ShuttingDownStream | ConnectionPhaseX::Terminating
        ) {
          tracing::warn!(
            sca_handle = self.handle,
            phase = ?self.current_phase,
            "Received ScaInitializePipes while shutting down. Dropping pipes."
          );
          drop(rx_from_core);
          return;
        }

        self.incoming_pipe_sender = incoming_pipe_sender;
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
        self.transition_to_shutdown_stream(None).await;
      }
      SystemEvent::SocketClosing { socket_id } => {
        if socket_id == self.parent_socket_id {
          self.transition_to_shutdown_stream(None).await;
        }
      }
      _ => {}
    }
  }

  async fn handle_handshake_progression(&mut self, progress: ZmtpHandshakeProgressX) {
    if self.current_phase != ConnectionPhaseX::HandshakeInProgress {
      return;
    }

    match progress {
      ZmtpHandshakeProgressX::InProgress => {}
      ZmtpHandshakeProgressX::IdentityReady(blob) => {
        self.pending_peer_identity_from_handshake = Some(blob);
        if !self.zmtp_handler.is_handshake_complete() {
          return;
        }
      }
      ZmtpHandshakeProgressX::HandshakeComplete => {}
      ZmtpHandshakeProgressX::ProtocolError(s) => {
        self.set_fatal_error(ZmqError::ProtocolViolation(s)).await;
        return;
      }
      ZmtpHandshakeProgressX::FatalError(e) => {
        self.set_fatal_error(e).await;
        return;
      }
    }

    if self.zmtp_handler.is_handshake_complete() {
      tracing::info!(
        sca_handle = self.handle,
        "Handshake complete via ZmtpProtocolHandlerX."
      );
      self.current_phase = ConnectionPhaseX::WaitingForPipes;
      self.handshake_deadline = None;
      self.check_and_transition_to_operational().await;
    }
  }

  async fn check_and_transition_to_operational(&mut self) {
    if self.current_phase == ConnectionPhaseX::WaitingForPipes
      && self.zmtp_handler.is_handshake_complete()
      && self.core_pipe_manager.is_attached()
    {
      tracing::info!(
        sca_handle = self.handle,
        "Transitioning to Operational phase."
      );
      self.current_phase = ConnectionPhaseX::Operational;

      let socket_type = self.zmtp_handler.config.socket_type_name.as_str();
      if self.zmtp_handler.config.use_cork
        && matches!(socket_type, "PUSH" | "PULL" | "PUB" | "SUB")
      {
        if let Some(ref wh) = self.zmtp_handler.write_half {
          wh.set_cork(true);
        }
        #[cfg(target_os = "linux")]
        if let Some(ref mut ci) = self.zmtp_handler.cork_info {
          ci.apply_cork_state(true, self.handle).await;
        }
      }

      let final_peer_identity = self.pending_peer_identity_from_handshake.take();
      let peer_socket_type = self.zmtp_handler.handshake_state.peer_socket_type.clone();

      if let Some(routing_id) = self
        .core_pipe_manager
        .state
        .core_pipe_read_id_for_incoming_routing
      {
        let event = SystemEvent::PeerIdentityEstablished {
          parent_core_id: self.parent_socket_id,
          connection_identifier: routing_id,
          peer_identity: final_peer_identity,
          peer_socket_type,
        };
        if self.actor_config.context.event_bus().publish(event).is_err() {
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
    let pipe_read_id = self
      .core_pipe_manager
      .state
      .core_pipe_read_id_for_incoming_routing
      .expect("Cannot handle incoming message without core_pipe_read_id");

    if msg.is_command() {
      match self.zmtp_handler.process_incoming_data_command_frame(&msg) {
        Ok(Some(pong_reply)) => {
          if let Err(e) = self.zmtp_handler.write_data_msg(pong_reply, true).await {
            self.set_fatal_error(e).await;
          }
        }
        Ok(None) => {}
        Err(e) => self.set_fatal_error(e).await,
      }
    } else {
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

    let _ = self.zmtp_handler.initiate_stream_shutdown().await;
    self.current_phase = ConnectionPhaseX::Terminating;
  }

  async fn transition_to_shutdown_stream(&mut self, accompanying_error: Option<ZmqError>) {
    if self.current_phase != ConnectionPhaseX::ShuttingDownStream
      && self.current_phase != ConnectionPhaseX::Terminating
    {
      if let Some(err) = accompanying_error {
        if self.error_for_drop_guard.is_none() {
          self.error_for_drop_guard = Some(err);
        }
        tracing::info!(
          sca_handle = self.handle,
          "Transitioning directly to Terminating phase due to error."
        );
        self.current_phase = ConnectionPhaseX::Terminating;
      } else {
        tracing::info!(
          sca_handle = self.handle,
          "Transitioning to ShuttingDownStream phase."
        );
        self.current_phase = ConnectionPhaseX::ShuttingDownStream;
      }
    }
  }

  async fn perform_graceful_shutdown(&mut self) {
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
    self.perform_final_cleanup_actions().await;
  }

  async fn perform_final_cleanup_actions(&mut self) {
    if self.zmtp_handler.read_half.is_some() || self.zmtp_handler.write_half.is_some() {
      let _ = self.zmtp_handler.initiate_stream_shutdown().await;
    }
    self.core_pipe_manager.detach_and_clear_pipes();
    self.current_phase = ConnectionPhaseX::Terminating;
    tracing::debug!(
      sca_handle = self.handle,
      "SCA final cleanup actions complete."
    );
  }
}

fn clean_endpoint_uri(uri: &str) -> String {
  match uri.find('#') {
    Some(pos) => uri[..pos].to_string(),
    None => uri.to_string(),
  }
}
