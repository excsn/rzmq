#![allow(dead_code, unused_variables, unused_mut)]

use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::protocol::zmtp::actions::{AppAction, EngineOutput, NetAction};
use crate::protocol::zmtp::engine::{ZmtpEngine, ZmtpPhase};
use crate::protocol::zmtp::greeting::GREETING_LENGTH;
use crate::runtime::{ActorDropGuard, ActorType, Command, SystemEvent};
use crate::sessionx::ingress_future::IngressDriver;
use crate::sessionx::regulator::SessionRegulator;
use crate::socket::ISocket;
use crate::socket::events::SocketEvent;
use crate::socket::options::ZmtpEngineConfig;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::throttle::AdaptiveThrottle;
use crate::transport::{ZmtpStdStream, ZmtpWriteHalf};
use crate::{Blob, MailboxReceiver, counter, log_session_diagnostics};

use super::message_processor::ZmqMessageProcessor;

use bytes::BytesMut;
use futures::FutureExt;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, broadcast};
use tokio::task::{JoinHandle, yield_now};
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};

use super::egress_buffer::EgressBuffer;
use super::egress_future::EgressDriver;
use super::pipe_manager::CorePipeManagerX;
use super::states::ActorConfigX;
use super::types::ConnectionPhaseX;

use std::sync::atomic::Ordering;

pub(crate) struct SessionConnectionActorX<S: ZmtpStdStream> {
  handle: usize,
  current_phase: ConnectionPhaseX,
  parent_socket_id: usize,
  actor_config: ActorConfigX,

  zmtp_engine: ZmtpEngine,
  read_half: Option<S::ReadHalf>,
  write_half: Option<S::WriteHalf>,
  /// Temporary read buffer used only during the handshake loop.
  handshake_read_buf: BytesMut,

  core_pipe_manager: CorePipeManagerX,

  command_mailbox_receiver: MailboxReceiver,
  system_event_receiver: broadcast::Receiver<SystemEvent>,

  pending_peer_identity_from_handshake: Option<Blob>,
  pending_peer_socket_type: Option<String>,
  error_for_drop_guard: Option<ZmqError>,

  ping_check_timer: Option<tokio::time::Interval>,

  handshake_deadline: Option<TokioInstant>,
  socket_logic: Arc<dyn ISocket>,
  session_regulator: SessionRegulator,
  _connection_permit: Option<OwnedSemaphorePermit>,
  incoming_pipe_sender: Option<PipeMessageSender>,
  is_currently_congested: bool,

  #[cfg(target_os = "linux")]
  cork_info: Option<crate::sessionx::cork::TcpCorkInfoX>,
  #[cfg(not(target_os = "linux"))]
  cork_info: Option<()>,
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
    // Capture the raw fd for cork setup BEFORE consuming the stream via into_split.
    #[cfg(target_os = "linux")]
    let cork_info = {
      use std::os::fd::AsRawFd;
      if engine_config.use_cork {
        crate::sessionx::cork::try_create_cork_info(Some(&stream), true)
      } else {
        None
      }
    };
    #[cfg(not(target_os = "linux"))]
    let cork_info: Option<()> = None;

    // Split the stream into independent owned halves.
    let (read_half, write_half) = stream.into_split();

    let zmtp_engine = ZmtpEngine::new(actor_config.is_server_role, engine_config.clone());

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
      zmtp_engine,
      read_half: Some(read_half),
      write_half: Some(write_half),
      handshake_read_buf: BytesMut::with_capacity(GREETING_LENGTH * 4),
      core_pipe_manager: CorePipeManagerX::new(),
      command_mailbox_receiver,
      system_event_receiver,
      pending_peer_identity_from_handshake: None,
      pending_peer_socket_type: None,
      error_for_drop_guard: None,
      ping_check_timer,
      handshake_deadline,
      socket_logic,
      session_regulator: SessionRegulator::new(regulator_min_lifespan),
      _connection_permit: connection_permit,
      incoming_pipe_sender: None,
      is_currently_congested: false,
      cork_info,
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
      let mut config = self.zmtp_engine.config().throttle_config.clone();
      if self.actor_config.is_server_role {
        config.priority = crate::throttle::types::Priority::Egress;
      } else {
        config.priority = crate::throttle::types::Priority::Ingress;
      }
      AdaptiveThrottle::new(config)
    };

    let mut outgoing_batch: Vec<FrameBatch> =
      Vec::with_capacity(self.zmtp_engine.config().sndbatch_count);

    let mut egress_buffer = EgressBuffer::new();

    // Local FIFO queue decouples network read from application pipe send to prevent deadlock.
    let mut ingress_buffer: std::collections::VecDeque<FrameBatch> =
      std::collections::VecDeque::new();

    // Send the initial greeting (client only; server returns empty output).
    let initial_out = self.zmtp_engine.start();
    self.apply_engine_output_handshake(initial_out).await;
    if matches!(self.current_phase, ConnectionPhaseX::Terminating) {
      self.finalize(actor_drop_guard).await;
      return;
    }

    // ── HANDSHAKE LOOP ────────────────────────────────────────────────────────
    // Synchronous: drive the engine to ZmtpPhase::Data before processing any
    // commands. This preserves the old behaviour where ScaInitializePipes stays
    // in the mailbox until the handshake is fully done.
    {
      let mut hs_read_half = self
        .read_half
        .take()
        .expect("read_half present at handshake start");

      let hs_timeout = self
        .zmtp_engine
        .config()
        .handshake_timeout
        .unwrap_or(Duration::from_secs(15));

      'handshake: loop {
        if self.zmtp_engine.phase == ZmtpPhase::Data
          || self.zmtp_engine.phase == ZmtpPhase::Closed
          || matches!(self.current_phase, ConnectionPhaseX::Terminating)
        {
          break 'handshake;
        }

        let read_result = tokio::time::timeout(
          hs_timeout,
          hs_read_half.read_buf(&mut self.handshake_read_buf),
        )
        .await;

        match read_result {
          Err(_elapsed) => {
            tracing::warn!(
              sca_handle = self.handle,
              uri = %self.actor_config.connected_endpoint_uri,
              "Overall ZMTP handshake timed out."
            );
            self.set_fatal_error(ZmqError::Timeout).await;
            break 'handshake;
          }
          Ok(Err(e)) => {
            self
              .set_fatal_error(ZmqError::from_io_endpoint(e, "handshake read"))
              .await;
            break 'handshake;
          }
          Ok(Ok(0)) => {
            self.set_fatal_error(ZmqError::ConnectionClosed).await;
            break 'handshake;
          }
          Ok(Ok(_)) => {
            let data = self.handshake_read_buf.split().freeze();
            let out = self.zmtp_engine.on_network_bytes(data);
            self.apply_engine_output_handshake(out).await;
          }
        }
      }

      self.read_half = Some(hs_read_half);
    }

    // Handshake done — wait for ScaInitializePipes before entering Operational.
    // ScaInitializePipes may already be in the mailbox (sent by SocketCore as soon
    // as it learned about the connection) or may arrive shortly after.
    if self.zmtp_engine.phase == ZmtpPhase::Data
      && !matches!(self.current_phase, ConnectionPhaseX::Terminating)
    {
      self.current_phase = ConnectionPhaseX::WaitingForPipes;
      while self.current_phase == ConnectionPhaseX::WaitingForPipes {
        match self.command_mailbox_receiver.recv().await {
          Ok(cmd) => self.process_command(cmd).await,
          Err(_) => {
            tracing::info!(
              sca_handle = self.handle,
              "SCA Command mailbox closed while waiting for pipes."
            );
            self.transition_to_shutdown_stream(None).await;
            break;
          }
        }
      }
    }

    // ── OPERATIONAL LOOP ──────────────────────────────────────────────────────
    if self.current_phase == ConnectionPhaseX::Operational {
      let mut message_processor = ZmqMessageProcessor::new();

      let mut read_half = self
        .read_half
        .take()
        .expect("read_half must be present at operational start");
      let mut write_half = self
        .write_half
        .take()
        .expect("write_half must be present at operational start");

      let use_owned_write = write_half.supports_owned_write();
      let sndhwm = self.zmtp_engine.config().sndhwm.max(1);
      let sndbatch_count = self.zmtp_engine.config().sndbatch_count;
      let mut pending_vectored: std::collections::VecDeque<Vec<bytes::Bytes>> =
        std::collections::VecDeque::new();

      let mut core_carryover: std::collections::VecDeque<FrameBatch> =
        std::collections::VecDeque::new();

      #[cfg(any(debug_assertions, feature = "diagnostics"))]
      let mut last_log_ms = 0u64;

      'operational: while self.current_phase == ConnectionPhaseX::Operational {
        if !core_carryover.is_empty()
          && self.current_phase == ConnectionPhaseX::Operational
          && if use_owned_write {
            pending_vectored.is_empty()
          } else {
            egress_buffer.pending_messages() < sndhwm
          }
        {
          println!(
            "[Carryover Debug] ID: {} | Draining carryover queue. Starting size: {}",
            self.handle,
            core_carryover.len()
          );
          outgoing_batch.clear();
          let wire_size = |msgs: &FrameBatch| msgs.iter().map(|m| m.size() + 9).sum::<usize>();
          let hwm_budget = sndhwm
            .saturating_sub(egress_buffer.pending_messages())
            .max(1);
          let max_count = self.zmtp_engine.config().sndbatch_count.min(hwm_budget);
          let max_bytes = self.zmtp_engine.config().sndbatch_bytes;

          // Use our pre-calculated on-demand physical limit for overfill checks
          let max_bytes = self.zmtp_engine.config().sndbatch_bytes_physical;
          let logical_max_bytes = self.zmtp_engine.config().sndbatch_bytes;
          let mut total_bytes = 0usize;

          // First, drain as much as possible from core_carryover
          while !core_carryover.is_empty() && outgoing_batch.len() < max_count {
            let next_msg = core_carryover.pop_front().unwrap();
            let size = wire_size(&next_msg);
            if total_bytes + size > max_bytes && !outgoing_batch.is_empty() {
              core_carryover.push_front(next_msg);
              break;
            }
            total_bytes += size;
            outgoing_batch.push(next_msg);
          }

          // Then, if there is still room, top up from the core_pipe_manager
          let start_len = outgoing_batch.len();
          if start_len < max_count && total_bytes < logical_max_bytes {
            // Dynamically calculate actual remaining slots based on the average size of current messages
            let avg_size = if start_len > 0 {
              total_bytes / start_len
            } else {
              32768
            };
            let remaining_bytes = max_bytes.saturating_sub(total_bytes);
            let needed_by_bytes = if avg_size > 0 {
              remaining_bytes / avg_size
            } else {
              0
            };

            let needed = (max_count - start_len).min(needed_by_bytes);

            if needed > 0 {
              let drained = self
                .core_pipe_manager
                .try_recv_batch_from_core(&mut outgoing_batch, needed);

              if drained > 0 {
                let mut i = start_len;
                while i < outgoing_batch.len() {
                  let size = wire_size(&outgoing_batch[i]);
                  if total_bytes + size > max_bytes && i > 0 {
                    // We exceeded the byte limit!
                    // Move the overflow into carryover for the next cycle and stop.
                    core_carryover.extend(outgoing_batch.drain(i..));
                    break;
                  }
                  total_bytes += size;
                  i += 1;
                }
              }
            }
          }

          if !outgoing_batch.is_empty() {
            counter!(global, global_drained_msgs, add, outgoing_batch.len() as u64);
          }

          let throttle_guard = adaptive_throttle.begin_work_bulk(
            crate::throttle::Direction::Egress,
            outgoing_batch.len() as u32,
          );

          if use_owned_write {
            match self.zmtp_engine.frame_batch_vectored(&outgoing_batch) {
              Ok(bufs) => pending_vectored.push_back(bufs),
              Err(e) => self.set_fatal_error(e).await,
            }
          } else {
            match self.zmtp_engine.frame_batch(&outgoing_batch) {
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
        if self.zmtp_engine.is_waiting_for_pong() {
          if let Some(deadline_std) = self.zmtp_engine.get_pong_deadline() {
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
            let ping_out = self.zmtp_engine.on_tick(std::time::Instant::now());
            for action in ping_out.net_actions {
              if let NetAction::Send { data, .. } = action {
                egress_buffer.push_priority(data);
              }
            }
            for action in ping_out.app_actions {
              if let AppAction::PeerError(e) = action {
                self.set_fatal_error(e).await;
              }
            }
          }

          _ = pong_timeout_future, if self.zmtp_engine.is_waiting_for_pong() => {
            self.set_fatal_error(ZmqError::Timeout).await;
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
          ingress_res = message_processor.read_and_process(&mut read_half, &mut self.zmtp_engine),
            if ingress_buffer.is_empty() => {
            match ingress_res {
              Ok(engine_out) => {
                // Net actions: PONG frames and any other protocol-level sends.
                for action in engine_out.net_actions {
                  match action {
                    NetAction::Send { data, .. } => {
                      egress_buffer.push_priority(data);
                    }
                    NetAction::SetCork(enable) => {
                      #[cfg(target_os = "linux")]
                      if let Some(ci) = self.cork_info.as_mut() {
                        ci.apply_cork_state(enable, self.handle).await;
                      }
                    }
                    NetAction::ScheduleClose(_) => {
                      self.transition_to_shutdown_stream(None).await;
                    }
                  }
                }
                // App actions: fully-assembled data messages only (commands handled by engine).
                for action in engine_out.app_actions {
                  match action {
                    AppAction::DeliverMessage(batch) => {
                      ingress_buffer.push_back(batch);
                    }
                    AppAction::PeerError(e) => {
                      self.set_fatal_error(e).await;
                    }
                    AppAction::HandshakeComplete { .. } => {}
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
              let r = EgressDriver::new(&mut write_half, &mut egress_buffer, sndbatch_count, self.handle).await;
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
                self.zmtp_engine.record_activity();
                if self.is_currently_congested
                  && egress_buffer.pending_messages() < sndhwm
                {
                  self.is_currently_congested = false;
                  if let Some(ref tx) = self.actor_config.monitor_tx {
                    let ep = clean_endpoint_uri(
                      &self.actor_config.logical_target_endpoint_uri,
                    );
                    let _ = tx.try_send(SocketEvent::ConnectionUncongested {
                      endpoint: ep,
                    });
                  }
                }
              }
              Ok(false) => {
                pending_vectored.pop_front();
                self.zmtp_engine.record_activity();
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
            // Single-print alert if gating fails
            let pending_msgs = egress_buffer.pending_messages();
            if pending_msgs >= sndhwm {
              static PRINTED_ALERT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
              if !PRINTED_ALERT.swap(true, std::sync::atomic::Ordering::Relaxed) {
                println!(
                  "[ACTOR LOOP ERROR] Gating failed! Draining pipe even though egress_buffer.pending_messages() = {} (sndhwm = {})",
                  pending_msgs,
                  sndhwm
                );
              }
            }
            match maybe_msgs_from_core {
              Ok(first_msgs) => {
                outgoing_batch.clear();
                let wire_size = |msgs: &FrameBatch| {
                  msgs.iter().map(|m| m.size() + 9).sum::<usize>()
                };
                let mut total_bytes = wire_size(&first_msgs);
                outgoing_batch.push(first_msgs);

                let hwm_budget =
                  sndhwm.saturating_sub(egress_buffer.pending_messages());
                let max_count = self
                  .zmtp_engine
                  .config()
                  .sndbatch_count
                  .min(hwm_budget.max(1));
                // Use our pre-calculated on-demand physical limit for overfill checks
                let max_bytes = self.zmtp_engine.config().sndbatch_bytes_physical;
                let logical_max_bytes = self.zmtp_engine.config().sndbatch_bytes;

                let start_len = outgoing_batch.len();

                let start_len = outgoing_batch.len();
                if start_len < max_count && total_bytes < logical_max_bytes {
                  let avg_size = total_bytes / start_len;
                  let remaining_bytes = max_bytes.saturating_sub(total_bytes);
                  let needed_by_bytes = if avg_size > 0 { remaining_bytes / avg_size } else { 0 };

                  let needed = (max_count - start_len).min(needed_by_bytes);

                  if needed > 0 {
                    let drained = self.core_pipe_manager.try_recv_batch_from_core(
                      &mut outgoing_batch,
                      needed,
                    );

                    if drained > 0 {
                      let mut i = start_len;
                      while i < outgoing_batch.len() {
                        let size = wire_size(&outgoing_batch[i]);
                        if total_bytes + size > max_bytes && i > 0 {
                          // We exceeded the byte limit!
                          // Move the overflow into carryover for the next cycle and stop.
                          core_carryover.extend(outgoing_batch.drain(i..));
                          break;
                        }
                        total_bytes += size;
                        i += 1;
                      }
                    }
                  }
                }

                if !outgoing_batch.is_empty() {
                  counter!(global, global_drained_msgs, add, outgoing_batch.len() as u64);
                }

                let throttle_guard = adaptive_throttle.begin_work_bulk(
                  crate::throttle::Direction::Egress,
                  outgoing_batch.len() as u32,
                );

                if use_owned_write {
                  match self.zmtp_engine.frame_batch_vectored(&outgoing_batch) {
                    Ok(bufs) => pending_vectored.push_back(bufs),
                    Err(e) => self.set_fatal_error(e).await,
                  }
                } else {
                  match self.zmtp_engine.frame_batch(&outgoing_batch) {
                    Ok(bytes) => {
                      egress_buffer.push(bytes, outgoing_batch.len());
                      if !self.is_currently_congested
                        && egress_buffer.pending_messages() >= sndhwm
                      {
                        self.is_currently_congested = true;
                        if let Some(ref tx) =
                          self.actor_config.monitor_tx
                        {
                          let ep = clean_endpoint_uri(
                            &self
                              .actor_config
                              .logical_target_endpoint_uri,
                          );
                          let _ = tx.try_send(
                            SocketEvent::ConnectionCongested {
                              endpoint: ep,
                            },
                          );
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

        log_session_diagnostics!(last_log_ms, self, ingress_buffer, egress_buffer, sndhwm, core_carryover);
      }

      self.read_half = Some(read_half);
      self.write_half = Some(write_half);
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

    self.finalize(actor_drop_guard).await;
  }

  async fn finalize(mut self, mut actor_drop_guard: ActorDropGuard) {
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
          && self.zmtp_engine.phase == ZmtpPhase::Data
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

  /// Process a completed `EngineOutput` during the ZMTP handshake phase.
  ///
  /// Writes any `NetAction::Send` bytes synchronously (within the handshake timeout),
  /// applies cork state changes, and handles `HandshakeComplete` / `PeerError`.
  async fn apply_engine_output_handshake(&mut self, output: EngineOutput) {
    let hs_timeout = self
      .zmtp_engine
      .config()
      .handshake_timeout
      .unwrap_or(Duration::from_secs(15));

    for action in output.net_actions {
      match action {
        NetAction::Send { data, .. } => {
          let wh = match self.write_half.as_mut() {
            Some(w) => w,
            None => {
              self
                .set_fatal_error(ZmqError::Internal(
                  "write_half unavailable during handshake send".into(),
                ))
                .await;
              return;
            }
          };
          match tokio::time::timeout(hs_timeout, wh.write_all(&data)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
              self
                .set_fatal_error(ZmqError::from_io_endpoint(e, "hs write"))
                .await;
              return;
            }
            Err(_) => {
              self.set_fatal_error(ZmqError::Timeout).await;
              return;
            }
          }
          // Flush so the peer receives it immediately.
          let _ = tokio::time::timeout(hs_timeout, self.write_half.as_mut().unwrap().flush()).await;
        }
        NetAction::SetCork(enable) =>
        {
          #[cfg(target_os = "linux")]
          if let Some(ci) = self.cork_info.as_mut() {
            ci.apply_cork_state(enable, self.handle).await;
          }
        }
        NetAction::ScheduleClose(_) => {
          self.transition_to_shutdown_stream(None).await;
          return;
        }
      }
    }

    for action in output.app_actions {
      match action {
        AppAction::HandshakeComplete {
          peer_identity,
          peer_socket_type,
        } => {
          self.pending_peer_identity_from_handshake = peer_identity;
          self.pending_peer_socket_type = peer_socket_type;
          self.current_phase = ConnectionPhaseX::WaitingForPipes;
          self.handshake_deadline = None;
          self.check_and_transition_to_operational().await;
        }
        AppAction::PeerError(e) => {
          self.set_fatal_error(e).await;
          return;
        }
        AppAction::DeliverMessage(_) => {}
      }
    }
  }

  async fn check_and_transition_to_operational(&mut self) {
    if self.current_phase == ConnectionPhaseX::WaitingForPipes
      && self.zmtp_engine.phase == ZmtpPhase::Data
      && self.core_pipe_manager.is_attached()
    {
      tracing::info!(
        sca_handle = self.handle,
        "Transitioning to Operational phase."
      );
      self.current_phase = ConnectionPhaseX::Operational;

      let final_peer_identity = self.pending_peer_identity_from_handshake.take();
      let peer_socket_type = self.pending_peer_socket_type.take();

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

  async fn set_fatal_error(&mut self, error: ZmqError) {
    tracing::error!(sca_handle = self.handle, uri = %self.actor_config.connected_endpoint_uri, err = %error, "SCA fatal error.");

    if self.error_for_drop_guard.is_none() {
      self.error_for_drop_guard = Some(error);
    }

    // Uncork before close.
    #[cfg(target_os = "linux")]
    if let Some(ci) = self.cork_info.as_mut() {
      if ci.is_corked() {
        ci.apply_cork_state(false, self.handle).await;
      }
    }

    if let Some(mut wh) = self.write_half.take() {
      match wh.shutdown().await {
        Ok(()) => {
          tracing::debug!(sca_handle = self.handle, "Write half shutdown successful.");
        }
        Err(e) => {
          tracing::warn!(sca_handle = self.handle, error = %e, "Error during write half shutdown.");
        }
      }
    }
    self.read_half = None;
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

    let close_out = self.zmtp_engine.close();
    for action in close_out.net_actions {
      match action {
        NetAction::SetCork(false) =>
        {
          #[cfg(target_os = "linux")]
          if let Some(ci) = self.cork_info.as_mut() {
            if ci.is_corked() {
              ci.apply_cork_state(false, self.handle).await;
            }
          }
        }
        NetAction::ScheduleClose(Some(delay)) => {
          // Allow in-flight TCP segments to flush before tearing down the socket.
          tokio::time::sleep(delay).await;
        }
        _ => {}
      }
    }

    if let Some(mut wh) = self.write_half.take() {
      match wh.shutdown().await {
        Ok(()) => {
          tracing::debug!(sca_handle = self.handle, "Write half shutdown successful.");
        }
        Err(e) => {
          tracing::warn!(sca_handle = self.handle, error = %e, "Error during write half shutdown.");
          if self.error_for_drop_guard.is_none() {
            self.error_for_drop_guard = Some(ZmqError::from_io_endpoint(e, "graceful shutdown"));
          }
        }
      }
    }
    self.read_half = None;
    self.perform_final_cleanup_actions().await;
  }

  async fn perform_final_cleanup_actions(&mut self) {
    // If halves were taken back from the operational loop but not yet shut down, close them.
    if self.write_half.is_some() || self.read_half.is_some() {
      // No need to call engine.close() again if already called from graceful shutdown.
      if let Some(mut wh) = self.write_half.take() {
        let _ = wh.shutdown().await;
      }
      self.read_half = None;
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
