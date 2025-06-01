use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::socket::core::SocketCore;
use crate::socket::patterns::fair_queue::{FairQueue, PushError};
// use parking_lot::Mutex; // No longer needed if only used for partial_pipe_messages
use dashmap::DashMap; // Added
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout as tokio_timeout;

#[derive(Debug)]
pub(crate) struct IncomingMessageOrchestrator {
    socket_core_handle: usize,
    main_incoming_queue: FairQueue,
    queue_push_timeout: Option<Duration>, // Timeout for pushing to main_incoming_queue
    partial_pipe_messages: DashMap<usize, Vec<Msg>>, // Changed from Mutex<HashMap<...>>
}

impl IncomingMessageOrchestrator {
    pub fn new(core: &Arc<SocketCore>) -> Self {
        let rcvhwm = core.core_state.read().options.rcvhwm;
        Self::new_with_hwm(&core, rcvhwm)
    }

    pub(crate) fn new_with_hwm(core: &Arc<SocketCore>, rcvhwm: usize) -> Self {
        Self {
            socket_core_handle: core.handle,
            main_incoming_queue: FairQueue::new(rcvhwm.max(1)),
            queue_push_timeout: Some(Duration::from_millis(1000)),
            partial_pipe_messages: DashMap::new(), // Changed
        }
    }

    pub fn accumulate_pipe_frame(
        &self,
        pipe_read_id: usize,
        incoming_frame: Msg,
    ) -> Result<Option<Vec<Msg>>, ZmqError> {
        let is_last_frame_of_zmtp_message = !incoming_frame.is_more();

        if is_last_frame_of_zmtp_message {
            // This is the last frame.
            // Atomically remove any existing partial message parts.
            let mut assembled_message = self
                .partial_pipe_messages
                .remove(&pipe_read_id)
                .map(|(_key, val)| val) // Extract the Vec<Msg> from (key, value) tuple
                .unwrap_or_else(Vec::new); // If no prior parts, start with an empty Vec

            assembled_message.push(incoming_frame); // Add the current (last) frame

            // After pushing the current frame, the message should not be empty
            // unless Msg::new() creates an "empty" message that Vec considers non-empty
            // or if incoming_frame was somehow an "empty" frame representation.
            if assembled_message.is_empty() {
                tracing::warn!(
                    "Orchestrator: Assembled an empty ZMTP message frame vector for pipe {}. This is unusual.",
                    pipe_read_id
                );
            }
            Ok(Some(assembled_message))
        } else {
            // This is not the last frame, accumulate it.
            // `entry` provides fine-grained locking for the specific key.
            let mut pipe_buffer_entry = self
                .partial_pipe_messages
                .entry(pipe_read_id)
                .or_insert_with(Vec::new);
            pipe_buffer_entry.value_mut().push(incoming_frame);
            // The lock on the entry is released when pipe_buffer_entry goes out of scope.
            Ok(None)
        }
    }

    /// Queues a processed, application-level logical message.
    /// Corrects MORE flags, then frames are pushed to the main_incoming_queue
    /// with a timeout if the queue is full. Drops frames if timeout occurs.
    pub async fn queue_application_message_frames(
        &self,
        pipe_read_id_for_logging: usize,
        mut app_logical_message: Vec<Msg>,
    ) -> Result<(), ZmqError> {
        if app_logical_message.is_empty() {
            tracing::trace!(
                handle = self.socket_core_handle,
                pipe_id = pipe_read_id_for_logging,
                "Orchestrator: Processor returned empty logical message. Queueing a single empty frame."
            );
            app_logical_message.push(Msg::new());
        }

        let num_frames = app_logical_message.len();
        // This check ensures we don't panic on last_idx if num_frames is 0,
        // though the above block ensures num_frames is at least 1.
        if num_frames > 0 {
            let last_idx = num_frames - 1;
            for (i, frame) in app_logical_message.iter_mut().enumerate() {
                if i < last_idx {
                    frame.set_flags(frame.flags() | MsgFlags::MORE);
                } else {
                    frame.set_flags(frame.flags() & !MsgFlags::MORE);
                }
            }
        }

        for (idx, frame_to_push) in app_logical_message.into_iter().enumerate() {
            let push_result = match self.queue_push_timeout {
                None => {
                    // Infinite timeout (blocking send)
                    self.main_incoming_queue.push_message(frame_to_push).await
                }
                Some(duration) if duration.is_zero() => {
                    // Non-blocking send (try_send)
                    match self.main_incoming_queue.try_push_message(frame_to_push) {
                        Ok(()) => Ok(()),
                        Err(PushError::Full(_returned_msg)) => {
                            tracing::warn!(handle = self.socket_core_handle, pipe_id = pipe_read_id_for_logging, "Orchestrator try_push to main queue failed: Full (ResourceLimitReached).");
                            Err(ZmqError::ResourceLimitReached)
                        }
                        Err(PushError::Closed(_returned_msg)) => {
                            tracing::error!(handle = self.socket_core_handle, pipe_id = pipe_read_id_for_logging, "Orchestrator try_push to main queue failed: Closed.");
                            Err(ZmqError::Internal("Main incoming queue closed".into()))
                        }
                    }
                }
                Some(duration) => {
                    // Timed blocking send
                    match tokio_timeout(duration, self.main_incoming_queue.push_message(frame_to_push)).await
                    {
                        Ok(Ok(())) => Ok(()), // Successfully sent within timeout
                        Ok(Err(e)) => Err(e), // Error from push_message (e.g. closed queue)
                        Err(_timeout_elapsed) => {
                            // Timeout elapsed
                            tracing::trace!(handle = self.socket_core_handle, pipe_id = pipe_read_id_for_logging, "Orchestrator timed push to main queue failed: Timeout.");
                            Err(ZmqError::Timeout)
                        }
                    }
                }
            };

            match push_result {
                Ok(()) => { /* Frame successfully pushed */ }
                Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
                    tracing::warn!(
                        handle = self.socket_core_handle,
                        pipe_id = pipe_read_id_for_logging,
                        frame_index = idx,
                        total_frames = num_frames,
                        "Orchestrator: Main queue full/timeout when pushing frame. Dropping this and subsequent frames of the logical message."
                    );
                    // The remaining frames are implicitly dropped as we break the loop.
                    return Ok(());
                }
                Err(e) => {
                    // Other errors (e.g., queue closed)
                    tracing::error!(
                        handle = self.socket_core_handle,
                        pipe_id = pipe_read_id_for_logging,
                        error = %e,
                        "Orchestrator: Error pushing to main queue. Dropping message and propagating error."
                    );
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Called by ISocket::recv(). Returns the next single application-level frame.
    pub async fn recv_message(&self, rcvtimeo_opt: Option<Duration>) -> Result<Msg, ZmqError> {
        let pop_future = self.main_incoming_queue.pop_message();
        match rcvtimeo_opt {
            Some(duration) if !duration.is_zero() => {
                // Timed pop
                match tokio_timeout(duration, pop_future).await {
                    Ok(Ok(Some(msg))) => Ok(msg),
                    Ok(Ok(None)) => Err(ZmqError::Internal(
                        "Orchestrator: Main receive queue closed".into(),
                    )),
                    Ok(Err(e)) => Err(e), // Error from pop_message itself
                    Err(_timeout_elapsed) => Err(ZmqError::Timeout),
                }
            }
            // Handles None (infinite wait) and Some(Duration::ZERO).
            // Note: If Duration::ZERO is passed, it currently results in an infinite wait
            // unless FairQueue::pop_message() itself has special handling for zero duration
            // or a FairQueue::try_pop_message() method is used for that case.
            _ => {
                // Infinite wait (or how FairQueue handles zero duration if passed)
                match pop_future.await? {
                    Some(msg) => Ok(msg),
                    None => Err(ZmqError::Internal(
                        "Orchestrator: Main receive queue closed (inf/zero wait)".into(),
                    )),
                }
            }
        }
    }

    /// Called by ISocket::recv_multipart(). Returns all frames of one application-level logical message.
    pub async fn recv_logical_message(
        &self,
        rcvtimeo_opt: Option<Duration>,
    ) -> Result<Vec<Msg>, ZmqError> {
        let mut message_parts = Vec::new();
        let overall_deadline_opt = rcvtimeo_opt.map(|d| tokio::time::Instant::now() + d);

        loop {
            let current_part_rcvtimeo: Option<Duration> = {
                if let Some(deadline) = overall_deadline_opt {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        // Overall timeout for the logical message has been reached.
                        if message_parts.is_empty() {
                            return Err(ZmqError::Timeout); // Timeout before receiving any part.
                        } else {
                            // Timeout after receiving some parts. This is a protocol violation.
                            tracing::warn!("Orchestrator: Timeout receiving subsequent part of multi-part message. Partial message ({} parts) will be discarded.", message_parts.len());
                            return Err(ZmqError::ProtocolViolation(
                                "Timeout during multi-part recv, discarding partial.".into(),
                            ));
                        }
                    }
                    Some(deadline - now) // Calculate remaining time for this part.
                } else {
                    None // No overall timeout, so no timeout for this part either (infinite).
                }
            };

            match self.recv_message(current_part_rcvtimeo).await {
                Ok(frame) => {
                    let is_last_part = !frame.is_more();
                    message_parts.push(frame);
                    if is_last_part {
                        return Ok(message_parts);
                    }
                    // If not the last part, loop to receive the next part.
                    // The deadline logic will handle timeouts for subsequent parts.
                }
                Err(ZmqError::Timeout) => {
                    // Timeout receiving a specific part.
                    if message_parts.is_empty() {
                        return Err(ZmqError::Timeout); // Timeout on the very first part.
                    } else {
                        // Timeout after receiving some parts. This is a protocol violation.
                        tracing::warn!(
                            "Orchestrator: Timeout receiving subsequent part of multi-part message. Discarding partial ({} frames).",
                            message_parts.len()
                        );
                        return Err(ZmqError::ProtocolViolation(
                            "Timeout during multi-part recv, discarding partial.".into(),
                        ));
                    }
                }
                Err(e) => {
                    // Other error receiving a part.
                    if message_parts.is_empty() {
                        return Err(e); // Error on the very first part.
                    } else {
                        // Error after receiving some parts. This is a protocol violation.
                        tracing::warn!("Orchestrator: Error ({:?}) receiving subsequent part of multi-part message. Discarding partial ({} frames).", e, message_parts.len());
                        return Err(ZmqError::ProtocolViolation(format!(
                            "Error during multi-part recv: {}, discarding partial.",
                            e
                        )));
                    }
                }
            }
        }
    }

    /// Called by ISocket's `pipe_detached` to clean up.
    pub async fn clear_pipe_state(&self, pipe_read_id: usize) {
        if self.partial_pipe_messages.remove(&pipe_read_id).is_some() {
            tracing::debug!(
                "Orchestrator: Cleared partial message buffer for detached pipe {}",
                pipe_read_id
            );
        }
    }
}