// core/src/socket/patterns/incoming_orchestrator.rs

use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::socket::patterns::fair_queue::{FairQueue, PushError};
use linked_hash_map::LinkedHashMap;
use parking_lot::{Mutex, RwLock}; // Using parking_lot locks for sync access
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout as tokio_timeout;

/// Holds a complete, processed, application-level logical message if staged.
#[derive(Debug, Default)]
struct PipeStagingSlot {
  message: RwLock<Option<Vec<Msg>>>, // Vec<Msg> is an application-level logical message
}

#[derive(Debug)]
pub(crate) struct IncomingMessageOrchestrator {
  socket_core_handle: usize,
  main_incoming_queue: FairQueue, // Stores individual Msg frames of processed application messages

  // Stores entire Vec<Msg> (application-level logical messages) if main_incoming_queue is full
  per_pipe_staging_slots: RwLock<LinkedHashMap<usize /*pipe_read_id*/, Arc<PipeStagingSlot>>>,

  // Stores raw ZMTP frames until a complete ZMTP message from a pipe is assembled
  // Uses parking_lot::Mutex for synchronous locking within accumulate_pipe_frame
  partial_pipe_messages: Mutex<HashMap<usize, Vec<Msg>>>,
}

impl IncomingMessageOrchestrator {
  pub fn new(socket_core_handle: usize, rcvhwm: usize) -> Self {
    Self {
      socket_core_handle,
      main_incoming_queue: FairQueue::new(rcvhwm.max(1)),
      per_pipe_staging_slots: RwLock::new(LinkedHashMap::new()),
      partial_pipe_messages: Mutex::new(HashMap::new()),
    }
  }

  /// Accumulates an incoming frame from a pipe.
  /// If this frame completes a raw ZMTP message from that pipe, the assembled
  /// Vec<Msg> (raw ZMTP message) is returned. Otherwise, Ok(None) is returned.
  /// This method uses synchronous locks for `partial_pipe_messages` and should not .await while holding them.
  pub fn accumulate_pipe_frame(
    // Changed to sync fn as it only uses sync locks
    &self,
    pipe_read_id: usize,
    incoming_frame: Msg,
  ) -> Result<Option<Vec<Msg>>, ZmqError> {
    // Return type doesn't need to be async Future
    let mut partial_guard = self.partial_pipe_messages.lock(); // parking_lot::Mutex guard (sync)

    let pipe_buffer = partial_guard.entry(pipe_read_id).or_insert_with(Vec::new);
    let is_last_frame_of_zmtp_message = !incoming_frame.is_more();
    pipe_buffer.push(incoming_frame);

    if is_last_frame_of_zmtp_message {
      let assembled_raw_message = partial_guard.remove(&pipe_read_id).unwrap_or_else(|| {
        tracing::error!(
          "Orchestrator: Inconsistency, key {} not found after or_insert_with in accumulate_pipe_frame.",
          pipe_read_id
        );
        Vec::new() // Should ideally not happen
      });
      drop(partial_guard);

      if assembled_raw_message.is_empty() && !is_last_frame_of_zmtp_message
      /* this condition is tricky if it was only one frame */
      {
        // This case, an empty message that isn't the last part, is odd for ZMTP.
        // If it was a single empty frame that *is* the last part, it's valid.
        // Let's assume if it's empty, it's the only frame of an empty ZMTP message.
        tracing::warn!(
          "Orchestrator: Assembled an empty raw ZMTP message vector for pipe {}. This is unusual.",
          pipe_read_id
        );
      }
      Ok(Some(assembled_raw_message))
    } else {
      drop(partial_guard);
      Ok(None)
    }
  }

  /// Queues a processed, application-level logical message.
  /// Corrects MORE flags, then frames are pushed to the main_incoming_queue
  /// or the entire logical message is staged if the main queue is full.
  pub async fn queue_application_message_frames(
    &self,
    pipe_read_id_for_staging_key: usize,
    mut app_logical_message: Vec<Msg>,
  ) -> Result<(), ZmqError> {
    if app_logical_message.is_empty() {
      tracing::trace!(
        handle = self.socket_core_handle,
        pipe_id = pipe_read_id_for_staging_key,
        "Orchestrator: Processor returned empty logical message. Queueing a single empty frame."
      );
      app_logical_message.push(Msg::new()); // Default Msg has no MORE flag.
    }

    // Correct MORE flags for the application_logical_message
    let num_frames = app_logical_message.len();
    if num_frames > 0 {
      // Ensure there's at least one frame before trying to get last_idx
      let last_idx = num_frames - 1;
      for (i, frame) in app_logical_message.iter_mut().enumerate() {
        if i < last_idx {
          frame.set_flags(frame.flags() | MsgFlags::MORE);
        } else {
          frame.set_flags(frame.flags() & !MsgFlags::MORE);
        }
      }
    }

    match self
      .attempt_push_app_logical_message_to_main_queue(app_logical_message)
      .await
    {
      Ok(None) => {
        // All frames successfully pushed to main_incoming_queue.
        tracing::trace!(
          handle = self.socket_core_handle,
          pipe_id = pipe_read_id_for_staging_key,
          "Orchestrator: All application message frames queued to main_incoming_queue."
        );
        let _ = self.try_flush_one_staged_message_from_ordered_list().await;
      }
      Ok(Some(message_to_stage)) => {
        // Main queue became full, stage the entire logical message.
        tracing::debug!(
          handle = self.socket_core_handle,
          pipe_id = pipe_read_id_for_staging_key,
          num_frames_to_stage = message_to_stage.len(),
          "Orchestrator: Main queue full. Staging application logical message."
        );

        let slot_arc = {
          let mut map_write_guard = self.per_pipe_staging_slots.write();
          map_write_guard
            .entry(pipe_read_id_for_staging_key)
            .or_insert_with(|| Arc::new(PipeStagingSlot::default()))
            .clone()
        };
        let mut specific_pipe_slot_guard = slot_arc.message.write();
        if specific_pipe_slot_guard.is_none() {
          *specific_pipe_slot_guard = Some(message_to_stage);
        } else {
          tracing::warn!(
                handle = self.socket_core_handle,
                pipe_id = pipe_read_id_for_staging_key,
                "Orchestrator: Per-pipe staging slot for pipe {} is already full. Dropping new logical message ({} frames).",
                pipe_read_id_for_staging_key, message_to_stage.len()
            );
        }
      }
      Err((_failed_parts, e)) => {
        return Err(e);
      }
    }
    Ok(())
  }

  /// Helper: Tries to push all frames of an application logical message to the main_incoming_queue.
  /// - Ok(None): All frames were pushed successfully. Consumes `app_logical_message_frames`.
  /// - Ok(Some(remainder_frames)): Main queue became full. `remainder_frames` are the ones not pushed
  ///   (including the one that failed and all subsequent ones from the original `app_logical_message_frames`).
  /// - Err((original_message_on_close, ZmqError)): Critical error (queue closed).
  async fn attempt_push_app_logical_message_to_main_queue(
    &self,
    app_logical_message_frames: Vec<Msg>,
  ) -> Result<Option<Vec<Msg>>, (Vec<Msg>, ZmqError)> {
    // Changed return type to Vec<Msg> for failed_parts
    let mut unpushed_frames: Vec<Msg> = Vec::new();
    let mut iter = app_logical_message_frames.into_iter();

    for frame_to_try in iter.by_ref() {
      // by_ref() allows extending iter later
      match self.main_incoming_queue.try_push_message(frame_to_try) {
        Ok(()) => { /* Successfully pushed this frame */ }
        Err(PushError::Full(returned_frame)) => {
          unpushed_frames.push(returned_frame);
          unpushed_frames.extend(iter); // Add all subsequent frames from the original iterator
          break; // Stop trying to push
        }
        Err(PushError::Closed(returned_frame)) => {
          unpushed_frames.push(returned_frame);
          unpushed_frames.extend(iter);
          let err_msg = "Main incoming queue closed while pushing application frames.".to_string();
          tracing::error!(handle = self.socket_core_handle, "{}", err_msg);
          return Err((unpushed_frames, ZmqError::Internal(err_msg)));
        }
      }
    }

    if unpushed_frames.is_empty() {
      Ok(None) // All successfully pushed
    } else {
      Ok(Some(unpushed_frames)) // Some frames couldn't be pushed, return the remainder
    }
  }

  /// Tries to flush ONE logical message from the oldest pipe in staging to the main_incoming_queue.
  async fn try_flush_one_staged_message_from_ordered_list(&self) -> Result<bool, ZmqError> {
    let pipe_id_and_slot_arc_to_flush: Option<(usize, Arc<PipeStagingSlot>)> = {
      let map_read_guard = self.per_pipe_staging_slots.read();
      if map_read_guard.is_empty() {
        return Ok(true);
      }
      map_read_guard.iter().find_map(|(id, slot_arc)| {
        if slot_arc.message.read().is_some() {
          Some((*id, slot_arc.clone()))
        } else {
          None
        }
      })
    };

    if let Some((pipe_id_to_flush, slot_arc_to_flush)) = pipe_id_and_slot_arc_to_flush {
      let app_message_to_flush_opt: Option<Vec<Msg>> = {
        slot_arc_to_flush.message.write().take() // Atomically take the Vec<Msg>
      };

      if let Some(app_message_to_flush) = app_message_to_flush_opt {
        match self
          .attempt_push_app_logical_message_to_main_queue(app_message_to_flush)
          .await
        {
          Ok(None) => {
            // All frames successfully pushed from staging to main queue
            tracing::debug!(
              "Orchestrator: Flushed staged message from pipe {} to main queue.",
              pipe_id_to_flush
            );
            self.per_pipe_staging_slots.write().remove(&pipe_id_to_flush);
            return Ok(true);
          }
          Ok(Some(remainder_to_re_stage)) => {
            // Main queue became full during flush
            tracing::debug!(
              "Orchestrator: Main queue full during staging flush for pipe {}. Re-staging remainder.",
              pipe_id_to_flush
            );
            *slot_arc_to_flush.message.write() = Some(remainder_to_re_stage); // Put it back
                                                                              // Move to end of LinkedHashMap for fairness
            let mut map_write_guard = self.per_pipe_staging_slots.write();
            if let Some(arc_val) = map_write_guard.remove(&pipe_id_to_flush) {
              map_write_guard.insert(pipe_id_to_flush, arc_val);
            } // If not found, it means another thread removed it (e.g. clear_pipe_state) - slot_arc_to_flush is still valid
            return Ok(false); // Progress not fully made
          }
          Err((failed_message_parts_on_err, e)) => {
            // Critical error
            tracing::error!(
              "Orchestrator: Error flushing staged for pipe {}: {:?}. Re-staging.",
              pipe_id_to_flush,
              e
            );
            *slot_arc_to_flush.message.write() = Some(failed_message_parts_on_err);
            return Err(e);
          }
        }
      } else {
        // Slot was empty when we tried to take
        let mut map_write_guard = self.per_pipe_staging_slots.write();
        if let Some(slot_in_map) = map_write_guard.get(&pipe_id_to_flush) {
          if slot_in_map.message.read().is_none() {
            map_write_guard.remove(&pipe_id_to_flush);
          }
        }
        return Ok(true);
      }
    }
    Ok(true) // No pipe found with a message staged
  }

  /// Called by ISocket::recv(). Returns the next single application-level frame.
  pub async fn recv_message(&self, rcvtimeo_opt: Option<Duration>) -> Result<Msg, ZmqError> {
    match self.try_flush_one_staged_message_from_ordered_list().await {
      Ok(true) => { /* Staging was empty or successfully flushed one message. */ }
      Ok(false) => { /* Staging had a message, but main queue was full. `pop_message` below might still succeed if something else was already in mainQ. */
      }
      Err(e) => {
        tracing::warn!(
          "Orchestrator: Error flushing staged message during recv_message: {:?}",
          e
        );
      }
    }

    let pop_future = self.main_incoming_queue.pop_message();
    match rcvtimeo_opt {
      Some(duration) if !duration.is_zero() => match tokio_timeout(duration, pop_future).await {
        Ok(Ok(Some(msg))) => Ok(msg),
        Ok(Ok(None)) => Err(ZmqError::Internal("Orchestrator: Main receive queue closed".into())),
        Ok(Err(e)) => Err(e),
        Err(_timeout_elapsed) => Err(ZmqError::Timeout),
      },
      _ => match pop_future.await? {
        // Handles None (infinite) or Zero duration as blocking for pop_message
        Some(msg) => Ok(msg),
        None => Err(ZmqError::Internal(
          "Orchestrator: Main receive queue closed (inf wait)".into(),
        )),
      },
    }
  }

  /// Called by ISocket::recv_multipart(). Returns all frames of one application-level logical message.
  pub async fn recv_logical_message(&self, rcvtimeo_opt: Option<Duration>) -> Result<Vec<Msg>, ZmqError> {
    let mut message_parts = Vec::new();
    let overall_deadline_opt = rcvtimeo_opt.map(|d| tokio::time::Instant::now() + d);

    loop {
      let current_part_rcvtimeo: Option<Duration> = {
        if let Some(deadline) = overall_deadline_opt {
          let now = tokio::time::Instant::now();
          if now >= deadline {
            // If timeout already passed and we have no parts, it's a timeout.
            // If we have parts, it means timeout waiting for *next* part.
            if message_parts.is_empty() {
              return Err(ZmqError::Timeout);
            } else {
              tracing::warn!("Orchestrator: Timeout receiving subsequent part of multi-part message. Partial message ({} parts) will be discarded by caller if error returned.", message_parts.len());
              return Err(ZmqError::ProtocolViolation("Timeout during multi-part recv".into()));
            }
          }
          Some(deadline - now)
        } else {
          None
        }
      };

      // Use self.recv_message() which handles staging flush + pop from main_incoming_queue
      match self.recv_message(current_part_rcvtimeo).await {
        Ok(frame) => {
          let is_last_part = !frame.is_more();
          message_parts.push(frame);
          if is_last_part {
            return Ok(message_parts);
          }
        }
        Err(ZmqError::Timeout) => {
          // Explicitly from recv_message's timeout logic
          if message_parts.is_empty() {
            return Err(ZmqError::Timeout);
          } else {
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
          // Other errors like queue closed
          if message_parts.is_empty() {
            return Err(e);
          }
          // If error on first frame, propagate
          else {
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
    if self.partial_pipe_messages.lock().remove(&pipe_read_id).is_some() {
      tracing::debug!(
        "Orchestrator: Cleared partial message buffer for detached pipe {}",
        pipe_read_id
      );
    }
    if self.per_pipe_staging_slots.write().remove(&pipe_read_id).is_some() {
      tracing::debug!(
        "Orchestrator: Cleared staged message slot for detached pipe {}",
        pipe_read_id
      );
    }
    // Note: main_incoming_queue is shared and not cleared per-pipe here.
    // Messages already in main_incoming_queue from this pipe will remain.
    // This is generally fine as they are valid application messages.
  }
}
