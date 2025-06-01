// core/src/io_uring_backend/worker/profiler.rs

#![cfg(feature = "io-uring")] // Keep this if the profiler is io_uring specific

// Conditional imports for profiling, only active when debug_assertions is true
#[cfg(debug_assertions)]
use std::time::{Duration, Instant};
#[cfg(debug_assertions)]
use tracing; // Assuming tracing is used for logging (already a dependency)
#[cfg(debug_assertions)]
use std::vec::Vec; // Explicit import for clarity if needed

// --- LoopProfiler definition: Active version for debug builds ---
#[cfg(debug_assertions)]
#[derive(Debug)]
pub(crate) struct SegmentTiming {
  label: &'static str,
  duration: Duration,
}

#[cfg(debug_assertions)]
#[derive(Debug)]
pub(crate) struct LoopProfiler {
  loop_start_time: Instant,
  current_segment_start_time: Instant,
  current_segment_label: &'static str,
  segment_timings: Vec<SegmentTiming>, 
  threshold: Duration, // Only log if total loop time exceeds this
  log_counter: usize,
  log_interval: usize, // Log every N iterations if below threshold (if > 0)
}

#[cfg(debug_assertions)]
impl LoopProfiler {
  const INITIAL_SEGMENT_LABEL: &'static str = "loop_setup";

  pub fn new(threshold: Duration, log_interval: usize) -> Self {
    let now = Instant::now();
    Self {
      loop_start_time: now,
      current_segment_start_time: now,
      current_segment_label: Self::INITIAL_SEGMENT_LABEL,
      segment_timings: Vec::with_capacity(10), // Pre-allocate
      threshold,
      log_counter: 0,
      log_interval,
    }
  }

  pub fn loop_start(&mut self) {
    self.loop_start_time = Instant::now();
    self.current_segment_start_time = self.loop_start_time;
    self.current_segment_label = Self::INITIAL_SEGMENT_LABEL;
    self.segment_timings.clear();
  }

  pub fn mark_segment_end_and_start_new(&mut self, next_segment_label: &'static str) {
    let now = Instant::now();
    let ended_segment_duration = now.duration_since(self.current_segment_start_time);
    self.segment_timings.push(SegmentTiming {
      label: self.current_segment_label,
      duration: ended_segment_duration,
    });
    self.current_segment_start_time = now;
    self.current_segment_label = next_segment_label;
  }

  pub fn log_and_reset_for_next_loop(&mut self) {
    let loop_end_time = Instant::now();
    let final_segment_duration = loop_end_time.duration_since(self.current_segment_start_time);
    self.segment_timings.push(SegmentTiming {
      label: self.current_segment_label,
      duration: final_segment_duration,
    });

    let total_loop_duration = loop_end_time.duration_since(self.loop_start_time);
    self.log_counter = self.log_counter.wrapping_add(1);

    if total_loop_duration > self.threshold || (self.log_interval > 0 && self.log_counter % self.log_interval == 0) {
      let mut log_output = format!("[UringWorker Latency] Total: {:?}", total_loop_duration);
      for timing in &self.segment_timings {
        if timing.duration > Duration::from_micros(1) || timing.duration.as_nanos() > 0 {
            log_output.push_str(&format!(", {}: {:?}", timing.label, timing.duration));
        } else if total_loop_duration > Duration::ZERO {
            log_output.push_str(&format!(", {}: ~0us", timing.label));
        }
      }
      // Use a specific tracing target for these latency logs
      tracing::warn!(target: "rzmq::worker_latency", "{}", log_output);
    }
    
    self.loop_start(); // Reset for the next loop iteration
  }
}

// --- LoopProfiler definition: Stub version for release builds (not debug_assertions) ---
#[cfg(not(debug_assertions))]
#[derive(Debug, Clone, Copy)] // Can be Copy if it's a ZST
pub(crate) struct LoopProfiler; // Zero-sized type

#[cfg(not(debug_assertions))]
impl LoopProfiler {
  // Constructor takes same args but does nothing with them, returns ZST
  #[inline(always)] // Hint to compiler to optimize away
  pub fn new(_threshold: std::time::Duration, _log_interval: usize) -> Self { LoopProfiler }
  
  // Methods are no-ops, should be optimized out by the compiler
  #[inline(always)]
  pub fn loop_start(&mut self) {}
  
  #[inline(always)]
  pub fn mark_segment_end_and_start_new(&mut self, _next_segment_label: &'static str) {}
  
  #[inline(always)]
  pub fn log_and_reset_for_next_loop(&mut self) {}
}