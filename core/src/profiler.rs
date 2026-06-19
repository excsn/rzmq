#[cfg(debug_assertions)]
mod active {
  use std::time::{Duration, Instant};

  const MAX_SEGMENTS: usize = 8;

  pub struct ProfileGuard<'a> {
    profiler: &'a mut LoopProfiler,
    index: usize,
    start_time: Instant,
  }

  impl<'a> Drop for ProfileGuard<'a> {
    #[inline(always)]
    fn drop(&mut self) {
      let elapsed = self.start_time.elapsed();
      if self.index < MAX_SEGMENTS {
        self.profiler.durations[self.index] = elapsed;
      }
    }
  }

  pub struct LoopProfiler {
    pub(super) loop_start_time: Instant,
    pub(super) durations: [Duration; MAX_SEGMENTS],
    pub(super) labels: [&'static str; MAX_SEGMENTS],
    pub(super) threshold: Duration,
    pub(super) log_counter: usize,
    pub(super) log_interval: usize,
  }

  impl LoopProfiler {
    pub fn new(threshold: Duration, log_interval: usize) -> Self {
      Self {
        loop_start_time: Instant::now(),
        durations: [Duration::ZERO; MAX_SEGMENTS],
        labels: [""; MAX_SEGMENTS],
        threshold,
        log_counter: 0,
        log_interval,
      }
    }

    #[inline(always)]
    pub fn loop_start(&mut self) {
      self.loop_start_time = Instant::now();
      self.durations = [Duration::ZERO; MAX_SEGMENTS];
    }

    #[inline(always)]
    pub fn profile(&mut self, index: usize, label: &'static str) -> ProfileGuard<'_> {
      if index < MAX_SEGMENTS {
        self.labels[index] = label;
      }
      ProfileGuard {
        profiler: self,
        index,
        start_time: Instant::now(),
      }
    }

    pub fn log_and_reset_for_next_loop(&mut self) {
      let total_loop_duration = self.loop_start_time.elapsed();
      self.log_counter = self.log_counter.wrapping_add(1);

      if total_loop_duration > self.threshold
        || (self.log_interval > 0 && self.log_counter % self.log_interval == 0)
      {
        tracing::trace!(
          target: "rzmq::worker_latency",
          total_us = total_loop_duration.as_micros(),
          s0_label = self.labels[0], s0_us = self.durations[0].as_micros(),
          s1_label = self.labels[1], s1_us = self.durations[1].as_micros(),
          s2_label = self.labels[2], s2_us = self.durations[2].as_micros(),
          s3_label = self.labels[3], s3_us = self.durations[3].as_micros(),
          s4_label = self.labels[4], s4_us = self.durations[4].as_micros(),
          s5_label = self.labels[5], s5_us = self.durations[5].as_micros(),
          s6_label = self.labels[6], s6_us = self.durations[6].as_micros(),
          s7_label = self.labels[7], s7_us = self.durations[7].as_micros(),
        );
      }
      self.loop_start();
    }
  }
}

#[cfg(not(debug_assertions))]
mod stub {
  use std::time::Duration;

  #[derive(Debug, Clone, Copy, Default)]
  pub struct ProfileGuard;

  #[derive(Debug, Clone, Copy, Default)]
  pub struct LoopProfiler;

  impl LoopProfiler {
    #[inline(always)]
    pub fn new(_threshold: Duration, _log_interval: usize) -> Self {
      LoopProfiler
    }

    #[inline(always)]
    pub fn loop_start(&mut self) {}

    #[inline(always)]
    pub fn profile(&mut self, _index: usize, _label: &'static str) -> ProfileGuard {
      ProfileGuard
    }

    #[inline(always)]
    pub fn log_and_reset_for_next_loop(&mut self) {}
  }
}

#[cfg(debug_assertions)]
pub(crate) use active::{LoopProfiler, ProfileGuard};

#[cfg(not(debug_assertions))]
pub(crate) use stub::{LoopProfiler, ProfileGuard};
