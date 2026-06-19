/// Increment, decrement, or load a counter.
///
/// Flat source increment (zero-cost no-op in non-debug, non-diagnostics builds):
///   `counter!(source_expr, field_name, inc)`
///
/// Flat source add:
///   `counter!(source_expr, field_name, add, value)`
///   `counter!(global, field_name, add, value)`
///
/// Load form (returns 0 in non-debug/non-diagnostics builds):
///   `counter!(global, field_name, load)`
#[macro_export]
macro_rules! counter {
  ($source:expr, $field:ident, inc) => {
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    {
      $source
        .$field
        .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
    }
  };
  (global, $field:ident, add, $val:expr) => {
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    {
      $crate::observability::active::METRICS
        .$field
        .fetch_add($val, ::std::sync::atomic::Ordering::Relaxed);
    }
  };
  ($source:expr, $field:ident, add, $val:expr) => {
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    {
      $source
        .$field
        .fetch_add($val, ::std::sync::atomic::Ordering::Relaxed);
    }
  };
  (global, $field:ident, load) => {{
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    {
      $crate::observability::active::METRICS
        .$field
        .load(::std::sync::atomic::Ordering::Relaxed)
    }
    #[cfg(not(any(debug_assertions, feature = "diagnostics")))]
    {
      0u64
    }
  }};
}

/// Record a write batch: categorizes by size and increments total_writes + total_messages.
/// Compiles to nothing when diagnostics are disabled.
#[macro_export]
macro_rules! metric_record_write_batch {
  ($source:expr, $count:expr) => {
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    {
      let count = $count;
      if count > 0 {
        $source
          .total_writes
          .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        $source
          .total_messages
          .fetch_add(count, ::std::sync::atomic::Ordering::Relaxed);
        if count == 1 {
          $source
            .batch_size_1
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        } else if count <= 8 {
          $source
            .batch_size_2_8
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        } else if count <= 16 {
          $source
            .batch_size_9_16
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        } else {
          $source
            .batch_size_17_32
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        }
      }
    }
  };
}

/// Categorize a CQE errno into the appropriate error counter.
/// Compiles to nothing when diagnostics are disabled.
#[macro_export]
macro_rules! metric_cqe_errno {
  ($source:expr, $errno:expr) => {
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    {
      let errno = $errno;
      match errno {
        libc::EPIPE | libc::ECONNRESET | libc::ENOTCONN => {
          $source
            .epipe_errors
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        }
        libc::EBADF => {
          $source
            .ebadf_errors
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        }
        libc::ECANCELED => {
          $source
            .ecanceled_errors
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        }
        libc::EINVAL => {
          $source
            .einval_errors
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        }
        _ => {
          $source
            .other_errors
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
        }
      }
    }
  };
}

/// Declare a mutable `Instant` timer only when diagnostics are enabled.
/// In release builds without `diagnostics`, expands to nothing — no stack allocation.
#[macro_export]
macro_rules! declare_timer {
  ($timer_name:ident) => {
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    let mut $timer_name = ::std::time::Instant::now();
  };
}

/// Measure elapsed time since `$timer_name`, add it to `$source.$field`, then reset the timer.
/// Compiles to nothing when diagnostics are disabled.
#[macro_export]
macro_rules! metric_time_phase {
  ($source:expr, $field:ident, $timer_name:ident) => {
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    {
      let elapsed = $timer_name.elapsed();
      $source
        .$field
        .fetch_add(elapsed.as_nanos() as u64, ::std::sync::atomic::Ordering::Relaxed);
      $timer_name = ::std::time::Instant::now();
    }
  };
}

/// Spawn the background io_uring observability thread.
/// Compiles to nothing when neither `debug_assertions` nor `diagnostics` is active,
/// or when the `io-uring` feature is not enabled.
#[macro_export]
macro_rules! spawn_uring_observability {
  ($metrics:expr) => {
    #[cfg(all(feature = "io-uring", any(debug_assertions, feature = "diagnostics")))]
    {
      $crate::observability::uring::spawn_observability_thread(::std::sync::Arc::clone(&$metrics));
    }
  };
}

/// Periodic session-actor diagnostic dump.
///
/// `$last_log_ms` must be a `&mut u64` local to the calling task — no atomics,
/// no cross-task sharing. Each actor tracks its own cadence independently.
/// Fires at most once every 2 seconds per actor; compiled out in release builds
/// unless the `diagnostics` feature is enabled.
///
/// Usage:
/// ```ignore
/// let mut last_log_ms = 0u64;
/// // ... inside the operational loop ...
/// log_session_diagnostics!(last_log_ms, self, ingress_buffer, egress_buffer, sndhwm, core_carryover);
/// ```
#[macro_export]
macro_rules! log_session_diagnostics {
  ($last_log_ms:ident, $actor:expr, $ingress_buf:expr, $egress_buf:expr, $sndhwm:expr, $core_carryover:expr) => {
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    {
      let now_ms = ::std::time::SystemTime::now()
        .duration_since(::std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

      if now_ms >= $last_log_ms + 2000 {
        $last_log_ms = now_ms;

        let pipe_sender_len = $actor.incoming_pipe_sender.as_ref().map(|s| s.len()).unwrap_or(0);
        let pipe_sender_cap = $actor.incoming_pipe_sender.as_ref().map(|s| s.capacity()).unwrap_or(0);
        let pipe_sender_queued = $actor.incoming_pipe_sender.as_ref().map(|s| s.queued_count()).unwrap_or(0);
        let pipe_sender_reserved = $actor.incoming_pipe_sender.as_ref().map(|s| s.reserved_count()).unwrap_or(0);

        println!(
          "[DIAG pid={} handle={}] ingress_buf={} | egress_pending={}/{} | carryover={} | core_pipe_len={} | pipe_sender={}/{} | queued={} reserved={} | congested={}",
          ::std::process::id(),
          $actor.handle,
          $ingress_buf.len(),
          $egress_buf.pending_messages(),
          $sndhwm,
          $core_carryover.len(),
          $actor.core_pipe_manager.len(),
          pipe_sender_len,
          pipe_sender_cap,
          pipe_sender_queued,
          pipe_sender_reserved,
          $actor.is_currently_congested,
        );

        if pipe_sender_len > 0 && pipe_sender_queued == 0 && pipe_sender_reserved == 0 {
          println!(
            "[INTEGRITY FAIL pid={} handle={}] pipe_sender has {} items in rx but queued={} reserved={} — DEADLOCK",
            ::std::process::id(),
            $actor.handle,
            pipe_sender_len,
            pipe_sender_queued,
            pipe_sender_reserved,
          );
        }
      }
    }
  };
}

/// Periodic egress-driver diagnostic dump.
///
/// Uses a global `OnceLock<Mutex<Instant>>` — multiple connections share one
/// print window (at most one line per second across all drivers). Compiled out
/// in release builds unless `diagnostics` is enabled.
#[macro_export]
macro_rules! log_egress_diagnostics {
  ($handle:expr, $pending_msgs:expr, $peak_msgs:expr, $peak_bytes:expr) => {
    #[cfg(any(debug_assertions, feature = "diagnostics"))]
    {
      use ::std::sync::atomic::Ordering;
      use ::std::time::{Duration, Instant};

      static LAST_PRINT_TIME: ::std::sync::OnceLock<::std::sync::Mutex<Instant>> =
        ::std::sync::OnceLock::new();

      let now = Instant::now();
      let mtx = LAST_PRINT_TIME.get_or_init(|| ::std::sync::Mutex::new(now));

      if let Ok(mut last) = mtx.try_lock() {
        let elapsed = now.duration_since(*last);
        if elapsed >= Duration::from_secs(1) {
          let total_bytes =
            $crate::observability::active::METRICS.global_bytes_pulled.load(Ordering::Relaxed);
          let last_bytes =
            $crate::observability::active::METRICS.last_bytes_sent.load(Ordering::Relaxed);
          let delta_bytes = total_bytes.saturating_sub(last_bytes);

          let mb_sec = (delta_bytes as f64 / 1_048_576.0) / elapsed.as_secs_f64();
          let total_mb = total_bytes as f64 / 1_048_576.0;

          println!(
            "[EgressDriver pid={} id={}] Total: {:.2} MB | Speed: {:.2} MB/s | Buffer Msgs: {} | Peak Msgs: {} | Peak Bytes: {} | Global Drained Msgs: {} | Global Pulled Msgs: {}",
            ::std::process::id(),
            $handle,
            total_mb,
            mb_sec,
            $pending_msgs,
            $peak_msgs,
            $peak_bytes,
            $crate::observability::active::METRICS.global_drained_msgs.load(Ordering::Relaxed),
            $crate::observability::active::METRICS.global_msgs_pulled.load(Ordering::Relaxed),
          );

          $crate::observability::active::METRICS
            .last_bytes_sent
            .store(total_bytes, Ordering::Relaxed);
          *last = now;
        }
      }
    }
  };
}
