#![cfg(feature = "io-uring")]

#[cfg(any(debug_assertions, feature = "diagnostics"))]
pub mod active {
  use std::sync::Arc;
  use std::sync::atomic::{AtomicU64, Ordering};
  use std::time::{Duration, Instant};

  #[derive(Debug, Default)]
  pub struct UringMetrics {
    pub loop_iterations: AtomicU64,
    pub sqes_submitted: AtomicU64,
    pub cqes_reaped: AtomicU64,
    pub wakeup_signals: AtomicU64,
    pub eagain_errors: AtomicU64,
    pub enobufs_errors: AtomicU64,
    pub total_writes: AtomicU64,
    pub total_messages: AtomicU64,
    pub batch_size_1: AtomicU64,
    pub batch_size_2_8: AtomicU64,
    pub batch_size_9_16: AtomicU64,
    pub batch_size_17_32: AtomicU64,
    pub time_gather_ns: AtomicU64,
    pub time_process_ns: AtomicU64,
    pub time_reads_ns: AtomicU64,
    pub time_submit_ns: AtomicU64,
    pub time_cqe_ns: AtomicU64,
    pub empty_loops: AtomicU64,
    pub write_stalls: AtomicU64,
    pub tokio_backpressures: AtomicU64,
    pub send_pool_exhaustions: AtomicU64,
    pub recv_ring_exhaustions: AtomicU64,
    pub epipe_errors: AtomicU64,
    pub ebadf_errors: AtomicU64,
    pub ecanceled_errors: AtomicU64,
    pub einval_errors: AtomicU64,
    pub other_errors: AtomicU64,
    pub sqe_op_read: AtomicU64,
    pub sqe_op_write: AtomicU64,
    pub sqe_op_poll: AtomicU64,
    pub sqe_op_eventfd: AtomicU64,
    pub sqe_op_other: AtomicU64,
    pub write_in_flight_state: AtomicU64,
    pub egress_queue_len: AtomicU64,
  }

  pub fn spawn_observability_thread(metrics: Arc<UringMetrics>) {
    let pid = std::process::id();
    std::thread::Builder::new()
      .name("rzmq-uring-obs".into())
      .spawn(move || {
        let mut last_check = Instant::now();
        let mut last_loops: u64 = 0;
        let mut last_sqes: u64 = 0;
        let mut last_cqes: u64 = 0;
        let mut last_wakeups: u64 = 0;
        let mut last_eagain: u64 = 0;
        let mut last_enobufs: u64 = 0;
        let mut last_writes: u64 = 0;
        let mut last_messages: u64 = 0;
        let mut last_b1: u64 = 0;
        let mut last_b2_8: u64 = 0;
        let mut last_b9_16: u64 = 0;
        let mut last_b17_32: u64 = 0;
        let mut last_empty_loops: u64 = 0;
        let mut last_write_stalls: u64 = 0;
        let mut last_tokio_backpressures: u64 = 0;
        let mut last_send_pool_exhaustions: u64 = 0;
        let mut last_recv_ring_exhaustions: u64 = 0;
        let mut last_epipe: u64 = 0;
        let mut last_ebadf: u64 = 0;
        let mut last_ecanceled: u64 = 0;
        let mut last_einval: u64 = 0;
        let mut last_other_errors: u64 = 0;

        loop {
          std::thread::sleep(Duration::from_millis(1000));

          let now = Instant::now();
          let elapsed_secs = now.duration_since(last_check).as_secs_f64();
          if elapsed_secs <= 0.001 {
            continue;
          }

          let loops    = metrics.loop_iterations.load(Ordering::Relaxed);
          let sqes     = metrics.sqes_submitted.load(Ordering::Relaxed);
          let cqes     = metrics.cqes_reaped.load(Ordering::Relaxed);
          let wakeups  = metrics.wakeup_signals.load(Ordering::Relaxed);
          let eagain   = metrics.eagain_errors.load(Ordering::Relaxed);
          let enobufs  = metrics.enobufs_errors.load(Ordering::Relaxed);
          let writes   = metrics.total_writes.load(Ordering::Relaxed);
          let messages = metrics.total_messages.load(Ordering::Relaxed);
          let b1       = metrics.batch_size_1.load(Ordering::Relaxed);
          let b2_8     = metrics.batch_size_2_8.load(Ordering::Relaxed);
          let b9_16    = metrics.batch_size_9_16.load(Ordering::Relaxed);
          let b17_32   = metrics.batch_size_17_32.load(Ordering::Relaxed);

          let empty_loops           = metrics.empty_loops.load(Ordering::Relaxed);
          let write_stalls          = metrics.write_stalls.load(Ordering::Relaxed);
          let tokio_backpressures   = metrics.tokio_backpressures.load(Ordering::Relaxed);
          let send_pool_exhaustions = metrics.send_pool_exhaustions.load(Ordering::Relaxed);
          let recv_ring_exhaustions = metrics.recv_ring_exhaustions.load(Ordering::Relaxed);
          let epipe      = metrics.epipe_errors.load(Ordering::Relaxed);
          let ebadf      = metrics.ebadf_errors.load(Ordering::Relaxed);
          let ecanceled  = metrics.ecanceled_errors.load(Ordering::Relaxed);
          let einval     = metrics.einval_errors.load(Ordering::Relaxed);
          let other_errs = metrics.other_errors.load(Ordering::Relaxed);

          let inflight_lock = metrics.write_in_flight_state.load(Ordering::Relaxed);
          let q_len = metrics.egress_queue_len.load(Ordering::Relaxed);

          let t_gather  = metrics.time_gather_ns.swap(0, Ordering::Relaxed);
          let t_process = metrics.time_process_ns.swap(0, Ordering::Relaxed);
          let t_reads   = metrics.time_reads_ns.swap(0, Ordering::Relaxed);
          let t_submit  = metrics.time_submit_ns.swap(0, Ordering::Relaxed);
          let t_cqe     = metrics.time_cqe_ns.swap(0, Ordering::Relaxed);

          let r_loops   = ((loops   - last_loops)   as f64 / elapsed_secs) as u64;
          let r_sqes    = ((sqes    - last_sqes)    as f64 / elapsed_secs) as u64;
          let r_cqes    = ((cqes    - last_cqes)    as f64 / elapsed_secs) as u64;
          let r_wakeups = ((wakeups - last_wakeups) as f64 / elapsed_secs) as u64;
          let r_eagain  = ((eagain  - last_eagain)  as f64 / elapsed_secs) as u64;
          let r_enobufs = ((enobufs - last_enobufs) as f64 / elapsed_secs) as u64;
          let r_writes  = ((writes  - last_writes)  as f64 / elapsed_secs) as u64;

          let db1     = b1    - last_b1;
          let db2_8   = b2_8  - last_b2_8;
          let db9_16  = b9_16 - last_b9_16;
          let db17_32 = b17_32 - last_b17_32;

          let r_empty_loops  = ((empty_loops  - last_empty_loops)  as f64 / elapsed_secs) as u64;
          let r_write_stalls = ((write_stalls - last_write_stalls) as f64 / elapsed_secs) as u64;
          let r_tokio_bp     = ((tokio_backpressures - last_tokio_backpressures) as f64 / elapsed_secs) as u64;
          let r_send_exh     = ((send_pool_exhaustions - last_send_pool_exhaustions) as f64 / elapsed_secs) as u64;
          let r_recv_exh     = ((recv_ring_exhaustions - last_recv_ring_exhaustions) as f64 / elapsed_secs) as u64;
          let r_epipe        = ((epipe     - last_epipe)     as f64 / elapsed_secs) as u64;
          let r_ebadf        = ((ebadf     - last_ebadf)     as f64 / elapsed_secs) as u64;
          let r_ecanceled    = ((ecanceled - last_ecanceled) as f64 / elapsed_secs) as u64;
          let r_einval       = ((einval    - last_einval)    as f64 / elapsed_secs) as u64;
          let r_other_errs   = ((other_errs - last_other_errors) as f64 / elapsed_secs) as u64;

          let useful_pct = if loops - last_loops > 0 {
            100.0 * (1.0 - (empty_loops - last_empty_loops) as f64 / (loops - last_loops) as f64)
          } else {
            0.0
          };

          println!(
            "[uring-obs PID:{}] loops/s={} sqes/s={} cqes/s={} writes/s={} useful%={:.1} | wakes={} eagains={} enobufs={} | batches 1={} 2-8={} 9-16={} 17-32={} | phase_ns gather={} proc={} reads={} submit={} cqe={} | stalls={} tokio_bp={} send_exh={} recv_exh={} | err epipe={} ebadf={} ecanceled={} einval={} other={} | in_flight={} egress_q={}",
            pid,
            r_loops, r_sqes, r_cqes, r_writes, useful_pct,
            r_wakeups, r_eagain, r_enobufs,
            db1, db2_8, db9_16, db17_32,
            t_gather, t_process, t_reads, t_submit, t_cqe,
            r_write_stalls, r_tokio_bp, r_send_exh, r_recv_exh,
            r_epipe, r_ebadf, r_ecanceled, r_einval, r_other_errs,
            inflight_lock, q_len
          );

          let op_read  = metrics.sqe_op_read.load(Ordering::Relaxed);
          let op_write = metrics.sqe_op_write.load(Ordering::Relaxed);
          let op_poll  = metrics.sqe_op_poll.load(Ordering::Relaxed);
          let op_evfd  = metrics.sqe_op_eventfd.load(Ordering::Relaxed);
          let op_other = metrics.sqe_op_other.load(Ordering::Relaxed);

          println!(
            "[uring-ops PID:{}] OP_READ/s={} OP_WRITE/s={} OP_POLL/s={} OP_EVENTFD/s={} OP_OTHER/s={}",
            pid, op_read, op_write, op_poll, op_evfd, op_other
          );

          metrics.sqe_op_read.store(0, Ordering::Relaxed);
          metrics.sqe_op_write.store(0, Ordering::Relaxed);
          metrics.sqe_op_poll.store(0, Ordering::Relaxed);
          metrics.sqe_op_eventfd.store(0, Ordering::Relaxed);
          metrics.sqe_op_other.store(0, Ordering::Relaxed);

          last_check   = now;
          last_loops   = loops;
          last_sqes    = sqes;
          last_cqes    = cqes;
          last_empty_loops = empty_loops;
          last_write_stalls = write_stalls;
          last_tokio_backpressures = tokio_backpressures;
          last_send_pool_exhaustions = send_pool_exhaustions;
          last_recv_ring_exhaustions = recv_ring_exhaustions;
          last_epipe = epipe;
          last_ebadf = ebadf;
          last_ecanceled = ecanceled;
          last_einval = einval;
          last_other_errors = other_errs;
          last_wakeups = wakeups;
          last_eagain  = eagain;
          last_enobufs = enobufs;
          last_writes  = writes;
          last_messages = messages;
          last_b1      = b1;
          last_b2_8    = b2_8;
          last_b9_16   = b9_16;
          last_b17_32  = b17_32;
        }
      })
      .unwrap();
  }
}

#[cfg(any(debug_assertions, feature = "diagnostics"))]
pub use active::UringMetrics;

#[cfg(any(debug_assertions, feature = "diagnostics"))]
pub use active::spawn_observability_thread;

#[cfg(not(any(debug_assertions, feature = "diagnostics")))]
#[derive(Debug, Clone, Copy, Default)]
pub struct UringMetrics;
