use bytes::BytesMut;
use crate::cli::{Cli, Pattern};
use crate::metrics::BenchmarkCollector;
use hdrhistogram::Histogram;
use rzmq::socket::{SNDHWM, TCP_CORK};
use rzmq::{Context, Msg, SocketType, ZmqError};
use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

#[cfg(feature = "io-uring")]
use rzmq::socket::{IO_URING_SESSION_ENABLED, IO_URING_SNDZEROCOPY};

pub async fn run(args: Cli) -> Result<(), ZmqError> {
  let context = Context::new()?;
  run_with_context(args, context).await
}

pub async fn run_with_context(args: Cli, context: Context) -> Result<(), ZmqError> {
  let socket_type = match args.pattern {
    Pattern::ReqRep => SocketType::Req,
    Pattern::PushPull => SocketType::Push,
    Pattern::DealerRouter => SocketType::Dealer,
    Pattern::PubSub => SocketType::Pub,
  };

  let socket = context.socket(socket_type)?;

  // Configure common socket options
  socket.set_option(SNDHWM, args.hwm as i32).await?;
  socket.set_option(TCP_CORK, args.cork).await?;

  #[cfg(feature = "io-uring")]
  {
    if args.use_io_uring {
      info!("Enabling io_uring session backend on client FD.");
      socket.set_option(IO_URING_SESSION_ENABLED, true).await?;
      if args.uring_zerocopy {
        info!("Enabling io_uring Send Zero-Copy on client FD.");
        socket.set_option(IO_URING_SNDZEROCOPY, true).await?;
      }
    }
  }

  info!("Client connecting socket to: {}...", args.endpoint);
  socket.connect(&args.endpoint).await?;
  info!("Client connected. Starting workload execution.");

  let is_latency_test = matches!(args.pattern, Pattern::ReqRep | Pattern::DealerRouter);

  let collector = Arc::new(BenchmarkCollector::new(is_latency_test));
  let shutdown = Arc::new(AtomicBool::new(false));
  let payload = Arc::new(vec![0u8; args.msg_size]);
  let max_messages = args.messages.unwrap_or(usize::MAX);

  let start_time = Instant::now();
  let warmup_duration = Duration::from_secs(args.warmup);
  let duration_limit = Duration::from_secs(args.duration);
  // Workers run for warmup + measurement; deadline gates the entire window.
  let deadline = start_time + warmup_duration + duration_limit;
  let interim_interval = Duration::from_secs(1);

  // True during warmup; workers skip measurement while this is set.
  let warming_up = Arc::new(AtomicBool::new(args.warmup > 0));

  if args.warmup > 0 {
    info!("Warmup period: {} seconds.", args.warmup);
    let wu_collector = Arc::clone(&collector);
    let wu_flag = Arc::clone(&warming_up);
    tokio::spawn(async move {
      tokio::time::sleep(warmup_duration).await;
      wu_flag.store(false, Ordering::Release);
      wu_collector.begin_measurement();
      info!("Warmup complete. Measurement period started.");
    });
  }

  info!(
    "Workload constraints -> Warmup: {:?}, Duration: {:?}, Deadline: {:?}",
    warmup_duration, duration_limit, deadline
  );

  // Dedicated reporting task: workers only update atomic counters; all
  // interim printing happens here off the hot path, once per second.
  let report_collector = Arc::clone(&collector);
  let report_warming_up = Arc::clone(&warming_up);
  let report_handle = tokio::spawn(async move {
    let mut ticker = tokio::time::interval(interim_interval);
    ticker.tick().await; // discard the immediate first tick
    loop {
      ticker.tick().await;
      if report_warming_up.load(Ordering::Relaxed) {
        info!("[Client] Warming up...");
        continue;
      }
      report_collector.print_interim_report_if_due(interim_interval, "Client");
    }
  });

  let mut join_set: JoinSet<Result<(), ZmqError>> = JoinSet::new();

  match args.pattern {
    Pattern::PushPull | Pattern::PubSub => {
      let msg_counter = Arc::new(AtomicUsize::new(0));
      info!("Spawning {} independent throughput workers...", args.concurrency);

      for i in 0..args.concurrency {
        join_set.spawn(run_throughput_worker(
          i,
          context.clone(),
          socket_type,
          args.endpoint.clone(),
          args.hwm,
          args.cork,
          Arc::clone(&collector),
          Arc::clone(&shutdown),
          Arc::clone(&msg_counter),
          Arc::clone(&payload),
          args.msg_size,
          max_messages,
          deadline,
          Arc::clone(&warming_up),
        ));
      }
    }

    Pattern::ReqRep => {
      info!("Spawning {} independent ReqRep workers...", args.concurrency);
      for i in 0..args.concurrency {
        join_set.spawn(run_reqrep_worker(
          i,
          context.clone(),
          args.endpoint.clone(),
          args.hwm,
          args.cork,
          Arc::clone(&collector),
          Arc::clone(&shutdown),
          Arc::clone(&payload),
          args.msg_size,
          max_messages,
          deadline,
          Arc::clone(&warming_up),
        ));
      }
    }

    Pattern::DealerRouter => {
      let pipeline_depth = args.pipeline.max(1);
      let msg_counter = Arc::new(AtomicUsize::new(0));
      info!(
        "Spawning {} DealerRouter sender/receiver pairs...",
        args.concurrency
      );

      for i in 0..args.concurrency {
        // Each worker pair gets its own semaphore, eliminating cross-worker
        // atomic contention on the shared semaphore counter.
        let worker_semaphore = Arc::new(Semaphore::new(pipeline_depth));

        join_set.spawn(run_dealer_sender(
          i,
          socket.clone(),
          Arc::clone(&shutdown),
          worker_semaphore.clone(),
          Arc::clone(&msg_counter),
          Arc::clone(&payload),
          args.msg_size,
          max_messages,
          deadline,
          start_time,
        ));
        join_set.spawn(run_dealer_receiver(
          i,
          socket.clone(),
          Arc::clone(&collector),
          Arc::clone(&shutdown),
          worker_semaphore,
          args.msg_size,
          start_time,
          Arc::clone(&warming_up),
        ));
      }
    }
  }

  // Active Socket Closing Shutdown — fires after warmup + measurement window.
  {
    let sd = Arc::clone(&shutdown);
    let socket_clone = socket.clone();
    let total_duration = warmup_duration + duration_limit;
    tokio::spawn(async move {
      tokio::time::sleep(total_duration).await;
      info!("Main duration timer expired. Triggering socket.close() to unblock all workers.");
      sd.store(true, Ordering::Release);
      let _ = socket_clone.close().await;
    });
  }

  info!("All tasks spawned. Awaiting JoinSet completion...");
  let mut finished_tasks = 0;

  while let Some(result) = join_set.join_next().await {
    finished_tasks += 1;
    match result {
      Ok(Err(e)) => error!("Worker task #{} exited with error: {}", finished_tasks, e),
      Err(join_err) => error!(
        "Worker task #{} panicked on exit: {}",
        finished_tasks, join_err
      ),
      Ok(Ok(())) => info!("Worker task #{} joined successfully.", finished_tasks),
    }
  }

  // Stop the reporting task before printing the final report so no interim
  // line can interleave with the final output.
  report_handle.abort();

  info!("All worker tasks successfully joined. Preparing final reports.");
  let pattern_str = format!("{:?}", args.pattern);
  collector.print_final_report(args.output, &pattern_str, "Client", args.msg_size);

  info!("Client run completed successfully.");
  Ok(())
}

// ---------------------------------------------------------------------------
// Worker: unidirectional throughput (PushPull / PubSub)
//
// Each worker owns a private socket so N workers occupy N independent TCP
// connections, allowing true parallel throughput scaling.
// ---------------------------------------------------------------------------

async fn run_throughput_worker(
  id: usize,
  context: Context,
  socket_type: SocketType,
  endpoint: String,
  hwm: usize,
  cork: bool,
  collector: Arc<BenchmarkCollector>,
  shutdown: Arc<AtomicBool>,
  msg_counter: Arc<AtomicUsize>,
  payload: Arc<Vec<u8>>,
  msg_size: usize,
  max_messages: usize,
  deadline: Instant,
  warming_up: Arc<AtomicBool>,
) -> Result<(), ZmqError> {
  let socket = context.socket(socket_type)?;
  socket.set_option(SNDHWM, hwm as i32).await?;
  socket.set_option(TCP_CORK, cork).await?;
  socket.connect(&endpoint).await?;
  info!("[Throughput Worker {}] Connected. Loop started.", id);

  let mut loop_counter = 0u32;

  loop {
    loop_counter += 1;
    if loop_counter % 1024 == 0 {
      tokio::task::yield_now().await;
    }

    if shutdown.load(Ordering::Relaxed) {
      info!(
        "[Throughput Worker {}] Exiting loop: global shutdown flag detected.",
        id
      );
      break;
    }

    if Instant::now() >= deadline {
      info!(
        "[Throughput Worker {}] Exiting loop: local deadline reached.",
        id
      );
      shutdown.store(true, Ordering::Release);
      break;
    }

    let prev = msg_counter.fetch_add(1, Ordering::Relaxed);
    if prev >= max_messages {
      msg_counter.fetch_sub(1, Ordering::Relaxed);
      info!(
        "[Throughput Worker {}] Exiting loop: max message count reached.",
        id
      );
      shutdown.store(true, Ordering::Release);
      break;
    }

    match socket.send(Msg::from_vec((*payload).clone())).await {
      Ok(()) => {
        if !warming_up.load(Ordering::Relaxed) {
          collector.record_message(msg_size);
        }
      }
      Err(ZmqError::ConnectionClosed) | Err(ZmqError::InvalidState(_)) => {
        info!(
          "[Throughput Worker {}] Socket was closed. Exiting loop cleanly.",
          id
        );
        break;
      }
      Err(e) => {
        error!("[Throughput Worker {}] Send failed with error: {}", id, e);
        return Err(e);
      }
    }
  }

  let _ = socket.close().await;
  info!("[Throughput Worker {}] Exited cleanly.", id);
  Ok(())
}

// ---------------------------------------------------------------------------
// Worker: ping-pong latency (ReqRep)
//
// Each worker owns a private REQ socket so N workers can run concurrently
// without FSM violations. The context is cheap to clone (Arc-backed).
// ---------------------------------------------------------------------------

async fn run_reqrep_worker(
  id: usize,
  context: Context,
  endpoint: String,
  hwm: usize,
  cork: bool,
  collector: Arc<BenchmarkCollector>,
  shutdown: Arc<AtomicBool>,
  payload: Arc<Vec<u8>>,
  msg_size: usize,
  max_messages: usize,
  deadline: Instant,
  warming_up: Arc<AtomicBool>,
) -> Result<(), ZmqError> {
  let socket = context.socket(SocketType::Req)?;
  socket.set_option(SNDHWM, hwm as i32).await?;
  socket.set_option(TCP_CORK, cork).await?;
  socket.connect(&endpoint).await?;
  info!("[ReqRep Worker {}] Connected. Loop started.", id);

  let mut messages_sent = 0usize;
  let mut local_hist = Histogram::<u64>::new(3).expect("histogram");

  loop {
    if shutdown.load(Ordering::Relaxed) {
      info!(
        "[ReqRep Worker {}] Exiting loop: global shutdown flag set.",
        id
      );
      break;
    }

    if Instant::now() >= deadline {
      info!(
        "[ReqRep Worker {}] Exiting loop: local deadline reached.",
        id
      );
      shutdown.store(true, Ordering::Release);
      break;
    }

    if messages_sent >= max_messages {
      info!(
        "[ReqRep Worker {}] Exiting loop: max message count reached.",
        id
      );
      shutdown.store(true, Ordering::Release);
      break;
    }

    let start = Instant::now();
    match socket.send(Msg::from_vec((*payload).clone())).await {
      Ok(()) => {}
      Err(ZmqError::ConnectionClosed) | Err(ZmqError::InvalidState(_)) => {
        info!(
          "[ReqRep Worker {}] Socket was closed during send. Exiting loop cleanly.",
          id
        );
        break;
      }
      Err(e) => return Err(e),
    }

    match socket.recv().await {
      Ok(reply) => {
        let elapsed = start.elapsed();
        std::hint::black_box(reply);
        messages_sent += 1;

        if !warming_up.load(Ordering::Relaxed) {
          local_hist.record(elapsed.as_nanos() as u64).ok();
          collector.record_message(msg_size);
        }
      }
      Err(ZmqError::ConnectionClosed) | Err(ZmqError::InvalidState(_)) => {
        info!(
          "[ReqRep Worker {}] Socket was closed during recv. Exiting loop cleanly.",
          id
        );
        break;
      }
      Err(e) => return Err(e),
    }
  }

  let _ = socket.close().await;
  collector.merge_histogram(&local_hist);
  info!("[ReqRep Worker {}] Exited cleanly.", id);
  Ok(())
}

// ---------------------------------------------------------------------------
// Worker: pipelined sender (DealerRouter)
//
// Writes the send time as a nanosecond offset from `start_time` into the
// first 8 bytes of each payload. The paired receiver reads this value back
// from the echoed reply to compute RTT without any shared state.
// ---------------------------------------------------------------------------

async fn run_dealer_sender(
  id: usize,
  socket: rzmq::Socket,
  shutdown: Arc<AtomicBool>,
  semaphore: Arc<Semaphore>,
  msg_counter: Arc<AtomicUsize>,
  payload: Arc<Vec<u8>>,
  msg_size: usize,
  max_messages: usize,
  deadline: Instant,
  start_time: Instant,
) -> Result<(), ZmqError> {
  info!("[Dealer Sender {}] Loop started.", id);
  let mut loop_counter = 0u32;
  // Reusable buffer: avoids a fresh Vec allocation on every send iteration.
  let mut local_bytes = BytesMut::with_capacity(msg_size);
  let mut exit_result: Result<(), ZmqError> = Ok(());

  loop {
    loop_counter += 1;
    if loop_counter % 1024 == 0 {
      tokio::task::yield_now().await;
    }

    if shutdown.load(Ordering::Relaxed) {
      info!(
        "[Dealer Sender {}] Exiting loop: global shutdown flag set.",
        id
      );
      break;
    }

    if Instant::now() >= deadline {
      info!(
        "[Dealer Sender {}] Exiting loop: local deadline reached.",
        id
      );
      shutdown.store(true, Ordering::Release);
      let _ = socket.close().await;
      break;
    }

    let prev = msg_counter.fetch_add(1, Ordering::Relaxed);
    if prev >= max_messages {
      msg_counter.fetch_sub(1, Ordering::Relaxed);
      info!(
        "[Dealer Sender {}] Exiting loop: max message count reached.",
        id
      );
      shutdown.store(true, Ordering::Release);
      let _ = socket.close().await;
      break;
    }

    let permit = match semaphore.acquire().await {
      Ok(p) => p,
      Err(_) => {
        warn!("[Dealer Sender {}] Semaphore closed. Exiting.", id);
        break;
      }
    };
    std::mem::forget(permit);

    // Stamp the send time as nanoseconds elapsed since the shared start_time.
    // The receiver echoes the payload back unchanged, so it can reconstruct
    // the original send instant without any shared state.
    let elapsed_nanos = Instant::now().duration_since(start_time).as_nanos() as u64;
    local_bytes.clear();
    local_bytes.extend_from_slice(&*payload);
    local_bytes[..8].copy_from_slice(&elapsed_nanos.to_le_bytes());
    let frozen = local_bytes.split().freeze();

    match socket.send(Msg::from_bytes(frozen)).await {
      Ok(()) => {}
      Err(ZmqError::ConnectionClosed) | Err(ZmqError::InvalidState(_)) => {
        info!(
          "[Dealer Sender {}] Socket was closed during send. Exiting loop cleanly.",
          id
        );
        break;
      }
      Err(e) => {
        error!("[Dealer Sender {}] Send failed: {}. Exiting loop.", id, e);
        exit_result = Err(e);
        break;
      }
    }
  }

  // Cleanup always runs regardless of how the loop exited.
  // Wake the paired receiver if it is still waiting for a semaphore permit.
  semaphore.close();

  info!("[Dealer Sender {}] Exited cleanly.", id);
  exit_result
}

// ---------------------------------------------------------------------------
// Worker: pipelined receiver (DealerRouter)
//
// Reads the nanosecond-offset timestamp from the first 8 bytes of each
// echoed reply, reconstructs the send instant, and records the RTT into a
// task-local histogram (no locking). Merges into the shared histogram once
// at exit via collector.merge_histogram().
// ---------------------------------------------------------------------------

async fn run_dealer_receiver(
  id: usize,
  socket: rzmq::Socket,
  collector: Arc<BenchmarkCollector>,
  shutdown: Arc<AtomicBool>,
  semaphore: Arc<Semaphore>,
  msg_size: usize,
  start_time: Instant,
  warming_up: Arc<AtomicBool>,
) -> Result<(), ZmqError> {
  info!("[Dealer Receiver {}] Loop started.", id);
  let mut local_hist = Histogram::<u64>::new(3).expect("histogram");
  let mut exit_result: Result<(), ZmqError> = Ok(());

  loop {
    match tokio::time::timeout(Duration::from_millis(10), socket.recv()).await {
      Ok(Ok(reply)) => {
        let now = Instant::now();

        let elapsed_nanos = reply
          .data()
          .and_then(|bytes| bytes.get(..8))
          .map(|slice| {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(slice);
            u64::from_le_bytes(arr)
          })
          .unwrap_or(0);

        let send_time = start_time + Duration::from_nanos(elapsed_nanos);
        let rtt = now.duration_since(send_time);

        std::hint::black_box(reply);
        semaphore.add_permits(1);

        if !warming_up.load(Ordering::Relaxed) {
          local_hist.record(rtt.as_nanos() as u64).ok();
          collector.record_message(msg_size);
        }
      }
      Ok(Err(ZmqError::ConnectionClosed)) | Ok(Err(ZmqError::InvalidState(_))) => {
        info!(
          "[Dealer Receiver {}] Connection closed by peer. Exiting loop.",
          id
        );
        break;
      }
      Ok(Err(e)) => {
        error!("[Dealer Receiver {}] Received socket error: {}. Exiting loop.", id, e);
        exit_result = Err(e);
        break;
      }
      Err(_timeout) => {
        if shutdown.load(Ordering::Acquire) {
          info!(
            "[Dealer Receiver {}] Exiting loop: global shutdown flag detected.",
            id
          );
          break;
        }
      }
    }
  }

  // Cleanup always runs regardless of how the loop exited.
  collector.merge_histogram(&local_hist);
  // Wake the paired sender if it is blocked in semaphore.acquire().
  semaphore.close();

  info!("[Dealer Receiver {}] Exited cleanly.", id);
  exit_result
}
