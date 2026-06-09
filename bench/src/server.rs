use crate::cli::{Cli, Pattern};
use crate::metrics::{BenchStats, BenchmarkCollector};
use fibre::mpsc;
use rzmq::socket::{ADAPTIVE_THROTTLE, RCVBUF, RCVHWM, SNDBUF, SUBSCRIBE, TCP_CORK};
use rzmq::{Context, SocketType, ZmqError};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

#[cfg(feature = "io-uring")]
use rzmq::socket::{IO_URING_RCVMULTISHOT, IO_URING_SESSION_ENABLED};

pub async fn run(args: Cli) -> Result<(), ZmqError> {
  let context = Context::new()?;
  run_with_context(args, context).await.map(|_| ())
}

pub async fn run_with_context(args: Cli, context: Context) -> Result<BenchStats, ZmqError> {
  // Determine target socket type based on pattern
  let socket_type = match args.pattern {
    Pattern::ReqRep => SocketType::Rep,
    Pattern::PushPull => SocketType::Pull,
    Pattern::DealerRouter => SocketType::Router,
    Pattern::PubSub => SocketType::Sub,
  };

  let socket = context.socket(socket_type)?;

  // Configure common socket options
  socket.set_option(RCVHWM, args.hwm as i32).await?;
  socket.set_option(TCP_CORK, args.cork).await?;
  socket.set_option(ADAPTIVE_THROTTLE, 0i32).await?;
  socket.set_option(SNDBUF, 65536i32).await?;
  socket.set_option(RCVBUF, 65536i32).await?;

  // Configure Linux-specific io_uring options if compiled and enabled
  #[cfg(feature = "io-uring")]
  {
    if args.use_io_uring {
      info!("Enabling io_uring session backend on server FD.");
      socket.set_option(IO_URING_SESSION_ENABLED, true).await?;
      if args.uring_multishot {
        info!("Enabling io_uring Multishot Recv on server FD.");
        socket.set_option(IO_URING_RCVMULTISHOT, true).await?;
      }
    }
  }

  // Apply SUB specific subscription filter before binding/connecting
  if args.pattern == Pattern::PubSub {
    debug!("SUB socket subscribing to all topics (empty prefix).");
    socket.set_option(SUBSCRIBE, b"").await?;
  }

  info!("Server binding socket to: {}...", args.endpoint);
  socket.bind(&args.endpoint).await?;
  info!("Server bound successfully. Starting operational loop.");

  let warmup_secs = args.warmup.unwrap_or(0);
  let collector = Arc::new(BenchmarkCollector::new(false));
  let warming_up = Arc::new(AtomicBool::new(warmup_secs > 0));
  let mut last_interim_check = Instant::now();
  let interim_interval = Duration::from_secs(1);
  let mut total_received = 0u64;

  if warmup_secs > 0 {
    let wu_collector = Arc::clone(&collector);
    let wu_flag = Arc::clone(&warming_up);
    let warmup_duration = Duration::from_secs(warmup_secs);
    tokio::spawn(async move {
      tokio::time::sleep(warmup_duration).await;
      wu_flag.store(false, Ordering::Release);
      wu_collector.begin_measurement();
    });
  }

  match args.pattern {
    Pattern::PushPull | Pattern::PubSub => {
      // Unidirectional Drain (Throughput)
      loop {
        match socket.recv().await {
          Ok(msg) => {
            let size = msg.size();
            std::hint::black_box(msg);
            total_received += 1;

            if !warming_up.load(Ordering::Relaxed) {
              collector.record_message(size);
            }

            if last_interim_check.elapsed() >= interim_interval {
              last_interim_check = Instant::now();
              if warming_up.load(Ordering::Relaxed) {
                info!("[Server] Warming up...");
              } else {
                collector.print_interim_report_if_due(interim_interval, "Server");
                if let Ok(base) = std::env::var("RZMQ_BENCH_RUN") {
                  collector.snapshot("server").write_to_file(&format!("{base}-server.json"));
                }
              }
            }
          }
          Err(ZmqError::ConnectionClosed) => {
            info!("Server: Connection closed by peer. Stopping receiver loop. Total received: {}", total_received);
            break;
          }
          Err(e) => {
            error!("Server receiver loop error: {}", e);
            return Err(e);
          }
        }
      }
    }
    Pattern::ReqRep => {
      // Bidirectional Echo (Latency)
      loop {
        match socket.recv().await {
          Ok(msg) => {
            let size = msg.size();
            socket.send(msg).await?;
            total_received += 1;

            if !warming_up.load(Ordering::Relaxed) {
              collector.record_message(size);
            }

            if last_interim_check.elapsed() >= interim_interval {
              last_interim_check = Instant::now();
              if warming_up.load(Ordering::Relaxed) {
                info!("[Server] Warming up...");
              } else {
                collector.print_interim_report_if_due(interim_interval, "Server");
                if let Ok(base) = std::env::var("RZMQ_BENCH_RUN") {
                  collector.snapshot("server").write_to_file(&format!("{base}-server.json"));
                }
              }
            }
          }
          Err(ZmqError::ConnectionClosed) => {
            info!("Server: Connection closed by peer. Stopping ReqRep loop. Total handled: {}", total_received);
            break;
          }
          Err(e) => {
            error!("Server ReqRep loop error: {}", e);
            return Err(e);
          }
        }
      }
    }
    Pattern::DealerRouter => {
      // Bidirectional Multipart Echo: decoupled read/write paths.
      //
      // A single dedicated sender task drains a lock-free MPSC queue in FIFO
      // order, guaranteeing sequential consistency for echoed replies. The main
      // read loop never blocks on network backpressure.
      let (send_tx, send_rx) = mpsc::unbounded_async::<Vec<rzmq::Msg>>();

      let socket_clone = socket.clone();
      tokio::spawn(async move {
        while let Ok(frames) = send_rx.recv().await {
          if let Err(e) = socket_clone.send_multipart(frames).await {
            if !matches!(e, ZmqError::ConnectionClosed | ZmqError::InvalidState(_)) {
              error!("Server echo failed: {}", e);
            }
            if matches!(e, ZmqError::ConnectionClosed | ZmqError::InvalidState(_)) {
              break;
            }
          }
        }
      });

      loop {
        match socket.recv_multipart().await {
          Ok(frames) => {
            let size: usize = frames.iter().map(|f| f.size()).sum();
            total_received += 1;

            if !warming_up.load(Ordering::Relaxed) {
              collector.record_message(size);
            }

            if send_tx.send(frames).await.is_err() {
              error!("Server internal send channel closed unexpectedly.");
              break;
            }

            if last_interim_check.elapsed() >= interim_interval {
              last_interim_check = Instant::now();
              if warming_up.load(Ordering::Relaxed) {
                info!("[Server] Warming up...");
              } else {
                collector.print_interim_report_if_due(interim_interval, "Server");
                if let Ok(base) = std::env::var("RZMQ_BENCH_RUN") {
                  collector.snapshot("server").write_to_file(&format!("{base}-server.json"));
                }
              }
            }
          }
          Err(ZmqError::ConnectionClosed) => {
            info!("Server: Connection closed by peer. Stopping DealerRouter loop. Total handled: {}", total_received);
            break;
          }
          Err(e) => {
            error!("Server DealerRouter loop error: {}", e);
            return Err(e);
          }
        }
      }
    }
  }

  info!("Server task run finished cleanly.");
  let stats = collector.snapshot("server");
  if let Ok(base) = std::env::var("RZMQ_BENCH_RUN") {
    stats.write_to_file(&format!("{base}-server.json"));
  }
  Ok(stats)
}