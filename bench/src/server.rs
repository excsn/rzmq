use crate::cli::{Cli, Pattern};
use crate::metrics::BenchmarkCollector;
use rzmq::socket::{RCVHWM, SUBSCRIBE, TCP_CORK};
use rzmq::{Context, SocketType, ZmqError};
use std::time::{Duration, Instant};
use tracing::{debug, info};

#[cfg(feature = "io-uring")]
use rzmq::socket::{IO_URING_RCVMULTISHOT, IO_URING_SESSION_ENABLED};

pub async fn run(args: Cli) -> Result<(), ZmqError> {
  let context = Context::new()?;

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

  let mut collector = BenchmarkCollector::new(false);
  let mut last_interim_check = Instant::now();
  let interim_interval = Duration::from_secs(1);

  match args.pattern {
    Pattern::PushPull | Pattern::PubSub => {
      // Unidirectional Drain (Throughput)
      loop {
        let msg = socket.recv().await?;
        let size = msg.size();
        std::hint::black_box(msg);

        collector.record_message(size, None);

        if last_interim_check.elapsed() >= interim_interval {
          collector.print_interim_report_if_due(interim_interval, "Server");
          last_interim_check = Instant::now();
        }
      }
    }
    Pattern::ReqRep => {
      // Bidirectional Echo (Latency)
      loop {
        let msg = socket.recv().await?;
        let size = msg.size();
        socket.send(msg).await?;

        collector.record_message(size, None);

        if last_interim_check.elapsed() >= interim_interval {
          collector.print_interim_report_if_due(interim_interval, "Server");
          last_interim_check = Instant::now();
        }
      }
    }
    Pattern::DealerRouter => {
      // Bidirectional Multipart Echo
      loop {
        let frames = socket.recv_multipart().await?;

        // Calculate total payload size of the frames
        let size: usize = frames.iter().map(|f| f.size()).sum();
        socket.send_multipart(frames).await?;

        collector.record_message(size, None);

        if last_interim_check.elapsed() >= interim_interval {
          collector.print_interim_report_if_due(interim_interval, "Server");
          last_interim_check = Instant::now();
        }
      }
    }
  }
}
