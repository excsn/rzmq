use crate::cli::{Cli, Pattern};
use crate::metrics::BenchmarkCollector;
use rzmq::socket::{SNDHWM, TCP_CORK};
use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::{Duration, Instant};
use tracing::info;

#[cfg(feature = "io-uring")]
use rzmq::socket::{IO_URING_SESSION_ENABLED, IO_URING_SNDZEROCOPY};

pub async fn run(args: Cli) -> Result<(), ZmqError> {
  let context = Context::new()?;

  // Determine target socket type based on pattern
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

  // Configure Linux-specific io_uring options if compiled and enabled
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

  let payload = vec![0u8; args.msg_size];
  let max_messages = args.messages.unwrap_or(usize::MAX);
  let mut messages_sent = 0usize;

  // Determine if we need to record high-resolution latency RTT
  let is_latency_test = matches!(args.pattern, Pattern::ReqRep | Pattern::DealerRouter);
  let mut collector = BenchmarkCollector::new(is_latency_test);

  let start_time = Instant::now();
  let duration_limit = Duration::from_secs(args.duration);
  let mut last_interim_check = Instant::now();
  let interim_interval = Duration::from_secs(1);

  match args.pattern {
    Pattern::PushPull | Pattern::PubSub => {
      // Unidirectional Streaming (Throughput)
      while messages_sent < max_messages {
        // If message limit is not specified, break when duration limit is met
        if args.messages.is_none() && start_time.elapsed() >= duration_limit {
          break;
        }

        let msg = Msg::from_vec(payload.clone());
        socket.send(msg).await?;
        messages_sent += 1;

        collector.record_message(args.msg_size, None);

        if last_interim_check.elapsed() >= interim_interval {
          collector.print_interim_report_if_due(interim_interval, "Client");
          last_interim_check = Instant::now();
        }
      }
    }
    Pattern::ReqRep | Pattern::DealerRouter => {
      // Bidirectional Ping-Pong (Latency)
      while messages_sent < max_messages {
        if args.messages.is_none() && start_time.elapsed() >= duration_limit {
          break;
        }

        let msg = Msg::from_vec(payload.clone());

        let start = Instant::now();
        socket.send(msg).await?;
        let reply = socket.recv().await?;
        let elapsed = start.elapsed();

        std::hint::black_box(reply);
        messages_sent += 1;

        collector.record_message(args.msg_size, Some(elapsed));

        if last_interim_check.elapsed() >= interim_interval {
          collector.print_interim_report_if_due(interim_interval, "Client");
          last_interim_check = Instant::now();
        }
      }
    }
  }

  // Print final report summary
  let pattern_str = format!("{:?}", args.pattern);
  collector.print_final_report(args.output, &pattern_str, "Client", args.msg_size);

  Ok(())
}
