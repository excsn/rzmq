use clap::{Parser, ValueEnum};

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
  Server,
  Client,
  Orchestrate,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pattern {
  ReqRep,
  PushPull,
  DealerRouter,
  PubSub,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputFormat {
  Text,
  Json,
  Csv,
}

#[derive(Parser, Debug, Clone)]
#[command(
  name = "rzmq_bench",
  author = "rzmq Contributors",
  version,
  about = "A dedicated benchmarking tool for rzmq"
)]
pub struct Cli {
  /// Operational role for this execution instance
  #[arg(long, value_enum, default_value_t = Role::Orchestrate)]
  pub role: Role,

  /// Target network address/endpoint for binding or connecting
  #[arg(long, default_value = "tcp://127.0.0.1:19876")]
  pub endpoint: String,

  /// ZeroMQ messaging pattern to utilize
  #[arg(long, value_enum, default_value_t = Pattern::PushPull)]
  pub pattern: Pattern,

  /// Size of individual message payloads in bytes
  #[arg(long, default_value_t = 64)]
  pub msg_size: usize,

  /// Total number of messages to process (omitting runs indefinitely or uses duration)
  #[arg(long)]
  pub messages: Option<usize>,

  /// Maximum duration of the test run in seconds
  #[arg(long, default_value_t = 10)]
  pub duration: u64,

  /// High-Water Mark (HWM) limit applied to socket queues
  #[arg(long, default_value_t = 1000)]
  pub hwm: usize,

  /// Enable TCP_CORK on Linux to aggregate small packets
  #[arg(long, default_value_t = false)]
  pub cork: bool,

  /// Output format for the final benchmark metrics report
  #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
  pub output: OutputFormat,

  /// Pin server/client processes to dedicated CPU cores (Linux only, off by default)
  #[cfg(target_os = "linux")]
  #[arg(long, default_value_t = false)]
  pub pin_cpus: bool,

  /// Enable the io_uring backend instead of standard Epoll (Linux only)
  #[cfg(feature = "io-uring")]
  #[arg(long, default_value_t = false)]
  pub use_io_uring: bool,

  /// Enable Zero-Copy send operations within the io_uring engine (Linux only)
  #[cfg(feature = "io-uring")]
  #[arg(long, default_value_t = false)]
  pub uring_zerocopy: bool,

  /// Enable Multishot receive operations within the io_uring engine (Linux only)
  #[cfg(feature = "io-uring")]
  #[arg(long, default_value_t = false)]
  pub uring_multishot: bool,
}
