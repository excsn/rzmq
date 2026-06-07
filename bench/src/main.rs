mod cli;
mod client;
mod metrics;
mod orchestrator;
mod server;

use clap::Parser;
use cli::{Cli, Role};
use tracing::{error, info};

#[cfg(feature = "io-uring")]
use cli::UringStrategy;
#[cfg(feature = "io-uring")]
use rzmq::uring::{UringConfig, UringPollingStrategy, initialize_uring_backend};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt()
    .with_env_filter(
      tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    )
    .init();

  let args = Cli::parse();
  info!("Parsed configuration: {:?}", args);

  // Pre-initialize the io_uring backend with the user's chosen polling strategy
  // before any socket or context is created. This pre-empts the lazy default
  // initialization that would otherwise apply UringPollingStrategy::balanced().
  #[cfg(feature = "io-uring")]
  if args.use_io_uring {
    let polling_strategy = match args.uring_strategy {
      UringStrategy::Performance => UringPollingStrategy::ultra_low_latency(),
      UringStrategy::Balanced => UringPollingStrategy::balanced(),
      UringStrategy::LowPower => UringPollingStrategy::low_power(),
    };
    let config = UringConfig {
      polling_strategy,
      default_send_zerocopy: args.uring_zerocopy,
      default_recv_multishot: args.uring_multishot,
      ..UringConfig::default()
    };
    if let Err(e) = initialize_uring_backend(config) {
      error!("Failed to initialize io_uring backend: {}", e);
      return Err(e.into());
    }
    info!("io_uring backend initialized with strategy: {:?}", args.uring_strategy);
  }

  match args.role {
    Role::Server => {
      info!("Launching Server role...");
      if let Err(e) = server::run(args).await {
        error!("Server execution failed: {}", e);
        return Err(e.into());
      }
    }
    Role::Client => {
      info!("Launching Client role...");
      if let Err(e) = client::run(args).await {
        error!("Client execution failed: {}", e);
        return Err(e.into());
      }
    }
    Role::Orchestrate => {
      info!("Launching Orchestrator coordinator...");
      if let Err(e) = orchestrator::run(args).await {
        error!("Orchestrator execution failed: {}", e);
        return Err(e.into());
      }
    }
  }

  Ok(())
}
