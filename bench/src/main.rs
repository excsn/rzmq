mod cli;
mod client;
mod metrics;
mod orchestrator;
mod server;

use clap::Parser;
use cli::{Cli, Role};
use tracing::{error, info};

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
