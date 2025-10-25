mod cli;
mod commands;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands, KeygenSubcommands};

fn main() -> Result<()> {
  let cli_args = Cli::parse();

  match cli_args.command {
    Commands::Keygen(keygen_subcommands) => match keygen_subcommands {
      KeygenSubcommands::NoiseXx(noise_args) => commands::keygen::generate_noise_xx_keys(noise_args),
      KeygenSubcommands::Curve(curve_args) => commands::keygen::generate_curve_keys(curve_args),
    },
    // Handle other top-level Commands here if added later
  }
}
