mod cli;
mod commands;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands, KeygenSubcommands};

fn main() -> Result<()> {
  let cli_args = Cli::parse();

  match cli_args.command {
    Commands::Keygen(keygen_subcommands) => match keygen_subcommands {
      KeygenSubcommands::NoiseXx(noise_args) => commands::keygen::generate_noise_xx_keys(noise_args), // Handle other KeygenSubcommands here if added later
    },
    // Handle other top-level Commands here if added later
  }
}
