use clap::{builder::TypedValueParser, Parser};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(name = "rzmq", version, about = "RZMQ Command Line Utility")]
pub struct Cli {
  #[clap(subcommand)]
  pub command: Commands,
}

#[derive(clap::Subcommand, Debug)]
pub enum Commands {
  /// Key generation utilities
  #[clap(subcommand)] // Keygen itself will have subcommands like NoiseXx
  Keygen(KeygenSubcommands),
  // Add other top-level commands here later, e.g., `rzmq bench ...`
}

#[derive(clap::Subcommand, Debug)]
pub enum KeygenSubcommands {
  /// Generate Noise_XX (X25519) keypair
  NoiseXx(NoiseXxArgs),
}

#[derive(Parser, Debug)]
pub struct NoiseXxArgs {
  /// Base name for the generated key files (e.g., "server", "client1")
  #[clap(long, short)]
  pub name: String,

  /// Directory to save the key files
  #[clap(long, short = 'o', default_value = ".")]
  pub output_dir: PathBuf,

  /// Format for the content stored IN THE KEY FILES
  #[clap(long, value_parser = clap::builder::PossibleValuesParser::new(["hex", "base64", "binary"]).map(|s| s.to_lowercase()), default_value = "hex")]
  pub file_format: String,

  /// Output format for displaying keys on STDOUT
  #[clap(long, value_parser = clap::builder::PossibleValuesParser::new(["hex", "base64", "rust"]).map(|s| s.to_lowercase()), default_value = "hex")]
  pub display_format: String,

  /// Overwrite existing key files without prompting
  #[clap(long, short, action)]
  pub force: bool,
}
