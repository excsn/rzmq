use crate::cli::{CurveArgs, NoiseXxArgs};

use std::{
  fs::{self, OpenOptions},
  io::Write,
  os::unix::fs::OpenOptionsExt,
  path::Path,
};

use anyhow::{Context as AnyhowContext, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{SeedableRng, rngs::StdRng};
use x25519_dalek::{PublicKey, StaticSecret};

// --- Generic Arguments Trait ---
// A small trait to unify the arguments for different key types.
trait KeygenArgs {
  fn name(&self) -> &str;
  fn output_dir(&self) -> &Path;
  fn file_format(&self) -> &str;
  fn display_format(&self) -> &str;
  fn force(&self) -> bool;
}

impl KeygenArgs for NoiseXxArgs {
  fn name(&self) -> &str {
    &self.name
  }
  fn output_dir(&self) -> &Path {
    &self.output_dir
  }
  fn file_format(&self) -> &str {
    &self.file_format
  }
  fn display_format(&self) -> &str {
    &self.display_format
  }
  fn force(&self) -> bool {
    self.force
  }
}

impl KeygenArgs for CurveArgs {
  fn name(&self) -> &str {
    &self.name
  }
  fn output_dir(&self) -> &Path {
    &self.output_dir
  }
  fn file_format(&self) -> &str {
    &self.file_format
  }
  fn display_format(&self) -> &str {
    &self.display_format
  }
  fn force(&self) -> bool {
    self.force
  }
}

// --- Public Functions (Now just wrappers) ---

pub fn generate_noise_xx_keys(args: NoiseXxArgs) -> Result<()> {
  generate_keypair_internal("Noise_XX (X25519)", "noise", &args)
}

pub fn generate_curve_keys(args: CurveArgs) -> Result<()> {
  generate_keypair_internal("CURVE (Curve25519)", "curve", &args)
}

// --- Internal Generic Implementation ---

fn generate_keypair_internal(
  protocol_name: &str,
  file_ext: &str,
  args: &dyn KeygenArgs,
) -> Result<()> {
  println!(
    "Generating {} keypair with name: '{}' in directory: '{}'",
    protocol_name,
    args.name(),
    args.output_dir().display()
  );
  println!(
    "File content format: {}, Stdout display format: {}",
    args.file_format(),
    args.display_format()
  );

  fs::create_dir_all(args.output_dir())
    .with_context(|| format!("Failed to create output directory: {:?}", args.output_dir()))?;

  let sk_file_path = args
    .output_dir()
    .join(format!("{}.{}.sk", args.name(), file_ext));
  let pk_file_path = args
    .output_dir()
    .join(format!("{}.{}.pk", args.name(), file_ext));

  if !args.force() {
    if sk_file_path.exists() {
      anyhow::bail!(
        "Secret key file already exists: {:?}. Use --force.",
        sk_file_path
      );
    }
    if pk_file_path.exists() {
      anyhow::bail!(
        "Public key file already exists: {:?}. Use --force.",
        pk_file_path
      );
    }
  }

  // This is the only part that is truly identical.
  let static_secret = StaticSecret::random_from_rng(StdRng::from_rng(&mut rand::rng()));
  let public_key = PublicKey::from(&static_secret);

  let sk_bytes_raw: [u8; 32] = static_secret.to_bytes();
  let pk_bytes_raw: [u8; 32] = public_key.to_bytes();

  let (sk_file_content, pk_file_content) = match args.file_format() {
    "hex" => (
      hex::encode(sk_bytes_raw).into_bytes(),
      hex::encode(pk_bytes_raw).into_bytes(),
    ),
    "base64" => (
      URL_SAFE_NO_PAD.encode(sk_bytes_raw).into_bytes(),
      URL_SAFE_NO_PAD.encode(pk_bytes_raw).into_bytes(),
    ),
    "binary" => (sk_bytes_raw.to_vec(), pk_bytes_raw.to_vec()),
    _ => unreachable!(),
  };

  write_key_to_file_with_permissions(&sk_file_path, &sk_file_content, true)?;
  println!(
    "Secret key ({}) saved to: {:?}",
    args.file_format(),
    sk_file_path
  );

  write_key_to_file_with_permissions(&pk_file_path, &pk_file_content, false)?;
  println!(
    "Public key ({}) saved to: {:?}",
    args.file_format(),
    pk_file_path
  );

  println!(
    "\n--- Generated Keys (Display Format: {}) ---",
    args.display_format()
  );
  match args.display_format() {
    "hex" => {
      println!("Secret Key (Hex): {}", hex::encode(sk_bytes_raw));
      println!("Public Key (Hex): {}", hex::encode(pk_bytes_raw));
    }
    "base64" => {
      println!(
        "Secret Key (Base64): {}",
        URL_SAFE_NO_PAD.encode(sk_bytes_raw)
      );
      println!(
        "Public Key (Base64): {}",
        URL_SAFE_NO_PAD.encode(pk_bytes_raw)
      );
    }
    "rust" => {
      println!("Secret Key (Rust array): {:?}", sk_bytes_raw);
      println!("Public Key (Rust array): {:?}", pk_bytes_raw);
    }
    _ => unreachable!(),
  }

  println!(
    "\nIMPORTANT: Store the secret key file ({:?}) securely!",
    sk_file_path
  );
  println!(
    "The public key file ({:?}) can be shared with peers.",
    pk_file_path
  );
  println!("\nRemember: rzmq socket options expect RAW BYTES for keys.");
  println!(
    "If your key files are in hex or base64 format, your application will need to decode them before setting the socket options."
  );

  Ok(())
}

fn write_key_to_file_with_permissions(path: &Path, content: &[u8], is_secret: bool) -> Result<()> {
  let mut options = OpenOptions::new();
  options.write(true).create(true).truncate(true);

  if is_secret {
    #[cfg(unix)]
    options.mode(0o600);
  } else {
    #[cfg(unix)]
    options.mode(0o644);
  }

  let mut file = options
    .open(path)
    .with_context(|| format!("Failed to create/open key file: {:?}", path))?;
  file
    .write_all(content)
    .with_context(|| format!("Failed to write to key file: {:?}", path))?;
  Ok(())
}
