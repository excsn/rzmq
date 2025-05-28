use crate::cli::NoiseXxArgs;
use anyhow::{Context as AnyhowContext, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::{rngs::StdRng, SeedableRng};
use std::{
  fs::{self, File, OpenOptions},
  io::Write,
  os::unix::fs::OpenOptionsExt,
  path::Path,
};
use x25519_dalek::{PublicKey, StaticSecret};

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

pub fn generate_noise_xx_keys(args: NoiseXxArgs) -> Result<()> {
  println!(
    "Generating Noise_XX (X25519) keypair with name: '{}' in directory: '{}'",
    args.name,
    args.output_dir.display()
  );
  println!(
    "File content format: {}, Stdout display format: {}",
    args.file_format, args.display_format
  );

  fs::create_dir_all(&args.output_dir)
    .with_context(|| format!("Failed to create output directory: {:?}", args.output_dir))?;

  let sk_file_path = args.output_dir.join(format!("{}.noise.sk", args.name));
  let pk_file_path = args.output_dir.join(format!("{}.noise.pk", args.name));

  if !args.force {
    if sk_file_path.exists() {
      anyhow::bail!("Secret key file already exists: {:?}. Use --force.", sk_file_path);
    }
    if pk_file_path.exists() {
      anyhow::bail!("Public key file already exists: {:?}. Use --force.", pk_file_path);
    }
  }

  let static_secret = StaticSecret::random_from_rng(StdRng::from_rng(&mut rand::rng()));
  let public_key = PublicKey::from(&static_secret);

  let sk_bytes_raw: [u8; 32] = static_secret.to_bytes();
  let pk_bytes_raw: [u8; 32] = public_key.to_bytes();

  let sk_file_content: Vec<u8>;
  let pk_file_content: Vec<u8>;

  match args.file_format.as_str() {
    "hex" => {
      sk_file_content = hex::encode(sk_bytes_raw).into_bytes();
      pk_file_content = hex::encode(pk_bytes_raw).into_bytes();
    }
    "base64" => {
      sk_file_content = URL_SAFE_NO_PAD.encode(sk_bytes_raw).into_bytes();
      pk_file_content = URL_SAFE_NO_PAD.encode(pk_bytes_raw).into_bytes();
    }
    "binary" => {
      sk_file_content = sk_bytes_raw.to_vec();
      pk_file_content = pk_bytes_raw.to_vec();
    }
    _ => unreachable!("clap should prevent unknown file_format values"),
  }

  write_key_to_file_with_permissions(&sk_file_path, &sk_file_content, true)?;
  println!("Secret key ({}) saved to: {:?}", args.file_format, sk_file_path);

  write_key_to_file_with_permissions(&pk_file_path, &pk_file_content, false)?;
  println!("Public key ({}) saved to: {:?}", args.file_format, pk_file_path);

  println!("\n--- Generated Keys (Display Format: {}) ---", args.display_format);
  match args.display_format.as_str() {
    "hex" => {
      println!("Secret Key (Hex): {}", hex::encode(sk_bytes_raw));
      println!("Public Key (Hex): {}", hex::encode(pk_bytes_raw));
    }
    "base64" => {
      println!("Secret Key (Base64): {}", URL_SAFE_NO_PAD.encode(sk_bytes_raw));
      println!("Public Key (Base64): {}", URL_SAFE_NO_PAD.encode(pk_bytes_raw));
    }
    "rust" => {
      println!("Secret Key (Rust array): {:?}", sk_bytes_raw);
      println!("Public Key (Rust array): {:?}", pk_bytes_raw);
    }
    _ => unreachable!("clap should prevent unknown display_format values"),
  }

  println!("\nIMPORTANT: Store the secret key file ({:?}) securely!", sk_file_path);
  println!("The public key file ({:?}) can be shared with peers.", pk_file_path);
  println!("\nRemember: rzmq socket options expect RAW BYTES for keys.");
  println!("If your key files are in hex or base64 format, your application will need to decode them before setting the socket options.");

  Ok(())
}
