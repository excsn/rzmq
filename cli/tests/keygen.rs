use std::fs;
use std::path::Path;
use std::process::Command;

use rzmq::security::z85;
use x25519_dalek::{PublicKey, StaticSecret};

fn keygen(dir: &Path, name: &str, extra: &[&str]) -> String {
  let output = Command::new(env!("CARGO_BIN_EXE_rzmq"))
    .args(["keygen", "curve", "--name", name, "--output-dir"])
    .arg(dir)
    .args(extra)
    .output()
    .expect("run rzmq keygen curve");
  assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
  String::from_utf8(output.stdout).unwrap()
}

fn decoded_key(path: &Path) -> [u8; 32] {
  let text = fs::read_to_string(path).unwrap();
  assert_eq!(text.len(), 40, "{:?} is not 40 Z85 characters: {:?}", path, text);
  z85::decode(&text).expect("file holds valid Z85").try_into().unwrap()
}

#[test]
fn curve_files_are_z85_and_the_public_key_matches_the_secret() {
  let dir = tempdir("files");
  keygen(&dir, "server", &[]);
  let secret = decoded_key(&dir.join("server.curve.sk"));
  let public = decoded_key(&dir.join("server.curve.pk"));
  assert_eq!(PublicKey::from(&StaticSecret::from(secret)).to_bytes(), public);
}

#[test]
fn displayed_keys_match_the_files_in_every_format() {
  let dir = tempdir("display");
  let shown = keygen(&dir, "k", &["--display-format", "hex"]);
  let secret = decoded_key(&dir.join("k.curve.sk"));
  let public = decoded_key(&dir.join("k.curve.pk"));
  assert!(shown.contains(&format!("Secret Key (Hex): {}", hex::encode(secret))), "{shown}");
  assert!(shown.contains(&format!("Public Key (Hex): {}", hex::encode(public))), "{shown}");

  let shown = keygen(&dir, "k", &["--force"]);
  let secret_text = fs::read_to_string(dir.join("k.curve.sk")).unwrap();
  let public_text = fs::read_to_string(dir.join("k.curve.pk")).unwrap();
  assert!(shown.contains(&format!("Secret Key (Z85): {secret_text}")), "{shown}");
  assert!(shown.contains(&format!("Public Key (Z85): {public_text}")), "{shown}");
}

#[test]
fn hex_files_hold_the_same_key_as_z85_display() {
  let dir = tempdir("hex");
  let shown = keygen(&dir, "h", &["--file-format", "hex"]);
  let secret = hex::decode(fs::read_to_string(dir.join("h.curve.sk")).unwrap().trim()).unwrap();
  let z85_secret = z85::encode(&secret).unwrap();
  assert!(shown.contains(&format!("Secret Key (Z85): {z85_secret}")), "{shown}");
}

fn tempdir(tag: &str) -> std::path::PathBuf {
  let dir = std::env::temp_dir().join(format!("rzmq_cli_keygen_{}_{}", tag, std::process::id()));
  let _ = fs::remove_dir_all(&dir);
  fs::create_dir_all(&dir).unwrap();
  dir
}
