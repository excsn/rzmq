use std::path::Path;
use std::process::Command;

/// Runs scripts/check-features.sh. Its cargo invocations use their own target
/// directory, since the outer `cargo test` holds the lock on the main one.
///
/// Cargo runs test binaries in byte order of their names and lowercase `z` is
/// the highest character a target name can hold, so the `zzz_` prefix keeps this
/// slow test after every other one.
#[test]
fn every_feature_set_compiles() {
  let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
  let output = Command::new("sh")
    .arg(root.join("scripts/check-features.sh"))
    .env("CARGO_TARGET_DIR", root.join("target/feature-check"))
    .output()
    .expect("run scripts/check-features.sh");
  assert!(
    output.status.success(),
    "feature matrix failed:\n{}\n{}",
    String::from_utf8_lossy(&output.stdout),
    String::from_utf8_lossy(&output.stderr)
  );
}
