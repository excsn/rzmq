//! ZMTP/2.0 cross-process interop tests.
//!
//! These drive rzmq against a raw-socket ZMTP/2.0 peer (`python_scripts/zmtp2_*.py`)
//! that emits the exact wire bytes a legacy libzmq (3.0-3.2) peer produces. Modern
//! libzmq cannot be forced to speak ZMTP/2.0 — it only downgrades when it detects a
//! v2 peer — so a hand-emitted v2 byte stream is the faithful way to prove rzmq's
//! handshake interoperates over a real TCP socket and through the full actor stack.
//!
//! Coverage:
//!   * rzmq accepts an inbound v2 peer (rzmq binds, v2 client connects).
//!   * rzmq initiates to a v2 peer and downgrades (rzmq connects, v2 server listens).
//!   * v2 identity exchange propagates to a rzmq ROUTER.

mod common;

use anyhow::Result;
use rzmq::{context::context, Msg, SocketType};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ZMTP/2.0 greeting socket-type bytes.
const V2_PUSH: &str = "8";
const V2_PULL: &str = "7";
const V2_DEALER: &str = "5";

struct ChildProcessGuard {
  child: Child,
}
impl Drop for ChildProcessGuard {
  fn drop(&mut self) {
    let _ = self.child.kill();
    let _ = self.child.wait();
  }
}

/// Spawn a python script with piped stdout, returning a guard and a line reader.
fn spawn_script(script: &str, args: &[&str]) -> Result<(ChildProcessGuard, BufReader<std::process::ChildStdout>)> {
  let mut cmd = Command::new("python3")
    .arg(format!("python_scripts/{script}"))
    .args(args)
    .stdout(Stdio::piped())
    .stderr(Stdio::inherit())
    .spawn()?;
  let stdout = cmd.stdout.take().expect("piped stdout");
  Ok((ChildProcessGuard { child: cmd }, BufReader::new(stdout)))
}

/// Block until the reader yields a line containing `token` (or time out).
fn wait_for_line(reader: &mut BufReader<std::process::ChildStdout>, token: &str, timeout: Duration) -> Result<()> {
  let start = Instant::now();
  let mut line = String::new();
  while start.elapsed() < timeout {
    line.clear();
    if reader.read_line(&mut line)? == 0 {
      anyhow::bail!("python peer closed stdout before emitting {token:?}");
    }
    if line.contains(token) {
      return Ok(());
    }
  }
  anyhow::bail!("timed out waiting for {token:?} from python peer")
}

/// rzmq PULL (bind) accepts an inbound raw ZMTP/2.0 PUSH client and receives data.
///
/// Multi-threaded runtime is required: these tests block the test thread on a
/// synchronous `read_line` while waiting for the python peer, so rzmq's spawned
/// actors (accept loop, session) must keep running on worker threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rzmq_pull_accepts_v2_push() -> Result<()> {
  common::setup_logging();
  let endpoint = "tcp://127.0.0.1:5591";

  let ctx = context()?;
  let pull = ctx.socket(SocketType::Pull)?;
  pull.bind(endpoint).await?;

  let (_guard, mut reader) = spawn_script("zmtp2_client.py", &[endpoint, V2_PUSH])?;
  wait_for_line(&mut reader, "READY", Duration::from_secs(5))?;

  let msg = tokio::time::timeout(Duration::from_secs(5), pull.recv()).await??;
  assert_eq!(msg.data().unwrap(), b"Hello", "payload from v2 PUSH peer mismatch");

  ctx.term().await?;
  Ok(())
}

/// rzmq PUSH (connect) downgrades to ZMTP/2.0 against a raw v2 PULL listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rzmq_push_connects_to_v2_pull() -> Result<()> {
  common::setup_logging();
  let endpoint = "tcp://127.0.0.1:5592";

  // The v2 server must be listening before rzmq connects.
  let (_guard, mut reader) = spawn_script("zmtp2_server.py", &[endpoint, V2_PULL])?;
  wait_for_line(&mut reader, "READY", Duration::from_secs(5))?;

  let ctx = context()?;
  let push = ctx.socket(SocketType::Push)?;
  push.connect(endpoint).await?;
  // Give the staged downgrade handshake time to complete before sending.
  tokio::time::sleep(Duration::from_millis(300)).await;
  push.send(Msg::from_static(b"Hello")).await?;

  // The v2 peer prints SUCCESS once it has read and validated our data frame.
  wait_for_line(&mut reader, "SUCCESS", Duration::from_secs(5))?;

  ctx.term().await?;
  Ok(())
}

/// A raw ZMTP/2.0 DEALER announces an identity; the rzmq ROUTER must surface it
/// as the routing-id frame, proving v2 identity exchange end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rzmq_router_receives_v2_dealer_identity() -> Result<()> {
  common::setup_logging();
  let endpoint = "tcp://127.0.0.1:5593";
  let peer_identity = "V2PEER";

  let ctx = context()?;
  let router = ctx.socket(SocketType::Router)?;
  router.bind(endpoint).await?;

  let (_guard, mut reader) = spawn_script("zmtp2_client.py", &[endpoint, V2_DEALER, peer_identity])?;
  wait_for_line(&mut reader, "READY", Duration::from_secs(5))?;

  let frames = tokio::time::timeout(Duration::from_secs(5), router.recv_multipart()).await??;
  assert_eq!(frames.len(), 2, "ROUTER should receive [identity, payload]");
  assert_eq!(
    frames[0].data().unwrap(),
    peer_identity.as_bytes(),
    "ROUTER routing-id must match the v2 DEALER's announced identity"
  );
  assert_eq!(frames[1].data().unwrap(), b"Hello", "payload mismatch");

  ctx.term().await?;
  Ok(())
}
