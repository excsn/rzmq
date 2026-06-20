//! Shared harness for the interop tests.
//!
//! Every interop test drives rzmq against a real `pyzmq` / raw-ZMTP peer launched
//! as a child python process. The helpers here are the single source of truth for
//! spawning those peers, synchronizing on their `READY` line, allocating a free TCP
//! endpoint, and tearing the child down when the test ends.
//!
//! Note on the runtime flavor: every test that uses these helpers must run on a
//! `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` runtime, because the
//! helpers block the test thread on a synchronous `read_line` while waiting for the
//! python peer — on a current-thread runtime that would starve rzmq's spawned actors.

#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::Once;
use std::time::{Duration, Instant};

use anyhow::Result;

static INIT: Once = Once::new();

/// Initialize the tracing subscriber once per test binary.
pub fn setup_logging() {
  INIT.call_once(|| {
    tracing_subscriber::fmt()
      .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
      .with_test_writer()
      .init();
  });
}

/// Kills and reaps the child python peer when the test ends (pass *or* panic),
/// which is also what keeps a long-lived peer's TCP connection open until the
/// test is done with it.
pub struct ChildProcessGuard {
  pub child: Child,
}

impl Drop for ChildProcessGuard {
  fn drop(&mut self) {
    let _ = self.child.kill();
    let _ = self.child.wait();
  }
}

/// Reserve a currently-free TCP port and return it as a `tcp://127.0.0.1:<port>`
/// endpoint.
///
/// Binds `127.0.0.1:0`, reads the OS-assigned port, then drops the probe listener
/// so the real peer (rzmq or python, whichever binds) can take it. There is a tiny
/// drop->rebind window, but this eliminates the cross-run / `TIME_WAIT` collisions
/// that hardcoded ports caused.
pub fn alloc_endpoint() -> String {
  let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
  let port = listener.local_addr().expect("local_addr").port();
  drop(listener);
  format!("tcp://127.0.0.1:{port}")
}

/// Spawn `python_scripts/<script>` with `args`, piping stdout (for `READY` /
/// handshake synchronization) and inheriting stderr (surfaced in test output).
pub fn spawn_script(
  script: &str,
  args: &[&str],
) -> Result<(ChildProcessGuard, BufReader<ChildStdout>)> {
  let mut child = Command::new("python3")
    .arg(format!("python_scripts/{script}"))
    .args(args)
    .stdout(Stdio::piped())
    .stderr(Stdio::inherit())
    .spawn()?;
  let stdout = child.stdout.take().expect("piped stdout");
  Ok((ChildProcessGuard { child }, BufReader::new(stdout)))
}

/// Block until `reader` yields a line containing `token`, or time out.
pub fn wait_for_line(
  reader: &mut BufReader<ChildStdout>,
  token: &str,
  timeout: Duration,
) -> Result<()> {
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

/// Spawn a peer and block until it prints a line containing `READY`.
pub fn spawn_and_wait_ready(
  script: &str,
  args: &[&str],
  timeout: Duration,
) -> Result<(ChildProcessGuard, BufReader<ChildStdout>)> {
  let (guard, mut reader) = spawn_script(script, args)?;
  wait_for_line(&mut reader, "READY", timeout)?;
  Ok((guard, reader))
}
