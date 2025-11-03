mod common;

use anyhow::Result;
use rzmq::{Msg, SocketType, ZmqError, context::context};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// Helper to ensure the child process is killed on test panic/completion
struct ChildProcessGuard {
  child: Child,
}
impl Drop for ChildProcessGuard {
  fn drop(&mut self) {
    if let Err(e) = self.child.kill() {
      if e.kind() != std::io::ErrorKind::InvalidInput {
        eprintln!(
          "[RUST] Failed to kill child process {}: {}",
          self.child.id(),
          e
        );
      }
    }
    let _ = self.child.wait();
    eprintln!("[RUST] Child process {} reaped.", self.child.id());
  }
}

#[tokio::test]
async fn test_req_rep_with_pyzmq() -> Result<()> {
  // Call the logging setup function from our common module.
  common::setup_logging();

  let endpoint = "tcp://127.0.0.1:5571";

  // 1. Start Python Server
  tracing::info!("Spawning python3 process...");
  let mut cmd = Command::new("python3")
    .arg("python_scripts/rep_server.py") // IMPORTANT: Path is relative to the crate root
    .arg(endpoint)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;

  let child_id = cmd.id();
  tracing::info!(child_pid = child_id, "Python process spawned.");

  let stdout = cmd.stdout.take().expect("Failed to open stdout");
  let stderr = cmd.stderr.take().expect("Failed to open stderr");
  let _guard = ChildProcessGuard { child: cmd };

  // --- Real-time output from Python ---
  let is_ready = Arc::new(Mutex::new(false));
  let is_ready_clone = Arc::clone(&is_ready);

  // Thread for reading stdout
  thread::spawn(move || {
    let reader = BufReader::new(stdout);
    for line in reader.lines() {
      match line {
        Ok(l) => {
          println!("[py-stdout] {}", l);
          if l.contains("READY") {
            *is_ready_clone.lock().unwrap() = true;
          }
        }
        Err(e) => eprintln!("[py-stdout-ERROR] Failed to read line: {}", e),
      }
    }
  });

  // Thread for reading stderr
  thread::spawn(move || {
    let reader = BufReader::new(stderr);
    for line in reader.lines() {
      match line {
        Ok(l) => eprintln!("[py-stderr] {}", l),
        Err(e) => eprintln!("[py-stderr-ERROR] Failed to read line: {}", e),
      }
    }
  });

  // 2. Synchronize by waiting for the flag to be set
  let startup_timeout = Duration::from_secs(5);
  let start = std::time::Instant::now();

  while start.elapsed() < startup_timeout {
    if *is_ready.lock().unwrap() {
      break;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
  }

  if !*is_ready.lock().unwrap() {
    // The stderr thread will have already printed any errors.
    anyhow::bail!("Python REP server did not signal READY within timeout.");
  }

  tracing::info!("Python server is READY. Proceeding with Rust client.");

  // 3. Run Rust Client
  let ctx = context()?;
  let req_socket = ctx.socket(SocketType::Req)?;
  req_socket.connect(endpoint).await?;

  // 4. Test Communication
  tracing::info!("Sending 'hello' to Python server...");
  req_socket.send(Msg::from_static(b"hello")).await?;
  let reply = req_socket.recv().await?;
  tracing::info!("Received reply from Python server.");

  assert_eq!(reply.data().unwrap(), b"hello-reply");

  // 5. Shutdown
  tracing::info!("Sending 'shutdown' command to Python server.");
  req_socket.send(Msg::from_static(b"shutdown")).await?;

  Ok(())
}
