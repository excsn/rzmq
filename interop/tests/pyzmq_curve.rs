mod common;

use anyhow::Result;
use rzmq::socket::options::{CURVE_PUBLIC_KEY, CURVE_SECRET_KEY, CURVE_SERVER, CURVE_SERVER_KEY, RCVTIMEO};
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Msg, Socket, SocketType, context::context};
use std::io::{BufRead, BufReader};
use std::process::ChildStdout;
use std::time::Duration;

/// Reads `PY-...: <label> <value>` and returns the value.
fn read_labelled(reader: &mut BufReader<ChildStdout>, label: &str) -> Result<String> {
  let mut line = String::new();
  loop {
    line.clear();
    if reader.read_line(&mut line)? == 0 {
      anyhow::bail!("python peer closed stdout before {label}");
    }
    if let Some(idx) = line.find(label) {
      return Ok(line[idx + label.len()..].trim().to_string());
    }
  }
}

async fn handshake_outcome(monitor: &MonitorReceiver) -> Result<SocketEvent> {
  let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
  loop {
    let event = tokio::time::timeout_at(deadline, monitor.recv()).await??;
    if matches!(event, SocketEvent::HandshakeFailed { .. } | SocketEvent::HandshakeSucceeded { .. }) {
      return Ok(event);
    }
  }
}

/// A fresh keypair from libzmq, as Z85 text.
fn libzmq_keypair() -> Result<(String, String)> {
  let (_guard, mut reader) = common::spawn_script("curve_keypair.py", &[])?;
  let public = read_labelled(&mut reader, "PUBLIC ")?;
  let secret = read_labelled(&mut reader, "SECRET ")?;
  Ok((public, secret))
}

async fn rzmq_curve_client(endpoint: &str, server_key_z85: &str) -> Result<(Socket, MonitorReceiver)> {
  let (public, secret) = libzmq_keypair()?;
  let ctx = context()?;
  let req = ctx.socket(SocketType::Req)?;
  req.set_option(RCVTIMEO, 3000i32).await?;
  req.set_option(CURVE_SERVER_KEY, server_key_z85).await?;
  req.set_option(CURVE_PUBLIC_KEY, public.as_str()).await?;
  req.set_option(CURVE_SECRET_KEY, secret.as_str()).await?;
  let monitor = req.monitor_default().await?;
  req.connect(endpoint).await?;
  Ok((req, monitor))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rzmq_curve_client_against_libzmq_curve_server() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();
  let (_guard, mut reader) = common::spawn_script("curve_rep_server.py", &[endpoint.as_str()])?;
  let server_key = read_labelled(&mut reader, "PUBLIC ")?;
  common::wait_for_line(&mut reader, "READY", Duration::from_secs(5))?;

  let (req, monitor) = rzmq_curve_client(&endpoint, &server_key).await?;
  let outcome = handshake_outcome(&monitor).await?;
  assert!(matches!(outcome, SocketEvent::HandshakeSucceeded { .. }), "got {:?}", outcome);

  // 5120 bytes: a long ZMTP frame, boxed as one MESSAGE command.
  let payload: Vec<u8> = (0..=255u8).cycle().take(5120).collect();
  req.send(Msg::from_vec(payload.clone())).await?;
  let reply = req.recv().await?;
  assert_eq!(reply.data().unwrap(), [payload.as_slice(), b"-reply"].concat().as_slice());

  req.send(Msg::from_static(b"shutdown")).await?;
  assert_eq!(req.recv().await?.data().unwrap(), b"shutdown-reply");
  Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rzmq_curve_client_with_wrong_server_key_is_rejected() -> Result<()> {
  common::setup_logging();
  let endpoint = common::alloc_endpoint();
  let (_guard, mut reader) = common::spawn_script("curve_rep_server.py", &[endpoint.as_str()])?;
  read_labelled(&mut reader, "PUBLIC ")?;
  common::wait_for_line(&mut reader, "READY", Duration::from_secs(5))?;

  let (other_public, _) = libzmq_keypair()?;
  let (_req, monitor) = rzmq_curve_client(&endpoint, &other_public).await?;
  // libzmq cannot open a HELLO sealed to another key and drops the connection
  // without a reply, so the client sees a disconnect or a failed handshake.
  let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
  loop {
    let event = tokio::time::timeout_at(deadline, monitor.recv()).await??;
    match event {
      SocketEvent::HandshakeSucceeded { .. } => panic!("handshake succeeded with the wrong server key"),
      SocketEvent::HandshakeFailed { .. } | SocketEvent::Disconnected { .. } => return Ok(()),
      _ => {}
    }
  }
}

async fn rzmq_curve_server() -> Result<(Socket, MonitorReceiver, String, String)> {
  let (public, secret) = libzmq_keypair()?;
  let ctx = context()?;
  let rep = ctx.socket(SocketType::Rep)?;
  rep.set_option(CURVE_SERVER, true).await?;
  rep.set_option(CURVE_SECRET_KEY, secret.as_str()).await?;
  let monitor = rep.monitor_default().await?;
  let endpoint = common::alloc_endpoint();
  rep.bind(&endpoint).await?;
  Ok((rep, monitor, endpoint, public))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn libzmq_curve_client_against_rzmq_curve_server() -> Result<()> {
  common::setup_logging();
  let (rep, monitor, endpoint, public) = rzmq_curve_server().await?;
  let (_guard, mut reader) =
    common::spawn_and_wait_ready("curve_req_client.py", &[endpoint.as_str(), public.as_str()], Duration::from_secs(5))?;

  let outcome = handshake_outcome(&monitor).await?;
  assert!(matches!(outcome, SocketEvent::HandshakeSucceeded { .. }), "got {:?}", outcome);
  common::wait_for_line(&mut reader, "HANDSHAKE OK", Duration::from_secs(5))?;

  let frames = tokio::time::timeout(Duration::from_secs(5), rep.recv_multipart()).await??;
  assert_eq!(frames.len(), 2);
  assert_eq!(frames[0].data().unwrap(), b"hello");
  assert_eq!(frames[1].size(), 5120);
  let reply: Vec<Msg> = frames
    .iter()
    .map(|f| Msg::from_vec([f.data().unwrap(), b"-reply"].concat()))
    .collect();
  rep.send_multipart(reply).await?;
  common::wait_for_line(&mut reader, "SUCCESS", Duration::from_secs(5))?;
  Ok(())
}

/// The server's public key reaches libzmq as Z85 text written by rzmq's encoder,
/// from the 32 raw bytes `get_option(CURVE_PUBLIC_KEY)` derives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn libzmq_decodes_z85_encoded_by_rzmq() -> Result<()> {
  common::setup_logging();
  let (_, secret) = libzmq_keypair()?;
  let ctx = context()?;
  let rep = ctx.socket(SocketType::Rep)?;
  rep.set_option(CURVE_SERVER, true).await?;
  rep.set_option(CURVE_SECRET_KEY, secret.as_str()).await?;
  let public_raw = rep.get_option(CURVE_PUBLIC_KEY).await?;
  assert_eq!(public_raw.len(), 32);
  let public_z85 = rzmq::security::z85::encode(&public_raw).unwrap();
  let monitor = rep.monitor_default().await?;
  let endpoint = common::alloc_endpoint();
  rep.bind(&endpoint).await?;

  let (_guard, mut reader) =
    common::spawn_and_wait_ready("curve_req_client.py", &[endpoint.as_str(), public_z85.as_str()], Duration::from_secs(5))?;
  let outcome = handshake_outcome(&monitor).await?;
  assert!(matches!(outcome, SocketEvent::HandshakeSucceeded { .. }), "got {:?}", outcome);
  common::wait_for_line(&mut reader, "HANDSHAKE OK", Duration::from_secs(5))?;
  Ok(())
}

/// Keys written by `rzmq keygen curve` work as a server key in rzmq and as the
/// server's public key in libzmq.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_curve_key_files_are_read_by_rzmq_and_libzmq() -> Result<()> {
  common::setup_logging();
  let dir = std::env::temp_dir().join(format!("rzmq_interop_cli_keys_{}", std::process::id()));
  let _ = std::fs::remove_dir_all(&dir);
  let status = std::process::Command::new("cargo")
    .args(["run", "-q", "-p", "rzmq_cli", "--", "keygen", "curve", "--name", "server", "--output-dir"])
    .arg(&dir)
    .current_dir("..")
    .status()?;
  anyhow::ensure!(status.success(), "rzmq keygen curve failed");
  let secret = std::fs::read_to_string(dir.join("server.curve.sk"))?;
  let public = std::fs::read_to_string(dir.join("server.curve.pk"))?;

  let ctx = context()?;
  let rep = ctx.socket(SocketType::Rep)?;
  rep.set_option(CURVE_SERVER, true).await?;
  rep.set_option(CURVE_SECRET_KEY, secret.trim()).await?;
  let monitor = rep.monitor_default().await?;
  let endpoint = common::alloc_endpoint();
  rep.bind(&endpoint).await?;

  let (_guard, mut reader) =
    common::spawn_and_wait_ready("curve_req_client.py", &[endpoint.as_str(), public.trim()], Duration::from_secs(5))?;
  let outcome = handshake_outcome(&monitor).await?;
  assert!(matches!(outcome, SocketEvent::HandshakeSucceeded { .. }), "got {:?}", outcome);
  common::wait_for_line(&mut reader, "HANDSHAKE OK", Duration::from_secs(5))?;
  Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn libzmq_curve_client_with_wrong_server_key_is_rejected_by_rzmq() -> Result<()> {
  common::setup_logging();
  let (_rep, monitor, endpoint, _public) = rzmq_curve_server().await?;
  let (other_public, _) = libzmq_keypair()?;
  let (_guard, mut reader) =
    common::spawn_and_wait_ready("curve_req_client.py", &[endpoint.as_str(), other_public.as_str()], Duration::from_secs(5))?;

  let outcome = handshake_outcome(&monitor).await?;
  assert!(matches!(outcome, SocketEvent::HandshakeFailed { .. }), "got {:?}", outcome);
  common::wait_for_line(&mut reader, "HANDSHAKE FAILED", Duration::from_secs(5))?;
  Ok(())
}
