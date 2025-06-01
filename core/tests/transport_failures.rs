// tests/transport_failures.rs

use rzmq::socket::options::SNDTIMEO; // Import SNDTIMEO
use rzmq::{Context, Msg, SocketType, ZmqError};
use std::fs::{self, File}; // For creating dummy files/dirs
use std::io::Write;
use std::path::Path;
use std::time::Duration;
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(250);
const CONNECT_RETRY_WAIT: Duration = Duration::from_millis(300); // Time for connect attempts

// --- Test: Connect to TCP address with no listener ---
#[tokio::test]
async fn test_tcp_connect_fail_no_listener() -> Result<(), ZmqError> {
  println!("Starting test_tcp_connect_fail_no_listener...");
  let ctx = common::test_context();
  let req = ctx.socket(SocketType::Req)?; // Using REQ as an example client

  let unused_endpoint = "tcp://127.0.0.1:5690"; // Port unlikely to be in use
  println!("REQ connecting to non-existent endpoint {}...", unused_endpoint);

  // Connect call itself should succeed immediately as it's async setup
  req.connect(unused_endpoint).await?;
  println!("REQ connect call returned Ok.");

  // Set SNDTIMEO=0 to make send non-blocking if no peer is ready
  req.set_option_raw(SNDTIMEO, &(0i32).to_ne_bytes()).await?;
  println!("REQ set SNDTIMEO=0.");

  // Allow some time for background connection attempts to fail
  tokio::time::sleep(CONNECT_RETRY_WAIT).await;

  // Attempt to send - should fail as no connection was ever established
  println!("REQ attempting send (should fail)...");
  let send_result = req.send(Msg::from_static(b"Request")).await;
  println!("REQ send result: {:?}", send_result);

  // Expect ResourceLimitReached because SNDTIMEO=0 and no peer available
  assert!(
    matches!(send_result, Err(ZmqError::ResourceLimitReached)),
    "Expected ResourceLimitReached error, got {:?}",
    send_result
  );
  println!("REQ correctly failed with ResourceLimitReached.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_tcp_connect_fail_no_listener finished.");
  Ok(())
}

// --- Test: Connect to IPC path with no listener ---
#[tokio::test]
#[cfg(feature = "ipc")]
async fn test_ipc_connect_fail_no_listener() -> Result<(), ZmqError> {
  println!("Starting test_ipc_connect_fail_no_listener...");
  let ctx = common::test_context();
  let req = ctx.socket(SocketType::Req)?;

  let non_existent_path = "/tmp/rzmq_test_non_existent_socket_connect";
  // Ensure the path doesn't exist before test
  let _ = fs::remove_file(non_existent_path);
  let _ = fs::remove_dir_all(non_existent_path); // Remove dir too just in case

  let endpoint = format!("ipc://{}", non_existent_path);
  println!("REQ connecting to non-existent endpoint {}...", endpoint);

  // Connect call itself should succeed immediately
  req.connect(&endpoint).await?;
  println!("REQ connect call returned Ok.");

  // Set SNDTIMEO=0
  req.set_option_raw(SNDTIMEO, &(0i32).to_ne_bytes()).await?;
  println!("REQ set SNDTIMEO=0.");

  // Allow time for background connection attempts to fail
  tokio::time::sleep(CONNECT_RETRY_WAIT).await;

  // Attempt to send - should fail
  println!("REQ attempting send (should fail)...");
  let send_result = req.send(Msg::from_static(b"Request")).await;
  println!("REQ send result: {:?}", send_result);

  assert!(
    matches!(send_result, Err(ZmqError::ResourceLimitReached)),
    "Expected ResourceLimitReached error, got {:?}",
    send_result
  );
  println!("REQ correctly failed with ResourceLimitReached.");

  println!("Terminating context...");
  ctx.term().await?;
  // Clean up dummy path if somehow created (shouldn't be)
  let _ = fs::remove_file(non_existent_path);
  println!("Test test_ipc_connect_fail_no_listener finished.");
  Ok(())
}

// --- Test: Bind IPC fails if path is an existing directory ---
#[tokio::test]
#[cfg(feature = "ipc")]
async fn test_ipc_bind_fail_directory_exists() -> Result<(), ZmqError> {
  println!("Starting test_ipc_bind_fail_directory_exists...");
  let ctx = common::test_context();
  let rep = ctx.socket(SocketType::Rep)?;

  let dir_path_str = "/tmp/rzmq_test_existing_dir";
  let endpoint = format!("ipc://{}", dir_path_str);

  // Ensure clean state before test
  let _ = fs::remove_file(dir_path_str); // Remove if it was a file
  let _ = fs::remove_dir_all(dir_path_str); // Remove if it was a dir

  // Create a directory at the target path
  println!("Creating directory at {}...", dir_path_str);
  fs::create_dir(dir_path_str)?;
  println!("Directory created.");

  // Attempt to bind the socket to the directory path
  println!("REP attempting to bind to directory {}...", endpoint);
  let bind_result = rep.bind(&endpoint).await;
  println!("REP bind result: {:?}", bind_result);

  // Check for expected error
  // Binding to a directory usually results in AddrInUse or a specific Io error
  // Let's check for AddrInUse primarily, or a generic IO error.
  assert!(
    matches!(bind_result, Err(ZmqError::AddrInUse(_)) | Err(ZmqError::IoError { .. })),
    "Expected AddrInUse or Io error, got {:?}",
    bind_result
  );
  println!("REP correctly failed to bind to directory.");

  println!("Terminating context...");
  ctx.term().await?;
  // Clean up the directory
  let _ = fs::remove_dir_all(dir_path_str);
  println!("Test test_ipc_bind_fail_directory_exists finished.");
  Ok(())
}

// --- Test: Bind IPC succeeds if path is an existing file (non-socket) ---
#[tokio::test]
#[cfg(feature = "ipc")]
async fn test_ipc_bind_succeeds_over_existing_file() -> Result<(), ZmqError> {
  println!("Starting test_ipc_bind_succeeds_over_existing_file...");
  let ctx = common::test_context();
  let rep = ctx.socket(SocketType::Rep)?;

  let file_path_str = "/tmp/rzmq_test_existing_file";
  let endpoint = format!("ipc://{}", file_path_str);

  // Ensure clean state before test
  let _ = fs::remove_file(file_path_str);
  let _ = fs::remove_dir_all(file_path_str);

  // Create a regular file at the target path
  println!("Creating regular file at {}...", file_path_str);
  {
    let mut file = File::create(file_path_str)?;
    file.write_all(b"This is not a socket")?;
  } // File closed here
  println!("Regular file created.");

  // Attempt to bind the socket - IpcListener should remove the file first
  println!("REP attempting to bind to file {} (should succeed)...", endpoint);
  let bind_result = rep.bind(&endpoint).await;
  println!("REP bind result: {:?}", bind_result);

  assert!(bind_result.is_ok(), "Expected bind to succeed over existing file");
  println!("REP correctly bound, overwriting existing file.");

  // Verify socket file now exists (optional, but good check)
  // Need to check metadata to confirm it's a socket, Path::exists isn't enough
  // let metadata = fs::symlink_metadata(file_path_str)?;
  // assert!(metadata.file_type().is_socket(), "Path is not a socket after bind");
  // Note: std::os::unix::fs::FileTypeExt needed for is_socket()

  println!("Terminating context...");
  ctx.term().await?; // This will trigger Drop on IpcListener which removes the file again
  println!("Test test_ipc_bind_succeeds_over_existing_file finished.");
  Ok(())
}
