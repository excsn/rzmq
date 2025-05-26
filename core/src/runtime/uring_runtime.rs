#![cfg(feature = "io-uring")]

use super::UringLaunchInformation; // From runtime/mod.rs, originally in engine/uring_core.rs
use crate::engine::uring_core::ZmtpEngineCoreUring;
use crate::error::ZmqError;
use crate::runtime::ActorType; // For publishing ActorStarted for the uring engine

use once_cell::sync::OnceCell;
use std::os::unix::io::FromRawFd;
use std::thread;
use tokio::runtime::Runtime; // Tokio's blocking runtime

// Sender for launch requests to the uring runtime manager thread.
static URING_LAUNCH_TX: OnceCell<async_channel::Sender<UringLaunchInformation>> = OnceCell::new();
// JoinHandle for the manager thread, to allow cleanup on main context drop (optional).
static URING_THREAD_HANDLE: OnceCell<thread::JoinHandle<()>> = OnceCell::new();

/// Initializes and starts the global UringRuntimeManager thread.
/// This should be called once, typically when the first rzmq Context is created
/// and the "io-uring" feature is active.
pub fn ensure_uring_runtime_manager_started() {
  URING_LAUNCH_TX.get_or_init(|| {
    let (tx, rx) = async_channel::bounded::<UringLaunchInformation>(128); // Capacity for pending launches

    let manager_thread_join_handle = thread::Builder::new()
      .name("rzmq-uring-mgr".to_string())
      .spawn(move || {
        // Create a current_thread Tokio runtime specifically for io_uring tasks.
        let uring_runtime = match tokio::runtime::Builder::new_current_thread()
          .enable_all() // enable_io(), enable_time() might be sufficient
          .on_thread_park(|| {
            // Hint to ensure tokio-uring SQEs are submitted.
            // This is a common pattern if the uring runtime might idle.
            tokio_uring::park();
          })
          .build()
        {
          Ok(rt) => rt,
          Err(e) => {
            tracing::error!("Failed to build Tokio current_thread runtime for io_uring: {}", e);
            // If the runtime fails to build, this thread can't do anything.
            // Further submissions to URING_LAUNCH_TX will likely fail or block indefinitely.
            return;
          }
        };

        tracing::info!("rzmq-uring-mgr thread started, running on dedicated Tokio current_thread runtime.");

        // Enter the runtime context for this thread.
        let _guard = uring_runtime.enter();

        // Main loop for the UringRuntimeManager:
        // Process launch requests received on the `rx` channel.
        uring_runtime.block_on(async move {
          // Ensure we are on a runtime that has uring enabled.
          tokio_uring::assert_uring_available();

          while let Ok(launch_info) = rx.recv().await {
            tracing::debug!(
              "UringRuntimeManager: Received launch request for engine handle {}",
              launch_info.engine_handle_id
            );
            // This task is spawned on the uring_runtime.
            tokio_uring::spawn_local(async move {
              // Convert OwnedFd back to std::net::TcpStream
              let std_tcp_stream = match unsafe { std::net::TcpStream::from_raw_fd(launch_info.owned_fd.into_raw_fd()) }
              {
                Ok(s) => s,
                Err(e) => {
                  tracing::error!(
                    engine_handle = launch_info.engine_handle_id,
                    "Failed to create std::net::TcpStream from OwnedFd: {}",
                    e
                  );
                  // Notify SessionBase of failure? This path is tricky.
                  // For now, the engine just won't start.
                  // The session might time out waiting for EngineReady.
                  // A more robust solution might involve a reply channel in UringLaunchInformation.
                  let _ = launch_info
                    .session_base_mailbox
                    .try_send(crate::runtime::Command::EngineError {
                      error: ZmqError::Internal("Failed to init uring stream from FD".into()),
                    });
                  return;
                }
              };

              // Set non-blocking, though tokio_uring::from_std might handle this.
              if let Err(e) = std_tcp_stream.set_nonblocking(true) {
                tracing::error!(
                  engine_handle = launch_info.engine_handle_id,
                  "Failed to set std::net::TcpStream to non-blocking: {}",
                  e
                );
                let _ = launch_info
                  .session_base_mailbox
                  .try_send(crate::runtime::Command::EngineError {
                    error: ZmqError::Internal("Failed to set uring stream non-blocking".into()),
                  });
                return;
              }

              // Convert std::net::TcpStream to tokio_uring::net::TcpStream
              let uring_tcp_stream = match tokio_uring::net::TcpStream::from_std(std_tcp_stream) {
                Ok(s) => s,
                Err(e) => {
                  tracing::error!(
                    engine_handle = launch_info.engine_handle_id,
                    "Failed to create tokio_uring::net::TcpStream from std: {}",
                    e
                  );
                  let _ = launch_info
                    .session_base_mailbox
                    .try_send(crate::runtime::Command::EngineError {
                      error: ZmqError::Internal("Failed to create uring stream from std".into()),
                    });
                  return;
                }
              };

              launch_info.context_clone.publish_actor_started(
                launch_info.engine_handle_id,
                ActorType::Engine, // Assuming Uring Engine is still 'Engine' type
                Some(launch_info.parent_session_handle_id),
              );

              // Create and run the ZmtpEngineCoreUring
              let uring_engine = ZmtpEngineCoreUring::new(
                launch_info.engine_handle_id,
                launch_info.session_base_mailbox,
                launch_info.app_to_engine_cmd_rx,
                uring_tcp_stream,
                launch_info.config,
                launch_info.is_server,
                launch_info.context_clone.clone(), // Clone again for this task
              );

              // Publish ActorStarted for the uring engine.
              // It needs the SessionBase's handle_id as its parent for the event.
              // This requires UringLaunchInformation to carry session_handle_id.
              // For now, assuming SessionBase's handle is not readily available here.
              // A temporary solution: parent_id = None for uring engines, or SessionBase publishes it.
              // Better: Add `parent_actor_id: usize` to UringLaunchInformation.
              // Let's assume for now that ActorStarted for the engine is handled differently or
              // the bridge/session will publish it.
              // OR, the Context passed in launch_info can be used IF the parent_id is known.
              // This part needs refinement. Let's assume SessionBase/Bridge will handle this.

              // IMPORTANT: ZmtpEngineCoreUring::run_loop will now publish its own ActorStopping event.
              uring_engine.run_loop().await;
            });
          }
          tracing::info!("UringRuntimeManager: Launch request channel closed. Manager thread shutting down.");
        });
        // Uring runtime is dropped here when thread exits.
      })
      .expect("Failed to spawn rzqm-uring-mgr thread");

    URING_THREAD_HANDLE.set(manager_thread_join_handle).unwrap();
    tx
  });
}

/// Submits a request to launch an io_uring engine.
/// Returns `Ok(true)` if submitted, `Ok(false)` if uring runtime is not available/enabled,
/// `Err` if submission channel is broken.
pub fn submit_uring_engine_launch(
  launch_info: UringLaunchInformation,
) -> Result<bool, async_channel::SendError<UringLaunchInformation>> {
  if let Some(tx) = URING_LAUNCH_TX.get() {
    // Try to send without blocking the caller excessively.
    // If the launch queue is full, it indicates the uring manager is overwhelmed or stuck.
    match tx.try_send(launch_info) {
      Ok(()) => Ok(true),
      Err(async_channel::TrySendError::Full(info)) => {
        tracing::warn!(
          "UringRuntimeManager launch queue full. Engine launch for {} delayed or may fail if manager stuck.",
          info.engine_handle_id
        );
        // Fallback to blocking send with a timeout to prevent indefinite block
        // This part is tricky as this function might be called from an async context
        // that shouldn't block for long. For now, let's make it best-effort try_send.
        // A production system might need a strategy for full queues (e.g., drop, error, or bounded wait).
        // For now, let's return an error indicating it's full.
        Err(async_channel::SendError(info)) // Simulate a send error due to full
      }
      Err(async_channel::TrySendError::Closed(info)) => {
        tracing::error!(
          "UringRuntimeManager launch channel closed. Cannot launch engine {}.",
          info.engine_handle_id
        );
        Err(async_channel::SendError(info))
      }
    }
  } else {
    Ok(false) // Uring runtime not initialized
  }
}
