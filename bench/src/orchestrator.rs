use crate::cli::{Cli, OutputFormat, Pattern};
use rzmq::ZmqError;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tracing::{error, info};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

pub async fn run(args: Cli) -> Result<(), ZmqError> {
  if args.endpoint.starts_with("inproc://") {
    run_in_process_orchestration(args).await
  } else {
    run_multi_process_orchestration(args).await
  }
}

async fn run_in_process_orchestration(args: Cli) -> Result<(), ZmqError> {
  info!("Orchestrator: Initiating in-process benchmark execution for inproc.");
  let context = rzmq::Context::new()?;

  let server_args = args.clone();
  let server_context = context.clone();

  let server_handle = tokio::spawn(async move {
    if let Err(e) = crate::server::run_with_context(server_args, server_context).await {
      error!("In-process server task exited with error: {}", e);
    }
  });

  tokio::time::sleep(Duration::from_millis(250)).await;

  let client_result = crate::client::run_with_context(args, context).await;

  server_handle.abort();
  info!("Orchestrator: In-process execution complete.");

  client_result
}

async fn run_multi_process_orchestration(args: Cli) -> Result<(), ZmqError> {
  let current_exe = std::env::current_exe()
    .map_err(|e| ZmqError::Internal(format!("Failed to locate current binary: {}", e)))?;

  info!("Orchestrator: Locating current binary at {:?}", current_exe);

  // 1. Reconstruct shared CLI arguments to pass to child processes
  let pattern_arg = match args.pattern {
    Pattern::ReqRep => "req-rep",
    Pattern::PushPull => "push-pull",
    Pattern::DealerRouter => "dealer-router",
    Pattern::PubSub => "pub-sub",
  };

  let output_arg = match args.output {
    OutputFormat::Text => "text",
    OutputFormat::Json => "json",
    OutputFormat::Csv => "csv",
  };

  let mut base_args = vec![
    format!("--endpoint={}", args.endpoint),
    format!("--pattern={}", pattern_arg),
    format!("--msg-size={}", args.msg_size),
    format!("--hwm={}", args.hwm),
    format!("--output={}", output_arg),
  ];

  if args.cork {
    base_args.push("--cork".to_string());
  }
  #[cfg(target_os = "linux")]
  if args.pin_cpus {
    base_args.push("--pin-cpus".to_string());
  }
  if let Some(msg_count) = args.messages {
    base_args.push(format!("--messages={}", msg_count));
  } else {
    base_args.push(format!("--duration={}", args.duration));
  }
  base_args.push(format!("--concurrency={}", args.concurrency));
  base_args.push(format!("--pipeline={}", args.pipeline));

  // Include Linux io_uring options if the feature was compiled
  #[cfg(feature = "io-uring")]
  {
    if args.use_io_uring {
      base_args.push("--use-io-uring".to_string()); // FIXED
    }
    if args.uring_zerocopy {
      base_args.push("--uring-zerocopy".to_string()); // FIXED
    }
    if args.uring_multishot {
      base_args.push("--uring-multishot".to_string()); // FIXED
    }
  }

  // 2. Configure the Server Command
  let mut server_cmd = Command::new(&current_exe);
  server_cmd.arg("--role").arg("server");
  server_cmd.args(&base_args);

  // If structured output (JSON or CSV) is requested, redirect Server stdout
  // to stderr to avoid corrupting the structured client data on stdout.
  if args.output != OutputFormat::Text {
    server_cmd.stdout(Stdio::piped());
  } else {
    server_cmd.stdout(Stdio::inherit());
  }
  server_cmd.stderr(Stdio::inherit());

  // Apply CPU Pinning (Server to Core 0) on Linux, only when --pin-cpus is set
  #[cfg(target_os = "linux")]
  if args.pin_cpus {
    unsafe {
      server_cmd.pre_exec(|| {
        let mut cpuset = nix::sched::CpuSet::new();
        let _ = cpuset.set(0); // Bind server to Core 0
        if let Err(e) = nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpuset) {
          eprintln!("Warning: Failed to set server CPU affinity: {:?}", e);
        }
        Ok(())
      });
    }
  }

  // 3. Launch the Server
  info!("Orchestrator: Spawning Server process...");
  let mut server_child = server_cmd
    .spawn()
    .map_err(|e| ZmqError::Internal(format!("Failed to spawn Server child process: {}", e)))?;

  // Wait 250ms to allow the server to fully bind to the address/port
  tokio::time::sleep(Duration::from_millis(250)).await;

  // 4. Configure the Client Command
  let mut client_cmd = Command::new(&current_exe);
  client_cmd.arg("--role").arg("client");
  client_cmd.args(&base_args);
  client_cmd.stdout(Stdio::inherit());
  client_cmd.stderr(Stdio::inherit());

  // Apply CPU Pinning (Client to Core 1) on Linux, only when --pin-cpus is set
  #[cfg(target_os = "linux")]
  if args.pin_cpus {
    unsafe {
      client_cmd.pre_exec(|| {
        let mut cpuset = nix::sched::CpuSet::new();
        let _ = cpuset.set(1); // Bind client to Core 1
        if let Err(e) = nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpuset) {
          eprintln!("Warning: Failed to set client CPU affinity: {:?}", e);
        }
        Ok(())
      });
    }
  }

  // 5. Launch the Client
  info!("Orchestrator: Spawning Client process...");
  let mut client_child = client_cmd
    .spawn()
    .map_err(|e| ZmqError::Internal(format!("Failed to spawn Client child process: {}", e)))?;

  // 6. Wait for the Client to complete its workload
  let client_status = client_child
    .wait()
    .await
    .map_err(|e| ZmqError::Internal(format!("Error awaiting Client child process: {}", e)))?;

  // 7. Handshake/Execution Complete. Forcefully terminate the infinite Server loop.
  info!(
    "Orchestrator: Client finished (status: {}). Terminating Server process...",
    client_status
  );
  let _ = server_child.kill().await;

  if !client_status.success() {
    error!("Orchestrator: Client exited with non-zero status.");
    return Err(ZmqError::Internal("Client process failed".into()));
  }

  info!("Orchestrator: Benchmark execution complete.");
  Ok(())
}
