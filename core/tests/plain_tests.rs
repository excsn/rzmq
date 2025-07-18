mod common;

use rzmq::{
  socket::{SocketEvent, LINGER, PLAIN_PASSWORD, PLAIN_SERVER, PLAIN_USERNAME}, Socket, SocketType, ZmqError,
};
use serial_test::serial; // Add necessary rzmq imports
use std::time::Duration;
use tokio::{task::JoinHandle, time::timeout};

const TEST_TIMEOUT: Duration = Duration::from_secs(5); // Timeout for async test operations

async fn setup_server(
  ctx: &rzmq::Context,
  bind_addr: &str,
  enable_plain: bool,
  // Add other mechanism configs if needed for mixed tests
) -> Result<Socket, ZmqError> {
  let server = ctx.socket(SocketType::Rep)?;
  if enable_plain {
    server.set_option(PLAIN_SERVER, true).await?; // Enable PLAIN server role
  }
  // Add other security mechanism options here if testing mixed configs
  // e.g., server.set_option(rzmq::NOISE_XX_ENABLED, false).await?;
  server.set_option(LINGER, 0).await?; // Ensure quick close for tests
  server.bind(bind_addr).await?;
  Ok(server)
}

async fn setup_client(
  ctx: &rzmq::Context,
  connect_addr: &str,
  enable_plain: bool,
  username: Option<&str>,
  password: Option<&str>,
  // Add other mechanism configs
) -> Result<(Socket, fibre::mpmc::AsyncReceiver<SocketEvent>), ZmqError> {
  let client = ctx.socket(SocketType::Req)?;
  if enable_plain {
    let user_to_set = username.unwrap_or("");
    let pass_to_set = password.unwrap_or("");

    client.set_option_raw(PLAIN_USERNAME, user_to_set.as_bytes()).await?;
    client.set_option_raw(PLAIN_PASSWORD, pass_to_set.as_bytes()).await?;
    // Setting either PLAIN_USERNAME or PLAIN_PASSWORD makes client_socket.options.plain_options.enabled = true
  }
  // Add other security mechanism options here
  client.set_option_raw(LINGER, &0u32.to_ne_bytes()).await?;
  let client_monitor = client.monitor_default().await.unwrap();
  client.connect(connect_addr).await?;
  Ok((client, client_monitor))
}

// Test 1: Successful PLAIN Handshake
#[tokio::test]
#[serial]
async fn test_plain_successful_handshake() {
  let ctx = common::test_context();
  let addr = "tcp://127.0.0.1:15501";

  let server_socket = setup_server(&ctx, addr, true).await.expect("Server setup failed");
  let (client_socket, client_monitor) = setup_client(&ctx, addr, true, Some("user"), Some("pass"))
    .await
    .expect("Client setup failed");

  // Monitor client for handshake success
  let mut handshake_succeeded = false;

  tokio::spawn(async move {
    // Server REP logic: recv request, send reply
    let req = server_socket.recv().await.expect("Server failed to recv");
    assert_eq!(req.data().unwrap(), b"hello");
    server_socket
      .send(rzmq::Msg::from_static(b"world"))
      .await
      .expect("Server failed to send reply");
  });

  client_socket
    .send(rzmq::Msg::from_static(b"hello"))
    .await
    .expect("Client failed to send request");
  let reply = client_socket.recv().await.expect("Client failed to recv reply");
  assert_eq!(reply.data().unwrap(), b"world");

  // Check monitor events for handshake success on client
  loop {
    match timeout(Duration::from_millis(200), client_monitor.recv()).await {
      Ok(Ok(SocketEvent::HandshakeSucceeded { endpoint })) => {
        if endpoint.contains("15501") {
          // Check if it's our target endpoint
          handshake_succeeded = true;
          break;
        }
      }
      Ok(Ok(event)) => {
        tracing::debug!("Client monitor event: {:?}", event);
      }
      Ok(Err(_)) => {
        /* Monitor channel closed */
        break;
      }
      Err(_) => {
        /* Timeout */
        break;
      }
    }
  }
  assert!(
    handshake_succeeded,
    "Client did not receive HandshakeSucceeded event for PLAIN"
  );

  client_socket.close().await.unwrap();
  // server_socket.close().await.unwrap(); // Server closes when client drops
  ctx.term().await.unwrap();
}

// Test 2: PLAIN Client connecting to NULL Server (should fail)

#[tokio::test]
#[serial]
async fn test_plain_client_to_null_server_fails() {
  let ctx = common::test_context();
  let addr = "tcp://127.0.0.1:15502";

  let server_socket = setup_server(&ctx, addr, false).await.expect("Server setup failed");
  let server_task = tokio::spawn(async move {
    // Ensure server is running
    let _ = server_socket.recv().await; // It will likely never receive
    server_socket.close().await.ok();
  });

  // Client attempts PLAIN. `setup_client` includes connect.
  // The connect itself might return an error due to rapid handshake failure.
  let client_setup_result = setup_client(&ctx, addr, true, Some("user"), Some("pass")).await;

  if let Ok((client_socket, mut client_monitor)) = client_setup_result {
    // Connect "succeeded" at some level, now wait for monitor events for actual failure.
    let mut failure_event_received = false;
    for _ in 0..10 {
      // Poll a few times
      match timeout(Duration::from_millis(500), client_monitor.recv()).await {
        // Increased timeout slightly
        Ok(Ok(event)) => {
          tracing::info!("[Test Monitor - PLAIN Client to NULL Server] Event: {:?}", event);
          match event {
            SocketEvent::HandshakeFailed { .. }
            | SocketEvent::ConnectFailed { .. }
            | SocketEvent::Disconnected { .. }
            | SocketEvent::Closed { .. } => {
              failure_event_received = true;
              break;
            }
            _ => {}
          }
        }
        Ok(Err(_)) => {
          tracing::info!("[Test Monitor - PLAIN Client to NULL Server] Monitor closed.");
          failure_event_received = true;
          break;
        } // Monitor closed can also mean failure
        Err(_) => {
          tracing::info!("[Test Monitor - PLAIN Client to NULL Server] Timeout.");
        } // Just timeout, try again
      }
      if failure_event_received {
        break;
      }
    }
    assert!(
      failure_event_received,
      "PLAIN client to NULL server did not result in a failure-related monitor event."
    );
    client_socket.close().await.ok();
  } else {
    // setup_client (which includes connect) failed directly. This is also a pass.
    tracing::info!(
      "Client setup/connect failed as expected for PLAIN client to NULL server: {:?}",
      client_setup_result.as_ref().err().unwrap()
    );
    assert!(client_setup_result.is_err());
  }

  server_task.abort(); // Ensure server task is stopped
  ctx.term().await.unwrap();
}

// Test 3: NULL Client connecting to PLAIN Server (should fail)

#[tokio::test]
#[serial]
async fn test_null_client_to_plain_server_fails() {
  let ctx = common::test_context();
  let addr = "tcp://127.0.0.1:15503";

  // Server with PLAIN enabled
  let server_socket = setup_server(&ctx, addr, true).await.expect("Server setup failed");
  let _server_task: JoinHandle<()> = tokio::spawn(async move {
    // Keep server running
    // Server might receive a connection attempt that fails handshake,
    // so recv() might error or timeout if no valid client ever fully connects.
    match server_socket.recv().await {
      Ok(_) => tracing::info!("[Test Server {}] Unexpectedly received a message.", addr),
      Err(e) => tracing::info!(
        "[Test Server {}] Recv ended (expected for failed handshake): {}",
        addr,
        e
      ),
    }
    tracing::info!("[Test Server {}] Exiting task.", addr);
  });

  // Client with NO PLAIN (defaults to NULL)
  let client_setup_result = setup_client(&ctx, addr, false, None, None).await;
  assert!(
    client_setup_result.is_ok(),
    "Client setup failed unexpectedly: {:?}",
    client_setup_result.err()
  );

  let (client_socket, client_monitor) = client_setup_result.unwrap();

  // Spawn the client operations in a separate task
  let client_task: JoinHandle<Result<(), ZmqError>> = tokio::spawn(async move {
    // Server expects HELLO, client sends NULL. Handshake should fail.
    let send_res = client_socket.send(rzmq::Msg::from_static(b"hello")).await;

    if send_res.is_ok() {
      println!("OK (client task)"); // This may or may not print depending on how fast handshake fails
                                    // Attempt to recv. This should now be interrupted by socket.close() from ctx.term()
                                    // or fail due to peer detachment if handshake failure propagates error to recv.
      let recv_res = client_socket.recv().await;
      println!("Client task recv result: {:?}", recv_res);
      if recv_res.is_ok() {
        println!("(client task) - recv succeeded unexpectedly");
        return Err(ZmqError::Internal(
          "Client recv() succeeded when handshake should have failed".into(),
        ));
      } else {
        // Check for specific errors indicating termination or peer loss
        match recv_res.as_ref().err().unwrap() {
          ZmqError::ConnectionClosed | ZmqError::HostUnreachable(_) | ZmqError::SecurityError(_) | ZmqError::ResourceLimitReached => {
            println!(
              "Client task recv() failed as expected: {:?}",
              recv_res.as_ref().err().unwrap()
            );
          }
          e => {
            println!("NOOKSEND (client task) - recv failed with unexpected error: {}", e);
            return Err(e.clone()); // Propagate other errors
          }
        }
      }
    } else {
      println!(
        "NOOKSEND (client task) - send failed directly: {:?}",
        send_res.as_ref().err().unwrap()
      );
      // If send_res itself is an error, that's a valid failure path for handshake fail.
      // Ensure it's a relevant error.
      match send_res.as_ref().err().unwrap() {
        ZmqError::SecurityError(_) | ZmqError::HostUnreachable(_) | ZmqError::ResourceLimitReached => {
          println!("Client task send() failed as expected due to handshake issue.");
        }
        e => return Err(e.clone()), // Propagate other errors
      }
    }
    // Explicitly close the client socket from its task when done or errored.
    // This is good practice, though ctx.term() should also handle it.
    let close_res = client_socket.close().await;
    println!("Client task: client_socket.close() result: {:?}", close_res);
    Ok(())
  });

  // Main test task now waits for monitor events and then ctx.term()

  let mut handshake_failed_event_observed = false;
  let mut connect_failed_event_observed = false;
  let mut disconnected_event_observed = false;

  println!("Main test: Waiting for failure indication on client monitor...");
  // Give client task some time to attempt send/recv and for monitor events to propagate
  let monitor_check_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
  loop {
    if tokio::time::Instant::now() > monitor_check_deadline {
      tracing::warn!("Main test: Timeout waiting for specific failure monitor event.");
      break;
    }
    match timeout(Duration::from_millis(200), client_monitor.recv()).await {
      Ok(Ok(event)) => {
        tracing::debug!("Main test: Client monitor event: {:?}", event);
        match event {
          SocketEvent::HandshakeFailed { .. } => {
            handshake_failed_event_observed = true;
            println!("Main test: Monitor observed HandshakeFailed.");
            break;
          }
          SocketEvent::ConnectFailed { .. } => {
            connect_failed_event_observed = true;
            println!("Main test: Monitor observed ConnectFailed.");
            break;
          }
          SocketEvent::Disconnected { .. } => {
            disconnected_event_observed = true;
            println!("Main test: Monitor observed Disconnected.");
            break;
          }
          _ => {} // Ignore other events
        }
      }
      Ok(Err(e)) => {
        tracing::warn!("Main test: Monitor channel closed or error: {:?}", e);
        break;
      }
      Err(_) => {} // Timeout on this poll, loop again
    }
  }

  assert!(
    handshake_failed_event_observed || connect_failed_event_observed || disconnected_event_observed,
    "NULL client to PLAIN server did not result in HandshakeFailed, ConnectFailed, or Disconnected monitor event"
  );

  println!("Main test: Proceeding to ctx.term()...");
  ctx.term().await.unwrap(); // This should now work as client_task will finish
  println!("Main test: ctx.term() completed.");

  // Wait for the client task to finish and check its result
  println!("Main test: Waiting for client_task to join...");
  match timeout(Duration::from_secs(2), client_task).await {
    Ok(Ok(Ok(()))) => {
      println!("Main test: Client task completed successfully (meaning it handled send/recv failure correctly).")
    }
    Ok(Ok(Err(e))) => panic!("Main test: Client task failed with ZmqError: {}", e),
    Ok(Err(e)) => panic!("Main test: Client task panicked: {:?}", e),
    Err(_) => panic!("Main test: Timed out waiting for client task to join!"),
  }

  // _server_task.abort(); // Not strictly needed if ctx.term() shuts down server socket too, but good for explicit cleanup
  println!("Test test_null_client_to_plain_server_fails finished.");
}

// Test 4: PLAIN Client with no credentials (should still "succeed" ZMTP handshake for now)

#[tokio::test]
#[serial]
async fn test_plain_client_no_credentials() {
  let ctx = common::test_context();
  let addr = "tcp://127.0.0.1:15504";

  let server_socket = setup_server(&ctx, addr, true).await.expect("Server setup failed");

  let (client_socket, client_monitor) = setup_client(&ctx, addr, true, None, None) // enable_plain=true, but no creds
    .await
    .expect("Client setup failed");

  let mut handshake_succeeded = false;

  tokio::spawn(async move {
    let req = server_socket.recv().await.expect("Server failed to recv");
    assert_eq!(req.data().unwrap(), b"empty_req");
    server_socket
      .send(rzmq::Msg::from_static(b"empty_reply"))
      .await
      .expect("Server failed to send reply");
  });

  client_socket
    .send(rzmq::Msg::from_static(b"empty_req"))
    .await
    .expect("Client failed to send request");
  let reply = client_socket.recv().await.expect("Client failed to recv reply");
  assert_eq!(reply.data().unwrap(), b"empty_reply");

  loop {
    match timeout(Duration::from_millis(200), client_monitor.recv()).await {
      Ok(Ok(SocketEvent::HandshakeSucceeded { endpoint })) => {
        if endpoint.contains("15504") {
          handshake_succeeded = true;
          break;
        }
      }
      Ok(Ok(event)) => {
        tracing::debug!("Client (no creds) monitor: {:?}", event);
      }
      Ok(Err(_)) | Err(_) => break,
    }
  }
  assert!(
    handshake_succeeded,
    "Client (no creds) did not receive HandshakeSucceeded for PLAIN"
  );

  client_socket.close().await.unwrap();
  ctx.term().await.unwrap();
}
