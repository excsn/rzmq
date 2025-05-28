mod common;

use rzmq::{
  socket::{SocketEvent, LINGER, PLAIN_PASSWORD, PLAIN_SERVER, PLAIN_USERNAME},
  Context, Socket, SocketType, ZmqError,
};
use serial_test::serial; // Add necessary rzmq imports
use std::time::Duration;
use tokio::time::timeout;

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
) -> Result<(Socket, async_channel::Receiver<SocketEvent>), ZmqError> {
  let client = ctx.socket(SocketType::Req)?;
  if enable_plain {
    // If the test wants the client to use PLAIN, we ensure the necessary
    // socket options are set to trigger `options.plain_options.enabled = true`.
    // If specific credentials are given, use them. Otherwise, use empty strings
    // to signify an "anonymous" PLAIN attempt.
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
#[rzmq_macros::test]
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

#[rzmq_macros::test]
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

#[rzmq_macros::test]
#[serial]
async fn test_null_client_to_plain_server_fails() {
  let ctx = common::test_context();
  let addr = "tcp://127.0.0.1:15503";

  // Server with PLAIN enabled
  let server_socket = setup_server(&ctx, addr, true).await.expect("Server setup failed");
  tokio::spawn(async move {
    let _ = server_socket.recv().await; // Server expects HELLO
  });

  // Client with NO PLAIN (defaults to NULL)
  let client_result = setup_client(&ctx, addr, false, None, None).await;

  let client_socket: Socket;
  if client_result.is_ok() {
    let (inner_client_socket, client_monitor) = client_result.unwrap();
    client_socket = inner_client_socket;
    let mut handshake_failed_event = false;
    let mut connect_failed_event = false;

    // Server expects HELLO, client sends NULL. Handshake should fail.
    let send_res = client_socket.send(rzmq::Msg::from_static(b"hello")).await;
    if send_res.is_ok() {
      let _ = client_socket.recv().await;
    }

    loop {
      match timeout(Duration::from_millis(200), client_monitor.recv()).await {
        Ok(Ok(SocketEvent::HandshakeFailed { .. })) => {
          handshake_failed_event = true;
          break;
        }
        Ok(Ok(SocketEvent::ConnectFailed { .. })) => {
          connect_failed_event = true;
          break;
        }
        Ok(Ok(event)) => {
          tracing::debug!("Client (to PLAIN server) monitor: {:?}", event);
        }
        Ok(Err(_)) | Err(_) => break,
      }
    }
    assert!(
      handshake_failed_event || connect_failed_event,
      "NULL client to PLAIN server did not result in HandshakeFailed or ConnectFailed"
    );
    client_socket.close().await.ok();
  } else {
    tracing::info!(
      "Client setup failed as expected for NULL client to PLAIN server: {:?}",
      client_result.as_ref().err().unwrap()
    );
    assert!(client_result.is_err());
  }
  ctx.term().await.unwrap();
}

// Test 4: PLAIN Client with no credentials (should still "succeed" ZMTP handshake for now)

#[rzmq_macros::test]
#[serial]
async fn test_plain_client_no_credentials() {
  let ctx = common::test_context();
  let addr = "tcp://127.0.0.1:15504";

  let server_socket = setup_server(&ctx, addr, true).await.expect("Server setup failed");
  // Client does not set username/password, but implies PLAIN by server config expectation
  // Or, more accurately, client will propose NULL if no credentials and no explicit PLAIN request.
  // To test client proposing PLAIN *without* credentials, client would need an option like ZMQ_MECHANISM=PLAIN
  // For now, this test relies on server proposing PLAIN and client accepting it with empty credentials.
  // This requires server to be configured with PLAIN_SERVER=true.
  // And client not explicitly setting credentials.

  // Let's adjust setup_client to truly enable PLAIN mechanism use even without creds,
  // if ZAP on server side is to handle anonymous.
  // For now, if client has no creds, it will propose NULL. If server *only* accepts PLAIN, this will fail.
  // If server can fallback to NULL, it would succeed with NULL.
  // The current setup_client will make the client behave as NULL client if enable_plain=true but no user/pass.
  //
  // To properly test client sending PLAIN HELLO with empty creds:
  //  1. Socket option to force PLAIN mechanism on client. (Not implemented yet)
  //  OR 2. Modify engine to propose PLAIN if `use_plain` is true, even if creds are empty.
  // Let's assume for now `setup_client(..., enable_plain=true, None, None)` means the client *tries* to use PLAIN
  // and the engine will propose it if `config.use_plain` is true.
  // This implies `config.use_plain` on client gets set if `PLAIN_USERNAME` or `PLAIN_PASSWORD` are set OR
  // if some other (hypothetical) `ZMQ_USE_PLAIN_MECHANISM` option is true.
  //
  // For this test to be meaningful *now*, the client should send an empty-credential HELLO.
  // This happens if `options.plain_options.enabled` is true, even if username/pass are None.
  // So, `setup_client` with `enable_plain=true` and `None` for user/pass should achieve this.

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
