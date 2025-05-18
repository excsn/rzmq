// tests/pub_sub.rs

use rzmq::socket::options::{SUBSCRIBE, UNSUBSCRIBE};
use rzmq::socket::SocketEvent;
use rzmq::{Context, Msg, SocketType, ZmqError};
use serial_test::serial;
use std::collections::HashSet;
use std::time::Duration;
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);
const MONITOR_EVENT_TIMEOUT: Duration = Duration::from_secs(3);

// --- TCP Tests ---

#[rzmq::main]
#[serial]
async fn test_pub_sub_tcp_basic() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let pub_socket = ctx.socket(SocketType::Pub)?;
    let sub_socket = ctx.socket(SocketType::Sub)?;
    let endpoint = "tcp://127.0.0.1:5562"; // Unique port

    pub_socket.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind

    sub_socket.connect(endpoint).await?;
    // Subscribe to all messages
    sub_socket.set_option(SUBSCRIBE, b"").await?;
    tokio::time::sleep(Duration::from_millis(150)).await; // Allow connect + subscribe propagation

    // Send message
    let msg_data = b"Hello Subscriber";
    pub_socket.send(Msg::from_static(msg_data)).await?;
    println!("PUB sent message");

    // Receive message
    let received_msg = common::recv_timeout(&sub_socket, LONG_TIMEOUT).await?;
    assert_eq!(received_msg.data().unwrap(), msg_data);
    println!("SUB received message");
  }
  ctx.term().await?;
  Ok(())
}

#[rzmq::main]
#[serial]
async fn test_pub_sub_tcp_topic_filter() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let pub_socket = ctx.socket(SocketType::Pub)?;
    let sub_socket = ctx.socket(SocketType::Sub)?;
    let endpoint = "tcp://127.0.0.1:5563"; // Unique port

    pub_socket.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    sub_socket.connect(endpoint).await?;
    // Subscribe only to "TopicA"
    sub_socket.set_option(SUBSCRIBE, b"TopicA").await?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Send messages on different topics
    pub_socket.send(Msg::from_static(b"TopicB: Data for B")).await?;
    pub_socket.send(Msg::from_static(b"TopicA: Data for A")).await?;
    println!("PUB sent messages");

    // Receive message - should only get TopicA
    let received_msg = common::recv_timeout(&sub_socket, LONG_TIMEOUT).await?;
    assert_eq!(received_msg.data().unwrap(), b"TopicA: Data for A");
    println!("SUB received message");

    // Check no more messages arrive (TopicB should be filtered)
    let result = common::recv_timeout(&sub_socket, SHORT_TIMEOUT).await;
    assert!(matches!(result, Err(ZmqError::Timeout)));
    println!("SUB correctly timed out waiting for TopicB");
  }
  ctx.term().await?;
  Ok(())
}

#[rzmq::main]
#[serial]
async fn test_pub_sub_tcp_multiple_subs() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let pub_socket = ctx.socket(SocketType::Pub)?;
    let sub1 = ctx.socket(SocketType::Sub)?;
    let sub2 = ctx.socket(SocketType::Sub)?;
    let endpoint = "tcp://127.0.0.1:5564";

    // Monitor PUB socket
    let pub_monitor_rx = pub_socket.monitor_default().await?;

    pub_socket.bind(endpoint).await?;
    common::wait_for_monitor_event(
      // Wait for PUB to be listening
      &pub_monitor_rx,
      MONITOR_EVENT_TIMEOUT,
      SHORT_TIMEOUT,
      |e| matches!(e, SocketEvent::Listening { endpoint: ep } if ep == endpoint),
    )
    .await
    .map_err(|e| ZmqError::Internal(format!("PUB Listening event: {}", e)))?;

    sub1.connect(endpoint).await?;
    sub2.connect(endpoint).await?;

    // Wait for SUB1 to connect (from PUB's perspective)
    common::wait_for_monitor_event(&pub_monitor_rx, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
      matches!(e, SocketEvent::Accepted { .. } | SocketEvent::HandshakeSucceeded { .. })
    })
    .await
    .map_err(|e| ZmqError::Internal(format!("SUB1 connect event: {}", e)))?;
    println!("PUB saw SUB1 connect");

    // Wait for SUB2 to connect (from PUB's perspective)
    common::wait_for_monitor_event(&pub_monitor_rx, MONITOR_EVENT_TIMEOUT, SHORT_TIMEOUT, |e| {
      matches!(e, SocketEvent::Accepted { .. } | SocketEvent::HandshakeSucceeded { .. })
    })
    .await
    .map_err(|e| ZmqError::Internal(format!("SUB2 connect event: {}", e)))?;
    println!("PUB saw SUB2 connect");

    // Now that PUB has seen both connections, proceed with subscriptions
    sub1.set_option(SUBSCRIBE, b"").await?;
    sub2.set_option(SUBSCRIBE, b"").await?;

    // Allow a brief moment for SUBSCRIBE messages to be sent by SUBs and processed by PUB side
    // (though PUB doesn't strictly need to process them for basic fanout)
    // More importantly, ensure the pipes are fully registered in PubSocket's Distributor.
    // The monitor events above confirm transport connection, pipe_attached should follow quickly.
    tokio::time::sleep(Duration::from_millis(100)).await; // Reduced from 150, as monitor helps.

    let msg_data = b"Broadcast";
    pub_socket.send(Msg::from_static(msg_data)).await?;
    println!("PUB sent broadcast");

    println!("sub1 receiving...");
    let rec1 = common::recv_timeout(&sub1, LONG_TIMEOUT).await?;
    println!("sub2 receiving...");
    let rec2 = common::recv_timeout(&sub2, LONG_TIMEOUT).await?;
    assert_eq!(rec1.data().unwrap(), msg_data);
    assert_eq!(rec2.data().unwrap(), msg_data);
    println!("Both SUBs received broadcast");
  }
  ctx.term().await?;
  Ok(())
}

// --- IPC Tests ---

#[rzmq::main]
#[serial]
#[cfg(feature = "ipc")]
async fn test_pub_sub_ipc_basic() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let pub_socket = ctx.socket(SocketType::Pub)?;
    let sub_socket = ctx.socket(SocketType::Sub)?;
    let endpoint = common::unique_ipc_endpoint();
    println!("Using IPC endpoint: {}", endpoint);

    pub_socket.bind(&endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    sub_socket.connect(&endpoint).await?;
    sub_socket.set_option(SUBSCRIBE, b"IPC").await?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    pub_socket.send(Msg::from_static(b"IPC Data")).await?;
    let received_msg = common::recv_timeout(&sub_socket, LONG_TIMEOUT).await?;
    assert_eq!(received_msg.data().unwrap(), b"IPC Data");
  }
  ctx.term().await?;
  Ok(())
}

// --- Inproc Tests ---

#[rzmq::main]
#[serial]
#[cfg(feature = "inproc")]
async fn test_pub_sub_inproc_basic() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let pub_socket = ctx.socket(SocketType::Pub)?;
    let sub_socket = ctx.socket(SocketType::Sub)?;
    let endpoint = common::unique_inproc_endpoint();

    pub_socket.bind(&endpoint).await?;
    sub_socket.connect(&endpoint).await?;
    sub_socket.set_option(SUBSCRIBE, b"").await?;
    tokio::time::sleep(Duration::from_millis(20)).await; // Short delay

    pub_socket.send(Msg::from_static(b"Inproc Msg")).await?;
    let received_msg = common::recv_timeout(&sub_socket, LONG_TIMEOUT).await?;
    assert_eq!(received_msg.data().unwrap(), b"Inproc Msg");
  }
  ctx.term().await?;
  Ok(())
}

// --- Test: Unsubscribe ---
#[rzmq::main]
#[serial]
async fn test_pub_sub_unsubscribe() -> Result<(), ZmqError> {
  println!("Starting test_pub_sub_unsubscribe...");
  let ctx = common::test_context();
  let pub_socket = ctx.socket(SocketType::Pub)?;
  let sub_socket = ctx.socket(SocketType::Sub)?;
  let endpoint = "tcp://127.0.0.1:5620"; // Unique port

  println!("Binding PUB to {}...", endpoint);
  pub_socket.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("Connecting SUB to {}...", endpoint);
  sub_socket.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect

  // Subscribe
  let topic = b"TopicToUnsub";
  println!("SUB subscribing to '{:?}'...", topic);
  sub_socket.set_option(SUBSCRIBE, topic).await?;
  tokio::time::sleep(Duration::from_millis(150)).await; // Allow subscribe propagation

  // Send and receive first message
  println!("PUB sending message 1...");
  pub_socket.send(Msg::from_static(b"TopicToUnsub:Data1")).await?;
  println!("SUB receiving message 1...");
  let rec1 = common::recv_timeout(&sub_socket, LONG_TIMEOUT).await?;
  assert_eq!(rec1.data().unwrap(), b"TopicToUnsub:Data1");
  println!("SUB received message 1.");

  // Unsubscribe
  println!("SUB unsubscribing from '{:?}'...", topic);
  sub_socket.set_option(UNSUBSCRIBE, topic).await?;
  tokio::time::sleep(Duration::from_millis(150)).await; // Allow unsubscribe propagation (if applicable)

  // Send second message
  println!("PUB sending message 2...");
  pub_socket.send(Msg::from_static(b"TopicToUnsub:Data2")).await?;

  // Attempt receive - should timeout
  println!("SUB attempting recv (should timeout)...");
  let rec2_result = common::recv_timeout(&sub_socket, LONG_TIMEOUT).await;
  assert!(
    matches!(rec2_result, Err(ZmqError::Timeout)),
    "Expected Timeout after unsubscribe, got {:?}",
    rec2_result
  );
  println!("SUB correctly timed out after unsubscribe.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_pub_sub_unsubscribe finished.");
  Ok(())
}

// --- Test: Late Subscriber ---
#[rzmq::main]
#[serial]
async fn test_pub_sub_late_subscriber() -> Result<(), ZmqError> {
  println!("Starting test_pub_sub_late_subscriber...");
  let ctx = common::test_context();
  let pub_socket = ctx.socket(SocketType::Pub)?;
  let endpoint = "tcp://127.0.0.1:5621"; // Unique port

  println!("Binding PUB to {}...", endpoint);
  pub_socket.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  // PUB sends message before SUB connects
  println!("PUB sending Message 1 (before SUB connects)...");
  pub_socket.send(Msg::from_static(b"Message 1")).await?;
  println!("PUB sent Message 1.");

  // Give message some time on the wire (though likely dropped by transport if no sub)
  tokio::time::sleep(Duration::from_millis(50)).await;

  // SUB connects late
  let sub_socket = ctx.socket(SocketType::Sub)?;
  println!("Connecting late SUB to {}...", endpoint);
  sub_socket.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect

  // SUB subscribes
  println!("Late SUB subscribing to all ('')...");
  sub_socket.set_option(SUBSCRIBE, b"").await?;
  tokio::time::sleep(Duration::from_millis(150)).await; // Allow subscribe propagation

  // PUB sends second message
  println!("PUB sending Message 2...");
  pub_socket.send(Msg::from_static(b"Message 2")).await?;
  println!("PUB sent Message 2.");

  // SUB receives - should only get Message 2
  println!("Late SUB receiving (should get Message 2)...");
  let rec_msg = common::recv_timeout(&sub_socket, LONG_TIMEOUT).await?;
  assert_eq!(rec_msg.data().unwrap(), b"Message 2");
  println!("Late SUB received Message 2.");

  // SUB attempts receive again - should timeout
  println!("Late SUB attempting recv again (should timeout)...");
  let next_recv_result = common::recv_timeout(&sub_socket, SHORT_TIMEOUT).await;
  assert!(
    matches!(next_recv_result, Err(ZmqError::Timeout)),
    "Expected Timeout for Message 1, got {:?}",
    next_recv_result
  );
  println!("Late SUB correctly did not receive Message 1.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_pub_sub_late_subscriber finished.");
  Ok(())
}

// --- Test: Subscriber Disconnects ---
#[rzmq::main]
#[serial]
async fn test_pub_sub_subscriber_disconnects() -> Result<(), ZmqError> {
  println!("Starting test_pub_sub_subscriber_disconnects...");
  let ctx = common::test_context();
  let pub_socket = ctx.socket(SocketType::Pub)?;
  let endpoint = "tcp://127.0.0.1:5622"; // Unique port

  println!("Binding PUB to {}...", endpoint);
  pub_socket.bind(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  {
    // Scope for subscriber
    let sub_socket = ctx.socket(SocketType::Sub)?;
    println!("Connecting SUB to {}...", endpoint);
    sub_socket.connect(endpoint).await?;
    println!("SUB subscribing...");
    sub_socket.set_option(SUBSCRIBE, b"").await?;
    tokio::time::sleep(Duration::from_millis(150)).await; // Connect and subscribe

    println!("PUB sending Message 1...");
    pub_socket.send(Msg::from_static(b"Message 1")).await?;
    let rec1 = common::recv_timeout(&sub_socket, LONG_TIMEOUT).await?;
    assert_eq!(rec1.data().unwrap(), b"Message 1");
    println!("SUB received Message 1.");

    println!("SUB disconnecting (going out of scope)...");
    // sub_socket dropped here
  } // sub_socket dropped

  // Allow time for disconnect to potentially propagate (though PUB doesn't care)
  println!("Waiting after SUB disconnect...");
  tokio::time::sleep(Duration::from_millis(100)).await;

  // PUB sends another message
  println!("PUB sending Message 2 (after SUB disconnect)...");
  let send_result = pub_socket.send(Msg::from_static(b"Message 2")).await;
  println!("PUB send result: {:?}", send_result);

  // PUB send should still succeed, it doesn't know/care about subscribers disconnecting
  assert!(
    send_result.is_ok(),
    "PUB send should succeed even if subscriber disconnected"
  );
  println!("PUB send correctly succeeded after SUB disconnect.");

  println!("Terminating context...");
  ctx.term().await?;
  println!("Test test_pub_sub_subscriber_disconnects finished.");
  Ok(())
}
