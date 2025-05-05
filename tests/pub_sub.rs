// tests/pub_sub.rs

use rzmq::socket::options::SUBSCRIBE;
use rzmq::{Context, Msg, SocketType, ZmqError};
use std::collections::HashSet;
use std::time::Duration;
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(2);

// --- TCP Tests ---

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
async fn test_pub_sub_tcp_multiple_subs() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let pub_socket = ctx.socket(SocketType::Pub)?;
    let sub1 = ctx.socket(SocketType::Sub)?;
    let sub2 = ctx.socket(SocketType::Sub)?;
    let endpoint = "tcp://127.0.0.1:5564"; // Unique port

    pub_socket.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    sub1.connect(endpoint).await?;
    sub2.connect(endpoint).await?;
    sub1.set_option(SUBSCRIBE, b"").await?;
    sub2.set_option(SUBSCRIBE, b"").await?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let msg_data = b"Broadcast";
    pub_socket.send(Msg::from_static(msg_data)).await?;
    println!("PUB sent broadcast");

    // Both subscribers should receive it
    let rec1 = common::recv_timeout(&sub1, LONG_TIMEOUT).await?;
    let rec2 = common::recv_timeout(&sub2, LONG_TIMEOUT).await?;
    assert_eq!(rec1.data().unwrap(), msg_data);
    assert_eq!(rec2.data().unwrap(), msg_data);
    println!("Both SUBs received broadcast");
  }
  ctx.term().await?;
  Ok(())
}

// --- IPC Tests ---

#[tokio::test]
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

#[tokio::test]
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

// TODO: Add UNSUBSCRIBE tests
