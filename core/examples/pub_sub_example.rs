use rzmq::{socket::options as rzmq_options, Context, Msg, SocketType, ZmqError};
use std::time::Duration;
use tokio::time::sleep;

const PUB_ADDR: &str = "tcp://127.0.0.1:5556";
const NUM_UPDATES: usize = 5;
const TOPIC_A: &str = "TOPIC_A";
const TOPIC_B: &str = "TOPIC_B";

async fn run_publisher(ctx: Context) -> Result<(), ZmqError> {
  let pub_socket = ctx.socket(SocketType::Pub)?;
  println!("[PUB] Binding to {}...", PUB_ADDR);
  pub_socket.bind(PUB_ADDR).await?;
  println!("[PUB] Bound. Will start publishing shortly...");

  // Give subscribers time to connect and subscribe
  sleep(Duration::from_secs(1)).await;

  for i in 0..NUM_UPDATES {
    let topic = if i % 2 == 0 { TOPIC_A } else { TOPIC_B };
    let message_content = format!("{} Update #{}", topic, i);

    // In ZMQ, PUB sends the topic as part of the message payload.
    // The message itself *is* the topic + data.
    // SUB filters based on the beginning of the message.
    pub_socket
      .send(Msg::from_vec(message_content.clone().into_bytes()))
      .await?;
    println!("[PUB] Sent: '{}'", message_content);
    sleep(Duration::from_millis(500)).await;
  }

  // Send a final "shutdown" message for subscribers to exit cleanly (optional pattern)
  pub_socket.send(Msg::from_static(b"CONTROL:END")).await?;
  println!("[PUB] Sent all updates. Closing socket.");
  pub_socket.close().await?;
  Ok(())
}

async fn run_subscriber(ctx: Context, identity: &str, topic_to_subscribe: &str) -> Result<(), ZmqError> {
  let sub_socket = ctx.socket(SocketType::Sub)?;
  println!("[SUB {}] Connecting to {}...", identity, PUB_ADDR);
  sub_socket.connect(PUB_ADDR).await?;
  println!("[SUB {}] Connected.", identity);

  // Subscribe to the specified topic prefix
  println!("[SUB {}] Subscribing to topic: '{}'", identity, topic_to_subscribe);
  sub_socket
    .set_option_raw(rzmq_options::SUBSCRIBE, topic_to_subscribe.as_bytes())
    .await?;
  // To subscribe to all messages, use an empty byte slice: b""

  loop {
    let received_msg = sub_socket.recv().await?;
    let message_str = String::from_utf8_lossy(received_msg.data().unwrap_or_default());

    if message_str == "CONTROL:END" {
      println!("[SUB {}] Received END signal. Exiting.", identity);
      break;
    }
    println!("[SUB {}] Received: '{}'", identity, message_str);
  }

  println!("[SUB {}] Closing socket.", identity);
  sub_socket.close().await?;
  Ok(())
}

#[rzmq::main]
async fn main() -> Result<(), ZmqError> {
  tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

  println!("--- Publish-Subscribe Example ---");
  let ctx = Context::new()?;

  let publisher_ctx = ctx.clone();
  let publisher_handle = tokio::spawn(async move {
    if let Err(e) = run_publisher(publisher_ctx).await {
      eprintln!("[PUB] Error: {}", e);
    }
  });

  // Give publisher time to bind
  sleep(Duration::from_millis(200)).await;

  let sub1_ctx = ctx.clone();
  let subscriber1_handle = tokio::spawn(async move {
    if let Err(e) = run_subscriber(sub1_ctx, "SUB1", TOPIC_A).await {
      // Subscribes to TOPIC_A
      eprintln!("[SUB1] Error: {}", e);
    }
  });

  let sub2_ctx = ctx.clone();
  let subscriber2_handle = tokio::spawn(async move {
    if let Err(e) = run_subscriber(sub2_ctx, "SUB2", "").await {
      // Subscribes to ALL topics
      eprintln!("[SUB2] Error: {}", e);
    }
  });

  let sub3_ctx = ctx.clone();
  let subscriber3_handle = tokio::spawn(async move {
    if let Err(e) = run_subscriber(sub3_ctx, "SUB3", TOPIC_B).await {
      // Subscribes to TOPIC_B
      eprintln!("[SUB3] Error: {}", e);
    }
  });

  // Wait for tasks to complete
  let _ = tokio::try_join!(
    publisher_handle,
    subscriber1_handle,
    subscriber2_handle,
    subscriber3_handle
  );

  println!("[Main] Terminating context...");
  ctx.term().await?;
  println!("--- Publish-Subscribe Example Finished ---");
  Ok(())
}
