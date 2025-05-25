// examples/dealer_router_multi_example.rs

use futures::future::join_all;
use rzmq::{Context, Msg, MsgFlags, SocketType, ZmqError};
use std::time::Duration;
use tokio::task::JoinHandle; // Added for explicit JoinHandle type
use tokio::time::sleep;

// --- Example Constants ---
const NUM_DEALERS_EXAMPLE: usize = 32;
const NUM_MSGS_PER_DEALER_EXAMPLE: usize = 500000;
const BIND_ADDR_EXAMPLE: &str = "tcp://127.0.0.1:8960";
const SETUP_TIMEOUT_EXAMPLE: Duration = Duration::from_secs(60);
const HWM_EXAMPLE: i32 = 1000;

// --- Setup Function (remains largely the same) ---
async fn setup_dealer_router_example(
  ctx: &Context,
  num_dealers: usize,
) -> Result<(Vec<rzmq::Socket>, rzmq::Socket), ZmqError> {
  println!("[Setup] Creating ROUTER socket...");
  let router = ctx.socket(SocketType::Router)?;
  router
    .set_option(rzmq::socket::options::SNDHWM, &HWM_EXAMPLE.to_ne_bytes())
    .await?;
  router
    .set_option(rzmq::socket::options::RCVHWM, &HWM_EXAMPLE.to_ne_bytes())
    .await?;

  println!("[Setup] ROUTER binding to {}...", BIND_ADDR_EXAMPLE);
  router.bind(BIND_ADDR_EXAMPLE).await?;
  sleep(Duration::from_millis(100)).await;
  println!("[Setup] ROUTER bound.");

  let mut dealers = Vec::with_capacity(num_dealers);
  println!("[Setup] Creating and connecting {} DEALER sockets...", num_dealers);
  for i in 0..num_dealers {
    let dealer = ctx.socket(SocketType::Dealer)?;
    dealer
      .set_option(rzmq::socket::options::ROUTING_ID, format!("dealer_{}", i).as_bytes())
      .await?;
    dealer
      .set_option(rzmq::socket::options::SNDHWM, &HWM_EXAMPLE.to_ne_bytes())
      .await?;
    dealer
      .set_option(rzmq::socket::options::RCVHWM, &HWM_EXAMPLE.to_ne_bytes())
      .await?;
    // dealer
    //   .set_option(rzmq::socket::options::RECONNECT_IVL, &(-1i32).to_ne_bytes())
    //   .await?;
    println!("[Setup] DEALER {} connecting to {}...", i, BIND_ADDR_EXAMPLE);
    dealer.connect(BIND_ADDR_EXAMPLE).await?;
    println!("[Setup] DEALER {} connected.", i);
    dealers.push(dealer);
  }

  println!("[Setup] Waiting for connections to establish...");
  sleep(Duration::from_millis(200 * num_dealers as u64)).await;

  println!("[Setup] Dealers sending initial PING to router...");
  for (i, dealer) in dealers.iter().enumerate() {
    let ping_payload = format!("PING_FROM_DEALER_{}", i);
    dealer.send(Msg::from_vec(ping_payload.as_bytes().to_vec())).await?; // Using Msg::from(&[u8])
    println!("[Setup] DEALER {} sent PING.", i);
  }

  println!("[Setup] Router receiving initial PINGs...");
  for _ in 0..num_dealers {
    // Iterate num_dealers times to receive one PING from each
    let identity_frame = router.recv().await?;
    let ping_payload_frame = router.recv().await?;
    println!(
      "[Setup] ROUTER received PING: Identity='{}', Payload='{}'",
      String::from_utf8_lossy(identity_frame.data().unwrap_or_default()),
      String::from_utf8_lossy(ping_payload_frame.data().unwrap_or_default())
    );
    let pong_identity = Msg::from_vec(identity_frame.data().unwrap().to_vec());
    router
      .send_multipart(vec![pong_identity, Msg::from_vec(b"PONG_FROM_ROUTER".to_vec())])
      .await?;
  }

  println!("[Setup] Dealers receiving PONGs from router...");
  for (i, dealer) in dealers.iter().enumerate() {
    let pong_reply = dealer.recv().await?;
    println!(
      "[Setup] DEALER {} received PONG: '{}'",
      i,
      String::from_utf8_lossy(pong_reply.data().unwrap_or_default())
    );
  }

  println!("[Setup] All sockets connected and PING/PONG complete.");
  Ok((dealers, router))
}

// --- ROUTER Task Logic ---
async fn run_router_task(
  router_socket: rzmq::Socket, // Takes ownership of a (cloned) socket handle
  num_total_expected_messages: usize,
) -> Result<(), ZmqError> {
  println!(
    "[Router] Task started, expecting {} total messages.",
    num_total_expected_messages
  );
  for msg_count in 0..num_total_expected_messages {
    let identity_frame = match router_socket.recv().await {
      Ok(frame) => frame,
      Err(e) => {
        eprintln!("[Router] Error receiving identity frame (msg {}): {}", msg_count, e);
        return Err(e);
      }
    };
    if !identity_frame.is_more() {
      let err_msg = "[Router] Error: Identity frame missing MORE flag.";
      eprintln!("{}", err_msg);
      return Err(ZmqError::ProtocolViolation(err_msg.into()));
    }

    let payload_frame = match router_socket.recv().await {
      Ok(frame) => frame,
      Err(e) => {
        eprintln!("[Router] Error receiving payload frame (msg {}): {}", msg_count, e);
        return Err(e);
      }
    };
    println!(
      "[Router] Received (msg {}): Identity='{}', Payload='{}'",
      msg_count,
      String::from_utf8_lossy(identity_frame.data().unwrap_or_default()),
      String::from_utf8_lossy(payload_frame.data().unwrap_or_default())
    );

    let mut reply_identity_frame = Msg::from_vec(identity_frame.data().unwrap_or_default().to_vec());
    reply_identity_frame.set_flags(MsgFlags::MORE);
    if let Err(e) = router_socket.send(reply_identity_frame).await {
      eprintln!("[Router] Error sending reply identity (msg {}): {}", msg_count, e);
      return Err(e);
    }

    let reply_payload_data = format!(
      "RouterAck:{}",
      String::from_utf8_lossy(payload_frame.data().unwrap_or_default())
    );
    let reply_payload_frame = Msg::from_vec(reply_payload_data.as_bytes().to_vec()); // Using Msg::from(&[u8])
    if let Err(e) = router_socket.send(reply_payload_frame).await {
      eprintln!("[Router] Error sending reply payload (msg {}): {}", msg_count, e);
      return Err(e);
    }
    println!("[Router] Sent reply for msg {}.", msg_count);
  }
  println!("[Router] Task finished.");
  Ok(())
}

// --- DEALER Task Logic ---
async fn run_dealer_task(
  dealer_socket: rzmq::Socket, // Takes ownership of a (cloned) socket handle
  dealer_id: usize,
  num_messages_to_send: usize,
) -> Result<(), ZmqError> {
  println!("[Dealer {}] Task started.", dealer_id);
  for j in 0..num_messages_to_send {
    let message_content = format!("Hello from Dealer {} Msg {}", dealer_id, j);
    let msg_to_send = Msg::from_vec(message_content.as_bytes().to_vec()); // Using Msg::from(&[u8])

    if let Err(e) = dealer_socket.send(msg_to_send).await {
      eprintln!("[Dealer {}] Error sending message {}: {}", dealer_id, j, e);
      return Err(e);
    }
    println!("[Dealer {}] Sent: '{}'", dealer_id, message_content);

    match dealer_socket.recv().await {
      Ok(reply_msg) => {
        println!(
          "[Dealer {}] Received reply: '{}'",
          dealer_id,
          String::from_utf8_lossy(reply_msg.data().unwrap_or_default())
        );
      }
      Err(e) => {
        eprintln!("[Dealer {}] Error receiving reply for message {}: {}", dealer_id, j, e);
        return Err(e);
      }
    }
  }
  println!("[Dealer {}] Task finished.", dealer_id);
  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO) // Adjust log level (INFO, DEBUG, TRACE)
    .with_thread_ids(true)
    .with_thread_names(true)
    .with_target(true)
    .compact()
    .init();
  println!(
    "Starting Dealer-Router Multi Example ({} Dealers, {} Msgs/Dealer)...",
    NUM_DEALERS_EXAMPLE, NUM_MSGS_PER_DEALER_EXAMPLE
  );

  let ctx = Context::new()?;

  // `dealer_sockets` and `router_socket` are the original handles from setup.
  let (dealer_sockets_orig, router_socket_orig) = match tokio::time::timeout(
    SETUP_TIMEOUT_EXAMPLE,
    setup_dealer_router_example(&ctx, NUM_DEALERS_EXAMPLE),
  )
  .await
  {
    Ok(Ok(sockets)) => sockets,
    Ok(Err(e)) => {
      eprintln!("Example setup failed: {}", e);
      return Err(e);
    }
    Err(_) => {
      eprintln!("Example setup timed out");
      return Err(ZmqError::Timeout);
    }
  };

  // --- Spawn ROUTER Task ---
  // Pass a clone of the router socket handle to the spawned task.
  let router_join_handle: JoinHandle<Result<(), ZmqError>> = tokio::spawn(run_router_task(
    router_socket_orig.clone(),
    NUM_DEALERS_EXAMPLE * NUM_MSGS_PER_DEALER_EXAMPLE,
  ));
  println!("[Main] ROUTER task spawned.");

  // --- Spawn DEALER Tasks ---
  let mut dealer_join_handles: Vec<JoinHandle<Result<(), ZmqError>>> = Vec::new();
  for i in 0..NUM_DEALERS_EXAMPLE {
    // Pass a clone of the specific dealer socket handle to its spawned task.
    let dealer_join_handle = tokio::spawn(run_dealer_task(
      dealer_sockets_orig[i].clone(),
      i,
      NUM_MSGS_PER_DEALER_EXAMPLE,
    ));
    dealer_join_handles.push(dealer_join_handle);
    println!("[Main] DEALER {} task spawned.", i);
  }

  // --- Wait for all tasks to complete ---
  println!("[Main] Waiting for ROUTER task to complete...");
  match router_join_handle.await {
    Ok(Ok(())) => println!("[Main] ROUTER task completed successfully."),
    Ok(Err(e)) => eprintln!("[Main] ROUTER task failed: {}", e),
    Err(e) => eprintln!("[Main] ROUTER task panicked: {}", e),
  }

  println!("[Main] Waiting for all DEALER tasks to complete...");
  let all_dealer_results = join_all(dealer_join_handles).await;
  for (idx, dealer_join_result) in all_dealer_results.into_iter().enumerate() {
    match dealer_join_result {
      Ok(Ok(())) => println!("[Main] DEALER {} task completed successfully.", idx),
      Ok(Err(e)) => eprintln!("[Main] DEALER {} task failed: {}", idx, e),
      Err(e) => eprintln!("[Main] DEALER {} task panicked: {}", idx, e),
    }
  }

  println!("[Main] All tasks completed. Closing sockets.");

  // --- Cleanup ---
  // Use the original handles stored in `dealer_sockets_orig` and `router_socket_orig` for closing.
  for (i, dealer_socket_handle) in dealer_sockets_orig.into_iter().enumerate() {
    if let Err(e) = dealer_socket_handle.close().await {
      eprintln!("[Main] Error closing DEALER {}: {}", i, e);
    }
  }
  if let Err(e) = router_socket_orig.close().await {
    eprintln!("[Main] Error closing ROUTER: {}", e);
  }
  println!("[Main] Sockets closed.");

  ctx.term().await?;
  println!("[Main] Context terminated. Example finished.");
  Ok(())
}
