use rzmq::{
  Context, Msg, SocketType, ZmqError,
  socket::SocketEvent,
  socket::options::{RCVHWM, SNDHWM, SNDTIMEO},
};
use std::time::Duration;

mod common;

#[tokio::test]
async fn test_mre_ingress_deadlock_on_shutdown() -> Result<(), ZmqError> {
  println!("--- Starting MRE Inproc Ingress Deadlock Test ---");
  let ctx = common::test_context();

  let pull = ctx.socket(SocketType::Pull)?;
  let push = ctx.socket(SocketType::Push)?;

  let push_monitor = push.monitor_default().await?;

  // 1. Match the exact HWM and timeout options of the failing test
  push.set_option_raw(SNDHWM, &1i32.to_ne_bytes()).await?;
  push.set_option_raw(SNDTIMEO, &0i32.to_ne_bytes()).await?;
  pull.set_option_raw(RCVHWM, &1i32.to_ne_bytes()).await?;

  let endpoint = "inproc://mre-deadlock-hang-fixed";
  pull.bind(endpoint).await?;
  push.connect(endpoint).await?;
  tokio::time::sleep(Duration::from_millis(50)).await;

  // Drain exactly one message from pull in the background (the 65KB block)
  let pull_a_drain = tokio::spawn({
    let pull_a = pull.clone();
    async move {
      let _ = pull_a.recv().await;
    }
  });

  // 2. Send the large block to trigger congestion and saturate the inproc stream
  push.send(Msg::from_vec(vec![0u8; 65537])).await?;

  // 3. Wait for the ConnectionCongested event
  common::wait_for_monitor_event(
    &push_monitor,
    Duration::from_secs(3),
    Duration::from_millis(10),
    |e| matches!(e, SocketEvent::ConnectionCongested { endpoint: ep } if ep == endpoint),
  )
  .await
  .map_err(|e| ZmqError::Internal(format!("Congestion event not received: {}", e)))?;

  tokio::time::sleep(Duration::from_millis(50)).await;
  let _ = pull_a_drain.await;

  // 4. Blast 10,000 messages to fill the unread inproc queues
  const N: usize = 10_000;
  for i in 0..N {
    let msg = Msg::from_vec(format!("payload-{}", i).into_bytes());
    let _ = push.send(msg).await;
    tokio::task::yield_now().await;
  }

  println!("Initiating Context Termination...");

  match tokio::time::timeout(Duration::from_secs(2), ctx.term()).await {
    Ok(Ok(())) => {
      println!("Context terminated successfully! No deadlock.");
      Ok(())
    }
    Ok(Err(e)) => panic!("Context term failed: {}", e),
    Err(_) => panic!(
      "FATAL DEADLOCK: The context failed to terminate because the actors are permanently wedged."
    ),
  }
}
