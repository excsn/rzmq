mod common;

use rzmq::socket::types::FromBytes;
use rzmq::socket::{
  AdaptiveThrottleSocketConfig, ThrottlePriority, ThrottleStats, ThrottleStrategy,
  ADAPTIVE_THROTTLE, ADAPTIVE_THROTTLE_STATS,
};
use rzmq::{Msg, Socket, SocketType, ZmqError};
use std::time::Duration;

async fn get_config(socket: &Socket) -> Result<AdaptiveThrottleSocketConfig, ZmqError> {
  AdaptiveThrottleSocketConfig::from_bytes(&socket.get_option(ADAPTIVE_THROTTLE).await?)
}

async fn get_stats(socket: &Socket) -> Result<Vec<ThrottleStats>, ZmqError> {
  Vec::<ThrottleStats>::from_bytes(&socket.get_option(ADAPTIVE_THROTTLE_STATS).await?)
}

async fn wait_for_stats(socket: &Socket) -> Result<Vec<ThrottleStats>, ZmqError> {
  for _ in 0..100 {
    let stats = get_stats(socket).await?;
    if !stats.is_empty() {
      return Ok(stats);
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
  }
  Err(ZmqError::Timeout)
}

#[tokio::test]
async fn test_throttle_config_set_and_get() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let socket = ctx.socket(SocketType::Push)?;

  assert_eq!(get_config(&socket).await?, AdaptiveThrottleSocketConfig::default());

  let cfg = AdaptiveThrottleSocketConfig {
    healthy_balance_width: 500,
    max_imbalance: 4000,
    yield_after_n_consecutive: 64,
    strategy: ThrottleStrategy::Linear,
    priority: ThrottlePriority::Egress,
    ..Default::default()
  };
  socket.set_option(ADAPTIVE_THROTTLE, &cfg).await?;
  assert_eq!(get_config(&socket).await?, cfg);

  socket.set_option(ADAPTIVE_THROTTLE, 0i32).await?;
  assert_eq!(
    get_config(&socket).await?,
    AdaptiveThrottleSocketConfig { enabled: false, ..cfg }
  );

  assert!(matches!(
    socket.set_option_raw(ADAPTIVE_THROTTLE, &[1, 2, 3, 4, 5]).await,
    Err(ZmqError::InvalidOptionValue(ADAPTIVE_THROTTLE))
  ));
  assert!(matches!(
    socket.set_option(ADAPTIVE_THROTTLE_STATS, 1i32).await,
    Err(ZmqError::UnsupportedOption(ADAPTIVE_THROTTLE_STATS))
  ));

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_throttle_config_rejected_after_bind_or_connect() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let bound = ctx.socket(SocketType::Pull)?;
  let connected = ctx.socket(SocketType::Push)?;

  let endpoint = common::bind_and_resolve_tcp(&bound).await?;
  connected.connect(&endpoint).await?;

  for socket in [&bound, &connected] {
    assert!(matches!(
      socket.set_option(ADAPTIVE_THROTTLE, AdaptiveThrottleSocketConfig::default()).await,
      Err(ZmqError::InvalidState(_))
    ));
    assert!(matches!(
      socket.set_option(ADAPTIVE_THROTTLE, 0i32).await,
      Err(ZmqError::InvalidState(_))
    ));
  }

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_throttle_stats_per_connection() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;
  let explicit = ctx.socket(SocketType::Pull)?;
  explicit
    .set_option(
      ADAPTIVE_THROTTLE,
      AdaptiveThrottleSocketConfig { priority: ThrottlePriority::None, ..Default::default() },
    )
    .await?;

  assert!(get_stats(&push).await?.is_empty());

  let endpoint = common::bind_and_resolve_tcp(&push).await?;
  pull.connect(&endpoint).await?;

  let push_stats = wait_for_stats(&push).await?;
  let pull_stats = wait_for_stats(&pull).await?;
  assert_eq!(push_stats.len(), 1);
  assert_eq!(pull_stats.len(), 1);
  assert_eq!(push_stats[0].priority, ThrottlePriority::Egress);
  assert_eq!(pull_stats[0].priority, ThrottlePriority::Ingress);

  for i in 0u8..10 {
    push.send(Msg::from_vec(vec![i])).await?;
  }
  for _ in 0..10 {
    common::recv_timeout(&pull, Duration::from_secs(3)).await?;
  }
  let pull_stats = get_stats(&pull).await?;
  assert!(pull_stats[0].current_balance > 0);

  explicit.connect(&endpoint).await?;
  let explicit_stats = wait_for_stats(&explicit).await?;
  assert_eq!(explicit_stats[0].priority, ThrottlePriority::None);

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_with_throttle_config() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let cfg = AdaptiveThrottleSocketConfig {
    yield_after_n_consecutive: 32,
    priority: ThrottlePriority::Ingress,
    ..Default::default()
  };
  let socket = ctx.socket(SocketType::Pull)?.with_throttle_config(cfg.clone()).await?;
  assert_eq!(get_config(&socket).await?, cfg);

  common::bind_and_resolve_tcp(&socket).await?;
  assert!(matches!(
    socket.clone().with_throttle_config(cfg).await,
    Err(ZmqError::InvalidState(_))
  ));

  ctx.term().await?;
  Ok(())
}

async fn wait_for_empty_stats(socket: &Socket) -> Result<(), ZmqError> {
  for _ in 0..100 {
    if get_stats(socket).await?.is_empty() {
      return Ok(());
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
  }
  Err(ZmqError::Timeout)
}

#[tokio::test]
async fn test_throttle_stats_entry_removed_on_disconnect() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  let endpoint = common::bind_and_resolve_tcp(&push).await?;
  pull.connect(&endpoint).await?;
  wait_for_stats(&push).await?;
  wait_for_stats(&pull).await?;

  pull.disconnect(&endpoint).await?;
  wait_for_empty_stats(&pull).await?;
  wait_for_empty_stats(&push).await?;

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_throttle_stats_ipc() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;

  let endpoint = common::unique_ipc_endpoint();
  push.bind(&endpoint).await?;
  pull.connect(&endpoint).await?;

  let push_stats = wait_for_stats(&push).await?;
  let pull_stats = wait_for_stats(&pull).await?;
  assert_eq!(push_stats.len(), 1);
  assert_eq!(pull_stats.len(), 1);
  assert_eq!(push_stats[0].priority, ThrottlePriority::Egress);
  assert_eq!(pull_stats[0].priority, ThrottlePriority::Ingress);

  ctx.term().await?;
  Ok(())
}

const RECEIVED: u32 = 10;

async fn pull_stats_after_traffic(cfg: AdaptiveThrottleSocketConfig) -> Result<(rzmq::Context, Socket), ZmqError> {
  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let pull = ctx.socket(SocketType::Pull)?;
  pull.set_option(ADAPTIVE_THROTTLE, cfg).await?;

  let endpoint = common::bind_and_resolve_tcp(&push).await?;
  pull.connect(&endpoint).await?;
  wait_for_stats(&pull).await?;

  for i in 0..RECEIVED {
    push.send(Msg::from_vec(vec![i as u8])).await?;
  }
  for _ in 0..RECEIVED {
    common::recv_timeout(&pull, Duration::from_secs(3)).await?;
  }
  Ok((ctx, pull))
}

async fn wait_for_balance(socket: &Socket, balance: i32) -> Result<ThrottleStats, ZmqError> {
  for _ in 0..100 {
    let stats = get_stats(socket).await?;
    if stats[0].current_balance == balance {
      return Ok(stats[0].clone());
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
  }
  Err(ZmqError::Timeout)
}

#[tokio::test]
async fn test_throttle_config_credit_per_message_applies() -> Result<(), ZmqError> {
  let (ctx, pull) = pull_stats_after_traffic(AdaptiveThrottleSocketConfig {
    credit_per_message: 7,
    yield_after_n_consecutive: u32::MAX,
    ..Default::default()
  })
  .await?;

  let stats = wait_for_balance(&pull, 7 * RECEIVED as i32).await?;
  assert_eq!(stats.consecutive_ingress, RECEIVED);
  assert_eq!(stats.consecutive_egress, 0);

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_throttle_config_yield_after_n_consecutive_applies() -> Result<(), ZmqError> {
  let (ctx, pull) = pull_stats_after_traffic(AdaptiveThrottleSocketConfig {
    credit_per_message: 7,
    yield_after_n_consecutive: 1,
    ..Default::default()
  })
  .await?;

  let stats = wait_for_balance(&pull, 7 * RECEIVED as i32).await?;
  assert_eq!(stats.consecutive_ingress, 0);

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_throttle_disabled_leaves_state_untouched() -> Result<(), ZmqError> {
  let (ctx, pull) = pull_stats_after_traffic(AdaptiveThrottleSocketConfig {
    enabled: false,
    ..Default::default()
  })
  .await?;

  tokio::time::sleep(Duration::from_millis(100)).await;
  let stats = get_stats(&pull).await?;
  assert_eq!(stats[0].current_balance, 0);
  assert_eq!(stats[0].consecutive_ingress, 0);
  assert_eq!(stats[0].learned_balance, 0.0);

  ctx.term().await?;
  Ok(())
}
