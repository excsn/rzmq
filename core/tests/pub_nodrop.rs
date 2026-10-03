use rzmq::socket::options::{RCVHWM, SNDHWM, SNDTIMEO, SUBSCRIBE, XPUB_NODROP};
use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use std::time::{Duration, Instant};
mod common;

const PAYLOAD: usize = 16 * 1024;
const DRAIN_GAP: Duration = Duration::from_millis(500);
const SAFETY: Duration = Duration::from_secs(20);

fn message(seq: u64) -> Vec<Msg> {
  let mut body = vec![0u8; PAYLOAD];
  body[..8].copy_from_slice(&seq.to_le_bytes());
  vec![Msg::from_static(b"t"), Msg::from_vec(body)]
}

fn seq_of(frames: &[Msg]) -> u64 {
  u64::from_le_bytes(frames[1].data().unwrap()[..8].try_into().unwrap())
}

async fn nodrop_pub(ctx: &Context, sndtimeo_ms: i32) -> Result<Socket, ZmqError> {
  let publisher = ctx.socket(SocketType::Pub)?;
  publisher.set_option(SNDHWM, 10i32).await?;
  publisher.set_option(SNDTIMEO, sndtimeo_ms).await?;
  publisher.set_option(XPUB_NODROP, 1i32).await?;
  Ok(publisher)
}

async fn subscriber(ctx: &Context, endpoint: &str) -> Result<Socket, ZmqError> {
  subscriber_with_rcvhwm(ctx, endpoint, 10).await
}

async fn subscriber_with_rcvhwm(ctx: &Context, endpoint: &str, rcvhwm: i32) -> Result<Socket, ZmqError> {
  let sub = ctx.socket(SocketType::Sub)?;
  sub.set_option(RCVHWM, rcvhwm).await?;
  sub.set_option_raw(SUBSCRIBE, b"").await?;
  sub.connect(endpoint).await?;
  Ok(sub)
}

async fn drain(sub: &Socket) -> Vec<u64> {
  let mut seqs = Vec::new();
  while let Ok(Ok(frames)) = tokio::time::timeout(DRAIN_GAP, sub.recv_multipart()).await {
    seqs.push(seq_of(&frames));
  }
  seqs
}

fn spawn_reader(sub: Socket) -> tokio::task::JoinHandle<Vec<u64>> {
  tokio::spawn(async move { drain(&sub).await })
}

#[tokio::test]
async fn nodrop_option_round_trips() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let publisher = ctx.socket(SocketType::Pub)?;
    let read = |v: Vec<u8>| i32::from_ne_bytes(v.try_into().unwrap());
    assert_eq!(read(publisher.get_option(XPUB_NODROP).await?), 0);
    publisher.set_option(XPUB_NODROP, 1i32).await?;
    assert_eq!(read(publisher.get_option(XPUB_NODROP).await?), 1);
    publisher.set_option(XPUB_NODROP, 0i32).await?;
    assert_eq!(read(publisher.get_option(XPUB_NODROP).await?), 0);
  }
  ctx.term().await
}

#[tokio::test]
async fn nodrop_with_zero_sndtimeo_returns_resource_limit_reached() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let publisher = nodrop_pub(&ctx, 0).await?;
    let endpoint = common::bind_and_resolve_tcp(&publisher).await?;
    let _slow = subscriber(&ctx, &endpoint).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut sent = 0u64;
    let outcome = loop {
      match publisher.send_multipart(message(sent)).await {
        Ok(()) => sent += 1,
        Err(e) => break e,
      }
      assert!(sent < 1_000_000, "NODROP send never reported a full subscriber");
    };
    assert!(matches!(outcome, ZmqError::ResourceLimitReached), "got {:?}", outcome);
  }
  ctx.term().await
}

#[tokio::test]
async fn nodrop_with_finite_sndtimeo_returns_timeout() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let publisher = nodrop_pub(&ctx, 150).await?;
    let endpoint = common::bind_and_resolve_tcp(&publisher).await?;
    let _slow = subscriber(&ctx, &endpoint).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut sent = 0u64;
    let (outcome, waited) = loop {
      let started = Instant::now();
      match publisher.send_multipart(message(sent)).await {
        Ok(()) => sent += 1,
        Err(e) => break (e, started.elapsed()),
      }
      assert!(sent < 1_000_000, "NODROP send never reported a full subscriber");
    };
    assert!(matches!(outcome, ZmqError::Timeout), "got {:?}", outcome);
    assert!(waited >= Duration::from_millis(150), "gave up after {:?}", waited);
  }
  ctx.term().await
}

/// Every message the PUB accepted reaches both subscribers and a refused one
/// reaches neither.
async fn assert_all_or_nothing(endpoint_for: impl AsyncFnOnce(&Socket) -> Result<String, ZmqError>, cancel: bool) -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let publisher = nodrop_pub(&ctx, if cancel { -1 } else { 0 }).await?;
    let endpoint = endpoint_for(&publisher).await?;
    let fast_first = subscriber_with_rcvhwm(&ctx, &endpoint, 100_000).await?;
    let slow = subscriber(&ctx, &endpoint).await?;
    let fast_last = subscriber_with_rcvhwm(&ctx, &endpoint, 100_000).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let fast_readers = [spawn_reader(fast_first), spawn_reader(fast_last)];

    let mut accepted = Vec::new();
    let mut refused = 0;
    let mut seq = 0u64;
    while refused < 20 {
      let attempt = publisher.send_multipart(message(seq));
      let ok = if cancel {
        matches!(tokio::time::timeout(Duration::from_millis(20), attempt).await, Ok(Ok(())))
      } else {
        attempt.await.is_ok()
      };
      if ok {
        accepted.push(seq);
      } else {
        refused += 1;
        tokio::time::sleep(Duration::from_millis(5)).await;
      }
      seq += 1;
      assert!(seq < 1_000_000, "NODROP send never refused a message");
    }

    assert!(!accepted.is_empty(), "no message was accepted");
    let slow_seqs = drain(&slow).await;
    assert_eq!(slow_seqs, accepted, "slow subscriber");
    for reader in fast_readers {
      assert_eq!(reader.await.unwrap(), accepted, "fast subscriber");
    }
  }
  ctx.term().await
}

#[tokio::test]
async fn nodrop_refused_send_reaches_no_subscriber_tcp() -> Result<(), ZmqError> {
  assert_all_or_nothing(async |s: &Socket| common::bind_and_resolve_tcp(s).await, false).await
}

#[tokio::test]
async fn nodrop_cancelled_send_reaches_no_subscriber_tcp() -> Result<(), ZmqError> {
  assert_all_or_nothing(async |s: &Socket| common::bind_and_resolve_tcp(s).await, true).await
}

#[tokio::test]
async fn nodrop_refused_send_reaches_no_subscriber_inproc() -> Result<(), ZmqError> {
  assert_all_or_nothing(
    async |s: &Socket| {
      let endpoint = common::unique_inproc_endpoint();
      s.bind(&endpoint).await?;
      Ok(endpoint)
    },
    false,
  )
  .await
}

#[tokio::test]
async fn nodrop_cancelled_send_reaches_no_subscriber_inproc() -> Result<(), ZmqError> {
  assert_all_or_nothing(
    async |s: &Socket| {
      let endpoint = common::unique_inproc_endpoint();
      s.bind(&endpoint).await?;
      Ok(endpoint)
    },
    true,
  )
  .await
}

async fn assert_blocked_send_resumes_without_loss(endpoint_for: impl AsyncFnOnce(&Socket) -> Result<String, ZmqError>) -> Result<(), ZmqError> {
  const TOTAL: u64 = 2_000;
  let ctx = common::test_context();
  {
    let publisher = nodrop_pub(&ctx, -1).await?;
    let endpoint = endpoint_for(&publisher).await?;
    let sub = subscriber(&ctx, &endpoint).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let publishing = tokio::spawn(async move {
      for seq in 0..TOTAL {
        publisher.send_multipart(message(seq)).await?;
      }
      Ok::<Socket, ZmqError>(publisher)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!publishing.is_finished(), "publisher did not block on the non-reading subscriber");

    let seqs = tokio::time::timeout(SAFETY, async {
      let mut seqs = Vec::new();
      while seqs.len() < TOTAL as usize {
        seqs.push(seq_of(&sub.recv_multipart().await?));
      }
      Ok::<_, ZmqError>(seqs)
    })
    .await
    .expect("subscriber did not receive every message")?;
    assert_eq!(seqs, (0..TOTAL).collect::<Vec<_>>());
    tokio::time::timeout(SAFETY, publishing).await.expect("publisher stayed blocked").unwrap()?;
  }
  ctx.term().await
}

#[tokio::test]
async fn nodrop_blocked_send_resumes_without_loss_tcp() -> Result<(), ZmqError> {
  assert_blocked_send_resumes_without_loss(async |s: &Socket| common::bind_and_resolve_tcp(s).await).await
}

#[tokio::test]
async fn nodrop_blocked_send_resumes_without_loss_inproc() -> Result<(), ZmqError> {
  assert_blocked_send_resumes_without_loss(async |s: &Socket| {
    let endpoint = common::unique_inproc_endpoint();
    s.bind(&endpoint).await?;
    Ok(endpoint)
  })
  .await
}

#[tokio::test]
async fn nodrop_blocked_send_completes_when_subscriber_disconnects() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let publisher = nodrop_pub(&ctx, -1).await?;
    let endpoint = common::bind_and_resolve_tcp(&publisher).await?;
    let slow = subscriber(&ctx, &endpoint).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let publishing = tokio::spawn(async move {
      let mut seq = 0u64;
      loop {
        publisher.send_multipart(message(seq)).await?;
        seq += 1;
        if seq == 100_000 {
          return Ok::<u64, ZmqError>(seq);
        }
      }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!publishing.is_finished(), "publisher did not block on the non-reading subscriber");

    slow.close().await?;
    let sent = tokio::time::timeout(SAFETY, publishing)
      .await
      .expect("blocked send was not released by the disconnect")
      .unwrap()?;
    assert_eq!(sent, 100_000);
  }
  ctx.term().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodrop_concurrent_clones_deliver_every_message_to_every_subscriber() -> Result<(), ZmqError> {
  const CLONES: u64 = 4;
  const PER_CLONE: u64 = 500;
  let ctx = common::test_context();
  {
    let publisher = nodrop_pub(&ctx, -1).await?;
    let endpoint = common::bind_and_resolve_tcp(&publisher).await?;
    let subs = [subscriber(&ctx, &endpoint).await?, subscriber(&ctx, &endpoint).await?];
    tokio::time::sleep(Duration::from_millis(300)).await;

    let readers: Vec<_> = subs
      .into_iter()
      .map(|sub| {
        tokio::spawn(async move {
          let mut seqs = Vec::new();
          while seqs.len() < (CLONES * PER_CLONE) as usize {
            let frames = sub.recv_multipart().await?;
            seqs.push(seq_of(&frames));
            if seqs.len() % 64 == 0 {
              tokio::time::sleep(Duration::from_millis(1)).await;
            }
          }
          Ok::<_, ZmqError>(seqs)
        })
      })
      .collect();

    let senders: Vec<_> = (0..CLONES)
      .map(|clone_id| {
        let handle = publisher.clone();
        tokio::spawn(async move {
          for i in 0..PER_CLONE {
            handle.send_multipart(message(clone_id * PER_CLONE + i)).await?;
          }
          Ok::<(), ZmqError>(())
        })
      })
      .collect();
    for sender in senders {
      tokio::time::timeout(SAFETY, sender).await.expect("sender stuck").unwrap()?;
    }

    for reader in readers {
      let mut seqs = tokio::time::timeout(SAFETY, reader).await.expect("reader stuck").unwrap()?;
      seqs.sort_unstable();
      assert_eq!(seqs, (0..CLONES * PER_CLONE).collect::<Vec<_>>());
    }
  }
  ctx.term().await
}
