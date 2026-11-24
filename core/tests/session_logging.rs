use parking_lot::Mutex;
use rzmq::socket::options::{HANDSHAKE_IVL, LINGER, MAXMSGSIZE, RECONNECT_IVL};
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Context, Msg, Socket, SocketType, ZmqError};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context as LayerContext, Layer, SubscriberExt};

mod common;

const EVENT_TIMEOUT: Duration = Duration::from_secs(3);
const MONITOR_POLL: Duration = Duration::from_millis(25);

#[derive(Clone, Debug)]
struct LogLine {
  level: Level,
  message: String,
}

#[derive(Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<LogLine>>>);

struct MessageText(String);

impl Visit for MessageText {
  fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
    if field.name() == "message" {
      let _ = write!(self.0, "{value:?}");
    }
  }
}

impl<S: Subscriber> Layer<S> for LogCapture {
  fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
    if !event.metadata().target().starts_with("rzmq") {
      return;
    }
    let mut text = MessageText(String::new());
    event.record(&mut text);
    self.0.lock().push(LogLine {
      level: *event.metadata().level(),
      message: text.0,
    });
  }
}

impl LogCapture {
  /// The subscriber is thread-local. On a current-thread runtime every actor
  /// logs on the test's thread, so this sees the whole context and nothing of
  /// the tests running beside it.
  fn install() -> (Self, tracing::subscriber::DefaultGuard) {
    let capture = Self::default();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    (capture, guard)
  }

  fn at(&self, level: Level) -> Vec<String> {
    self.0.lock().iter().filter(|l| l.level == level).map(|l| l.message.clone()).collect()
  }

  fn above_info(&self) -> Vec<LogLine> {
    self.0.lock().iter().filter(|l| l.level < Level::INFO).cloned().collect()
  }

  fn assert_nothing_above_info(&self) {
    let loud = self.above_info();
    assert!(loud.is_empty(), "lines above INFO: {loud:#?}");
  }

  /// Returns once a line at any level contains `marker`.
  async fn wait_for_text(&self, marker: &str) {
    let seen = async {
      while !self.0.lock().iter().any(|l| l.message.contains(marker)) {
        tokio::time::sleep(MONITOR_POLL).await;
      }
    };
    if tokio::time::timeout(EVENT_TIMEOUT, seen).await.is_err() {
      panic!("no line containing {marker:?}");
    }
  }

  async fn wait_for_line(&self, level: Level, marker: &str) {
    let seen = async {
      while !self.at(level).iter().any(|m| m.contains(marker)) {
        tokio::time::sleep(MONITOR_POLL).await;
      }
    };
    if tokio::time::timeout(EVENT_TIMEOUT, seen).await.is_err() {
      panic!("no {level} line containing {marker:?} in {:#?}", self.at(level));
    }
  }

  /// The first line at `level` whose text contains `marker`.
  fn line(&self, level: Level, marker: &str) -> String {
    let lines = self.at(level);
    lines
      .iter()
      .find(|m| m.contains(marker))
      .cloned()
      .unwrap_or_else(|| panic!("no {level} line containing {marker:?} in {lines:#?}"))
  }
}

async fn wait_for(monitor: &MonitorReceiver, what: &str, pick: impl Fn(&SocketEvent) -> bool) -> SocketEvent {
  match common::wait_for_monitor_event(monitor, EVENT_TIMEOUT, MONITOR_POLL, pick).await {
    Ok(event) => event,
    Err(e) => panic!("{what}: {e}"),
  }
}

/// Returns once `socket`'s core has finished handling the stop of the session
/// behind the next `Disconnected`: the core answers a command only after the
/// event handler that reported the disconnect has returned.
async fn session_stopped(socket: &Socket, monitor: &MonitorReceiver) {
  wait_for(monitor, "session stop", |e| matches!(e, SocketEvent::Disconnected { .. })).await;
  socket.get_option(LINGER).await.expect("core answers after the stop");
}

async fn bound_pull(ctx: &Context, max_msg_size: Option<i64>) -> Result<(Socket, MonitorReceiver, String), ZmqError> {
  let pull = ctx.socket(SocketType::Pull)?;
  if let Some(limit) = max_msg_size {
    pull.set_option_raw(MAXMSGSIZE, &limit.to_ne_bytes()).await?;
  }
  let monitor = pull.monitor_default().await?;
  let endpoint = common::bind_and_resolve_tcp(&pull).await?;
  Ok((pull, monitor, endpoint))
}

fn unused_tcp_endpoint() -> String {
  let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
  let port = listener.local_addr().expect("local addr").port();
  format!("tcp://127.0.0.1:{port}")
}

/// A PUSH retrying `endpoint` every 50 ms that has failed to connect `failures` times.
async fn push_failing_to_connect(ctx: &Context, endpoint: &str, failures: usize) -> Result<(Socket, MonitorReceiver), ZmqError> {
  let push = ctx.socket(SocketType::Push)?;
  push.set_option(RECONNECT_IVL, 50i32).await?;
  let monitor = push.monitor_default().await?;
  push.connect(endpoint).await?;
  for _ in 0..failures {
    wait_for(&monitor, "connect retry", |e| matches!(e, SocketEvent::ConnectRetried { .. })).await;
  }
  Ok((push, monitor))
}

/// A listener that never accepts and whose accept queue is full. The kernel
/// drops the SYN of a further connect, so that connect stays in progress.
struct SaturatedListener {
  endpoint: String,
  _listener: TcpListener,
  _queued: Vec<TcpStream>,
}

async fn saturated_listener() -> SaturatedListener {
  let socket = TcpSocket::new_v4().expect("listener socket");
  socket.bind("127.0.0.1:0".parse().expect("loopback address")).expect("bind");
  let listener = socket.listen(1).expect("listen");
  let address = listener.local_addr().expect("local addr");
  let mut queued = Vec::new();
  for _ in 0..64 {
    match tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(address)).await {
      Ok(Ok(stream)) => queued.push(stream),
      Ok(Err(e)) => panic!("connect to a full accept queue failed: {e}"),
      Err(_) => {
        return SaturatedListener {
          endpoint: format!("tcp://{address}"),
          _listener: listener,
          _queued: queued,
        }
      }
    }
  }
  panic!("accept queue of {address} never filled");
}

/// A PUSH whose connecter is inside its first connect attempt to `endpoint`.
async fn push_connecting(ctx: &Context, logs: &LogCapture, endpoint: &str) -> Result<Socket, ZmqError> {
  let push = ctx.socket(SocketType::Push)?;
  push.connect(endpoint).await?;
  logs.wait_for_line(Level::DEBUG, "TCP connect attempt #1").await;
  Ok(push)
}

fn assert_connecter_stopped_quietly(logs: &LogCapture, endpoint: &str) {
  logs.assert_nothing_above_info();
  let line = logs.line(Level::DEBUG, "stopped during TCP connect attempt");
  assert!(line.contains(endpoint), "{line:?} does not name {endpoint}");
}

async fn raw_connect(endpoint: &str) -> TcpStream {
  TcpStream::connect(endpoint.strip_prefix("tcp://").expect("tcp endpoint"))
    .await
    .expect("raw connect")
}

/// A PUSH in its own context on its own thread, as a peer process would be. Its
/// log lines go to the global subscriber and stay out of the test's capture.
struct RemotePush {
  finish: Option<tokio::sync::oneshot::Sender<()>>,
  thread: Option<std::thread::JoinHandle<()>>,
}

impl RemotePush {
  /// Connects, sends `payload` once its handshake completes, then closes right
  /// away with `close_now` or when the test ends.
  fn start(endpoint: String, payload: Option<Vec<u8>>, close_now: bool) -> Self {
    let (finish, finished) = tokio::sync::oneshot::channel::<()>();
    let thread = std::thread::spawn(move || {
      let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("peer runtime");
      runtime.block_on(async move {
        let ctx = common::test_context();
        let push = ctx.socket(SocketType::Push).expect("peer socket");
        let monitor = push.monitor_default().await.expect("peer monitor");
        push.connect(&endpoint).await.expect("peer connect");
        wait_for(&monitor, "peer handshake", |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;
        if let Some(payload) = payload {
          push.send(Msg::from_vec(payload)).await.expect("peer send");
        }
        if !close_now {
          let _ = finished.await;
        }
        push.close().await.expect("peer close");
        ctx.term().await.expect("peer term");
      });
    });
    Self {
      finish: Some(finish),
      thread: Some(thread),
    }
  }
}

impl Drop for RemotePush {
  fn drop(&mut self) {
    drop(self.finish.take());
    if let Some(thread) = self.thread.take() {
      let _ = thread.join();
    }
  }
}

/// A bound PULL whose one peer has completed its handshake. Returns the
/// session's endpoint as the PULL side names it.
async fn pull_with_peer(
  ctx: &Context,
  max_msg_size: Option<i64>,
  payload: Option<Vec<u8>>,
  close_now: bool,
) -> Result<(Socket, MonitorReceiver, RemotePush, String), ZmqError> {
  let (pull, monitor, endpoint) = bound_pull(ctx, max_msg_size).await?;
  let peer = RemotePush::start(endpoint, payload, close_now);
  let event = wait_for(&monitor, "handshake", |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;
  let SocketEvent::HandshakeSucceeded { endpoint: session } = event else {
    unreachable!()
  };
  Ok((pull, monitor, peer, session))
}

/// A session that completes its handshake, then fails on a frame over the
/// receiver's MAXMSGSIZE well inside its first second.
async fn session_failing_after_handshake() -> Result<(LogCapture, String), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let (pull, monitor, _peer, session) = pull_with_peer(&ctx, Some(64), Some(vec![0u8; 128]), false).await?;

  session_stopped(&pull, &monitor).await;
  Ok((logs, session))
}

/// libzmq reports a connection that closes before its handshake as an event and
/// has no log. A port probe is not an error of this library.
#[tokio::test(flavor = "current_thread")]
async fn peer_closing_before_the_handshake_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let (pull, monitor, endpoint) = bound_pull(&ctx, None).await?;

  drop(raw_connect(&endpoint).await);
  session_stopped(&pull, &monitor).await;

  logs.assert_nothing_above_info();
  ctx.term().await
}

#[cfg(all(unix, feature = "ipc"))]
#[tokio::test(flavor = "current_thread")]
async fn ipc_peer_closing_before_the_handshake_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  let monitor = pull.monitor_default().await?;
  let endpoint = common::unique_ipc_endpoint();
  pull.bind(&endpoint).await?;

  let path = endpoint.strip_prefix("ipc://").expect("ipc endpoint");
  drop(tokio::net::UnixStream::connect(path).await.expect("raw connect"));
  session_stopped(&pull, &monitor).await;

  logs.assert_nothing_above_info();
  ctx.term().await
}

#[tokio::test(flavor = "current_thread")]
async fn peer_reset_before_the_handshake_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let (pull, monitor, endpoint) = bound_pull(&ctx, None).await?;

  let stream = raw_connect(&endpoint).await;
  wait_for(&monitor, "accept", |e| matches!(e, SocketEvent::Accepted { .. })).await;
  stream.set_linger(Some(Duration::ZERO)).expect("linger 0");
  drop(stream);
  session_stopped(&pull, &monitor).await;

  logs.assert_nothing_above_info();
  ctx.term().await
}

#[tokio::test(flavor = "current_thread")]
async fn handshake_timeout_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  pull.set_option(HANDSHAKE_IVL, 200i32).await?;
  let monitor = pull.monitor_default().await?;
  let endpoint = common::bind_and_resolve_tcp(&pull).await?;

  let _silent = raw_connect(&endpoint).await;
  session_stopped(&pull, &monitor).await;

  logs.assert_nothing_above_info();
  ctx.term().await
}

#[tokio::test(flavor = "current_thread")]
async fn peer_sending_a_bad_greeting_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let (pull, monitor, endpoint) = bound_pull(&ctx, None).await?;

  let mut stream = raw_connect(&endpoint).await;
  stream.write_all(&[b'x'; 64]).await.expect("write greeting");
  session_stopped(&pull, &monitor).await;

  logs.assert_nothing_above_info();
  ctx.term().await
}

#[tokio::test(flavor = "current_thread")]
async fn connecting_socket_logs_nothing_above_info_when_its_peer_closes_before_the_handshake() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind raw peer");
  let endpoint = format!("tcp://{}", listener.local_addr().expect("local addr"));
  tokio::spawn(async move {
    while let Ok((stream, _)) = listener.accept().await {
      drop(stream);
    }
  });

  let ctx = common::test_context();
  let push = ctx.socket(SocketType::Push)?;
  let monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;
  session_stopped(&push, &monitor).await;

  logs.assert_nothing_above_info();
  ctx.term().await
}

/// A peer that connects, delivers and closes inside a second is ordinary use.
#[tokio::test(flavor = "current_thread")]
async fn peer_closing_cleanly_right_after_its_handshake_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let (pull, monitor, _peer, _session) = pull_with_peer(&ctx, None, Some(b"only".to_vec()), true).await?;

  let received = common::recv_timeout(&pull, EVENT_TIMEOUT).await?;
  assert_eq!(received.data().unwrap(), b"only");
  session_stopped(&pull, &monitor).await;

  logs.assert_nothing_above_info();
  ctx.term().await
}

#[tokio::test(flavor = "current_thread")]
async fn peer_close_line_names_its_endpoint() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let (pull, monitor, _peer, session) = pull_with_peer(&ctx, None, None, true).await?;

  session_stopped(&pull, &monitor).await;

  let line = logs.line(Level::INFO, "Peer closed");
  assert!(line.contains(&session), "{line:?} does not name {session}");
  ctx.term().await
}

#[tokio::test(flavor = "current_thread")]
async fn fatal_session_error_line_names_its_endpoint_phase_and_cause() -> Result<(), ZmqError> {
  let (logs, session) = session_failing_after_handshake().await?;

  let line = logs.line(Level::ERROR, "SCA fatal error");
  assert!(line.contains(&session), "{line:?} does not name {session}");
  assert!(line.contains("Operational"), "{line:?} does not name the phase");
  assert!(line.contains("exceeds ZMQ_MAXMSGSIZE"), "{line:?} does not name the cause");
  Ok(())
}

/// A session that completes its handshake and dies on an error inside a second
/// still warns. The warning says which session.
#[tokio::test(flavor = "current_thread")]
async fn churn_warning_names_its_endpoint() -> Result<(), ZmqError> {
  let (logs, session) = session_failing_after_handshake().await?;

  let line = logs.line(Level::WARN, "died too quickly");
  assert!(line.contains(&session), "{line:?} does not name {session}");
  Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn core_warning_for_a_failed_session_names_its_endpoint_and_cause() -> Result<(), ZmqError> {
  let (logs, session) = session_failing_after_handshake().await?;

  let line = logs.line(Level::WARN, "stopped unexpectedly");
  assert!(line.contains(&session), "{line:?} does not name {session}");
  assert!(line.contains("exceeds ZMQ_MAXMSGSIZE"), "{line:?} does not name the cause");
  Ok(())
}

/// libzmq reports a failed connect attempt as `ZMQ_EVENT_CONNECT_RETRIED` and
/// has no log. One warning tells an operator the peer is down; one per retry
/// floods the log for as long as it stays down.
#[tokio::test(flavor = "current_thread")]
async fn connecter_warns_once_for_a_peer_that_stays_down() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let endpoint = unused_tcp_endpoint();
  let (_push, _monitor) = push_failing_to_connect(&ctx, &endpoint, 4).await?;

  let warnings = logs.at(Level::WARN);
  assert_eq!(warnings.len(), 1, "warnings: {warnings:#?}");
  assert!(warnings[0].contains(&endpoint), "{:?} does not name {endpoint}", warnings[0]);
  assert!(warnings[0].contains("refused"), "{:?} does not name the cause", warnings[0]);
  assert!(logs.at(Level::ERROR).is_empty());
  ctx.term().await
}

#[tokio::test(flavor = "current_thread")]
async fn connecter_success_line_names_its_endpoint() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let endpoint = unused_tcp_endpoint();
  let (_push, monitor) = push_failing_to_connect(&ctx, &endpoint, 2).await?;

  let pull = ctx.socket(SocketType::Pull)?;
  pull.bind(&endpoint).await?;
  wait_for(&monitor, "handshake", |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;

  let line = logs.line(Level::INFO, "TCP connect");
  assert!(line.contains(&endpoint), "{line:?} does not name {endpoint}");
  ctx.term().await
}

#[tokio::test(flavor = "current_thread")]
async fn closing_a_socket_during_a_connect_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let listener = saturated_listener().await;
  let push = push_connecting(&ctx, &logs, &listener.endpoint).await?;

  push.close().await?;
  ctx.term().await?;

  assert_connecter_stopped_quietly(&logs, &listener.endpoint);
  Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn terminating_a_context_during_a_connect_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let listener = saturated_listener().await;
  let _push = push_connecting(&ctx, &logs, &listener.endpoint).await?;

  ctx.term().await?;

  assert_connecter_stopped_quietly(&logs, &listener.endpoint);
  Ok(())
}

/// A PUSH connected to a PULL in `ctx`, both past their handshake, each with
/// its monitor. A socket warns when its monitor's receiver is gone.
async fn connected_pair(ctx: &Context) -> Result<((Socket, MonitorReceiver), (Socket, MonitorReceiver)), ZmqError> {
  let (pull, pull_monitor, endpoint) = bound_pull(ctx, None).await?;
  let push = ctx.socket(SocketType::Push)?;
  let push_monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;
  wait_for(&push_monitor, "push handshake", |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;
  wait_for(&pull_monitor, "pull handshake", |e| matches!(e, SocketEvent::HandshakeSucceeded { .. })).await;
  Ok(((push, push_monitor), (pull, pull_monitor)))
}

#[tokio::test(flavor = "current_thread")]
async fn closing_connected_sockets_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let ((push, _push_monitor), (pull, pull_monitor)) = connected_pair(&ctx).await?;

  push.close().await?;
  session_stopped(&pull, &pull_monitor).await;
  pull.close().await?;
  ctx.term().await?;

  logs.assert_nothing_above_info();
  Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn terminating_a_context_with_connected_sockets_logs_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let (_push, _pull) = connected_pair(&ctx).await?;

  ctx.term().await?;

  logs.assert_nothing_above_info();
  Ok(())
}

/// A DNS label over 63 bytes cannot be resolved, so the lookup fails without a
/// query leaving the machine.
#[tokio::test(flavor = "current_thread")]
async fn fatal_connect_error_is_one_line_naming_its_endpoint_and_cause() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let endpoint = format!("tcp://{}.invalid:5555", "a".repeat(64));
  let push = ctx.socket(SocketType::Push)?;
  let monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;
  wait_for(&monitor, "connect failure", |e| matches!(e, SocketEvent::ConnectFailed { .. })).await;
  push.get_option(LINGER).await?;

  let from_connecter = |level| -> Vec<String> { logs.at(level).into_iter().filter(|m| m.starts_with("Connecter:")).collect() };
  let errors = from_connecter(Level::ERROR);
  assert_eq!(errors.len(), 1, "connecter errors: {errors:#?}");
  assert!(errors[0].contains(&endpoint), "{:?} does not name {endpoint}", errors[0]);
  assert!(errors[0].contains("Failed to resolve"), "{:?} does not name the cause", errors[0]);
  let warnings = from_connecter(Level::WARN);
  assert!(warnings.is_empty(), "connecter warnings: {warnings:#?}");
  ctx.term().await
}

/// Waits until `socket`'s core has handled a connect that failed for good: the
/// failure report first, then the stop of the connecter that sent it.
async fn failed_connect_handled(logs: &LogCapture, monitor: &MonitorReceiver) -> usize {
  wait_for(monitor, "connect failure", |e| matches!(e, SocketEvent::ConnectFailed { .. })).await;
  logs.wait_for_text("reconnect not enabled or error is fatal").await;
  logs.wait_for_text("Child Connecter").await;
  let later = std::iter::from_fn(|| monitor.try_recv().ok())
    .filter(|e| matches!(e, SocketEvent::ConnectFailed { .. }))
    .count();
  1 + later
}

fn assert_one_loud_line(logs: &LogCapture, level: Level, endpoint: &str, cause: &str) {
  let loud = logs.above_info();
  assert_eq!(loud.len(), 1, "lines above INFO: {loud:#?}");
  assert_eq!(loud[0].level, level, "{:?}", loud[0]);
  assert!(loud[0].message.contains(endpoint), "{:?} does not name {endpoint}", loud[0].message);
  assert!(loud[0].message.contains(cause), "{:?} does not name the cause", loud[0].message);
}

#[tokio::test(flavor = "current_thread")]
async fn a_fatal_tcp_connect_error_is_reported_once() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let endpoint = format!("tcp://{}.invalid:5555", "a".repeat(64));
  let push = ctx.socket(SocketType::Push)?;
  let monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;

  let connect_failed_events = failed_connect_handled(&logs, &monitor).await;

  assert_eq!(connect_failed_events, 1);
  assert_one_loud_line(&logs, Level::ERROR, &endpoint, "Failed to resolve");
  Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_ipc_connect_without_retries_is_reported_once() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let endpoint = common::unique_ipc_endpoint();
  let push = ctx.socket(SocketType::Push)?;
  push.set_option(RECONNECT_IVL, 0i32).await?;
  let monitor = push.monitor_default().await?;
  push.connect(&endpoint).await?;

  let connect_failed_events = failed_connect_handled(&logs, &monitor).await;

  assert_eq!(connect_failed_events, 1);
  assert_one_loud_line(&logs, Level::ERROR, &endpoint, "No such file");
  Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn events_for_a_dropped_monitor_receiver_log_nothing_above_info() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let (pull, monitor, endpoint) = bound_pull(&ctx, None).await?;
  drop(monitor);

  let _peer = RemotePush::start(endpoint, Some(b"only".to_vec()), false);
  let received = common::recv_timeout(&pull, EVENT_TIMEOUT).await?;
  assert_eq!(received.data().unwrap(), b"only");

  logs.assert_nothing_above_info();
  Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_full_monitor_warns_with_the_event_it_drops() -> Result<(), ZmqError> {
  let (logs, _guard) = LogCapture::install();
  let ctx = common::test_context();
  let pull = ctx.socket(SocketType::Pull)?;
  let _undrained = pull.monitor(1).await?;
  let endpoint = common::bind_and_resolve_tcp(&pull).await?;

  let _peer = RemotePush::start(endpoint, Some(b"only".to_vec()), false);
  let received = common::recv_timeout(&pull, EVENT_TIMEOUT).await?;
  assert_eq!(received.data().unwrap(), b"only");

  let line = logs.line(Level::WARN, "full");
  assert!(line.contains("Accepted"), "{line:?} does not name the dropped event");
  Ok(())
}
