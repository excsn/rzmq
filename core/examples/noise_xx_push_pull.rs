use rzmq::{
  socket::options::{
    NOISE_XX_ENABLED,
    NOISE_XX_REMOTE_STATIC_PUBLIC_KEY,
    NOISE_XX_STATIC_SECRET_KEY,
    RCVHWM,
    SNDHWM, // Good to set for examples
  },
  Context, Msg, SocketType, ZmqError,
};
use std::time::Duration;
use tracing_subscriber::{EnvFilter, FmtSubscriber}; // For example logging

// For key generation
use rand::{rng, rngs::StdRng, SeedableRng};
use x25519_dalek::{PublicKey, StaticSecret};

// You can also generate keys with our cli tool `rzmq keygen noise-xx ...`
// Helper struct to hold a keypair
#[derive(Debug)] // Added Debug for printing if needed
struct Keypair {
  name: String, // For identifying in logs
  sk_bytes: [u8; 32],
  pk_bytes: [u8; 32],
}

impl Keypair {
  fn new(name: &str) -> Self {
    let static_secret = StaticSecret::random_from_rng(StdRng::from_rng(&mut rng()));
    let public_key = PublicKey::from(&static_secret);
    println!(
      "Generated Noise_XX keypair for '{}':\n  SK: {:?}\n  PK: {:?}",
      name,
      static_secret.to_bytes(), // Print raw bytes for easy verification if needed
      public_key.to_bytes()
    );
    Self {
      name: name.to_string(),
      sk_bytes: static_secret.to_bytes(),
      pk_bytes: public_key.to_bytes(),
    }
  }
}

fn setup_tracing_for_example() {
  // Allow RUST_LOG to override, otherwise default to info for rzmq and general info.
  let default_filter = "info,rzmq=info"; // Start with info
                                         // For deeper debugging of Noise, change rzmq=info to rzmq=debug or rzmq=trace
                                         // Example: RUST_LOG="rzmq=trace" cargo run --example noise_xx_push_pull --features noise_xx

  let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

  let subscriber = FmtSubscriber::builder()
    .with_max_level(tracing::Level::TRACE) // Allow all levels down to TRACE based on filter
    .with_env_filter(env_filter)
    .with_target(true)
    .with_line_number(true)
    .with_ansi(true) // Enable ANSI colors for better readability if terminal supports
    .finish();
  tracing::subscriber::set_global_default(subscriber).expect("Failed to set global tracing subscriber");
  println!("Tracing initialized for example. Use RUST_LOG environment variable to adjust verbosity (e.g., RUST_LOG=rzmq=trace).");
}

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
  setup_tracing_for_example();

  println!("--- Noise_XX PUSH/PULL Example with Generated Keys ---");

  // Generate keypairs for server (PULL) and client (PUSH)
  let server_keys = Keypair::new("PULL_Server");
  let client_keys = Keypair::new("PUSH_Client");

  let ctx = Context::new()?;
  let endpoint = "tcp://127.0.0.1:5589"; // Changed port slightly for uniqueness

  // --- PULL Server Setup ---
  let pull_server = ctx.socket(SocketType::Pull)?;
  println!("[{}] Configuring Noise_XX...", server_keys.name);
  pull_server
    .set_option_raw(NOISE_XX_ENABLED, &(1i32).to_ne_bytes())
    .await?;
  pull_server
    .set_option_raw(NOISE_XX_STATIC_SECRET_KEY, &server_keys.sk_bytes)
    .await?;
  // Server in XX pattern learns client's public key during handshake.

  pull_server.set_option_raw(RCVHWM, &(10i32).to_ne_bytes()).await?;
  println!("[{}] Binding to {}...", server_keys.name, endpoint);
  pull_server.bind(endpoint).await?;
  println!("[{}] Bound and listening with Noise_XX.", server_keys.name);

  // --- PUSH Client Setup ---
  let push_client = ctx.socket(SocketType::Push)?;
  println!("[{}] Configuring Noise_XX...", client_keys.name);
  push_client
    .set_option_raw(NOISE_XX_ENABLED, &(1i32).to_ne_bytes())
    .await?;
  push_client
    .set_option_raw(NOISE_XX_STATIC_SECRET_KEY, &client_keys.sk_bytes)
    .await?;
  // Client in XX pattern *must* know the server's static public key to authenticate it.
  push_client
    .set_option_raw(NOISE_XX_REMOTE_STATIC_PUBLIC_KEY, &server_keys.pk_bytes)
    .await?;

  push_client.set_option_raw(SNDHWM, &(10i32).to_ne_bytes()).await?;
  println!("[{}] Connecting to {} with Noise_XX...", client_keys.name, endpoint);
  push_client.connect(endpoint).await?;
  println!(
    "[{}] Connect call returned. Waiting for secure connection...",
    client_keys.name
  );

  // Allow time for connection and Noise handshake to complete.
  // With tracing, you can observe handshake messages if log level is debug/trace.
  tokio::time::sleep(Duration::from_millis(500)).await; // Handshake can take a moment
  println!("[SYSTEM] Assumed secure connection established.");

  // --- Message Exchange ---
  let message1_data = b"Hello securely from PUSH (1)";
  println!(
    "[{}] Sending: \"{}\"",
    client_keys.name,
    String::from_utf8_lossy(message1_data)
  );
  push_client.send(Msg::from_static(message1_data)).await?;

  let message2_data = b"Another secure message (2)";
  println!(
    "[{}] Sending: \"{}\"",
    client_keys.name,
    String::from_utf8_lossy(message2_data)
  );
  push_client.send(Msg::from_static(message2_data)).await?;

  println!("[{}] Attempting to receive messages...", server_keys.name);
  let received_msg1 = pull_server.recv().await?; // Default timeout for recv is infinite
  println!(
    "[{}] Received: \"{}\"",
    server_keys.name,
    String::from_utf8_lossy(received_msg1.data().unwrap_or_default())
  );
  assert_eq!(received_msg1.data().unwrap_or_default(), message1_data);

  let received_msg2 = pull_server.recv().await?;
  println!(
    "[{}] Received: \"{}\"",
    server_keys.name,
    String::from_utf8_lossy(received_msg2.data().unwrap_or_default())
  );
  assert_eq!(received_msg2.data().unwrap_or_default(), message2_data);

  println!("[SYSTEM] Secure message exchange successful!");

  // --- Teardown ---
  println!("[SYSTEM] Closing client and server sockets...");
  push_client.close().await?;
  pull_server.close().await?;
  println!("[SYSTEM] Sockets closed.");

  println!("[SYSTEM] Terminating context...");
  ctx.term().await?;
  println!("[SYSTEM] Context terminated.");
  println!("--- Noise_XX PUSH/PULL Example with Generated Keys Finished ---");

  Ok(())
}
