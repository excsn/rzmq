use std::sync::Once;

static INIT: Once = Once::new();

// This function will be called at the beginning of each test.
// The `Once` ensures the subscriber is only initialized a single time.
pub fn setup_logging() {
  INIT.call_once(|| {
    tracing_subscriber::fmt()
      .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
      .with_test_writer()
      .init();
  });
}
