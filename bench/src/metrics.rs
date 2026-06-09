use crate::cli::OutputFormat;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct LatencyReport {
  min_ns: u64,
  p50_ns: u64,
  p90_ns: u64,
  p95_ns: u64,
  p99_ns: u64,
  p999_ns: u64,
  max_ns: u64,
}

#[derive(Serialize)]
struct FinalReport {
  pattern: String,
  role: String,
  msg_size_bytes: usize,
  elapsed_seconds: f64,
  total_messages: usize,
  total_megabytes: f64,
  throughput_msg_sec: f64,
  throughput_mb_sec: f64,
  latency_ns: Option<LatencyReport>,
}

pub struct BenchmarkCollector {
  start_time: Instant,
  // Resets to Instant::now() when begin_measurement() is called after warmup.
  // The final report uses this as the elapsed-time base so warmup is excluded.
  measure_start: Mutex<Instant>,
  messages_count: AtomicUsize,
  bytes_count: AtomicU64,
  // Written only via merge_histogram() at worker-task exit, never per-message.
  histogram: Option<Mutex<Histogram<u64>>>,

  // Tracking fields for 1-second interim reports.
  interim_last_report: Mutex<Instant>,
  interim_messages_count: AtomicUsize,
  interim_bytes_count: AtomicU64,
}

impl BenchmarkCollector {
  pub fn new(record_latency: bool) -> Self {
    let now = Instant::now();
    let histogram = if record_latency {
      Some(Mutex::new(Histogram::<u64>::new(3).unwrap()))
    } else {
      None
    };

    Self {
      start_time: now,
      measure_start: Mutex::new(now),
      messages_count: AtomicUsize::new(0),
      bytes_count: AtomicU64::new(0),
      histogram,
      interim_last_report: Mutex::new(now),
      interim_messages_count: AtomicUsize::new(0),
      interim_bytes_count: AtomicU64::new(0),
    }
  }

  #[inline]
  pub fn record_message(&self, bytes_len: usize) {
    self.messages_count.fetch_add(1, Ordering::Relaxed);
    self.bytes_count.fetch_add(bytes_len as u64, Ordering::Relaxed);
    self.interim_messages_count.fetch_add(1, Ordering::Relaxed);
    self.interim_bytes_count.fetch_add(bytes_len as u64, Ordering::Relaxed);
  }

  // Merges a worker-local histogram into the shared one. Called once per
  // worker lifetime at task exit and never on the per-message hot path.
  pub fn merge_histogram(&self, local_hist: &Histogram<u64>) {
    if let Some(ref mutex) = self.histogram {
      let _ = mutex.lock().add(local_hist);
    }
  }

  // Resets all measurement counters and the measurement clock to now.
  // Called once when the warmup period ends so the final report only
  // covers the steady-state measurement window.
  pub fn begin_measurement(&self) {
    let now = Instant::now();
    self.messages_count.store(0, Ordering::Relaxed);
    self.bytes_count.store(0, Ordering::Relaxed);
    self.interim_messages_count.store(0, Ordering::Relaxed);
    self.interim_bytes_count.store(0, Ordering::Relaxed);
    *self.measure_start.lock() = now;
    *self.interim_last_report.lock() = now;
  }

  pub fn print_interim_report_if_due(&self, interval: Duration, role: &str) {
    // try_lock returns None immediately if another task holds the lock,
    // so only one task prints per interval window.
    let mut guard = match self.interim_last_report.try_lock() {
      Some(g) => g,
      None => return,
    };

    let now = Instant::now();
    let elapsed = now.duration_since(*guard);

    if elapsed < interval {
      return;
    }

    let msg_count = self.interim_messages_count.swap(0, Ordering::Relaxed);
    let byte_count = self.interim_bytes_count.swap(0, Ordering::Relaxed);
    *guard = now;
    drop(guard);

    let seconds = elapsed.as_secs_f64();
    let msg_sec = msg_count as f64 / seconds;
    let mb_sec = (byte_count as f64 / 1_048_576.0) / seconds;

    println!(
      "[{}] Interim: {:.2}s | throughput: {:10.2} msg/s | {:8.2} MB/s",
      role,
      now.duration_since(self.start_time).as_secs_f64(),
      msg_sec,
      mb_sec
    );
  }

  pub fn snapshot(&self, role: &str) -> BenchStats {
    let total_messages = self.messages_count.load(Ordering::Relaxed);
    let total_bytes = self.bytes_count.load(Ordering::Relaxed);
    let elapsed_secs = self.measure_start.lock().elapsed().as_secs_f64().max(0.000001);
    BenchStats { role: role.to_string(), total_messages, total_bytes, elapsed_secs }
  }

  pub fn print_final_report(
    &self,
    format: OutputFormat,
    pattern: &str,
    role: &str,
    msg_size: usize,
  ) {
    let total_messages = self.messages_count.load(Ordering::Relaxed);
    let total_bytes = self.bytes_count.load(Ordering::Relaxed);

    let elapsed = self.measure_start.lock().elapsed().as_secs_f64().max(0.000001);
    let msg_sec = total_messages as f64 / elapsed;
    let total_mb = total_bytes as f64 / 1_048_576.0;
    let mb_sec = total_mb / elapsed;

    let latency_metrics = self.histogram.as_ref().map(|m| {
      let hist = m.lock();
      LatencyReport {
        min_ns: hist.min(),
        p50_ns: hist.value_at_quantile(0.50),
        p90_ns: hist.value_at_quantile(0.90),
        p95_ns: hist.value_at_quantile(0.95),
        p99_ns: hist.value_at_quantile(0.99),
        p999_ns: hist.value_at_quantile(0.999),
        max_ns: hist.max(),
      }
    });

    let report = FinalReport {
      pattern: pattern.to_string(),
      role: role.to_string(),
      msg_size_bytes: msg_size,
      elapsed_seconds: elapsed,
      total_messages,
      total_megabytes: total_mb,
      throughput_msg_sec: msg_sec,
      throughput_mb_sec: mb_sec,
      latency_ns: latency_metrics,
    };

    match format {
      OutputFormat::Text => self.print_text_report(&report),
      OutputFormat::Json => self.print_json_report(&report),
      OutputFormat::Csv => self.print_csv_report(&report),
    }
  }

  fn print_text_report(&self, r: &FinalReport) {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(1024);

    let _ = writeln!(out, "\n========================================================");
    let _ = writeln!(out, "               rzmq BENCHMARK FINAL REPORT              ");
    let _ = writeln!(out, "========================================================");
    let _ = writeln!(out, "{:<25} : {}", "Pattern", r.pattern);
    let _ = writeln!(out, "{:<25} : {}", "Role", r.role);
    let _ = writeln!(out, "{:<25} : {} bytes", "Message Size", r.msg_size_bytes);
    let _ = writeln!(out, "{:<25} : {:.4} seconds", "Elapsed Time", r.elapsed_seconds);
    let _ = writeln!(out, "{:<25} : {}", "Total Messages", r.total_messages);
    let _ = writeln!(out, "{:<25} : {:.2} MB", "Total Data", r.total_megabytes);
    let _ = writeln!(out, "--------------------------------------------------------");
    let _ = writeln!(out, "{:<25} : {:.2} msg/s", "Throughput", r.throughput_msg_sec);
    let _ = writeln!(out, "{:<25} : {:.2} MB/s", "Throughput Rate", r.throughput_mb_sec);

    if let Some(ref lat) = r.latency_ns {
      let _ = writeln!(out, "--------------------------------------------------------");
      let _ = writeln!(out, "Latency Distribution:");
      let _ = writeln!(out, "  {:<15} : {:>12}", "Min", fmt_ns(lat.min_ns));
      let _ = writeln!(out, "  {:<15} : {:>12}", "p50 (Median)", fmt_ns(lat.p50_ns));
      let _ = writeln!(out, "  {:<15} : {:>12}", "p90", fmt_ns(lat.p90_ns));
      let _ = writeln!(out, "  {:<15} : {:>12}", "p95", fmt_ns(lat.p95_ns));
      let _ = writeln!(out, "  {:<15} : {:>12}", "p99", fmt_ns(lat.p99_ns));
      let _ = writeln!(out, "  {:<15} : {:>12}", "p99.9", fmt_ns(lat.p999_ns));
      let _ = writeln!(out, "  {:<15} : {:>12}", "Max", fmt_ns(lat.max_ns));
    }
    let _ = writeln!(out, "========================================================\n");

    print!("{out}");
  }

  fn print_json_report(&self, r: &FinalReport) {
    if let Ok(serialized) = serde_json::to_string_pretty(r) {
      println!("{}", serialized);
    }
  }

  fn print_csv_report(&self, r: &FinalReport) {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(256);

    let _ = writeln!(
      out,
      "pattern,role,msg_size_bytes,elapsed_seconds,total_messages,throughput_msg_sec,throughput_mb_sec,min_ns,p50_ns,p99_ns,max_ns"
    );
    let lat_vals = r
      .latency_ns
      .as_ref()
      .map(|l| format!("{},{},{},{}", l.min_ns, l.p50_ns, l.p99_ns, l.max_ns))
      .unwrap_or_else(|| "N/A,N/A,N/A,N/A".to_string());
    let _ = writeln!(
      out,
      "{},{},{},{:.4},{},{:.2},{:.2},{}",
      r.pattern,
      r.role,
      r.msg_size_bytes,
      r.elapsed_seconds,
      r.total_messages,
      r.throughput_msg_sec,
      r.throughput_mb_sec,
      lat_vals
    );

    print!("{out}");
  }
}

/// Snapshot of a single role's final measurements. Serialized to disk by each
/// child process; the orchestrator reads both and prints the combined report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchStats {
  pub role: String,
  pub total_messages: usize,
  pub total_bytes: u64,
  pub elapsed_secs: f64,
}

impl BenchStats {
  pub fn throughput_msg_sec(&self) -> f64 {
    self.total_messages as f64 / self.elapsed_secs
  }
  pub fn total_mb(&self) -> f64 {
    self.total_bytes as f64 / 1_048_576.0
  }
  pub fn throughput_mb_sec(&self) -> f64 {
    self.total_mb() / self.elapsed_secs
  }

  pub fn write_to_file(&self, path: &str) {
    if let Ok(json) = serde_json::to_string(self) {
      let tmp = format!("{path}.tmp");
      if std::fs::write(&tmp, &json).is_ok() {
        let _ = std::fs::rename(&tmp, path);
      }
    }
  }

  pub fn read_from_file(path: &str) -> Option<Self> {
    let data = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
  }
}

pub fn print_combined_report(
  format: OutputFormat,
  pattern: &str,
  msg_size: usize,
  client: &BenchStats,
  server: Option<&BenchStats>,
) {
  match format {
    OutputFormat::Text => print_combined_text(pattern, msg_size, client, server),
    OutputFormat::Json => print_combined_json(client, server),
    OutputFormat::Csv => print_combined_csv(client, server),
  }
}

fn print_combined_text(
  pattern: &str,
  msg_size: usize,
  client: &BenchStats,
  server: Option<&BenchStats>,
) {
  use std::fmt::Write as _;
  let mut out = String::with_capacity(2048);
  let _ = writeln!(out, "\n========================================================");
  let _ = writeln!(out, "               rzmq BENCHMARK FINAL REPORT              ");
  let _ = writeln!(out, "========================================================");
  let _ = writeln!(out, "{:<25} : {}", "Pattern", pattern);
  let _ = writeln!(out, "{:<25} : {} bytes", "Message Size", msg_size);
  let _ = writeln!(out, "--------------------------------------------------------");

  if let Some(srv) = server {
    let _ = writeln!(out, "{:<25}   {:>18}   {:>18}", "", "CLIENT (sent)", "SERVER (recv)");
    let _ = writeln!(out, "{:<25} : {:>18}   {:>18}", "Elapsed Time (s)",
      format!("{:.4}", client.elapsed_secs), format!("{:.4}", srv.elapsed_secs));
    let _ = writeln!(out, "{:<25} : {:>18}   {:>18}", "Total Messages",
      client.total_messages, srv.total_messages);
    let _ = writeln!(out, "{:<25} : {:>15.2} MB   {:>15.2} MB", "Total Data",
      client.total_mb(), srv.total_mb());
    let _ = writeln!(out, "--------------------------------------------------------");
    let _ = writeln!(out, "{:<25} : {:>14.2} msg/s   {:>14.2} msg/s", "Throughput",
      client.throughput_msg_sec(), srv.throughput_msg_sec());
    let _ = writeln!(out, "{:<25} : {:>14.2} MB/s    {:>14.2} MB/s", "Throughput Rate",
      client.throughput_mb_sec(), srv.throughput_mb_sec());
  } else {
    let _ = writeln!(out, "{:<25} : {:.4} seconds", "Elapsed Time", client.elapsed_secs);
    let _ = writeln!(out, "{:<25} : {}", "Total Messages", client.total_messages);
    let _ = writeln!(out, "{:<25} : {:.2} MB", "Total Data", client.total_mb());
    let _ = writeln!(out, "--------------------------------------------------------");
    let _ = writeln!(out, "{:<25} : {:.2} msg/s", "Throughput", client.throughput_msg_sec());
    let _ = writeln!(out, "{:<25} : {:.2} MB/s", "Throughput Rate", client.throughput_mb_sec());
  }
  let _ = writeln!(out, "========================================================\n");
  print!("{out}");
}

fn print_combined_json(client: &BenchStats, server: Option<&BenchStats>) {
  #[derive(Serialize)]
  struct Combined<'a> {
    client: &'a BenchStats,
    server: Option<&'a BenchStats>,
  }
  if let Ok(s) = serde_json::to_string_pretty(&Combined { client, server }) {
    println!("{s}");
  }
}

fn print_combined_csv(client: &BenchStats, server: Option<&BenchStats>) {
  println!("role,total_messages,total_bytes,elapsed_secs,throughput_msg_sec,throughput_mb_sec");
  println!("{},{},{},{:.4},{:.2},{:.2}",
    client.role, client.total_messages, client.total_bytes,
    client.elapsed_secs, client.throughput_msg_sec(), client.throughput_mb_sec());
  if let Some(srv) = server {
    println!("{},{},{},{:.4},{:.2},{:.2}",
      srv.role, srv.total_messages, srv.total_bytes,
      srv.elapsed_secs, srv.throughput_msg_sec(), srv.throughput_mb_sec());
  }
}

fn fmt_ns(ns: u64) -> String {
  if ns < 1_000 {
    format!("{} ns", ns)
  } else if ns < 1_000_000 {
    format!("{:.3} µs", ns as f64 / 1_000.0)
  } else if ns < 1_000_000_000 {
    format!("{:.3} ms", ns as f64 / 1_000_000.0)
  } else {
    format!("{:.3} s", ns as f64 / 1_000_000_000.0)
  }
}
