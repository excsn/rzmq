// src/metrics.rs

use crate::cli::OutputFormat;
use hdrhistogram::Histogram;
use serde::Serialize;
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct LatencyReport {
  min_us: u64,
  p50_us: u64,
  p90_us: u64,
  p95_us: u64,
  p99_us: u64,
  p999_us: u64,
  max_us: u64,
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
  latency_us: Option<LatencyReport>,
}

pub struct BenchmarkCollector {
  start_time: Instant,
  messages_count: usize,
  bytes_count: u64,
  histogram: Option<Histogram<u64>>,

  // Tracking fields for 1-second interim reports
  interim_last_report: Instant,
  interim_messages_count: usize,
  interim_bytes_count: u64,
}

impl BenchmarkCollector {
  pub fn new(record_latency: bool) -> Self {
    let now = Instant::now();
    let histogram = if record_latency {
      // Significant figures = 3 (retains 0.1% accuracy across the range)
      Some(Histogram::<u64>::new(3).unwrap())
    } else {
      None
    };

    Self {
      start_time: now,
      messages_count: 0,
      bytes_count: 0,
      histogram,
      interim_last_report: now,
      interim_messages_count: 0,
      interim_bytes_count: 0,
    }
  }

  #[inline]
  pub fn record_message(&mut self, bytes_len: usize, rtt_duration: Option<Duration>) {
    self.messages_count += 1;
    self.bytes_count += bytes_len as u64;

    self.interim_messages_count += 1;
    self.interim_bytes_count += bytes_len as u64;

    if let (Some(hist), Some(duration)) = (&mut self.histogram, rtt_duration) {
      // Record latency in microseconds
      let us = duration.as_micros() as u64;
      let _ = hist.record(us);
    }
  }

  pub fn print_interim_report_if_due(&mut self, interval: Duration, role: &str) {
    let now = Instant::now();
    let elapsed = now.duration_since(self.interim_last_report);

    if elapsed >= interval {
      let seconds = elapsed.as_secs_f64();
      let msg_sec = self.interim_messages_count as f64 / seconds;
      let mb_sec = (self.interim_bytes_count as f64 / 1_048_576.0) / seconds;

      println!(
        "[{}] Interim: {:.2}s | throughput: {:10.2} msg/s | {:8.2} MB/s",
        role,
        now.duration_since(self.start_time).as_secs_f64(),
        msg_sec,
        mb_sec
      );

      // Reset interim state
      self.interim_last_report = now;
      self.interim_messages_count = 0;
      self.interim_bytes_count = 0;
    }
  }

  pub fn print_final_report(
    &self,
    format: OutputFormat,
    pattern: &str,
    role: &str,
    msg_size: usize,
  ) {
    let elapsed = self.start_time.elapsed().as_secs_f64().max(0.000001);
    let msg_sec = self.messages_count as f64 / elapsed;
    let total_mb = self.bytes_count as f64 / 1_048_576.0;
    let mb_sec = total_mb / elapsed;

    let latency_metrics = self.histogram.as_ref().map(|hist| LatencyReport {
      min_us: hist.min(),
      p50_us: hist.value_at_quantile(0.50),
      p90_us: hist.value_at_quantile(0.90),
      p95_us: hist.value_at_quantile(0.95),
      p99_us: hist.value_at_quantile(0.99),
      p999_us: hist.value_at_quantile(0.999),
      max_us: hist.max(),
    });

    let report = FinalReport {
      pattern: pattern.to_string(),
      role: role.to_string(),
      msg_size_bytes: msg_size,
      elapsed_seconds: elapsed,
      total_messages: self.messages_count,
      total_megabytes: total_mb,
      throughput_msg_sec: msg_sec,
      throughput_mb_sec: mb_sec,
      latency_us: latency_metrics,
    };

    match format {
      OutputFormat::Text => self.print_text_report(&report),
      OutputFormat::Json => self.print_json_report(&report),
      OutputFormat::Csv => self.print_csv_report(&report),
    }
  }

  fn print_text_report(&self, r: &FinalReport) {
    println!("\n========================================================");
    println!("               rzmq BENCHMARK FINAL REPORT              ");
    println!("========================================================");
    println!("{:<25} : {}", "Pattern", r.pattern);
    println!("{:<25} : {}", "Role", r.role);
    println!("{:<25} : {} bytes", "Message Size", r.msg_size_bytes);
    println!("{:<25} : {:.4} seconds", "Elapsed Time", r.elapsed_seconds);
    println!("{:<25} : {}", "Total Messages", r.total_messages);
    println!("{:<25} : {:.2} MB", "Total Data", r.total_megabytes);
    println!("--------------------------------------------------------");
    println!("{:<25} : {:.2} msg/s", "Throughput", r.throughput_msg_sec);
    println!(
      "{:<25} : {:.2} MB/s",
      "Throughput Rate", r.throughput_mb_sec
    );

    if let Some(ref lat) = r.latency_us {
      println!("--------------------------------------------------------");
      println!("Latency Distribution (Microseconds):");
      println!("  {:<15} : {:>10} us", "Min", lat.min_us);
      println!("  {:<15} : {:>10} us", "p50 (Median)", lat.p50_us);
      println!("  {:<15} : {:>10} us", "p90", lat.p90_us);
      println!("  {:<15} : {:>10} us", "p95", lat.p95_us);
      println!("  {:<15} : {:>10} us", "p99", lat.p99_us);
      println!("  {:<15} : {:>10} us", "p99.9", lat.p999_us);
      println!("  {:<15} : {:>10} us", "Max", lat.max_us);
    }
    println!("========================================================\n");
  }

  fn print_json_report(&self, r: &FinalReport) {
    if let Ok(serialized) = serde_json::to_string_pretty(r) {
      println!("{}", serialized);
    }
  }

  fn print_csv_report(&self, r: &FinalReport) {
    println!(
      "pattern,role,msg_size_bytes,elapsed_seconds,total_messages,throughput_msg_sec,throughput_mb_sec,min_us,p50_us,p99_us,max_us"
    );
    let lat_vals = r
      .latency_us
      .as_ref()
      .map(|l| format!("{},{},{},{}", l.min_us, l.p50_us, l.p99_us, l.max_us))
      .unwrap_or_else(|| "N/A,N/A,N/A,N/A".to_string());

    println!(
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
  }
}
