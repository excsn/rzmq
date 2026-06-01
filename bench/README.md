# rzmq Benchmarks

This crate contains `rzmq_bench`, a standalone tool for measuring throughput and latency of `rzmq` across socket patterns, transports, and I/O backends.

For full usage, flags, and example commands see **[GUIDE.md](./GUIDE.md)**.

---

## ⚠️ Benchmark Results May Be Out of Date

The published results in [`docs/linux_bench.md`](./docs/linux_bench.md) and [`docs/mac_bench.md`](./docs/mac_bench.md) were collected across multiple development iterations. **Many of those runs were made with the adaptive I/O throttle enabled**, which is a probabilistic fairness engine that yields the async executor when ingress/egress work becomes imbalanced.

The bench tool now **disables the throttle by default** on all sockets (`ADAPTIVE_THROTTLE = 0`) to produce clean transport-layer measurements. This means published numbers and current numbers are not directly comparable.

**Run your own benchmarks on your own hardware before drawing conclusions.** Numbers vary significantly with CPU, kernel version, NIC, and workload shape.

---

## Quick Start

```bash
# PUSH/PULL throughput — standard TCP, 4 workers
cargo run --release --bin rzmq_bench -- \
  --role orchestrate --endpoint tcp://127.0.0.1:19876 \
  --pattern push-pull --concurrency 4 --duration 10

# PUSH/PULL — Linux io_uring + cork + zerocopy + multishot, 8 workers, 32 KB messages
cargo run --release --features io-uring --bin rzmq_bench -- \
  --role orchestrate --endpoint tcp://127.0.0.1:19876 \
  --pattern push-pull --msg-size 32768 --concurrency 8 --duration 10 \
  --use-io-uring --cork --uring-zerocopy --uring-multishot

# REQ/REP latency — standard TCP
cargo run --release --bin rzmq_bench -- \
  --role orchestrate --endpoint tcp://127.0.0.1:19876 \
  --pattern req-rep --duration 10
```

---

## Published Result Snapshots

These are point-in-time snapshots, not guarantees. See the caveat above.

| Platform | File |
|---|---|
| Linux (AMD Ryzen 5 7640U) | [`docs/linux_bench.md`](./docs/linux_bench.md) |
| macOS (Apple M4) | [`docs/mac_bench.md`](./docs/mac_bench.md) |

**Headline numbers from the Linux runs (throttle was ON at the time):**

| Configuration | Throughput |
|---|---|
| PushPull · 64 B · standard · 4 workers | ~593 K msg/s |
| PushPull · 64 B · cork · 4 workers | ~2.1 M msg/s |
| PushPull · 32 KB · io_uring + cork + zerocopy + multishot · 8 workers | ~6.6 GB/s |
