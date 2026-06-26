# rzmq Benchmarks

This crate contains `rzmq_bench`, a standalone tool for measuring throughput and latency of `rzmq` across socket patterns, transports, and I/O backends.

For full usage, flags, and example commands see **[GUIDE.md](./GUIDE.md)**.

---

## ⚠️ Benchmark Results May Be Out of Date

The published results in [`docs/linux_bench.md`](./docs/linux_bench.md) and [`docs/mac_bench.md`](./docs/mac_bench.md) were collected across multiple development iterations.

The bench tool **disables the throttle by default** on all sockets (`ADAPTIVE_THROTTLE = 0`) to produce clean transport-layer measurements.

**Run your own benchmarks on your own hardware before drawing conclusions.** Numbers vary significantly with CPU, kernel version, NIC, and workload shape.

---

## Quick Start

```bash
# PUSH/PULL throughput - standard TCP, 4 workers
cargo run --release --bin rzmq_bench -- \
  --role orchestrate --endpoint tcp://127.0.0.1:19876 \
  --pattern push-pull --concurrency 4 --duration 10

# PUSH/PULL - Linux io_uring + cork + zerocopy + multishot, 8 workers, 32 KB messages
cargo run --release --features io-uring --bin rzmq_bench -- \
  --role orchestrate --endpoint tcp://127.0.0.1:19876 \
  --pattern push-pull --msg-size 32768 --concurrency 8 --duration 10 \
  --use-io-uring --cork --uring-zerocopy --uring-multishot

# REQ/REP latency - standard TCP
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
