# Benchmark Runs

* **System:** Macbook Pro M4
* **Message Size:** 64 bytes
* **Role:** Client
* **Target Endpoint:** `tcp://127.0.0.1:19876` (unless otherwise specified)

---

## Summary of Results

| Pattern | Features / Flags | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 217,423 | 21,742.20 | 1.33 | 44.00 µs | 64.00 µs |
| **ReqRep** | Concurrency 4 | 812,804 | 81,286.29 | 4.96 | 48.03 µs | 80.32 µs |
| **ReqRep** | Concurrency 8 | 1,125,903 | 112,586.98 | 6.87 | 69.31 µs | 114.30 µs |
| **DealerRouter**| Standard | 279,851 | 27,953.15 | 1.71 | 34.59 µs | 51.84 µs |
| **DealerRouter**| Concurrency 2, Pipeline 4 | 862,548 | 86,246.83 | 5.26 | 43.26 µs | 66.43 µs |
| **DealerRouter**| Concurrency 4, Pipeline 8 | 863,394 | 86,248.34 | 5.26 | 43.26 µs | 63.49 µs |
| **PushPull** | Standard | 23,017,998 | 2,302,062.74 | 140.51 | — | — |
| **PushPull** | Concurrency 2 | 26,549,866 | 2,654,677.67 | 162.03 | — | — |
| **PushPull** | Concurrency 4, Msg Size 32KB | 4,209,965 | 421,013.45 | 13,156.67 | — | — |
| **PubSub** | Standard | 18,359,371 | 1,835,924.27 | 112.06 | — | — |
| **PubSub** | Concurrency 2 | 19,223,737 | 1,922,517.69 | 117.34 | — | — |
| **PubSub** | Concurrency 4 | 20,763,329 | 2,075,031.37 | 126.65 | — | — |
| **PubSub** | Concurrency 4, Msg Size 32KB | 3,974,567 | 397,468.95 | 12,420.90 | — | — |

---

## Detailed Benchmark Reports

### 1. ReqRep

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 217,423
* **Total Data:** 13.27 MB
* **Throughput:** 21,742.20 msg/s
* **Throughput Rate:** 1.33 MB/s

**Latency Distribution:**
* **Min:** 29.00 µs
* **p50 (Median):** 44.00 µs
* **p90:** 50.00 µs
* **p95:** 57.00 µs
* **p99:** 64.00 µs
* **p99.9:** 107.00 µs
* **Max:** 1,205.00 µs

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 812,804
* **Total Data:** 49.61 MB
* **Throughput:** 81,286.29 msg/s
* **Throughput Rate:** 4.96 MB/s

**Latency Distribution:**
* **Min:** 15.87 µs
* **p50 (Median):** 48.03 µs
* **p90:** 64.03 µs
* **p95:** 69.31 µs
* **p99:** 80.32 µs
* **p99.9:** 97.47 µs
* **Max:** 609.28 µs

#### Concurrency 8

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --concurrency 8
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 10.0003 seconds
* **Total Messages:** 1,125,903
* **Total Data:** 68.72 MB
* **Throughput:** 112,586.98 msg/s
* **Throughput Rate:** 6.87 MB/s

**Latency Distribution:**
* **Min:** 23.62 µs
* **p50 (Median):** 69.31 µs
* **p90:** 91.01 µs
* **p95:** 98.30 µs
* **p99:** 114.30 µs
* **p99.9:** 146.82 µs
* **Max:** 1.04 ms (1,044 µs)

---

### 2. DealerRouter

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0114 seconds
* **Total Messages:** 279,851
* **Total Data:** 17.08 MB
* **Throughput:** 27,953.15 msg/s
* **Throughput Rate:** 1.71 MB/s

**Latency Distribution:**
* **Min:** 22.368 µs
* **p50 (Median):** 34.591 µs
* **p90:** 38.687 µs
* **p95:** 45.439 µs
* **p99:** 51.839 µs
* **p99.9:** 78.079 µs
* **Max:** 278.783 µs

#### Concurrency 2, Pipeline 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 2 --pipeline 4 --duration 10
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0009 seconds
* **Total Messages:** 862,548
* **Total Data:** 52.65 MB
* **Throughput:** 86,246.83 msg/s
* **Throughput Rate:** 5.26 MB/s

**Latency Distribution:**
* **Min:** 22.704 µs
* **p50 (Median):** 43.263 µs
* **p90:** 49.727 µs
* **p95:** 55.647 µs
* **p99:** 66.431 µs
* **p99.9:** 119.167 µs
* **Max:** 268.031 µs

#### Concurrency 4, Pipeline 8

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 4 --pipeline 8 --duration 10
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0106 seconds
* **Total Messages:** 863,394
* **Total Data:** 52.70 MB
* **Throughput:** 86,248.34 msg/s
* **Throughput Rate:** 5.26 MB/s

**Latency Distribution:**
* **Min:** 21.152 µs
* **p50 (Median):** 43.263 µs
* **p90:** 49.471 µs
* **p95:** 55.167 µs
* **p99:** 63.487 µs
* **p99.9:** 109.311 µs
* **Max:** 211.327 µs

---

### 3. PushPull

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9989 seconds
* **Total Messages:** 23,017,998
* **Total Data:** 1,404.91 MB
* **Throughput:** 2,302,062.74 msg/s
* **Throughput Rate:** 140.51 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0012 seconds
* **Total Messages:** 26,549,866
* **Total Data:** 1,620.48 MB
* **Throughput:** 2,654,677.67 msg/s
* **Throughput Rate:** 162.03 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 4,209,965
* **Total Data:** 131,561.41 MB
* **Throughput:** 421,013.45 msg/s
* **Throughput Rate:** 13,156.67 MB/s

---

### 4. PubSub

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0001 seconds
* **Total Messages:** 18,359,371
* **Total Data:** 1,120.57 MB
* **Throughput:** 1,835,924.27 msg/s
* **Throughput Rate:** 112.06 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 19,223,737
* **Total Data:** 1,173.32 MB
* **Throughput:** 1,922,517.69 msg/s
* **Throughput Rate:** 117.34 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0063 seconds
* **Total Messages:** 20,763,329
* **Total Data:** 1,267.29 MB
* **Throughput:** 2,075,031.37 msg/s
* **Throughput Rate:** 126.65 MB/s

#### Concurrency 4, Msg size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9997 seconds
* **Total Messages:** 3,974,567
* **Total Data:** 124,205.22 MB
* **Throughput:** 397,468.95 msg/s
* **Throughput Rate:** 12,420.90 MB/s