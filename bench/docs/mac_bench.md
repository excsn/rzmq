# Benchmark Runs

* **System:** Macbook Pro M4
* **Message Size:** 64 bytes (unless otherwise specified)
* **Role:** Client/Server
* **Target Endpoint:** `tcp://127.0.0.1:19876` (unless otherwise specified)

---

## Summary of Results

| Pattern | Features / Flags | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **ReqRep** | Standard | 286,373 | 28,638.30 | 1.75 | 33.85 µs | 54.11 µs |
| **ReqRep** | Concurrency 4 | 796,415 | 79,643.94 | 4.86 | 48.86 µs | 83.33 µs |
| **ReqRep** | Concurrency 8 | 1,113,520 | 111,356.97 | 6.80 | 69.57 µs | 125.50 µs |
| **DealerRouter** | Standard | 282,389 | 28,239.02 | 1.89 | 33.85 µs | 56.86 µs |
| **DealerRouter** | Concurrency 2, Pipeline 4 | 1,731,789 | 173,182.68 | 11.56 | 43.26 µs | 83.26 µs |
| **DealerRouter** | Concurrency 4, Pipeline 8 | 4,346,692 | 434,662.44 | 29.22 | 69.95 µs | 113.66 µs |
| **PushPull** | Standard | 62,898,440 | 6,290,503.87 | 383.94 | — | — |
| **PushPull** | Concurrency 2 | 73,941,359 | 7,394,426.72 | 451.32 | — | — |
| **PushPull** | Concurrency 4, Msg Size 32KB | 4,858,617 | 485,874.58 | 15,183.58 | — | — |
| **PubSub** | Standard | 54,497,665 | 5,449,779.92 | 332.63 | — | — |
| **PubSub** | Concurrency 2 | 52,367,473 | 5,236,522.11 | 319.61 | — | — |
| **PubSub** | Concurrency 4 | 65,513,564 | 6,551,766.87 | 399.89 | — | — |
| **PubSub** | Concurrency 1, Msg Size 16KB | 7,312,280 | 731,257.57 | 11,425.90 | — | — |
| **PubSub** | Concurrency 4, Msg Size 16KB | 9,533,679 | 953,401.96 | 14,896.91 | — | — |
| **PubSub** | Concurrency 1, Msg Size 32KB | 4,000,599 | 400,153.70 | 12,504.80 | — | — |
| **PubSub** | Concurrency 4, Msg Size 32KB | 5,008,514 | 500,860.29 | 15,651.88 | — | — |

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
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 286,373
* **Total Data:** 17.48 MB
* **Throughput:** 28,638.30 msg/s
* **Throughput Rate:** 1.75 MB/s

**Latency Distribution:**
* **Min:** 21.040 µs
* **p50 (Median):** 33.855 µs
* **p90:** 38.303 µs
* **p95:** 45.855 µs
* **p99:** 54.111 µs
* **p99.9:** 91.519 µs
* **Max:** 7.184 ms

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 9.9997 seconds
* **Total Messages:** 796,415
* **Total Data:** 48.61 MB
* **Throughput:** 79,643.94 msg/s
* **Throughput Rate:** 4.86 MB/s

**Latency Distribution:**
* **Min:** 15.952 µs
* **p50 (Median):** 48.863 µs
* **p90:** 65.503 µs
* **p95:** 71.103 µs
* **p99:** 83.327 µs
* **p99.9:** 106.751 µs
* **Max:** 1.597 ms

#### Concurrency 8

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --concurrency 8
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 1,113,520
* **Total Data:** 67.96 MB
* **Throughput:** 111,356.97 msg/s
* **Throughput Rate:** 6.80 MB/s

**Latency Distribution:**
* **Min:** 23.536 µs
* **p50 (Median):** 69.567 µs
* **p90:** 93.375 µs
* **p95:** 102.079 µs
* **p99:** 125.503 µs
* **p99.9:** 178.815 µs
* **Max:** 3.455 ms

---

### 2. DealerRouter

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 282,389
* **Total Data:** 18.85 MB
* **Throughput:** 28,239.02 msg/s
* **Throughput Rate:** 1.89 MB/s

**Latency Distribution:**
* **Min:** 21.456 µs
* **p50 (Median):** 33.855 µs
* **p90:** 39.071 µs
* **p95:** 46.687 µs
* **p99:** 56.863 µs
* **p99.9:** 98.367 µs
* **Max:** 9.503 ms

#### Concurrency 2, Pipeline 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 2 --pipeline 4 --duration 10
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 1,731,789
* **Total Data:** 115.61 MB
* **Throughput:** 173,182.68 msg/s
* **Throughput Rate:** 11.56 MB/s

**Latency Distribution:**
* **Min:** 17.152 µs
* **p50 (Median):** 43.263 µs
* **p90:** 56.799 µs
* **p95:** 63.423 µs
* **p99:** 83.263 µs
* **p99.9:** 118.975 µs
* **Max:** 1.204 ms

#### Concurrency 4, Pipeline 8

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 4 --pipeline 8 --duration 10
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0002 seconds
* **Total Messages:** 4,346,692
* **Total Data:** 292.24 MB
* **Throughput:** 434,662.44 msg/s
* **Throughput Rate:** 29.22 MB/s

**Latency Distribution:**
* **Min:** 23.616 µs
* **p50 (Median):** 69.951 µs
* **p90:** 91.967 µs
* **p95:** 99.071 µs
* **p99:** 113.663 µs
* **p99.9:** 134.015 µs
* **Max:** 797.183 µs

---

### 3. PushPull

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9990 seconds
* **Total Messages:** 62,898,440
* **Total Data:** 3,839.02 MB
* **Throughput:** 6,290,503.87 msg/s
* **Throughput Rate:** 383.94 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 73,941,359
* **Total Data:** 4,513.02 MB
* **Throughput:** 7,394,426.72 msg/s
* **Throughput Rate:** 451.32 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9997 seconds
* **Total Messages:** 4,858,617
* **Total Data:** 151,831.78 MB
* **Throughput:** 485,874.58 msg/s
* **Throughput Rate:** 15,183.58 MB/s

---

### 4. PubSub

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 54,497,665
* **Total Data:** 3,326.27 MB
* **Throughput:** 5,449,779.92 msg/s
* **Throughput Rate:** 332.63 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0004 seconds
* **Total Messages:** 52,367,473
* **Total Data:** 3,196.26 MB
* **Throughput:** 5,236,522.11 msg/s
* **Throughput Rate:** 319.61 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9994 seconds
* **Total Messages:** 65,513,564
* **Total Data:** 3,998.63 MB
* **Throughput:** 6,551,766.87 msg/s
* **Throughput Rate:** 399.89 MB/s

#### Concurrency 1, Msg size 16KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 16384
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 7,312,280
* **Total Data:** 114,254.38 MB
* **Throughput:** 731,257.57 msg/s
* **Throughput Rate:** 11,425.90 MB/s

#### Concurrency 4, Msg size 16KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 16384 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 9,533,679
* **Total Data:** 148,963.73 MB
* **Throughput:** 953,401.96 msg/s
* **Throughput Rate:** 14,896.91 MB/s

#### Concurrency 1, Msg size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 32768
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9977 seconds
* **Total Messages:** 4,000,599
* **Total Data:** 125,018.72 MB
* **Throughput:** 400,153.70 msg/s
* **Throughput Rate:** 12,504.80 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 5,008,514
* **Total Data:** 156,516.06 MB
* **Throughput:** 500,860.29 msg/s
* **Throughput Rate:** 15,651.88 MB/s
