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
| **DealerRouter**| Standard | 273,205 | 27,289.41 | 1.67 | 34.66 µs | 65.85 µs |
| **DealerRouter**| Concurrency 2, Pipeline 4 | 810,724 | 81,065.91 | 4.95 | 46.69 µs | 87.74 µs |
| **DealerRouter**| Concurrency 4, Pipeline 8 | 805,289 | 80,435.53 | 4.91 | 47.01 µs | 88.13 µs |
| **PushPull** | Standard | 3,807,269 | 380,752.66 | 23.24 | — | — |
| **PushPull** | Concurrency 2 | 4,421,772 | 442,199.00 | 26.99 | — | — |
| **PubSub** | Standard | 3,839,605 | 383,960.74 | 23.44 | — | — |
| **PubSub** | Concurrency 2 | 4,159,363 | 415,948.38 | 25.39 | — | — |
| **PubSub** | Concurrency 4 | 5,560,808 | 556,105.58 | 33.94 | — | — |

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
* **Total Messages:** 273,205
* **Total Data:** 16.68 MB
* **Throughput:** 27,289.41 msg/s
* **Throughput Rate:** 1.67 MB/s

**Latency Distribution:**
* **Min:** 23.20 µs
* **p50 (Median):** 34.66 µs
* **p90:** 41.57 µs
* **p95:** 48.48 µs
* **p99:** 65.86 µs
* **p99.9:** 115.97 µs
* **Max:** 1.75 ms (1,746 µs)

#### Concurrency 2, Pipeline 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 2 --pipeline 4 --duration 10
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0008 seconds
* **Total Messages:** 810,724
* **Total Data:** 49.48 MB
* **Throughput:** 81,065.91 msg/s
* **Throughput Rate:** 4.95 MB/s

**Latency Distribution:**
* **Min:** 22.29 µs
* **p50 (Median):** 46.69 µs
* **p90:** 57.92 µs
* **p95:** 64.45 µs
* **p99:** 87.74 µs
* **p99.9:** 131.84 µs
* **Max:** 259.97 µs

#### Concurrency 4, Pipeline 8

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 4 --pipeline 8 --duration 10
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0116 seconds
* **Total Messages:** 805,289
* **Total Data:** 49.15 MB
* **Throughput:** 80,435.53 msg/s
* **Throughput Rate:** 4.91 MB/s

**Latency Distribution:**
* **Min:** 21.87 µs
* **p50 (Median):** 47.01 µs
* **p90:** 58.34 µs
* **p95:** 65.02 µs
* **p99:** 88.13 µs
* **p99.9:** 130.88 µs
* **Max:** 741.38 µs

---

### 3. PushPull

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 3,807,269
* **Total Data:** 232.38 MB
* **Throughput:** 380,752.66 msg/s
* **Throughput Rate:** 23.24 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9995 seconds
* **Total Messages:** 4,421,772
* **Total Data:** 269.88 MB
* **Throughput:** 442,199.00 msg/s
* **Throughput Rate:** 26.99 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0003 seconds
* **Total Messages:** 2,651,215
* **Total Data:** 82,850.47 MB
* **Throughput:** 265,114.72 msg/s
* **Throughput Rate:** 8,284.83 MB/s

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
* **Total Messages:** 3,839,605
* **Total Data:** 234.35 MB
* **Throughput:** 383,960.74 msg/s
* **Throughput Rate:** 23.44 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9997 seconds
* **Total Messages:** 4,159,363
* **Total Data:** 253.87 MB
* **Throughput:** 415,948.38 msg/s
* **Throughput Rate:** 25.39 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 5,560,808
* **Total Data:** 339.40 MB
* **Throughput:** 556,105.58 msg/s
* **Throughput Rate:** 33.94 MB/s

#### Concurrency 4, Msg size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0004 seconds
* **Total Messages:** 1,938,524
* **Total Data:** 60,578.88 MB
* **Throughput:** 193,844.85 msg/s
* **Throughput Rate:** 6,057.65 MB/s