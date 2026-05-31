# Benchmark Runs

* **System:** Macbook Pro M4
* **Message Size:** 64 bytes
* **Role:** Client
* **Target Endpoint:** `tcp://127.0.0.1:19876`

---

## Summary of Results

| Pattern | Features / Flags | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 217,423 | 21,742.20 | 1.33 | 44 us | 64 us |
| **DealerRouter**| Standard | 214,268 | 21,426.72 | 1.31 | 45 us | 66 us |
| **PushPull** | Standard | 2,827,166 | 282,716.45 | 17.26 | — | — |
| **PubSub** | Standard | 2,586,292 | 258,629.17 | 15.79 | — | — |

---

## Detailed Benchmark Reports

### 1. ReqRep (Standard)

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
* **Min:** 29 us
* **p50 (Median):** 44 us
* **p90:** 50 us
* **p95:** 57 us
* **p99:** 64 us
* **p99.9:** 107 us
* **Max:** 1,205 us

---

### 2. DealerRouter (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 214,268
* **Total Data:** 13.08 MB
* **Throughput:** 21,426.72 msg/s
* **Throughput Rate:** 1.31 MB/s

**Latency Distribution:**
* **Min:** 28 us
* **p50 (Median):** 45 us
* **p90:** 51 us
* **p95:** 58 us
* **p99:** 66 us
* **p99.9:** 118 us
* **Max:** 1,149 us

---

### 3. PushPull (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 2,827,166
* **Total Data:** 172.56 MB
* **Throughput:** 282,716.45 msg/s
* **Throughput Rate:** 17.26 MB/s

---

### 4. PubSub (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 2,586,292
* **Total Data:** 157.85 MB
* **Throughput:** 258,629.17 msg/s
* **Throughput Rate:** 15.79 MB/s