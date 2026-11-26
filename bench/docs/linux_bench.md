# Benchmark Runs

* **System:** AMD Ryzen 5 7640U
* **Message Size:** 64 bytes (unless specified otherwise)
* **Role:** Client/Server
* **Target Endpoint:** `tcp://127.0.0.1:19876`

---

## Results Overview

| Pattern | Features / Flags | Concurrency | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :---: | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 1 | 347,140 | 34,713.45 | 2.12 | 27.8 us | 40.7 us |
| **ReqRep** | `io-uring` + `--uring-workers 1` | 1 | 355,347 | 35,535.03 | 2.17 | 27.1 us | 40.0 us |
| **DealerRouter** | Standard | 1 | 341,352 | 34,135.48 | 2.28 | 28.2 us | 41.3 us |
| **PushPull** | Standard | 1 | 42,294,989 | 4,228,580.01 | 258.09 | — | — |
| **PushPull** | Standard | 4 | 61,794,793 | 6,179,566.29 | 377.17 | — | — |
| **PushPull** | Standard (16 KB msg) | 1 | 3,331,521 | 333,154.03 | 5,205.53 | — | — |
| **PushPull** | Standard (32 KB msg) | 1 | 1,667,953 | 166,797.21 | 5,212.41 | — | — |
| **PushPull** | Standard (32 KB msg) | 4 | 5,354,036 | 535,409.16 | 16,731.54 | — | — |
| **PubSub** | Standard | 1 | 39,901,592 | 3,990,264.22 | 243.55 | — | — |
| **PubSub** | `--cork` | 1 | 39,566,349 | 3,955,128.33 | 241.40 | — | — |
| **PushPull** | `--cork` | 1 | 39,688,752 | 3,967,317.10 | 242.15 | — | — |
| **PushPull** | `--cork` | 4 | 61,740,805 | 6,174,614.52 | 376.87 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 4 | 5,740,882 | 574,133.65 | 17,941.68 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` | 1 | 70,580,516 | 7,058,979.41 | 430.85 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` | 4 | 71,107,391 | 7,111,468.06 | 434.05 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--cork` | 1 | 54,305,895 | 5,431,284.23 | 331.50 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--cork` | 4 | 75,281,560 | 7,529,570.25 | 459.57 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` | 1 | 51,958,836 | 5,195,408.91 | 317.10 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` | 4 | 53,762,589 | 5,376,906.90 | 328.18 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` (32 KB msg) | 1 | 2,222,961 | 222,334.99 | 6,947.97 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` (32 KB msg) | 1 | 2,603,523 | 260,376.23 | 8,136.76 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` (32 KB msg) | 4 | 2,587,767 | 258,808.73 | 8,087.77 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 1 | 2,145,175 | 214,543.07 | 6,704.47 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 4 | 2,186,193 | 218,646.74 | 6,832.71 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-workers 4` (4 KB msg) | 8 | 35,857,140 | 3,585,847.90 | 14,007.22 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-workers 2` (32 KB msg) | 8 | 4,349,748 | 435,036.60 | 13,594.89 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-workers 4` (32 KB msg) | 8 | 5,677,524 | 567,798.62 | 17,743.71 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--cork` + `--uring-workers 4` (32 KB msg) | 8 | 4,805,398 | 480,593.47 | 15,018.55 | — | — |

---

## Detailed Benchmark Reports

### 1. ReqRep (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 10.0002 seconds
* **Total Messages:** 347,140
* **Total Data:** 21.19 MB
* **Throughput:** 34,713.45 msg/s
* **Throughput Rate:** 2.12 MB/s

**Latency Distribution:**
* **Min:** 24.080 us
* **p50 (Median):** 27.839 us
* **p90:** 31.695 us
* **p95:** 32.991 us
* **p99:** 40.671 us
* **p99.9:** 78.143 us
* **Max:** 639.487 us


#### io-uring

**Command:**
```bash
cargo run --release --bin rzmq_bench --features io-uring -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --use-io-uring --uring-workers 1
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 355,347
* **Total Data:** 21.69 MB
* **Throughput:** 35,535.03 msg/s
* **Throughput Rate:** 2.17 MB/s

**Latency Distribution:**
* **Min:** 20.720 us
* **p50 (Median):** 27.119 us
* **p90:** 29.391 us
* **p95:** 31.039 us
* **p99:** 40.031 us
* **p99.9:** 247.935 us
* **Max:** 3.824 ms

---

### 2. DealerRouter (Standard/io-uring)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 341,352
* **Total Data:** 22.79 MB
* **Throughput:** 34,135.48 msg/s
* **Throughput Rate:** 2.28 MB/s

**Latency Distribution:**
* **Min:** 23.552 us
* **p50 (Median):** 28.159 us
* **p90:** 31.615 us
* **p95:** 33.311 us
* **p99:** 41.311 us
* **p99.9:** 81.215 us
* **Max:** 724.479 us

---

### 3. PushPull (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0022 seconds
* **Total Messages:** 42,294,989
* **Total Data:** 2,581.48 MB
* **Throughput:** 4,228,580.01 msg/s
* **Throughput Rate:** 258.09 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 61,794,793
* **Total Data:** 3,771.65 MB
* **Throughput:** 6,179,566.29 msg/s
* **Throughput Rate:** 377.17 MB/s

#### Concurrency 1, Msg Size 16KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 16384
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 3,331,521
* **Total Data:** 52,055.02 MB
* **Throughput:** 333,154.03 msg/s
* **Throughput Rate:** 5,205.53 MB/s

#### Concurrency 1, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 1,667,953
* **Total Data:** 52,123.53 MB
* **Throughput:** 166,797.21 msg/s
* **Throughput Rate:** 5,212.41 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 5,354,036
* **Total Data:** 167,313.62 MB
* **Throughput:** 535,409.16 msg/s
* **Throughput Rate:** 16,731.54 MB/s

---

### 4. PubSub (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9997 seconds
* **Total Messages:** 39,901,592
* **Total Data:** 2,435.40 MB
* **Throughput:** 3,990,264.22 msg/s
* **Throughput Rate:** 243.55 MB/s

#### PubSub with Cork

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0038 seconds
* **Total Messages:** 39,566,349
* **Total Data:** 2,414.94 MB
* **Throughput:** 3,955,128.33 msg/s
* **Throughput Rate:** 241.40 MB/s

---

### 5. PushPull (with Cork)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0039 seconds
* **Total Messages:** 39,688,752
* **Total Data:** 2,422.41 MB
* **Throughput:** 3,967,317.10 msg/s
* **Throughput Rate:** 242.15 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9991 seconds
* **Total Messages:** 61,740,805
* **Total Data:** 3,768.36 MB
* **Throughput:** 6,174,614.52 msg/s
* **Throughput Rate:** 376.87 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9992 seconds
* **Total Messages:** 5,740,882
* **Total Data:** 179,402.56 MB
* **Throughput:** 574,133.65 msg/s
* **Throughput Rate:** 17,941.68 MB/s

---

### 6. PushPull (io-uring)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9987 seconds
* **Total Messages:** 70,580,516
* **Total Data:** 4,307.89 MB
* **Throughput:** 7,058,979.41 msg/s
* **Throughput Rate:** 430.85 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9990 seconds
* **Total Messages:** 71,107,391
* **Total Data:** 4,340.05 MB
* **Throughput:** 7,111,468.06 msg/s
* **Throughput Rate:** 434.05 MB/s

---

### 7. PushPull (io-uring with Cork)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9987 seconds
* **Total Messages:** 54,305,895
* **Total Data:** 3,314.57 MB
* **Throughput:** 5,431,284.23 msg/s
* **Throughput Rate:** 331.50 MB/s


#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9981 seconds
* **Total Messages:** 75,281,560
* **Total Data:** 4,594.82 MB
* **Throughput:** 7,529,570.25 msg/s
* **Throughput Rate:** 459.57 MB/s

---

### 8. PushPull (io-uring with Multishot)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --uring-multishot
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0009 seconds
* **Total Messages:** 51,958,836
* **Total Data:** 3,171.32 MB
* **Throughput:** 5,195,408.91 msg/s
* **Throughput Rate:** 317.10 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9988 seconds
* **Total Messages:** 53,762,589
* **Total Data:** 3,281.41 MB
* **Throughput:** 5,376,906.90 msg/s
* **Throughput Rate:** 328.18 MB/s

---

### 9. PushPull, io-uring, Msg Size 32KB

#### Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9983 seconds
* **Total Messages:** 2,222,961
* **Total Data:** 69,467.53 MB
* **Throughput:** 222,334.99 msg/s
* **Throughput Rate:** 6,947.97 MB/s

#### with Multishot, Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --uring-multishot --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9991 seconds
* **Total Messages:** 2,603,523
* **Total Data:** 81,360.09 MB
* **Throughput:** 260,376.23 msg/s
* **Throughput Rate:** 8,136.76 MB/s


#### with Multishot, Concurrency 4
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9988 seconds
* **Total Messages:** 2,587,767
* **Total Data:** 80,867.72 MB
* **Throughput:** 258,808.73 msg/s
* **Throughput Rate:** 8,087.77 MB/s

#### with Multishot and ZeroCopy, Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --uring-multishot --uring-zerocopy --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9988 seconds
* **Total Messages:** 2,145,175
* **Total Data:** 67,036.72 MB
* **Throughput:** 214,543.07 msg/s
* **Throughput Rate:** 6,704.47 MB/s

#### with Multishot and ZeroCopy, Concurrency 4
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --uring-multishot --uring-zerocopy --concurrency 4 --duration 600
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9987 seconds
* **Total Messages:** 2,186,193
* **Total Data:** 68,318.53 MB
* **Throughput:** 218,646.74 msg/s
* **Throughput Rate:** 6,832.71 MB/s

---

### 10. PushPull, io-uring Multishot, Sharded Workers (`--uring-workers`)

Connections are sharded round-robin across independent `UringWorker` rings. With `--concurrency 8` there are 8 connections to distribute; the 2→4 worker sweep at 32 KB shows the sharding scaling (435 K → 568 K msg/s).

#### 4 Workers, Msg Size 4KB, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 4096 --concurrency 8 --use-io-uring --uring-multishot --uring-workers 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 35,857,140
* **Total Data:** 140,066.95 MB
* **Throughput:** 3,585,847.90 msg/s
* **Throughput Rate:** 14,007.22 MB/s

#### 2 Workers, Msg Size 32KB, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 8 --use-io-uring --uring-multishot --uring-workers 2
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9986 seconds
* **Total Messages:** 4,349,748
* **Total Data:** 135,929.62 MB
* **Throughput:** 435,036.60 msg/s
* **Throughput Rate:** 13,594.89 MB/s

#### 4 Workers, Msg Size 32KB, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 8 --use-io-uring --uring-multishot --uring-workers 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9992 seconds
* **Total Messages:** 5,677,524
* **Total Data:** 177,422.62 MB
* **Throughput:** 567,798.62 msg/s
* **Throughput Rate:** 17,743.71 MB/s

#### 4 Workers with Cork, Msg Size 32KB, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 8 --use-io-uring --uring-multishot --cork --uring-workers 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9989 seconds
* **Total Messages:** 4,805,398
* **Total Data:** 150,168.69 MB
* **Throughput:** 480,593.47 msg/s
* **Throughput Rate:** 15,018.55 MB/s