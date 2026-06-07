# Benchmark Runs

* **System:** AMD Ryzen 5 7640U
* **Message Size:** 64 bytes (unless specified otherwise)
* **Role:** Client
* **Target Endpoint:** `tcp://127.0.0.1:19876`

---

## Results Overview

| Pattern | Features / Flags | Concurrency | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :---: | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 1 | 192,244 | 19,224.37 | 1.17 | 51 us | 62 us |
| **DealerRouter** | Standard | 1 | 247,153 | 24,715.10 | 1.51 | 37 us | 66 us |
| **PushPull** | Standard | 1 | 16,799,073 | 1,680,020.11 | 102.54 | — | — |
| **PushPull** | Standard | 4 | 31,448,877 | 3,144,779.68 | 191.94 | — | — |
| **PushPull** | Standard (32 KB msg) | 4 | 3,466,898 | 346,655.90 | 10,833.00 | — | — |
| **PubSub** | Standard | 1 | 12,752,722 | 1,274,969.70 | 77.82 | — | — |
| **PushPull** | `--cork` | 1 | 16,843,879 | 1,684,506.79 | 102.81 | — | — |
| **PushPull** | `--cork` | 4 | 31,429,832 | 3,142,795.87 | 191.82 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 4 | 3,635,594 | 363,536.15 | 11,360.50 | — | — |
| **PushPull** | `io-uring` | 1 | 14,349,214 | 1,428,757.75 | 87.20 | — | — |
| **PushPull** | `io-uring` | 4 | 13,313,464 | 1,286,098.82 | 78.50 | — | — |
| **PushPull** | `io-uring` + `--cork` | 1 | 22,383,879 | 2,238,510.98 | 136.63 | — | — |
| **PushPull** | `io-uring` + `--cork` | 2 | 21,642,618 | 2,107,855.48 | 128.65 | — | — |
| **PushPull** | `io-uring` + `--cork` | 4 | 21,996,956 | 2,199,792.54 | 134.26 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 1 | 14,501,283 | 1,443,085.89 | 88.08 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 4 | 6,970,714 | 697,117.52 | 42.55 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 4 | 3,040,601 | 303,788.72 | 9,493.40 | — | — |
| **PushPull** | `io-uring` + `--cork` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 8 | 2,118,477 | 211,594.15 | 6,612.32 | — | — |

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
* **Total Messages:** 192,244
* **Total Data:** 11.73 MB
* **Throughput:** 19,224.37 msg/s
* **Throughput Rate:** 1.17 MB/s

**Latency Distribution:**
* **Min:** 40 us
* **p50 (Median):** 51 us
* **p90:** 54 us
* **p95:** 56 us
* **p99:** 62 us
* **p99.9:** 121 us
* **Max:** 799 us

---

### 2. DealerRouter (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0001 seconds
* **Total Messages:** 247,153
* **Total Data:** 15.09 MB
* **Throughput:** 24,715.10 msg/s
* **Throughput Rate:** 1.51 MB/s

**Latency Distribution:**
* **Min:** 33 us
* **p50 (Median):** 37 us
* **p90:** 44 us
* **p95:** 53 us
* **p99:** 66 us
* **p99.9:** 117 us
* **Max:** 1,557 us

---

### 3. PushPull (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 16,799,073
* **Total Data:** 1025.33 MB
* **Throughput:** 1,680,020.11 msg/s
* **Throughput Rate:** 102.54 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0003 seconds
* **Total Messages:** 31,448,877
* **Total Data:** 1,919.49 MB
* **Throughput:** 3,144,779.68 msg/s
* **Throughput Rate:** 191.94 MB/s


#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0010 seconds
* **Total Messages:** 3,466,898
* **Total Data:** 108,340.56 MB
* **Throughput:** 346,655.90 msg/s
* **Throughput Rate:** 10,833.00 MB/s

---

### 4. PubSub (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0024 seconds
* **Total Messages:** 12,752,722
* **Total Data:** 778.36 MB
* **Throughput:** 1,274,969.70 msg/s
* **Throughput Rate:** 77.82 MB/s

---

### 5. PushPull (with Cork)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 16,843,879
* **Total Data:** 1,028.07 MB
* **Throughput:** 1,684,506.79 msg/s
* **Throughput Rate:** 102.81 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0006 seconds
* **Total Messages:** 31,429,832
* **Total Data:** 1,918.32 MB
* **Throughput:** 3,142,795.87 msg/s
* **Throughput Rate:** 191.82 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.006 seconds
* **Total Messages:** 3,635,594
* **Total Data:** 113,612.31 MB
* **Throughput:** 363,536.15 msg/s
* **Throughput Rate:** 11,360.50 MB/s

---

### 6. PushPull (io-uring)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0431 seconds
* **Total Messages:** 14,349,214
* **Total Data:** 875.81 MB
* **Throughput:** 1,428,757.75 msg/s
* **Throughput Rate:** 87.20 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.3518 seconds
* **Total Messages:** 13,313,464
* **Total Data:** 812.59 MB
* **Throughput:** 1,286,098.82 msg/s
* **Throughput Rate:** 78.50 MB/s

---

### 7. PushPull (io-uring with Cork)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9995 seconds
* **Total Messages:** 22,383,879
* **Total Data:** 1,366.20 MB
* **Throughput:** 2,238,510.98 msg/s
* **Throughput Rate:** 136.63 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork --concurrency 2
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.2676 seconds
* **Total Messages:** 21,642,618
* **Total Data:** 1,320.96 MB
* **Throughput:** 2,107,855.48 msg/s
* **Throughput Rate:** 128.65 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 21,996,956
* **Total Data:** 1,342.59 MB
* **Throughput:** 2,199,792.54 msg/s
* **Throughput Rate:** 134.26 MB/s

---

### 8. PushPull (io-uring with Multishot)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0488 seconds
* **Total Messages:** 14,501,283
* **Total Data:** 885.09 MB
* **Throughput:** 1,443,085.89 msg/s
* **Throughput Rate:** 88.08 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 6,970,714
* **Total Data:** 425.46 MB
* **Throughput:** 697,117.52 msg/s
* **Throughput Rate:** 42.55 MB/s

---

### 9. Bonus Round: PushPull, Msg Size 32KB, Concurrency 8, Cork

#### Standard
**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0089 seconds
* **Total Messages:** 3,040,601
* **Total Data:** 95,018.78 MB
* **Throughput:** 303,788.72 msg/s
* **Throughput Rate:** 9,493.40 MB/s

#### io-uring with Multishot and ZeroCopy
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --cork --uring-multishot --uring-zerocopy --concurrency 8
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0120 seconds
* **Total Messages:** 2,118,477
* **Total Data:** 66,202.41 MB
* **Throughput:** 211,594.15 msg/s
* **Throughput Rate:** 6,612.32 MB/s