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
| **PushPull** | Standard | 1 | 1,932,309 | 193,247.55 | 11.79 | — | — |
| **PushPull** | Standard | 4 | 5,928,231 | 592,867.56 | 36.19 | — | — |
| **PubSub** | Standard | 1 | 1,438,614 | 143,816.81 | 8.78 | — | — |
| **PushPull** | `--cork` | 1 | 7,733,889 | 773,470.23 | 47.21 | — | — |
| **PushPull** | `--cork` | 4 | 21,306,074 | 2,130,681.48 | 130.05 | — | — |
| **PushPull** | `io-uring` | 1 | 1,887,233 | 188,747.10 | 11.52 | — | — |
| **PushPull** | `io-uring` | 4 | 7,072,464 | 707,257.90 | 43.17 | — | — |
| **PushPull** | `io-uring` + `--cork` | 1 | 7,735,178 | 773,548.59 | 47.21 | — | — |
| **PushPull** | `io-uring` + `--cork` | 2 | 13,479,571 | 1,348,032.03 | 82.28 | — | — |
| **PushPull** | `io-uring` + `--cork` | 4 | 21,996,956 | 2,199,792.54 | 134.26 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 1 | 1,814,016 | 181,408.70 | 11.07 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 4 | 6,970,714 | 697,117.52 | 42.55 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 8 | 2,438,218 | 243,785.88 | 7,618.31 | — | — |
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
* **Elapsed Time:** 9.9992 seconds
* **Total Messages:** 13,693,316
* **Total Data:** 835.77 MB
* **Throughput:** 1,369,444.79 msg/s
* **Throughput Rate:** 83.58 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0180 seconds
* **Total Messages:** 23,402,817
* **Total Data:** 1428.39 MB
* **Throughput:** 2,336,078.05 msg/s
* **Throughput Rate:** 142.58 MB/s


#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 3,008,939
* **Total Data:** 94,029.34 MB
* **Throughput:** 300,893.90 msg/s
* **Throughput Rate:** 9402.93 MB/s

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
* **Elapsed Time:** 9.9992 seconds
* **Total Messages:** 13,623,742
* **Total Data:** 831.53 MB
* **Throughput:** 1,362,489.43 msg/s
* **Throughput Rate:** 83.16 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0176 seconds
* **Total Messages:** 23,618,160
* **Total Data:** 1,441.54 MB
* **Throughput:** 2,357,658.84 msg/s
* **Throughput Rate:** 143.90 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0031 seconds
* **Total Messages:** 3,009,107
* **Total Data:** 94,034.59 MB
* **Throughput:** 300,818.72 msg/s
* **Throughput Rate:** 9,400.59 MB/s

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
* **Total Data:** 1320.96 MB
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
* **Throughput Rate:** 9493.40 MB/s

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