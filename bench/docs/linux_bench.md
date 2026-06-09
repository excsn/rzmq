# Benchmark Runs

* **System:** AMD Ryzen 5 7640U, Balanced Power Profile
* **Message Size:** 64 bytes (unless specified otherwise)
* **Role:** Client
* **Target Endpoint:** `tcp://127.0.0.1:19876`

---

## Results Overview

| Pattern | Features / Flags | Concurrency | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :---: | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 1 | 192,244 | 19,224.37 | 1.17 | 51 us | 62 us |
| **DealerRouter** | Standard | 1 | 247,153 | 24,715.10 | 1.51 | 37 us | 66 us |
| **PushPull** | Standard | 1 | 28,179,700 | 2,818,072.87 | 172.00 | — | — |
| **PushPull** | Standard | 4 | 32,697,546 | 3,269,858.17 | 199.58 | — | — |
| **PushPull** | Standard (32 KB msg) | 4 | 2,799,716 | 279,982.38 | 8,749.45 | — | — |
| **PubSub** | Standard | 1 | 25,855,210 | 2,585,706.53 | 157.82 | — | — |
| **PubSub** | `--cork` | 1 | 26,223,062 | 2,622,501.09 | 160.06 | — | — |
| **PushPull** | `--cork` | 1 | 29,882,684 | 2,988,397.11 | 182.40 | — | — |
| **PushPull** | `--cork` | 4 | 32,080,701 | 3,206,541.52 | 195.71 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 4 | 2,394,404 | 239,456.93 | 7,483.03 | — | — |
| **PushPull** | `io-uring` | 1 | 21,602,689 | 2,160,111.87 | 131.84 | — | — |
| **PushPull** | `io-uring` | 4 | 30,960,825 | 3,096,307.99 | 188.98 | — | — |
| **PushPull** | `io-uring` + `--cork` | 1 | 22,383,879 | 2,238,510.98 | 136.63 | — | — |
| **PushPull** | `io-uring` + `--cork` | 2 | 31,715,151 | 3,171,293.67 | 193.56 | — | — |
| **PushPull** | `io-uring` + `--cork` | 4 | 32,812,183 | 3,280,899.61 | 200.25 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 1 | 21,529,422 | 2,152,982.17 | 131.41 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 4 | 31,528,145 | 3,138,975.93 | 191.59 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 8 | 2,286,920 | 228,688.71 | 7,146.52 | — | — |
| **PushPull** | `io-uring` + `--cork` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 8 | 2,360,709 | 236,048.02 | 7,376.50 | — | — |

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
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 28,179,700
* **Total Data:** 1,719.95 MB
* **Throughput:** 2,818,072.87 msg/s
* **Throughput Rate:** 172.00 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9997 seconds
* **Total Messages:** 32,697,546
* **Total Data:** 1,995.70 MB
* **Throughput:** 3,269,858.17 msg/s
* **Throughput Rate:** 199.58 MB/s


#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 2,799,716
* **Total Data:** 87,491.12 MB
* **Throughput:** 279,982.38 msg/s
* **Throughput Rate:** 8,749.45 MB/s

---

### 4. PubSub (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 25,855,210
* **Total Data:** 1,578.08 MB
* **Throughput:** 2,585,706.53 msg/s
* **Throughput Rate:** 157.82 MB/s


#### PubSub with Cork

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 26,223,062
* **Total Data:** 1,600.53 MB
* **Throughput:** 2,622,501.09 msg/s
* **Throughput Rate:** 160.06 MB/s

---

### 5. PushPull (with Cork)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 29,882,684
* **Total Data:** 1,823.89 MB
* **Throughput:** 2,988,397.11 msg/s
* **Throughput Rate:** 182.40 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0048 seconds
* **Total Messages:** 32,080,701
* **Total Data:** 1,958.05 MB
* **Throughput:** 3,206,541.52 msg/s
* **Throughput Rate:** 195.71 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 2,394,404
* **Total Data:** 74,825.12 MB
* **Throughput:** 239,456.93 msg/s
* **Throughput Rate:** 7,483.03 MB/s

---

### 6. PushPull (io-uring)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0007 seconds
* **Total Messages:** 21,602,689
* **Total Data:** 1,318.52 MB
* **Throughput:** 2,160,111.87 msg/s
* **Throughput Rate:** 131.84 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 30,960,825
* **Total Data:** 1,889.70 MB
* **Throughput:** 3,096,307.99 msg/s
* **Throughput Rate:** 188.98 MB/s

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
* **Elapsed Time:** 10.0007 seconds
* **Total Messages:** 31,715,151
* **Total Data:** 1,935.74 MB
* **Throughput:** 3,171,293.67 msg/s
* **Throughput Rate:** 193.56 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0010 seconds
* **Total Messages:** 32,812,183
* **Total Data:** 2,002.70 MB
* **Throughput:** 3,280,899.61 msg/s
* **Throughput Rate:** 200.25 MB/s

---

### 8. PushPull (io-uring with Multishot)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 21,529,422
* **Total Data:** 1,314.05 MB
* **Throughput:** 2,152,982.17 msg/s
* **Throughput Rate:** 131.41 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0441 seconds
* **Total Messages:** 31,528,145
* **Total Data:** 1,924.33 MB
* **Throughput:** 3,138,975.93 msg/s
* **Throughput Rate:** 191.59 MB/s

---

### 9. Bonus Round: PushPull, Msg Size 32KB, Concurrency 8, Cork

#### Standard
**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 8
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0001 seconds
* **Total Messages:** 2,286,920
* **Total Data:** 71,466.25 MB
* **Throughput:** 228,688.71 msg/s
* **Throughput Rate:** 7,146.52 MB/s

#### io-uring with Multishot and ZeroCopy
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --cork --uring-multishot --uring-zerocopy --concurrency 8
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0010 seconds
* **Total Messages:** 2,360,709
* **Total Data:** 73,772.16 MB
* **Throughput:** 236,048.02 msg/s
* **Throughput Rate:** 7,376.50 MB/s