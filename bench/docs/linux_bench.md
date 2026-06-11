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
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 50,809,050
* **Total Data:** 3,101.14 MB
* **Throughput:** 5,080,981.41 msg/s
* **Throughput Rate:** 310.12 MB/s


#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0005 seconds
* **Total Messages:** 3,124,097
* **Total Data:** 97,628.03 MB
* **Throughput:** 312,393.53 msg/s
* **Throughput Rate:** 9,762.30 MB/s

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
* **Elapsed Time:** 10.0022 seconds
* **Total Messages:** 50,332,551
* **Total Data:** 3,072.06 MB
* **Throughput:** 5,032,172.68 msg/s
* **Throughput Rate:** 307.14 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0007 seconds
* **Total Messages:** 2,683,392
* **Total Data:** 83,856.00 MB
* **Throughput:** 268,320.71 msg/s
* **Throughput Rate:** 8,385.02 MB/s

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
* **Elapsed Time:** 10.0003 seconds
* **Total Messages:** 41,799,583
* **Total Data:** 2,551.24 MB
* **Throughput:** 4,179,833.77 msg/s
* **Throughput Rate:** 255.12 MB/s

---

### 7. PushPull (io-uring with Cork)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9994 seconds
* **Total Messages:** 23,772,462
* **Total Data:** 1,450.96 MB
* **Throughput:** 2,377,393.48 msg/s
* **Throughput Rate:** 145.10 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork --concurrency 2
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 34,543,208
* **Total Data:** 2,108.35 MB
* **Throughput:** 3,454,466.59 msg/s
* **Throughput Rate:** 210.84 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0018 seconds
* **Total Messages:** 41,322,072
* **Total Data:** 2,522.10 MB
* **Throughput:** 4,131,451.07 msg/s
* **Throughput Rate:** 252.16 MB/s

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
* **Elapsed Time:** 9.9992 seconds
* **Total Messages:** 39,676,273
* **Total Data:** 2,421.65 MB
* **Throughput:** 3,967,953.46 msg/s
* **Throughput Rate:** 242.18 MB/s

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