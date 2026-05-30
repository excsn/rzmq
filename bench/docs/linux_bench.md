# Benchmark runs

System: Macbook Pro M4

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : ReqRep
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 192244
Total Data                : 11.73 MB
--------------------------------------------------------
Throughput                : 19224.37 msg/s
Throughput Rate           : 1.17 MB/s
--------------------------------------------------------
Latency Distribution (Microseconds):
  Min             :         40 us
  p50 (Median)    :         51 us
  p90             :         54 us
  p95             :         56 us
  p99             :         62 us
  p99.9           :        121 us
  Max             :        799 us
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : DealerRouter
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 193232
Total Data                : 11.79 MB
--------------------------------------------------------
Throughput                : 19323.12 msg/s
Throughput Rate           : 1.18 MB/s
--------------------------------------------------------
Latency Distribution (Microseconds):
  Min             :         40 us
  p50 (Median)    :         51 us
  p90             :         53 us
  p95             :         56 us
  p99             :         61 us
  p99.9           :         94 us
  Max             :       2775 us
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PushPull
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0032 seconds
Total Messages            : 1445554
Total Data                : 88.23 MB
--------------------------------------------------------
Throughput                : 144508.58 msg/s
Throughput Rate           : 8.82 MB/s
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PubSub
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0031 seconds
Total Messages            : 1438614
Total Data                : 87.81 MB
--------------------------------------------------------
Throughput                : 143816.81 msg/s
Throughput Rate           : 8.78 MB/s
========================================================


Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PushPull
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0007 seconds
Total Messages            : 5811066
Total Data                : 354.68 MB
--------------------------------------------------------
Throughput                : 581068.20 msg/s
Throughput Rate           : 35.47 MB/s
========================================================

Command: cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PushPull
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 4069997
Total Data                : 248.41 MB
--------------------------------------------------------
Throughput                : 406999.65 msg/s
Throughput Rate           : 24.84 MB/s
========================================================