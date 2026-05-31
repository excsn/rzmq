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
Total Messages            : 217423
Total Data                : 13.27 MB
--------------------------------------------------------
Throughput                : 21742.20 msg/s
Throughput Rate           : 1.33 MB/s
--------------------------------------------------------
Latency Distribution (Microseconds):
  Min             :         29 us
  p50 (Median)    :         44 us
  p90             :         50 us
  p95             :         57 us
  p99             :         64 us
  p99.9           :        107 us
  Max             :       1205 us
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : DealerRouter
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 214268
Total Data                : 13.08 MB
--------------------------------------------------------
Throughput                : 21426.72 msg/s
Throughput Rate           : 1.31 MB/s
--------------------------------------------------------
Latency Distribution (Microseconds):
  Min             :         28 us
  p50 (Median)    :         45 us
  p90             :         51 us
  p95             :         58 us
  p99             :         66 us
  p99.9           :        118 us
  Max             :       1149 us
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PushPull
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 2827166
Total Data                : 172.56 MB
--------------------------------------------------------
Throughput                : 282716.45 msg/s
Throughput Rate           : 17.26 MB/s
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PubSub
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 2586292
Total Data                : 157.85 MB
--------------------------------------------------------
Throughput                : 258629.17 msg/s
Throughput Rate           : 15.79 MB/s
========================================================