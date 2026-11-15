# Benchmark protocol

Run `k6 run benchmarks/payments.js` from the repository root after starting the distributed stack.
The benchmark sends **payment creation** requests only; it does not establish end-to-end settlement throughput.
Report hardware, Kafka partition count, Docker resources, test duration, total requests, errors, p50/p95/p99 latency, and ledger audit results.
Do not claim a target TPS or failover time until measured. The included k6 script is a starting workload, not a validated scale benchmark.
