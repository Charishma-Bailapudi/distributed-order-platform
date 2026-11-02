# Project Learnings & Benchmark History

This file tracks every experiment, the concepts proven, and the before/after performance numbers across all phases.

---

## Phase Baseline Summary Table

| Phase   | Description                             | Throughput (req/s) | p50 Latency | p95 Latency | p99 Latency | Failure Rate |
| ------- | --------------------------------------- | ------------------ | ----------- | ----------- | ----------- | ------------ |
| Phase 1 | Direct REST + DB baseline (no Kafka)    | -                  | -           | -           | -           | -            |
| Phase 3 | End-to-end Saga (4-partition Kafka)     | -                  | -           | -           | -           | -            |
| Phase 6 | Unsafe HashMap vs ConcurrentHashMap     | -                  | -           | -           | -           | -            |
| Phase 7 | Concurrency tuning (1 vs 4 vs 10 vs 50) | -                  | -           | -           | -           | -            |
| Phase 9 | Cache-Aside (No Cache vs Redis/Ignite)  | -                  | -           | -           | -           | -            |

---
