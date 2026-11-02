# 04 - Test Specification & Load Testing Protocol

## 1. Overview & Pedagogical Goal

The test harness is not an afterthought; it is our primary instrumentation tool. Every build phase must be measured against explicit scripts to prove concepts (concurrency race conditions, partition parallelism, caching efficiency, circuit breakers) using reproducible before/after numbers.

All load tests are executed using **k6** (open-source CLI) targeting containerized or locally hosted endpoints.

---

## 2. Test Pyramid & Tooling Boundaries

| Test Layer                  | Framework / Tool                    | Execution Target                                       | Pass/Fail Criteria                     |
| --------------------------- | ----------------------------------- | ------------------------------------------------------ | -------------------------------------- |
| **Unit Tests**              | JUnit 5 + Mockito                   | Isolated domain logic & state machines                 | 100% assertions pass                   |
| **Integration Tests**       | Testcontainers (PostgreSQL + Kafka) | Service-level DB operations, Kafka consumers/producers | Real container execution without mocks |
| **Load & Stress Tests**     | k6                                  | System endpoints under concurrent traffic              | Thresholds (p95 latency, error rate)   |
| **Chaos / Fault Injection** | Custom endpoint / Chaos configs     | Consumer crashes, slow network, downstream dropouts    | Observable recovery or routing to DLQ  |

---

## 3. Standard k6 Benchmark Profiles

To ensure every phase's before/after comparisons are statistically valid, tests must follow standard load profiles.

### Profile A: Baseline & Smoke (`load-tests/profile-a-baseline.js`)

- **Objective:** Measure clean-system baseline throughput and latency ceiling.
- **Ramping Profile:**
  - 0s - 10s: Ramp 0 -> 10 VUs (Warm-up)
  - 10s - 40s: Ramp 10 -> 50 VUs (Sustained)
  - 40s - 60s: Ramp 50 -> 100 VUs (Peak)
  - 60s - 70s: Ramp 100 -> 0 VUs (Cooldown)
- **Thresholds:**
  - `http_req_duration`: `p(95) < 500ms`
  - `http_req_failed`: `rate < 0.01` (less than 1% errors)

### Profile B: Concurrency Race Test (`load-tests/profile-b-wallet-race.js`)

- **Objective:** Exploit wallet check-then-act balance races and in-memory HashMap race conditions.
- **Characteristics:**
  - Fixed pool of 200 concurrent VUs firing requests simultaneously.
  - Target customer: Single customer (`cust-103`) with a starting wallet balance of ₹5,000.
  - Order amount: ₹1,000 per order.
  - Expected behavior without locking: Negative balance or lost update counts.
  - Expected behavior with fix: Exactly 5 orders succeed, 195 rejected with `INSUFFICIENT_FUNDS`, balance ends at exactly ₹0.00.

### Profile C: Cache Access Skew Test (`load-tests/profile-c-cache-skew.js`)

- **Objective:** Measure cache hit rate, p95 latency, and database query reduction under realistic Zipfian/Pareto traffic.
- **Traffic Distribution:**
  - 80% of requests query 20% of catalog (`prod-001`, `prod-004`).
  - 20% of requests query remaining 80% uniformly.
- **Comparison Metric:** Cache Hit Rate (%) and DB connection pool utilization.

### Profile D: Cache Stampede / Thundering Herd (`load-tests/profile-d-stampede.js`)

- **Objective:** Spike 500 concurrent requests for an expired key (`prod-003`).
- **Measurement:** DB query count spike vs single-flight request coalescing.

### Profile E: Circuit Breaker Failure & Recovery (`load-tests/profile-e-circuit-breaker.js`)

- **Objective:** Induce 100% downstream gateway timeouts, verify circuit trips to `OPEN`, measures fail-fast latency drop, then verify transition to `HALF_OPEN` -> `CLOSED`.

---

## 4. Experiment Protocol & `LEARNINGS.md` Contract

For every phase in the roadmap, the following 4-step protocol is mandatory:

1. **State the Premise:** What distributed systems hypothesis are we testing?
2. **Execute the Broken/Unoptimized State:**
   - Run the specified k6 profile.
   - Record exact numbers: Total Requests, Req/sec, p50, p95, p99, Error Rate.
   - Capture error logs or database anomalies (e.g., negative wallet, lost counter).
3. **Apply the Fix / Architectural Pattern:**
   - Implement the pattern (e.g., `SELECT FOR UPDATE`, `ConcurrentHashMap`, Cache-Aside, Circuit Breaker).
4. **Execute Post-Fix Verification:**
   - Re-run the identical k6 profile.
   - Record the new metrics.
   - Calculate performance delta and document in `LEARNINGS.md`.

---

## 5. Metrics & Measurement Matrix

Every k6 output must report these standard attributes:

```text
| Metric Name         | Unit  | Definition                                          |
|---------------------|-------|-----------------------------------------------------|
| Throughput          | req/s | Total successful HTTP/event completions per second  |
| p50 Latency         | ms    | Median response time (50th percentile)              |
| p95 Latency         | ms    | 95th percentile response time                       |
| p99 Latency         | ms    | Tail latency (99th percentile)                      |
| Consumer Lag        | msgs  | Difference between log-end offset and consumer offset|
| Cache Hit Rate      | %     | Hits / (Hits + Misses) * 100                        |
| DB Active Conns     | count | HikariCP active connections during peak             |
```
