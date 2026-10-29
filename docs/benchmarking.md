# SyncPulse — Benchmarking

> **All numbers produced by the benchmark are MEASURED against the running stack.** This document
> describes methodology and how to read the output. It contains **no fabricated results** — run the
> benchmark to fill in your own numbers. Any capacity extrapolation you add to a report must be
> labelled **PROJECTED**, per the project constraints.

## 1. What is measured

The benchmark drives real traffic through the **real** pipeline (real Redis Lua debounce, real
Cassandra writes, real Kafka emission) and reports:

- total requests, Cassandra writes (SyncPulse), naive-baseline writes
- **write reduction %** = `100 × (1 − writes / requests)`
- debounced requests, accepted updates, Kafka events emitted
- throughput (req/s), elapsed time
- request latency p50 / p95 / p99 / max / avg
- (companion consumer) event propagation latency p50 / p95 / p99

Implementation: [`BenchmarkService`](../backend/src/main/java/com/syncpulse/service/BenchmarkService.java),
exposed at `POST /api/v1/benchmark/run`.

## 2. Baselines

| Model | Behaviour |
|---|---|
| **Naive** | every ping → a Cassandra write (`writes = totalRequests`) |
| **SyncPulse** | every ping → Redis; only significant updates → Cassandra |

The naive baseline is *definitional* (one write per ping) — the honest comparison point, since a
system without debouncing would issue exactly one durable write per ping.

## 3. Methodology

- Each simulated device streams to its **own** content id, isolating the debounce/write-reduction
  behaviour from conflict resolution.
- Positions advance by `positionStepSeconds` each ping. With `DEBOUNCE_POSITION_THRESHOLD=5s`, a
  2s step means roughly one persist per ~3 pings from the position rule alone.
- Vector clocks are server-managed in the benchmark (each ping is intentionally causally-later), so
  there are no artificial conflicts — the benchmark isolates the write-reduction NFR.
- The write-reduction ratio is therefore driven by the **debounce thresholds**, which come from the
  deployed configuration, not the benchmark.

## 4. Run it

```bash
# args: users devicesPerUser pingsPerDevice positionStepSeconds concurrency
node scripts/benchmark/benchmark.mjs 5 3 400 2 8
```

or via the dashboard **Benchmark** panel, or directly:

```bash
curl -s -X POST http://localhost:8080/api/v1/benchmark/run \
  -H 'Content-Type: application/json' \
  -d '{"users":5,"devicesPerUser":3,"pingsPerDevice":400,"positionStepSeconds":2,"concurrency":8}'
```

## 5. Reading the output

```
── Results (MEASURED) ──
  Write reduction  : <measured> %
  Throughput       : <measured> req/s
  Latency p50/p95/p99/max : <measured> ms
```

Interpretation checklist:

- **Write reduction** should track the theoretical debounce ratio for your thresholds/step. A 2s
  step with a 5s position threshold and near-instant pings gives a large reduction dominated by the
  position rule; widen the step or lower the threshold to see it change.
- **Throughput / latency** reflect your machine (a single-node dev Cassandra + single-broker Kafka
  in Docker). They demonstrate the *shape* of the system, not production capacity.

## 6. Limitations

- Single-node Cassandra (RF 1) and single-broker Kafka on one host — absolute throughput is
  machine-bound and **not** a production capacity figure.
- The load generator runs in-process (server-side), which removes network round-trip time from the
  latency figures relative to over-the-wire clients; treat latency as pipeline latency.
- Propagation latency uses in-Docker clocks (same host), so it measures emit→consume time without
  cross-host clock skew.

Record the configuration alongside any results you cite, and mark projections explicitly.
