<div align="center">

# SyncPulse Enterprise

### Event-Driven NoSQL State Backend for Cross-Device Video Playhead Synchronization

**Redis debounce · distributed vector clocks · Apache Cassandra · CDC → Kafka → downstream consumer**

*BCSE406L — NoSQL Database · a polished, enterprise-grade prototype*

</div>

---

## 1. What this is

SyncPulse keeps a user's video watch position ("playhead") consistent across phone, tablet,
TV and web — at high update frequency, without overwhelming the durable database, and
without letting a stale, previously-offline device corrupt newer progress.

Think of it as the backend behind Netflix-style **"Continue Watching"**, engineered as a
serious distributed-systems problem rather than a CRUD app.

It solves three distinct problems with three complementary techniques:

| Problem | Technique | Where |
|---|---|---|
| **Write overload** — a position ping every few seconds per device | **Redis debounce** via an atomic Lua script; only *significant* updates reach Cassandra | [`debounce.lua`](backend/src/main/resources/redis/debounce.lua) |
| **Stale-device conflicts** — an offline device overwriting newer state | **Distributed vector clocks** — causal ordering, not wall-clock time | [`VectorClock`](backend/src/main/java/com/syncpulse/domain/VectorClock.java), [`ConflictResolver`](backend/src/main/java/com/syncpulse/conflict/ConflictResolver.java) |
| **Downstream propagation** — other services need changes without polling | **CDC → Kafka** to a downstream consumer | [`CdcEventPublisher`](backend/src/main/java/com/syncpulse/cdc/CdcEventPublisher.java), [`consumer/`](consumer/) |

---

## 2. Run it

**Prerequisites:** Docker + Docker Compose. That's all.

```bash
docker compose up --build
```

Then open:

| Surface | URL |
|---|---|
| **Control Center dashboard** | http://localhost:3000 |
| Backend API + Swagger UI | http://localhost:8080/swagger-ui.html |
| Companion (consumer) service | http://localhost:8081/consumer/metrics |
| Prometheus metrics | http://localhost:8080/actuator/prometheus |

The stack starts Redis, Cassandra, Kafka (KRaft), the backend, the downstream consumer and
the dashboard, with health checks and correct start-up ordering (Cassandra schema + Kafka
topic are created by one-shot init jobs). Demo devices are seeded automatically.

### Two interchangeable backends (identical API)

The backend + consumer ship in **two implementations behind the same REST contract**, so the
dashboard, smoke test, and benchmark work unchanged against either:

| Implementation | Location | Run it |
|---|---|---|
| **Python / FastAPI** *(default)* | `backend-python/`, `consumer-python/` | `docker compose up --build` |
| **Java / Spring Boot** | `backend/`, `consumer/` | `docker compose -f docker-compose.yml -f docker-compose.java.yml up --build` |

Both use the **same Redis Lua debounce script**, the same vector-clock semantics, the same
Cassandra schema, and the same Kafka event schema. The Java/JVM version sustains higher
concurrent throughput; the Python version is lighter and quicker to extend. Parity is proven by
the shared `scripts/test/smoke.mjs` passing 10/10 against both.

On the dashboard, click **▶ Run Live Demo** for the full 11-stage showcase, or the
**A / B / C / D** scenario buttons individually.

> First-run note: if Docker Desktop fails to start with an *"initializing Inference manager"*
> error (happens when your Windows username contains spaces), set
> `%APPDATA%\Docker\features-overrides.json` to `{ "InferenceAvailable": false }` and restart
> Docker Desktop.

---

## 3. Architecture

```
 Device simulators (phone / tv / web / tablet)
        │  POST /api/v1/playhead/update  (userId, contentId, deviceId, position, vectorClock)
        ▼
 ┌──────────────────────── Backend (Spring Boot, Java 21) ────────────────────────┐
 │  Ingest ─▶ Idempotency (Redis SET NX) ─▶ Redis DEBOUNCE (atomic Lua)            │
 │                                              │ significant only                 │
 │                                              ▼                                   │
 │                              Vector-clock CONFLICT ENGINE                        │
 │                              (BEFORE / AFTER / EQUAL / CONCURRENT)               │
 │                                              │ durable decisions                 │
 │                                              ▼                                   │
 │                                   Cassandra (authoritative state)                │
 │                                              │ committed change                  │
 │                                              ▼                                   │
 │                          CdcEventPublisher (LOCAL_ADAPTER | DEBEZIUM)            │
 └──────────────────────────────────────────────┼─────────────────────────────────┘
                                                 ▼
                                    Kafka  syncpulse.playhead.changes
                                                 ▼
                              Companion consumer (second-screen service)
                                                 ▼
                        Dashboard polls metrics / feeds / conflicts / events (live)
```

Full detail: [`docs/architecture.md`](docs/architecture.md).

### Why these technologies

- **Why Redis?** A position ping every few seconds is high-volume, low-value-per-write. Redis
  absorbs the storm in memory and an **atomic Lua script** decides — server-side, race-free —
  whether an update is significant enough to persist. ~70–90% of writes never reach Cassandra.
- **Why Cassandra?** Linear write scalability and a wide-column model that fits
  "device + position + clock" state. Every SyncPulse read/write is a **single-partition**
  operation. See [`docs/database-design.md`](docs/database-design.md).
- **Why vector clocks?** Wall-clock time cannot be trusted across independent devices. Vector
  clocks capture **causality** — "this happened after that" vs "these are concurrent" — which is
  exactly what's needed to reject a stale offline device and to *detect* genuine concurrency.
  See [`docs/vector-clocks.md`](docs/vector-clocks.md).
- **Why Kafka + CDC?** Downstream services (a second-screen companion) must react to committed
  changes **without polling** the primary DB. A committed change becomes an event on a Kafka
  topic; consumers subscribe.

---

## 4. Core behaviours (all demonstrable)

**Conflict decisions** (`ConflictResolver`):

| Case | Incoming vs stored clock | Decision |
|---|---|---|
| First update | no prior state | `ACCEPTED_FIRST` |
| Causally newer | AFTER | `ACCEPTED` |
| Causally stale | BEFORE | `REJECTED_STALE` (state never regresses) |
| Same | EQUAL | `IDEMPOTENT` |
| Concurrent | neither dominates | `CONCURRENT_ACCEPTED` / `CONCURRENT_REJECTED` |

**Concurrent resolution policy:** *highest position wins* (video progress is monotonic-forward),
and the **merged (element-wise max) clock is always persisted** so histories converge and the
losing update cannot re-trigger the conflict. This is deterministic and documented — SyncPulse
does **not** pretend concurrent clocks have a natural total order.

**Idempotency:** every update carries a `requestId`; a duplicate is claimed atomically in Redis
(`SET NX`) and short-circuits without a second state transition.

**Write reduction:** `writeReductionPercentage = 100 × (1 − cassandraWrites / totalPings)` —
computed from real runtime counters, never hardcoded.

---

## 5. API

Base path `/api/v1`. Interactive docs at `/swagger-ui.html`.

| Method | Path | Purpose |
|---|---|---|
| POST | `/playhead/update` | Ingest an update through the full pipeline |
| GET | `/playhead/{userId}/{contentId}` | Authoritative state |
| POST | `/devices/register` | Register a device |
| GET | `/users/{userId}/devices` | List a user's devices |
| GET | `/conflicts/{userId}` | Conflict audit trail (newest first) |
| GET | `/metrics` | Live pipeline metrics (dashboard) |
| GET | `/events?since=` | Live pipeline activity feed |
| GET | `/status` | Infrastructure component status |
| GET | `/health` | Liveness probe |
| POST | `/benchmark/run` | Load benchmark (measured) |
| POST | `/admin/reset` | Reset counters / demo state |

Example — a rejected stale update:

```json
{
  "decision": "REJECTED_STALE",
  "reason": "incoming vector clock is causally behind authoritative state; rejected to prevent regression",
  "comparison": "BEFORE",
  "persisted": false,
  "authoritativePosition": 420,
  "authoritativeVectorClock": { "tv-001": 12, "phone-001": 4 }
}
```

More examples: [`docs/api.md`](docs/api.md).

---

## 6. Demo, benchmark, tests

```bash
# Full visual demo: dashboard → ▶ Run Live Demo   (http://localhost:3000)

# CLI walkthrough of the headline scenarios
node scripts/demo/demo.mjs

# End-to-end smoke test (asserts debounce, stale-rejection, concurrency, Kafka propagation)
node scripts/test/smoke.mjs

# Load benchmark (naive baseline vs SyncPulse, measured)
node scripts/benchmark/benchmark.mjs 5 3 400 2 8

# Backend unit tests (vector clock + conflict engine)
#   Python (default):
docker run --rm -v "$PWD/backend-python":/app -w /app python:3.11-slim \
  sh -c "pip install -q -r requirements.txt pytest && pytest -q"
#   Java:
docker run --rm -v "$PWD/backend":/app -w /app maven:3.9-eclipse-temurin-21 mvn -q test
```

Benchmark methodology and interpretation: [`docs/benchmarking.md`](docs/benchmarking.md).

---

## 7. Failure handling

Redis / Cassandra / Kafka outages return structured `503`s (never silent failure); malformed
or negative positions and over-duration positions return `400`; unknown/duplicate requests are
handled idempotently. Kafka publishes use bounded timeouts and retries (no unsafe infinite
retries). See [`GlobalExceptionHandler`](backend/src/main/java/com/syncpulse/common/GlobalExceptionHandler.java).

---

## 8. CDC honesty

The default `LOCAL_ADAPTER` emits a Kafka event immediately after a Cassandra commit — clearly
labelled `cdcSource=LOCAL_ADAPTER`, **not** Debezium. The architectural contract
(`committed change → event → Kafka → consumer`) is identical to the real path. To run true
CDC, set `SYNCPULSE_CDC_MODE=debezium` and deploy the Debezium Cassandra connector — see
[`infrastructure/debezium`](infrastructure/debezium/README.md).

---

## 9. Configuration

All configuration is environment-driven; safe local defaults live in [`.env.example`](.env.example).
Key knobs: `DEBOUNCE_POSITION_THRESHOLD` (5s), `DEBOUNCE_TIME_THRESHOLD` (10s),
`SYNCPULSE_CDC_MODE` (local|debezium), `SYNCPULSE_SEED_ENABLED`.

---

## 10. Limitations & production evolution

This is a **polished prototype**, not a production deployment. Notably:

- Single-node Cassandra (RF 1) and single-broker Kafka for local reproducibility; production
  would use `NetworkTopologyStrategy` with RF 3 and `LOCAL_QUORUM`, and a replicated Kafka.
- No authentication/authorization (out of scope, per the project brief) — production needs
  real authn/z, rate limiting and multi-tenant isolation.
- The local CDC adapter stands in for a co-located Debezium Cassandra agent.
- Conflict resolution uses a domain-specific "highest-position-wins" policy; other domains
  might use CRDTs (discussed in the literature survey) or app-level reconciliation.

---

## 11. Repository layout

```
backend-python/ FastAPI backend (default) — debounce + vector-clock engine + CDC producer
consumer-python/ FastAPI companion service (default) — Kafka consumer
backend/        Java/Spring Boot backend (alternative, same API)
consumer/       Java/Spring Boot companion service (alternative)
frontend/       React + TypeScript + Vite dashboard (SyncPulse Control Center)
infrastructure/ Cassandra schema, Kafka/Redis/Debezium notes + connector config
scripts/        demo / benchmark / smoke-test (Node, no deps)
docs/           architecture · database-design · vector-clocks · benchmarking · api
docker-compose.yml   full local stack
```

Licensed under [MIT](LICENSE).
