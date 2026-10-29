# SyncPulse — Architecture

## 1. Layered pipeline

Every playhead update flows through a fixed set of layers with clean interfaces, so any
infrastructure piece can be replaced without touching the others.

```mermaid
flowchart TD
    C[Client / device simulators] -->|POST /playhead/update| API[Ingestion API]
    API --> IDEM[Idempotency guard\nRedis SET NX]
    IDEM --> DEB[Redis debounce\natomic Lua script]
    DEB -->|significant only| VC[Vector-clock conflict engine]
    DEB -.->|debounced| DROP[(dropped before Cassandra)]
    VC -->|durable decision| CAS[(Cassandra\nauthoritative state)]
    VC -->|conflict| LOG[(conflict_log)]
    CAS --> CDC[CdcEventPublisher]
    CDC -->|LOCAL_ADAPTER or DEBEZIUM| K[[Kafka\nsyncpulse.playhead.changes]]
    K --> CON[Companion consumer\nsecond screen]
    API --> OBS[Metrics + live feed]
    CON --> OBS
    OBS --> DASH[Control Center dashboard]
```

## 2. Component responsibilities

| Layer | Responsibility | Key class |
|---|---|---|
| Ingestion API | Validate, correlate (request id), route | `PlayheadController`, `PlayheadService` |
| Idempotency | Claim `requestId` atomically; dedupe retries | `IdempotencyService` |
| Debounce | Drop redundant high-frequency pings in Redis | `DebounceService` + `debounce.lua` |
| Conflict engine | Causal ordering; accept/reject/resolve | `ConflictResolver`, `VectorClock` |
| Durable store | Authoritative state, device registry, audit | `*Repository` (DataStax `CqlSession`) |
| CDC | Turn a commit into an event | `CdcEventPublisher` + adapters |
| Streaming | Transport events to consumers | `KafkaEventProducer`, Kafka |
| Consumer | React downstream without polling | `CompanionListener` |
| Observability | Metrics, feeds, status | `MetricsService`, `LiveEventLog`, `SystemStatusService` |

## 3. Request lifecycle (normal accept)

```mermaid
sequenceDiagram
    participant D as Device
    participant A as API/PlayheadService
    participant R as Redis
    participant E as ConflictResolver
    participant C as Cassandra
    participant K as Kafka
    participant S as Companion
    D->>A: update(pos, vectorClock, requestId)
    A->>R: isDuplicate(requestId)? (SET NX)
    A->>R: EVALSHA debounce.lua
    R-->>A: PERSIST
    A->>C: read authoritative state
    A->>E: resolve(incoming vs stored)
    E-->>A: ACCEPTED (+ merged clock)
    A->>C: upsert playhead_state
    A->>K: onCommitted(PlayheadEvent)
    K->>S: PLAYHEAD_UPDATED
    S->>S: record + measure propagation latency
    A-->>D: {decision, authoritativePosition, authoritativeVectorClock}
```

## 4. Data flow, in words

1. A device sends a playhead ping.
2. The `requestId` is claimed in Redis; a duplicate short-circuits as `IDEMPOTENT`.
3. The **atomic Lua debounce** decides `PERSIST` / `DEBOUNCED` / `FORCE_PERSIST`. Debounced
   pings never touch Cassandra — this is the write-reduction mechanism.
4. For significant pings, the **conflict engine** reads the stored clock and compares causally.
   Stale updates are rejected; concurrent updates are detected and deterministically resolved.
5. Durable decisions upsert `playhead_state` (single-partition write). Conflicts are appended to
   `conflict_log`.
6. The `CdcEventPublisher` turns the commit into a versioned event on Kafka.
7. The companion consumer receives it and measures commit→consume propagation latency.
8. The dashboard polls metrics/feeds/state/conflicts/consumer for a live view.

## 5. Failure handling

| Failure | Behaviour |
|---|---|
| Redis down | `503 REDIS_UNAVAILABLE`; debounce fails **open** (persist) on malformed script results rather than dropping data |
| Cassandra down | `503 CASSANDRA_UNAVAILABLE`; driver auto-reconnects; schema init retries |
| Kafka down | Publish uses bounded `delivery.timeout.ms`/`max.block.ms`; failures counted (`kafkaFailed`), never infinite-retried |
| Malformed / negative / over-duration position | `400 VALIDATION_ERROR` |
| Duplicate request | Idempotent no-op |
| Consumer failure | At-least-once redelivery via Kafka offsets |

No exception is silently swallowed; all map to structured JSON via `GlobalExceptionHandler`.

## 6. Kafka event schema

Versioned and extensible (`PlayheadEvent`):

```json
{
  "eventId": "uuid",
  "eventType": "PLAYHEAD_UPDATED",
  "version": 1,
  "occurredAt": "2026-09-02T16:08:40.123Z",
  "userId": "user-001",
  "contentId": "movie-inception",
  "deviceId": "tv-001",
  "positionSeconds": 420,
  "vectorClock": { "tv-001": 12, "phone-001": 4 },
  "decision": "ACCEPTED",
  "revision": 7,
  "cdcSource": "LOCAL_ADAPTER"
}
```

## 7. CDC adapters

```mermaid
flowchart LR
    subgraph local[SYNCPULSE_CDC_MODE=local]
      A1[Cassandra commit] --> A2[LocalDevelopmentCdcAdapter] --> A3[[Kafka]]
    end
    subgraph deb[SYNCPULSE_CDC_MODE=debezium]
      B1[Cassandra CDC commit log] --> B2[Debezium Cassandra connector] --> B3[[Kafka]]
      B4[DebeziumCdcAdapter = no-op]
    end
```

The interface (`CdcEventPublisher`) is identical; only the commit-detection mechanism differs,
and events are honestly labelled by `cdcSource`.
