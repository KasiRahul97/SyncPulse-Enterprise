# SyncPulse — API reference

Base path: `/api/v1`. Interactive docs (Swagger UI): `http://localhost:8080/swagger-ui.html`.
OpenAPI JSON: `/v3/api-docs`. Every request/response is JSON; a correlation id is echoed in the
`X-Request-Id` response header.

---

## POST `/playhead/update`

Ingest a playhead update through the full pipeline.

**Body**

| Field | Type | Required | Notes |
|---|---|---|---|
| `userId` | string | yes | |
| `contentId` | string | yes | |
| `deviceId` | string | yes | |
| `positionSeconds` | long ≥ 0 | yes | |
| `vectorClock` | map<string,long> | no | device-managed clock; omit to let the server manage it |
| `requestId` | string | no | idempotency key (recommended) |
| `force` | boolean | no | force past debounce (seek/pause/checkpoint) |
| `clientTimestamp` | long | no | epoch millis, observability only |
| `contentDurationSeconds` | long | no | if set, `positionSeconds` must not exceed it |

**Example — accepted**

```bash
curl -s -X POST http://localhost:8080/api/v1/playhead/update -H 'Content-Type: application/json' -d '{
  "userId":"user-001","contentId":"movie-inception","deviceId":"tv-001",
  "positionSeconds":420,"vectorClock":{"tv-001":12,"phone-001":4},
  "requestId":"11111111-1111-1111-1111-111111111111","force":true
}'
```

```json
{
  "decision": "ACCEPTED",
  "reason": "incoming vector clock is causally after authoritative state",
  "comparison": "AFTER",
  "debounceDecision": "FORCE_PERSIST",
  "persisted": true,
  "idempotent": false,
  "eventEmitted": true,
  "authoritativePosition": 420,
  "authoritativeVectorClock": { "tv-001": 12, "phone-001": 4 },
  "revision": 1,
  "requestId": "11111111-1111-1111-1111-111111111111",
  "processingMs": 7
}
```

**Example — rejected stale**

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

**Decisions:** `ACCEPTED_FIRST`, `ACCEPTED`, `REJECTED_STALE`, `IDEMPOTENT`,
`CONCURRENT_ACCEPTED`, `CONCURRENT_REJECTED`, `DEBOUNCED`.

---

## GET `/playhead/{userId}/{contentId}`

Authoritative state, or `404` if none yet.

```json
{
  "userId": "user-001", "contentId": "movie-inception", "deviceId": "tv-001",
  "positionSeconds": 420, "vectorClock": { "tv-001": 12, "phone-001": 4 },
  "status": "ACTIVE", "revision": 7, "lastUpdatedAt": "2026-09-02T16:08:40.123Z"
}
```

## POST `/devices/register`

```json
{ "userId": "user-001", "deviceId": "tv-001", "deviceType": "TV" }
```

`deviceType` ∈ `PHONE | TABLET | TV | WEB`.

## GET `/users/{userId}/devices`

List of registered devices for a user.

## GET `/conflicts/{userId}?limit=50`

Most-recent conflict decisions (newest first), each with both clocks, comparison, decision,
reason, positions and request id.

## GET `/metrics`

Live pipeline counters (all real): pings, debounced, writes, accepted, rejectedStale,
concurrentConflicts, kafkaEmitted, `writeReductionPercentage`, per-second rates, and latency
percentiles.

## GET `/events?since=<seq>`

`{ cursor, events[] }` — pipeline activity with `seq > since` (poll with the last cursor for a
live feed).

## GET `/status`

Infrastructure component status (`redis`, `cassandra`, `kafka`, `cdc`, `cdcAdapter`) and overall
`HEALTHY | DEGRADED`.

## GET `/health`

Liveness probe: `{ "status": "UP" }`.

## POST `/benchmark/run`

Body (all optional, with defaults): `users`, `devicesPerUser`, `pingsPerDevice`,
`positionStepSeconds`, `concurrency`. Returns the measured naive-vs-SyncPulse report.

## POST `/admin/reset?clearState=true&reseed=true`

Reset counters + live feed; optionally clear demo state and reseed devices.

## GET `/admin/demo-info`

Deterministic demo identifiers (user, content, devices).

---

## Companion consumer service (port 8081)

| Method | Path | Purpose |
|---|---|---|
| GET | `/consumer/health` | Liveness |
| GET | `/consumer/events?since=<seq>` | Consumed events (with propagation latency) |
| GET | `/consumer/metrics` | Total consumed + propagation latency percentiles |

---

## Error shape

```json
{
  "timestamp": "2026-09-02T16:09:00Z",
  "status": 400,
  "error": "VALIDATION_ERROR",
  "message": "positionSeconds must be >= 0",
  "requestId": "…"
}
```

`400` validation · `404` no state · `503` infrastructure unavailable (`REDIS_UNAVAILABLE`,
`CASSANDRA_UNAVAILABLE`, `KAFKA_UNAVAILABLE`) · `500` unexpected.
