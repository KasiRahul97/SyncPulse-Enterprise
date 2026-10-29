# Kafka — event streaming

Single-node **KRaft** Kafka (no ZooKeeper) transports committed change events from the
CDC layer to downstream consumers.

- Topic: `syncpulse.playhead.changes` (created by the `kafka-init` job: 3 partitions, RF 1)
- Producer: the backend's `KafkaEventProducer` (keyed by `userId:contentId` so a stream's
  events stay ordered within a partition)
- Consumer: the **Companion Service** (`consumer/`), group `syncpulse-companion`

Event schema (versioned, extensible) — see [`docs/architecture.md`](../../docs/architecture.md#kafka-event-schema):

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

`cdcSource` is honest provenance: `LOCAL_ADAPTER` (backend emits post-commit) or `DEBEZIUM`
(real CDC connector). See [`infrastructure/debezium`](../debezium/README.md).
