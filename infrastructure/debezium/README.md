# Real CDC with Debezium (production path)

The default local stack uses the **`LocalDevelopmentCdcAdapter`**, which emits a Kafka
event immediately after a successful Cassandra commit. That adapter is honest about what
it is — every event it produces carries `cdcSource = LOCAL_ADAPTER`. It is **not** Debezium.

This directory documents how to switch to a **real Change Data Capture** pipeline where
a Debezium Cassandra connector tails Cassandra's CDC commit log and publishes to Kafka,
with **no involvement from the backend** (the backend's `DebeziumCdcAdapter` becomes a
no-op so events are never double-published).

```
Cassandra (cdc=true tables + commitlog CDC)
        │  commit log segments
        ▼
Debezium Cassandra connector (agent)
        │
        ▼
Kafka topic  syncpulse.playhead.changes
        │
        ▼
Companion consumer (unchanged)
```

## 1. Enable CDC in Cassandra

`cassandra.yaml` must set:

```yaml
cdc_enabled: true
```

and every table to be captured must be created `WITH cdc = true`. Re-create
`playhead_state` accordingly:

```sql
ALTER TABLE syncpulse.playhead_state WITH cdc = true;
```

## 2. Deploy the Debezium Cassandra connector

The Debezium Cassandra connector runs as an **agent alongside each Cassandra node**
(it reads local commit-log segments), unlike JDBC/relational Debezium connectors that run
inside Kafka Connect. See the Debezium Cassandra documentation for the matching
connector/Cassandra versions. A representative configuration is in
[`cassandra-connector.properties`](cassandra-connector.properties).

## 3. Point SyncPulse at Debezium mode

Set the environment variable and restart the backend:

```bash
SYNCPULSE_CDC_MODE=debezium docker compose up -d backend
```

In this mode:

- `DebeziumCdcAdapter` is activated (`@ConditionalOnProperty syncpulse.cdc.mode=debezium`)
  and its `onCommitted(...)` is intentionally a **no-op**.
- Debezium becomes the sole producer of `PLAYHEAD_UPDATED` events.
- The event `cdcSource` field will read `DEBEZIUM` for events shaped by the SyncPulse
  event schema (a Single Message Transform can map Debezium's envelope to the schema in
  `docs/architecture.md`).

## Why the local adapter exists

Debezium's Cassandra connector is version-sensitive and expects an agent co-located with
the Cassandra node — awkward to reproduce reliably inside a single-command
`docker compose up` demo. The `CdcEventPublisher` interface keeps the **architectural
contract identical** (`committed change → event → Kafka → consumer`) while letting the
demo run deterministically. This separation is deliberate and documented rather than
pretending a simplified emitter is Debezium.
