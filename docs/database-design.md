# SyncPulse — Cassandra data model

Cassandra tables are modelled around **query patterns**, not entities. The guiding rule of
Cassandra data modelling — *one table per query, every read a single-partition lookup* — is
applied throughout. DDL: [`infrastructure/cassandra/schema.cql`](../infrastructure/cassandra/schema.cql)
(also created idempotently at backend startup by `SchemaInitializer`).

Keyspace (dev/demo): `SimpleStrategy`, RF 1. Production: `NetworkTopologyStrategy`, RF 3.

---

## 1. `playhead_state` — authoritative current position

```sql
CREATE TABLE playhead_state (
    user_id text, content_id text, device_id text,
    position_seconds bigint, vector_clock map<text,bigint>,
    status text, revision bigint, last_request_id text, last_updated_at timestamp,
    PRIMARY KEY ((user_id, content_id))
);
```

- **Partition key `(user_id, content_id)`** — the unit of access is "the state of this user on
  this content". Every read and write targets exactly one partition, tiny and hot-cache friendly.
- **No clustering column** — we keep only the *authoritative* row (last-writer-wins by causal
  order), not a time series. Writes are in-place upserts, so partitions never grow unbounded.
- **`vector_clock map<text,bigint>`** — the causal state travels with the row; no joins.
- **Access pattern:** `SELECT * FROM playhead_state WHERE user_id=? AND content_id=?` and the
  matching `UPDATE`. Both single-partition.
- **Consistency:** dev uses `LOCAL_ONE` (single node). On a real cluster, `LOCAL_QUORUM` on
  read and write gives read-your-writes within a datacenter.

## 2. `device_registry` — devices per user

```sql
CREATE TABLE device_registry (
    user_id text, device_id text, device_type text,
    registered_at timestamp, last_seen_at timestamp,
    PRIMARY KEY ((user_id), device_id)
);
```

- **Partition key `user_id`**, **clustering `device_id`** — "list all devices for a user" reads a
  single partition, already ordered by device id. Registration is an upsert.
- **Access pattern:** `SELECT * FROM device_registry WHERE user_id=?`.

## 3. `conflict_log` — conflict audit trail

```sql
CREATE TABLE conflict_log (
    user_id text, resolved_at timestamp, conflict_id uuid,
    content_id text, incoming_device_id text,
    incoming_vector_clock map<text,bigint>, stored_vector_clock map<text,bigint>,
    comparison text, decision text, reason text,
    incoming_position bigint, authoritative_position bigint, request_id text,
    PRIMARY KEY ((user_id), resolved_at, conflict_id)
) WITH CLUSTERING ORDER BY (resolved_at DESC, conflict_id ASC);
```

- **Partition key `user_id`**, **clustering `(resolved_at DESC, conflict_id)`** — "most recent
  conflicts for a user" reads the head of the partition with **no in-query sort** and a natural
  `LIMIT`. `conflict_id` (UUID) disambiguates same-timestamp entries.
- **Access pattern:** `SELECT * FROM conflict_log WHERE user_id=? LIMIT ?`.
- **Denormalization:** everything needed to render/audit a conflict is stored on the row
  (both clocks, positions, decision, reason) — no secondary lookups.

## 4. `cdc_audit` — emitted change events

```sql
CREATE TABLE cdc_audit (
    event_date text, emitted_at timestamp, event_id uuid,
    source_table text, change_type text, event_type text,
    user_id text, content_id text, device_id text, payload text, delivered boolean,
    PRIMARY KEY ((event_date), emitted_at, event_id)
) WITH CLUSTERING ORDER BY (emitted_at DESC, event_id ASC);
```

- **Partition key `event_date` (`yyyy-MM-dd`)** — time-buckets keep partitions bounded and make
  "recent events" a single-partition scan. **Clustering `(emitted_at DESC, event_id)`** yields
  newest-first ordering.
- **Why bucket by day?** A monotonically growing single partition ("all events ever") is a
  Cassandra anti-pattern; date bucketing bounds partition size and spreads load.

---

## 5. Why this is Cassandra-friendly

- **No multi-partition reads, no `ALLOW FILTERING`, no secondary indexes** on the hot path.
- **Write-optimised:** upserts and appends only; no read-modify-write races in the DB (causal
  correctness is enforced in the conflict engine before the write).
- **Bounded partitions:** authoritative state is a single row; audit tables are time/entity
  bucketed.
- **Scales horizontally** by `user_id` / `(user_id, content_id)` — natural, high-cardinality
  partition keys spread evenly across the ring.

## 6. Scalability notes

- `(user_id, content_id)` is high-cardinality → even token distribution, no hot partitions for a
  normal workload.
- The debounce layer removes ~70–90% of writes *before* Cassandra, so the durable write rate is a
  small fraction of ingest — the cluster scales for the *meaningful* write rate, not the raw ping
  rate.
- For very hot content, add a bucketing component to the partition key if a single
  `(user, content)` ever became a hotspot (not expected for per-user playhead state).
