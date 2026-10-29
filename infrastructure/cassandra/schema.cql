-- ===========================================================================
-- SyncPulse Enterprise — Cassandra schema
--
-- Design principle: model tables around QUERY PATTERNS, not entities.
-- Every read the application performs is a single-partition lookup.
-- See docs/database-design.md for the full rationale.
--
-- Dev/demo uses SimpleStrategy RF=1 (single node). For a real cluster use:
--   CREATE KEYSPACE syncpulse WITH replication =
--     {'class':'NetworkTopologyStrategy','datacenter1':3};
-- ===========================================================================

CREATE KEYSPACE IF NOT EXISTS syncpulse
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE syncpulse;

-- ---------------------------------------------------------------------------
-- playhead_state — authoritative current watch position per (user, content).
--
-- Partition key : (user_id, content_id)  -> one small partition per stream,
--                 every read/write is a single-partition operation.
-- No clustering : we keep ONLY the authoritative row (last-writer-wins by
--                 causal order), not a history, so writes are in-place upserts.
-- vector_clock  : map<text,bigint> of deviceId -> logical counter.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS playhead_state (
    user_id          text,
    content_id       text,
    device_id        text,            -- device that produced the authoritative value
    position_seconds bigint,
    vector_clock     map<text, bigint>,
    status           text,            -- ACTIVE | STALE | CONFLICTED
    revision         bigint,          -- monotonically increasing accepted-write counter
    last_request_id  text,            -- idempotency marker of the last accepted write
    last_updated_at  timestamp,
    PRIMARY KEY ((user_id, content_id))
);

-- ---------------------------------------------------------------------------
-- device_registry — devices known for a user.
--
-- Partition key   : user_id           -> "all devices for a user" = one partition.
-- Clustering key  : device_id         -> uniqueness + ordered listing.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS device_registry (
    user_id       text,
    device_id     text,
    device_type   text,               -- PHONE | TABLET | TV | WEB
    registered_at timestamp,
    last_seen_at  timestamp,
    PRIMARY KEY ((user_id), device_id)
);

-- ---------------------------------------------------------------------------
-- conflict_log — audit trail of every conflict decision.
--
-- Partition key   : user_id
-- Clustering keys : (resolved_at DESC, conflict_id)
--                   -> "most recent conflicts for a user" reads the head of
--                      the partition with no in-query sorting.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS conflict_log (
    user_id                text,
    resolved_at            timestamp,
    conflict_id            uuid,
    content_id             text,
    incoming_device_id     text,
    incoming_vector_clock  map<text, bigint>,
    stored_vector_clock    map<text, bigint>,
    comparison             text,       -- BEFORE | AFTER | EQUAL | CONCURRENT
    decision               text,       -- ACCEPTED | REJECTED_STALE | CONCURRENT_* | IDEMPOTENT
    reason                 text,
    incoming_position      bigint,
    authoritative_position bigint,
    request_id             text,
    PRIMARY KEY ((user_id), resolved_at, conflict_id)
) WITH CLUSTERING ORDER BY (resolved_at DESC, conflict_id ASC);

-- ---------------------------------------------------------------------------
-- cdc_audit — record of change events emitted toward the streaming layer.
--
-- Partition key   : event_date ('yyyy-MM-dd') -> bounded, time-bucketed partitions.
-- Clustering keys : (emitted_at DESC, event_id) -> recent events first.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cdc_audit (
    event_date   text,
    emitted_at   timestamp,
    event_id     uuid,
    source_table text,
    change_type  text,                -- INSERT | UPDATE | DELETE
    event_type   text,                -- PLAYHEAD_UPDATED
    user_id      text,
    content_id   text,
    device_id    text,
    payload      text,                -- serialized JSON event
    delivered    boolean,
    PRIMARY KEY ((event_date), emitted_at, event_id)
) WITH CLUSTERING ORDER BY (emitted_at DESC, event_id ASC);
