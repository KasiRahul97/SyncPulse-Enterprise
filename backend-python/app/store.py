"""Cassandra access: self-sufficient schema init + single-partition repositories."""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy

from .config import config

log = logging.getLogger("syncpulse.store")


def _day(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d")


class CassandraStore:
    def __init__(self) -> None:
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        self.ks = config.CASSANDRA_KEYSPACE
        self._stmts: Dict[str, object] = {}

    # ---- lifecycle ----
    def connect(self) -> None:
        last = None
        for attempt in range(1, 21):
            try:
                self.cluster = Cluster(
                    [config.CASSANDRA_HOST],
                    port=config.CASSANDRA_PORT,
                    load_balancing_policy=DCAwareRoundRobinPolicy(
                        local_dc=config.CASSANDRA_DATACENTER),
                    protocol_version=5,
                )
                self.session = self.cluster.connect()
                self.session.default_consistency_level = ConsistencyLevel.LOCAL_ONE
                log.info("Connected to Cassandra at %s:%s", config.CASSANDRA_HOST, config.CASSANDRA_PORT)
                return
            except Exception as e:  # noqa: BLE001
                last = e
                log.warning("Cassandra connect attempt %s/20 failed: %s", attempt, e)
                time.sleep(3)
        raise RuntimeError(f"Could not connect to Cassandra: {last}")

    def init_schema(self) -> None:
        rf = config.CASSANDRA_REPLICATION_FACTOR
        ks = self.ks
        ddl = [
            f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = "
            f"{{'class':'SimpleStrategy','replication_factor':{rf}}}",
            f"CREATE TABLE IF NOT EXISTS {ks}.playhead_state ("
            "user_id text, content_id text, device_id text, position_seconds bigint,"
            "vector_clock map<text,bigint>, status text, revision bigint,"
            "last_request_id text, last_updated_at timestamp,"
            "PRIMARY KEY ((user_id, content_id)))",
            f"CREATE TABLE IF NOT EXISTS {ks}.device_registry ("
            "user_id text, device_id text, device_type text,"
            "registered_at timestamp, last_seen_at timestamp,"
            "PRIMARY KEY ((user_id), device_id))",
            f"CREATE TABLE IF NOT EXISTS {ks}.conflict_log ("
            "user_id text, resolved_at timestamp, conflict_id uuid, content_id text,"
            "incoming_device_id text, incoming_vector_clock map<text,bigint>,"
            "stored_vector_clock map<text,bigint>, comparison text, decision text, reason text,"
            "incoming_position bigint, authoritative_position bigint, request_id text,"
            "PRIMARY KEY ((user_id), resolved_at, conflict_id)) "
            "WITH CLUSTERING ORDER BY (resolved_at DESC, conflict_id ASC)",
            f"CREATE TABLE IF NOT EXISTS {ks}.cdc_audit ("
            "event_date text, emitted_at timestamp, event_id uuid, source_table text,"
            "change_type text, event_type text, user_id text, content_id text, device_id text,"
            "payload text, delivered boolean,"
            "PRIMARY KEY ((event_date), emitted_at, event_id)) "
            "WITH CLUSTERING ORDER BY (emitted_at DESC, event_id ASC)",
        ]
        for stmt in ddl:
            for attempt in range(1, 11):
                try:
                    self.session.execute(stmt)
                    break
                except Exception as e:  # noqa: BLE001
                    log.warning("Schema stmt failed (%s/10): %s", attempt, e)
                    time.sleep(2)
            else:
                raise RuntimeError(f"Failed to apply schema: {stmt}")
        self._prepare()
        log.info("Cassandra schema ready.")

    def _prepare(self) -> None:
        ks = self.ks
        s = self.session
        self._stmts = {
            "select_state": s.prepare(
                f"SELECT * FROM {ks}.playhead_state WHERE user_id=? AND content_id=?"),
            "upsert_state": s.prepare(
                f"UPDATE {ks}.playhead_state SET device_id=?, position_seconds=?, vector_clock=?, "
                f"status=?, revision=?, last_request_id=?, last_updated_at=? "
                f"WHERE user_id=? AND content_id=?"),
            "delete_state": s.prepare(
                f"DELETE FROM {ks}.playhead_state WHERE user_id=? AND content_id=?"),
            "insert_device": s.prepare(
                f"INSERT INTO {ks}.device_registry (user_id, device_id, device_type, registered_at, "
                f"last_seen_at) VALUES (?,?,?,?,?)"),
            "list_devices": s.prepare(f"SELECT * FROM {ks}.device_registry WHERE user_id=?"),
            "touch_device": s.prepare(
                f"UPDATE {ks}.device_registry SET last_seen_at=? WHERE user_id=? AND device_id=?"),
            "insert_conflict": s.prepare(
                f"INSERT INTO {ks}.conflict_log (user_id, resolved_at, conflict_id, content_id, "
                f"incoming_device_id, incoming_vector_clock, stored_vector_clock, comparison, decision, "
                f"reason, incoming_position, authoritative_position, request_id) "
                f"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"),
            "recent_conflicts": s.prepare(
                f"SELECT * FROM {ks}.conflict_log WHERE user_id=? LIMIT ?"),
            "insert_cdc": s.prepare(
                f"INSERT INTO {ks}.cdc_audit (event_date, emitted_at, event_id, source_table, "
                f"change_type, event_type, user_id, content_id, device_id, payload, delivered) "
                f"VALUES (?,?,?,?,?,?,?,?,?,?,?)"),
        }

    # ---- playhead_state ----
    def find_state(self, user_id: str, content_id: str) -> Optional[dict]:
        row = self.session.execute(self._stmts["select_state"], (user_id, content_id)).one()
        if row is None:
            return None
        return {
            "userId": row.user_id,
            "contentId": row.content_id,
            "deviceId": row.device_id,
            "positionSeconds": row.position_seconds,
            "vectorClock": dict(row.vector_clock or {}),
            "status": row.status,
            "revision": row.revision,
            "lastRequestId": row.last_request_id,
            "lastUpdatedAt": row.last_updated_at,
        }

    def upsert_state(self, st: dict) -> None:
        self.session.execute(self._stmts["upsert_state"], (
            st["deviceId"], st["positionSeconds"], st["vectorClock"], st["status"],
            st["revision"], st.get("lastRequestId"), st["lastUpdatedAt"],
            st["userId"], st["contentId"],
        ))

    def delete_state(self, user_id: str, content_id: str) -> None:
        self.session.execute(self._stmts["delete_state"], (user_id, content_id))

    # ---- device_registry ----
    def register_device(self, user_id: str, device_id: str, dtype: str,
                        registered_at: datetime, last_seen_at: datetime) -> None:
        self.session.execute(self._stmts["insert_device"],
                             (user_id, device_id, dtype, registered_at, last_seen_at))

    def list_devices(self, user_id: str) -> List[dict]:
        out = []
        for row in self.session.execute(self._stmts["list_devices"], (user_id,)):
            out.append({
                "userId": row.user_id,
                "deviceId": row.device_id,
                "deviceType": row.device_type,
                "registeredAt": row.registered_at,
                "lastSeenAt": row.last_seen_at,
            })
        return out

    def touch_device(self, user_id: str, device_id: str, when: datetime) -> None:
        self.session.execute(self._stmts["touch_device"], (when, user_id, device_id))

    # ---- conflict_log ----
    def record_conflict(self, c: dict) -> None:
        self.session.execute(self._stmts["insert_conflict"], (
            c["userId"], c["resolvedAt"], c["conflictId"], c["contentId"], c["incomingDeviceId"],
            c["incomingVectorClock"], c["storedVectorClock"], c["comparison"], c["decision"],
            c["reason"], c["incomingPosition"], c["authoritativePosition"], c.get("requestId"),
        ))

    def recent_conflicts(self, user_id: str, limit: int) -> List[dict]:
        out = []
        for row in self.session.execute(self._stmts["recent_conflicts"], (user_id, limit)):
            out.append({
                "conflictId": str(row.conflict_id),
                "resolvedAt": row.resolved_at,
                "userId": row.user_id,
                "contentId": row.content_id,
                "incomingDeviceId": row.incoming_device_id,
                "incomingVectorClock": dict(row.incoming_vector_clock or {}),
                "storedVectorClock": dict(row.stored_vector_clock or {}),
                "comparison": row.comparison,
                "decision": row.decision,
                "reason": row.reason,
                "incomingPosition": row.incoming_position,
                "authoritativePosition": row.authoritative_position,
                "requestId": row.request_id,
            })
        return out

    # ---- cdc_audit ----
    def record_cdc(self, event: dict, payload: str, delivered: bool) -> None:
        now = event["_occurredAt_dt"]
        self.session.execute(self._stmts["insert_cdc"], (
            _day(now), now, uuid.UUID(event["eventId"]), "playhead_state", "UPDATE",
            event["eventType"], event["userId"], event["contentId"], event["deviceId"],
            payload, delivered,
        ))
