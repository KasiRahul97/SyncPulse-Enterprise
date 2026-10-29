"""Runtime metrics + live pipeline feed. Every value is real (nothing hardcoded)."""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import List, Optional

from .domain import Decision


class MetricsService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._c = {k: 0 for k in (
            "totalPings", "debounced", "persistCandidates", "accepted", "rejectedStale",
            "concurrentConflicts", "idempotentHits", "cassandraWrites", "conflictsLogged",
            "kafkaEmitted", "kafkaFailed",
        )}
        self._lat = deque(maxlen=4096)
        self._prev = {"pings": 0, "writes": 0, "events": 0}
        self._rates = {"pings": 0.0, "writes": 0.0, "events": 0.0}

    def _inc(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._c[key] += n

    def record_ping(self): self._inc("totalPings")
    def record_debounced(self): self._inc("debounced")
    def record_persist_candidate(self): self._inc("persistCandidates")
    def record_idempotent(self): self._inc("idempotentHits")
    def record_cassandra_write(self): self._inc("cassandraWrites")
    def record_conflict_logged(self): self._inc("conflictsLogged")
    def inc_kafka_emitted(self): self._inc("kafkaEmitted")
    def inc_kafka_failed(self): self._inc("kafkaFailed")

    def record_latency(self, ms: int) -> None:
        with self._lock:
            self._lat.append(ms)

    def record_decision(self, d: str) -> None:
        with self._lock:
            if d in (Decision.ACCEPTED, Decision.ACCEPTED_FIRST):
                self._c["accepted"] += 1
            elif d == Decision.CONCURRENT_ACCEPTED:
                self._c["accepted"] += 1
                self._c["concurrentConflicts"] += 1
            elif d == Decision.CONCURRENT_REJECTED:
                self._c["concurrentConflicts"] += 1
            elif d == Decision.REJECTED_STALE:
                self._c["rejectedStale"] += 1
            elif d == Decision.IDEMPOTENT:
                self._c["idempotentHits"] += 1

    def sample_rates(self) -> None:
        with self._lock:
            p, w, e = self._c["totalPings"], self._c["cassandraWrites"], self._c["kafkaEmitted"]
            self._rates = {
                "pings": p - self._prev["pings"],
                "writes": w - self._prev["writes"],
                "events": e - self._prev["events"],
            }
            self._prev = {"pings": p, "writes": w, "events": e}

    def write_reduction(self) -> float:
        with self._lock:
            p = self._c["totalPings"]
            if p == 0:
                return 0.0
            return 100.0 * (1.0 - self._c["cassandraWrites"] / p)

    def _percentiles(self) -> dict:
        data = sorted(self._lat)
        n = len(data)
        if n == 0:
            return {"count": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
        return {
            "count": n,
            "avg": round(sum(data) / n, 2),
            "p50": data[int(0.50 * (n - 1))],
            "p95": data[int(0.95 * (n - 1))],
            "p99": data[int(0.99 * (n - 1))],
            "max": data[-1],
        }

    def snapshot(self) -> dict:
        with self._lock:
            c = dict(self._c)
            rates = dict(self._rates)
            lat = self._percentiles()
        p = c["totalPings"]
        wr = 0.0 if p == 0 else round(100.0 * (1.0 - c["cassandraWrites"] / p), 2)
        return {
            **c,
            "writeReductionPercentage": wr,
            "pingsPerSecond": rates["pings"],
            "writesPerSecond": rates["writes"],
            "eventsPerSecond": rates["events"],
            "latencyMs": lat,
        }

    def reset(self) -> None:
        with self._lock:
            for k in self._c:
                self._c[k] = 0
            self._lat.clear()
            self._prev = {"pings": 0, "writes": 0, "events": 0}
            self._rates = {"pings": 0.0, "writes": 0.0, "events": 0.0}


class LiveEventLog:
    def __init__(self, capacity: int = 500) -> None:
        self._lock = threading.Lock()
        self._buf: deque = deque(maxlen=capacity)
        self._seq = 0

    def add(self, category: str, user_id: Optional[str], content_id: Optional[str],
            device_id: Optional[str], decision: Optional[str], comparison: Optional[str],
            position: Optional[int], message: str) -> None:
        with self._lock:
            self._seq += 1
            self._buf.append({
                "seq": self._seq, "timestamp": int(time.time() * 1000), "category": category,
                "userId": user_id, "contentId": content_id, "deviceId": device_id,
                "decision": decision, "comparison": comparison,
                "positionSeconds": position, "message": message,
            })

    def since(self, after_seq: int) -> List[dict]:
        with self._lock:
            return [e for e in self._buf if e["seq"] > after_seq]

    def current_seq(self) -> int:
        with self._lock:
            return self._seq

    def clear(self) -> None:
        with self._lock:
            self._buf.clear()
