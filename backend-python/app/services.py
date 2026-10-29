"""Orchestration: the playhead pipeline plus device/benchmark/admin/status helpers."""
from __future__ import annotations

import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import List, Optional

from confluent_kafka.admin import AdminClient

from . import conflict as conflict_engine
from . import vector_clock as vc
from .config import config
from .domain import (Decision, EVENT_VERSION, TYPE_PLAYHEAD_UPDATED, DEVICE_TYPES)

log = logging.getLogger("syncpulse.services")


class ValidationError(Exception):
    pass


# ---- deterministic demo data ----
class DemoData:
    USER = "user-001"
    CONTENT = "movie-inception"
    CONTENT_DURATION = 8880
    DEVICES = [
        ("phone-001", "PHONE"), ("tv-001", "TV"),
        ("web-001", "WEB"), ("tablet-001", "TABLET"),
    ]


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PlayheadService:
    def __init__(self, store, debounce, idem, metrics, live, cdc) -> None:
        self.store = store
        self.debounce = debounce
        self.idem = idem
        self.metrics = metrics
        self.live = live
        self.cdc = cdc

    def update(self, req) -> dict:
        start = time.perf_counter()
        self.metrics.record_ping()

        if req.contentDurationSeconds is not None and req.positionSeconds > req.contentDurationSeconds:
            raise ValidationError(
                f"positionSeconds ({req.positionSeconds}) exceeds "
                f"contentDurationSeconds ({req.contentDurationSeconds})")

        # idempotency
        if self.idem.is_duplicate(req.requestId):
            self.metrics.record_idempotent()
            self.live.add("DECISION", req.userId, req.contentId, req.deviceId,
                          "IDEMPOTENT", None, req.positionSeconds,
                          f"duplicate requestId {req.requestId} ignored")
            cur = self.store.find_state(req.userId, req.contentId)
            pos = cur["positionSeconds"] if cur else req.positionSeconds
            clock = cur["vectorClock"] if cur else {}
            rev = cur["revision"] if cur else 0
            return self._finish(start, self._resp(
                "IDEMPOTENT", "duplicate requestId; no new state transition", None,
                "SKIPPED", "duplicate request", False, True, False, pos, clock, rev, req.requestId))

        self.live.add("PING", req.userId, req.contentId, req.deviceId,
                      None, None, req.positionSeconds, "ping received")

        force = bool(req.force)
        db = self.debounce.evaluate(req.userId, req.contentId, req.deviceId, req.positionSeconds, force)

        if db["decision"] == "DEBOUNCED":
            self.metrics.record_debounced()
            self.metrics.record_decision(Decision.DEBOUNCED)
            self.idem.complete(req.requestId, "DEBOUNCED")
            self.live.add("DEBOUNCE", req.userId, req.contentId, req.deviceId,
                          "DEBOUNCED", None, req.positionSeconds,
                          f"debounced ({db['reason']}); dpos={db['delta']}s elapsed={db['elapsedMs']}ms")
            return self._finish(start, self._resp(
                "DEBOUNCED", db["reason"], None, db["decision"], db["reason"],
                False, False, False, req.positionSeconds, {}, 0, req.requestId))

        self.metrics.record_persist_candidate()
        self.live.add("DEBOUNCE", req.userId, req.contentId, req.deviceId,
                      db["decision"], None, req.positionSeconds, f"passed debounce ({db['reason']})")

        current = self.store.find_state(req.userId, req.contentId)
        incoming_clock = self._effective_clock(req, current)
        res = conflict_engine.resolve(current, req.userId, req.contentId, req.deviceId,
                                      req.positionSeconds, incoming_clock)

        persisted = False
        event_emitted = False
        authoritative_pos = res["authoritativePosition"]
        new_state = res["newState"]
        authoritative_clock = (new_state["vectorClock"] if new_state
                               else (current["vectorClock"] if current else {}))
        revision = (new_state["revision"] if new_state
                    else (current["revision"] if current else 0))

        if Decision.is_durable(res["decision"]) and new_state is not None:
            new_state["lastRequestId"] = req.requestId
            new_state["lastUpdatedAt"] = _now()
            self.store.upsert_state(new_state)
            self.metrics.record_cassandra_write()
            persisted = True

            dt = _now()
            event = {
                "eventId": str(uuid.uuid4()), "eventType": TYPE_PLAYHEAD_UPDATED,
                "version": EVENT_VERSION, "occurredAt": dt.isoformat(),
                "userId": new_state["userId"], "contentId": new_state["contentId"],
                "deviceId": new_state["deviceId"], "positionSeconds": new_state["positionSeconds"],
                "vectorClock": new_state["vectorClock"], "decision": res["decision"],
                "revision": new_state["revision"], "cdcSource": self.cdc.adapter_name,
                "_occurredAt_dt": dt,
            }
            self.cdc.on_committed(event)
            event_emitted = True

        if Decision.is_conflict(res["decision"]):
            self.store.record_conflict({
                "userId": req.userId, "resolvedAt": _now(), "conflictId": uuid.uuid4(),
                "contentId": req.contentId, "incomingDeviceId": req.deviceId,
                "incomingVectorClock": res["incomingClock"], "storedVectorClock": res["storedClock"],
                "comparison": res["comparison"], "decision": res["decision"], "reason": res["reason"],
                "incomingPosition": res["incomingPosition"],
                "authoritativePosition": res["authoritativePosition"], "requestId": req.requestId,
            })
            self.metrics.record_conflict_logged()

        self.metrics.record_decision(res["decision"])
        self.idem.complete(req.requestId, res["decision"])
        self.live.add("DECISION", req.userId, req.contentId, req.deviceId,
                      res["decision"], res["comparison"], req.positionSeconds, res["reason"])

        return self._finish(start, self._resp(
            res["decision"], res["reason"], res["comparison"], db["decision"], db["reason"],
            persisted, False, event_emitted, authoritative_pos, authoritative_clock,
            revision, req.requestId))

    def get_state(self, user_id: str, content_id: str) -> Optional[dict]:
        return self.store.find_state(user_id, content_id)

    def _effective_clock(self, req, current) -> dict:
        if req.vectorClock:
            return vc.normalize(req.vectorClock)
        base = current["vectorClock"] if current else {}
        return vc.increment(base, req.deviceId)

    @staticmethod
    def _resp(decision, reason, comparison, db_decision, db_reason, persisted, idempotent,
              event_emitted, authoritative_pos, authoritative_clock, revision, request_id) -> dict:
        return {
            "decision": decision, "reason": reason, "comparison": comparison,
            "debounceDecision": db_decision, "debounceReason": db_reason,
            "persisted": persisted, "idempotent": idempotent, "eventEmitted": event_emitted,
            "authoritativePosition": authoritative_pos,
            "authoritativeVectorClock": authoritative_clock, "revision": revision,
            "requestId": request_id, "processingMs": 0,
        }

    def _finish(self, start: float, resp: dict) -> dict:
        ms = round((time.perf_counter() - start) * 1000)
        self.metrics.record_latency(ms)
        resp["processingMs"] = ms
        return resp


class DeviceService:
    def __init__(self, store, live) -> None:
        self.store = store
        self.live = live

    def register(self, user_id: str, device_id: str, dtype: str) -> dict:
        if dtype not in DEVICE_TYPES:
            raise ValidationError(f"deviceType must be one of {sorted(DEVICE_TYPES)}")
        now = _now()
        existing = next((d for d in self.store.list_devices(user_id)
                         if d["deviceId"] == device_id), None)
        registered_at = existing["registeredAt"] if existing else now
        self.store.register_device(user_id, device_id, dtype, registered_at, now)
        self.live.add("SYSTEM", user_id, None, device_id, None, None, None,
                      f"device registered ({dtype})")
        return {"userId": user_id, "deviceId": device_id, "deviceType": dtype,
                "registeredAt": _iso(registered_at), "lastSeenAt": _iso(now)}

    def list_for_user(self, user_id: str) -> List[dict]:
        return [{
            "userId": d["userId"], "deviceId": d["deviceId"], "deviceType": d["deviceType"],
            "registeredAt": _iso(d["registeredAt"]), "lastSeenAt": _iso(d["lastSeenAt"]),
        } for d in self.store.list_devices(user_id)]

    def heartbeat(self, user_id: str, device_id: str) -> None:
        self.store.touch_device(user_id, device_id, _now())


class BenchmarkService:
    def __init__(self, playhead: PlayheadService) -> None:
        self.playhead = playhead

    def run(self, cfg) -> dict:
        users = _def(cfg.users, 5)
        devices = _def(cfg.devicesPerUser, 2)
        pings = _def(cfg.pingsPerDevice, 200)
        step = _def(cfg.positionStepSeconds, 2)
        concurrency = _def(cfg.concurrency, 8)
        total = users * devices * pings

        stats = {"persisted": 0, "debounced": 0, "accepted": 0, "events": 0}
        latencies: List[int] = []
        lock = __import__("threading").Lock()

        from .domain import UpdateRequest

        def worker(u, d):
            device_id = f"bench-dev-{u}-{d}"
            content_id = f"bench-content-{u}-{d}"
            user_id = f"bench-user-{u}"
            local_lat = []
            local = {"persisted": 0, "debounced": 0, "accepted": 0, "events": 0}
            for i in range(1, pings + 1):
                req = UpdateRequest(userId=user_id, contentId=content_id, deviceId=device_id,
                                    positionSeconds=i * step, requestId=f"bench-{device_id}-{i}",
                                    clientTimestamp=int(time.time() * 1000))
                r = self.playhead.update(req)
                local_lat.append(r["processingMs"])
                if r["persisted"]:
                    local["persisted"] += 1
                if r["decision"] == "DEBOUNCED":
                    local["debounced"] += 1
                if r["decision"].startswith("ACCEPTED"):
                    local["accepted"] += 1
                if r["eventEmitted"]:
                    local["events"] += 1
            with lock:
                for k in local:
                    stats[k] += local[k]
                latencies.extend(local_lat)

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futs = [pool.submit(worker, u, d) for u in range(users) for d in range(devices)]
            for f in futs:
                f.result()
        elapsed_ms = round((time.perf_counter() - start) * 1000)

        writes = stats["persisted"]
        wr = 0.0 if total == 0 else round(100.0 * (1.0 - writes / total), 2)
        throughput = 0.0 if elapsed_ms == 0 else round(total * 1000.0 / elapsed_ms, 2)
        latencies.sort()
        n = len(latencies)
        lat = {"count": n}
        if n:
            lat.update({
                "avg": round(sum(latencies) / n, 2),
                "p50": latencies[int(0.50 * (n - 1))],
                "p95": latencies[int(0.95 * (n - 1))],
                "p99": latencies[int(0.99 * (n - 1))],
                "max": latencies[-1],
            })
        return {
            "config": {"users": users, "devicesPerUser": devices, "pingsPerDevice": pings,
                       "positionStepSeconds": step, "concurrency": concurrency},
            "naiveBaseline": {"model": "Every ping -> Cassandra", "cassandraWrites": total},
            "syncpulse": {
                "model": "Every ping -> Redis debounce; only significant updates -> Cassandra",
                "totalRequests": total, "cassandraWrites": writes,
                "debouncedRequests": stats["debounced"], "acceptedUpdates": stats["accepted"],
                "kafkaEventsEmitted": stats["events"]},
            "results": {"writeReductionPercentage": wr, "elapsedMs": elapsed_ms,
                        "throughputRequestsPerSecond": throughput, "latencyMs": lat},
            "measured": True,
            "note": "All values measured against the running stack.",
        }


class AdminService:
    def __init__(self, store, redis, metrics, live, seed_fn) -> None:
        self.store = store
        self.redis = redis
        self.metrics = metrics
        self.live = live
        self.seed_fn = seed_fn

    def reset(self, clear_state: bool, reseed: bool) -> dict:
        self.metrics.reset()
        self.live.clear()
        cleared = self._clear("sp:debounce:*") + self._clear("sp:idem:*")
        if clear_state:
            self.store.delete_state(DemoData.USER, DemoData.CONTENT)
        if reseed:
            self.seed_fn()
        return {"reset": True, "redisKeysCleared": cleared,
                "demoStateCleared": clear_state, "reseeded": reseed}

    def _clear(self, pattern: str) -> int:
        keys = list(self.redis.scan_iter(match=pattern, count=500))
        if not keys:
            return 0
        return int(self.redis.delete(*keys))


class SystemStatus:
    def __init__(self, store, redis) -> None:
        self.store = store
        self.redis = redis
        self._admin = None

    def status(self) -> dict:
        redis_up = self._redis()
        cassandra_up = self._cassandra()
        kafka_up = self._kafka()
        components = {
            "api": True, "redis": redis_up, "cassandra": cassandra_up,
            "kafka": kafka_up, "cdc": kafka_up,
            "cdcAdapter": "LOCAL_ADAPTER" if config.CDC_MODE.lower() == "local" else "DEBEZIUM",
        }
        healthy = redis_up and cassandra_up and kafka_up
        return {"status": "HEALTHY" if healthy else "DEGRADED", "components": components}

    def _redis(self) -> bool:
        try:
            return bool(self.redis.ping())
        except Exception:  # noqa: BLE001
            return False

    def _cassandra(self) -> bool:
        try:
            return self.store.session.execute("SELECT release_version FROM system.local").one() is not None
        except Exception:  # noqa: BLE001
            return False

    def _kafka(self) -> bool:
        try:
            if self._admin is None:
                self._admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
            md = self._admin.list_topics(timeout=3)
            return md is not None
        except Exception:  # noqa: BLE001
            return False


def make_seed_fn(device_service: DeviceService):
    def seed():
        if not config.SEED_ENABLED:
            log.info("Seed disabled.")
            return
        for device_id, dtype in DemoData.DEVICES:
            try:
                device_service.register(DemoData.USER, device_id, dtype)
            except Exception as e:  # noqa: BLE001
                log.warning("seed device %s failed: %s", device_id, e)
        log.info("Seeded %d demo devices for %s", len(DemoData.DEVICES), DemoData.USER)
    return seed


def _def(v, default: int) -> int:
    return default if v is None else max(1, int(v))


def _iso(dt) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return str(dt)
