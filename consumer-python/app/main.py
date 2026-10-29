"""SyncPulse Companion Service (FastAPI) — downstream Kafka consumer.

Represents a second-screen app: reacts to committed playhead changes in near real
time and measures commit -> consume propagation latency. Never polls Cassandra.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Consumer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("companion")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "syncpulse.playhead.changes")
GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "syncpulse-companion")
ORIGINS = [o.strip() for o in os.getenv(
    "SYNCPULSE_CORS_ORIGINS", "http://localhost:3000,http://localhost:5173").split(",") if o.strip()]


class Store:
    def __init__(self, capacity: int = 500, lat_window: int = 512) -> None:
        self._lock = threading.Lock()
        self._buf: deque = deque(maxlen=capacity)
        self._lat: deque = deque(maxlen=lat_window)
        self._seq = 0
        self._total = 0
        self._lat_max = 0

    def add(self, ev: dict) -> None:
        with self._lock:
            self._seq += 1
            ev["seq"] = self._seq
            self._buf.append(ev)
            self._total += 1
            lat = max(0, ev["propagationLatencyMs"])
            self._lat.append(lat)
            self._lat_max = max(self._lat_max, lat)

    def since(self, after: int) -> list:
        with self._lock:
            return [e for e in self._buf if e["seq"] > after]

    def cursor(self) -> int:
        with self._lock:
            return self._seq

    def metrics(self) -> dict:
        with self._lock:
            data = sorted(self._lat)
            total = self._total
            mx = self._lat_max
        n = len(data)
        lat = {"count": n}
        if n:
            lat.update({
                "avg": round(sum(data) / n, 2),
                "p50": data[int(0.50 * (n - 1))],
                "p95": data[int(0.95 * (n - 1))],
                "p99": data[int(0.99 * (n - 1))],
                "max": mx,
            })
        return {"totalConsumed": total, "propagationLatencyMs": lat}


store = Store()
_running = True


def _parse_occurred(s: str | None) -> int | None:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        return None


def _consume_loop():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])
    log.info("Companion subscribed to %s (group %s)", TOPIC, GROUP)
    while _running:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            n = json.loads(msg.value().decode())
            received = int(time.time() * 1000)
            occurred_ms = _parse_occurred(n.get("occurredAt"))
            latency = (received - occurred_ms) if occurred_ms is not None else 0
            ev = {
                "eventId": n.get("eventId"), "eventType": n.get("eventType"),
                "userId": n.get("userId"), "contentId": n.get("contentId"),
                "deviceId": n.get("deviceId"), "positionSeconds": n.get("positionSeconds", 0),
                "vectorClock": n.get("vectorClock", {}), "decision": n.get("decision"),
                "cdcSource": n.get("cdcSource"), "occurredAt": n.get("occurredAt"),
                "receivedAtEpochMs": received, "propagationLatencyMs": latency,
            }
            store.add(ev)
            log.info("Consumed %s user=%s device=%s pos=%ss decision=%s latency=%sms",
                     ev["eventType"], ev["userId"], ev["deviceId"], ev["positionSeconds"],
                     ev["decision"], ev["propagationLatencyMs"])
        except Exception as e:  # noqa: BLE001
            log.error("Failed to process event: %s", e)
    c.close()


app = FastAPI(title="SyncPulse Companion", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=ORIGINS,
                   allow_methods=["GET", "OPTIONS"], allow_headers=["*"])


@app.on_event("startup")
def _startup():
    threading.Thread(target=_consume_loop, daemon=True).start()


@app.get("/consumer/health")
def health():
    return {"status": "UP", "service": "syncpulse-companion"}


@app.get("/consumer/events")
def events(since: int = 0):
    return {"cursor": store.cursor(), "events": store.since(since)}


@app.get("/consumer/metrics")
def metrics():
    return store.metrics()
