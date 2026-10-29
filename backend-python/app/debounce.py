"""Atomic Redis debounce — the decision is made inside Redis by the Lua script."""
from __future__ import annotations

import logging
import os
import time

from .config import config

log = logging.getLogger("syncpulse.debounce")

DEBOUNCED = "DEBOUNCED"


def _load_lua() -> str:
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(here, "debounce.lua"), "r", encoding="utf-8") as f:
        return f.read()


class DebounceService:
    def __init__(self, redis_client) -> None:
        self.redis = redis_client
        self.script = redis_client.register_script(_load_lua())

    def evaluate(self, user_id: str, content_id: str, device_id: str,
                 position: int, force: bool) -> dict:
        key = f"sp:debounce:{user_id}:{content_id}:{device_id}"
        now = int(time.time() * 1000)
        raw = self.script(keys=[key], args=[
            position, now,
            config.DEBOUNCE_POSITION_THRESHOLD, config.DEBOUNCE_TIME_THRESHOLD,
            1 if force else 0, config.DEBOUNCE_STATE_TTL,
        ])
        if not raw or len(raw) < 6:
            log.warning("Unexpected debounce result: %s", raw)
            return {"decision": "FORCE_PERSIST", "reason": "unexpected result; failing open",
                    "lastPos": 0, "lastTs": now, "delta": 0, "elapsedMs": 0}
        return {
            "decision": _s(raw[0]),
            "reason": _s(raw[1]),
            "lastPos": int(raw[2]),
            "lastTs": int(raw[3]),
            "delta": int(raw[4]),
            "elapsedMs": int(raw[5]),
        }

    def clear(self, user_id: str, content_id: str, device_id: str) -> None:
        self.redis.delete(f"sp:debounce:{user_id}:{content_id}:{device_id}")


def should_persist(decision: str) -> bool:
    return decision != DEBOUNCED


def _s(v) -> str:
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
