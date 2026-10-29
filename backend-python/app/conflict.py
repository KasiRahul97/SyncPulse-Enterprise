"""Causal conflict engine — decides accept/reject/resolve using vector clocks."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from . import vector_clock as vc
from .domain import Decision, STATUS_ACTIVE


def _now() -> datetime:
    return datetime.now(timezone.utc)


def resolve(current: Optional[dict], user_id: str, content_id: str, device_id: str,
            incoming_position: int, incoming_clock: dict) -> dict:
    """Returns a resolution dict; ``newState`` is set when the decision is durable."""
    # Case 1 — first update.
    if current is None:
        new_state = {
            "userId": user_id, "contentId": content_id, "deviceId": device_id,
            "positionSeconds": incoming_position, "vectorClock": incoming_clock,
            "status": STATUS_ACTIVE, "revision": 1, "lastRequestId": None, "lastUpdatedAt": _now(),
        }
        return _res(Decision.ACCEPTED_FIRST, vc.AFTER,
                    "first update for stream; no prior authoritative state",
                    incoming_clock, {}, incoming_position, incoming_position, new_state)

    stored_clock = current["vectorClock"]
    cmp = vc.compare(incoming_clock, stored_clock)

    if cmp == vc.AFTER:
        merged = vc.merge(stored_clock, incoming_clock)
        new_state = {
            "userId": user_id, "contentId": content_id, "deviceId": device_id,
            "positionSeconds": incoming_position, "vectorClock": merged,
            "status": STATUS_ACTIVE, "revision": current["revision"] + 1,
            "lastRequestId": None, "lastUpdatedAt": _now(),
        }
        return _res(Decision.ACCEPTED, cmp,
                    "incoming vector clock is causally after authoritative state",
                    incoming_clock, stored_clock, incoming_position, incoming_position, new_state)

    if cmp == vc.BEFORE:
        return _res(Decision.REJECTED_STALE, cmp,
                    "incoming vector clock is causally behind authoritative state; "
                    "rejected to prevent regression",
                    incoming_clock, stored_clock, incoming_position,
                    current["positionSeconds"], None)

    if cmp == vc.EQUAL:
        return _res(Decision.IDEMPOTENT, cmp,
                    "incoming vector clock equals authoritative state; no state transition",
                    incoming_clock, stored_clock, incoming_position,
                    current["positionSeconds"], None)

    # CONCURRENT — deterministic policy: highest position wins; merge clocks to converge.
    merged = vc.merge(stored_clock, incoming_clock)
    revision = current["revision"] + 1
    if incoming_position > current["positionSeconds"]:
        new_state = {
            "userId": user_id, "contentId": content_id, "deviceId": device_id,
            "positionSeconds": incoming_position, "vectorClock": merged,
            "status": STATUS_ACTIVE, "revision": revision, "lastRequestId": None,
            "lastUpdatedAt": _now(),
        }
        return _res(Decision.CONCURRENT_ACCEPTED, vc.CONCURRENT,
                    f"concurrent update; highest-position-wins selected INCOMING "
                    f"({incoming_position}s > {current['positionSeconds']}s); clocks merged",
                    incoming_clock, stored_clock, incoming_position, incoming_position, new_state)

    new_state = {
        "userId": user_id, "contentId": content_id, "deviceId": current["deviceId"],
        "positionSeconds": current["positionSeconds"], "vectorClock": merged,
        "status": STATUS_ACTIVE, "revision": revision, "lastRequestId": None,
        "lastUpdatedAt": _now(),
    }
    return _res(Decision.CONCURRENT_REJECTED, vc.CONCURRENT,
                f"concurrent update; highest-position-wins kept STORED "
                f"({current['positionSeconds']}s >= {incoming_position}s); clocks merged",
                incoming_clock, stored_clock, incoming_position,
                current["positionSeconds"], new_state)


def _res(decision, comparison, reason, incoming_clock, stored_clock,
         incoming_position, authoritative_position, new_state) -> dict:
    return {
        "decision": decision, "comparison": comparison, "reason": reason,
        "incomingClock": incoming_clock, "storedClock": stored_clock,
        "incomingPosition": incoming_position, "authoritativePosition": authoritative_position,
        "newState": new_state,
    }
