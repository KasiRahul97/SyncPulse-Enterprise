"""Distributed vector-clock algebra — the correctness backbone of SyncPulse.

A vector clock is a dict of ``deviceId -> logical counter``. A missing device is
implicitly 0. Comparison uses the causal (happened-before) partial order, NOT
wall-clock time.
"""
from __future__ import annotations

from typing import Dict

BEFORE = "BEFORE"
AFTER = "AFTER"
EQUAL = "EQUAL"
CONCURRENT = "CONCURRENT"

Clock = Dict[str, int]


def normalize(clock: Clock | None) -> Clock:
    if not clock:
        return {}
    return {k: int(v) for k, v in clock.items() if v and int(v) > 0}


def increment(clock: Clock, device_id: str) -> Clock:
    if not device_id or not device_id.strip():
        raise ValueError("device_id must not be blank")
    nxt = dict(clock)
    nxt[device_id] = nxt.get(device_id, 0) + 1
    return nxt


def merge(a: Clock, b: Clock) -> Clock:
    """Element-wise maximum (least upper bound) — dominates both inputs."""
    out = dict(a)
    for k, v in b.items():
        out[k] = max(out.get(k, 0), v)
    return out


def compare(a: Clock, b: Clock) -> str:
    a_less = False   # some component of A strictly < B
    a_greater = False  # some component of A strictly > B
    for device in set(a) | set(b):
        av, bv = a.get(device, 0), b.get(device, 0)
        if av < bv:
            a_less = True
        elif av > bv:
            a_greater = True
    if not a_less and not a_greater:
        return EQUAL
    if a_less and not a_greater:
        return BEFORE
    if a_greater and not a_less:
        return AFTER
    return CONCURRENT
