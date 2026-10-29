"""Domain enums, decision semantics, and API models (camelCase to match the contract)."""
from __future__ import annotations

from typing import Dict, Optional

from pydantic import BaseModel, Field

DEVICE_TYPES = {"PHONE", "TABLET", "TV", "WEB"}


class Decision:
    ACCEPTED_FIRST = "ACCEPTED_FIRST"
    ACCEPTED = "ACCEPTED"
    REJECTED_STALE = "REJECTED_STALE"
    IDEMPOTENT = "IDEMPOTENT"
    CONCURRENT_ACCEPTED = "CONCURRENT_ACCEPTED"
    CONCURRENT_REJECTED = "CONCURRENT_REJECTED"
    DEBOUNCED = "DEBOUNCED"

    # (durable -> writes to Cassandra, conflict -> recorded in conflict_log)
    _FLAGS = {
        ACCEPTED_FIRST: (True, False),
        ACCEPTED: (True, False),
        REJECTED_STALE: (False, True),
        IDEMPOTENT: (False, False),
        CONCURRENT_ACCEPTED: (True, True),
        CONCURRENT_REJECTED: (True, True),
        DEBOUNCED: (False, False),
    }

    @classmethod
    def is_durable(cls, d: str) -> bool:
        return cls._FLAGS.get(d, (False, False))[0]

    @classmethod
    def is_conflict(cls, d: str) -> bool:
        return cls._FLAGS.get(d, (False, False))[1]


STATUS_ACTIVE = "ACTIVE"

TYPE_PLAYHEAD_UPDATED = "PLAYHEAD_UPDATED"
EVENT_VERSION = 1


class UpdateRequest(BaseModel):
    userId: str = Field(min_length=1)
    contentId: str = Field(min_length=1)
    deviceId: str = Field(min_length=1)
    positionSeconds: int = Field(ge=0)
    vectorClock: Optional[Dict[str, int]] = None
    clientTimestamp: Optional[int] = None
    requestId: Optional[str] = None
    force: Optional[bool] = None
    contentDurationSeconds: Optional[int] = None


class DeviceRegistrationRequest(BaseModel):
    userId: str = Field(min_length=1)
    deviceId: str = Field(min_length=1)
    deviceType: str


class BenchmarkRequest(BaseModel):
    users: Optional[int] = None
    devicesPerUser: Optional[int] = None
    pingsPerDevice: Optional[int] = None
    positionStepSeconds: Optional[int] = None
    concurrency: Optional[int] = None
