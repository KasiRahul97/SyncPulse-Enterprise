"""Unit tests for the pure logic: vector clocks + conflict engine.

    pip install pytest && pytest        (from backend-python/)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import vector_clock as vc  # noqa: E402
from app import conflict as ce  # noqa: E402
from app.domain import Decision  # noqa: E402


# ---- vector clocks ----
def test_empty_equal():
    assert vc.compare({}, {}) == vc.EQUAL


def test_equal():
    assert vc.compare({"phone": 5, "tv": 8}, {"phone": 5, "tv": 8}) == vc.EQUAL


def test_before_after():
    assert vc.compare({"phone": 4, "tv": 8}, {"phone": 4, "tv": 12}) == vc.BEFORE
    assert vc.compare({"phone": 4, "tv": 12}, {"phone": 4, "tv": 8}) == vc.AFTER


def test_empty_before_nonempty():
    assert vc.compare({}, {"tv": 1}) == vc.BEFORE


def test_concurrent():
    assert vc.compare({"tv": 5, "phone": 2}, {"tv": 4, "phone": 3}) == vc.CONCURRENT


def test_missing_is_zero():
    assert vc.compare({"phone": 1}, {"phone": 1, "tv": 3}) == vc.BEFORE


def test_increment():
    b = vc.increment({"phone": 1, "tv": 2}, "phone")
    assert b["phone"] == 2 and b["tv"] == 2


def test_merge_dominates():
    m = vc.merge({"tv": 5, "phone": 2}, {"tv": 4, "phone": 3})
    assert m == {"tv": 5, "phone": 3}
    assert vc.compare(m, {"tv": 5, "phone": 2}) == vc.AFTER
    assert vc.compare(m, {"tv": 4, "phone": 3}) == vc.AFTER


# ---- conflict engine ----
def _stored(pos, clock):
    return {"userId": "u", "contentId": "c", "deviceId": "tv-001",
            "positionSeconds": pos, "vectorClock": clock, "status": "ACTIVE",
            "revision": 3, "lastRequestId": None, "lastUpdatedAt": None}


def test_first_update():
    r = ce.resolve(None, "u", "c", "phone-001", 42, {"phone-001": 1})
    assert r["decision"] == Decision.ACCEPTED_FIRST
    assert r["newState"]["positionSeconds"] == 42


def test_causally_newer():
    r = ce.resolve(_stored(120, {"phone": 4, "tv": 8}), "u", "c", "tv-001", 200,
                   {"phone": 4, "tv": 12})
    assert r["decision"] == Decision.ACCEPTED and r["comparison"] == vc.AFTER


def test_stale_rejected():
    r = ce.resolve(_stored(420, {"phone": 4, "tv": 12}), "u", "c", "phone-001", 120,
                   {"phone": 4, "tv": 8})
    assert r["decision"] == Decision.REJECTED_STALE
    assert r["authoritativePosition"] == 420
    assert not Decision.is_durable(r["decision"])


def test_equal_idempotent():
    r = ce.resolve(_stored(300, {"phone": 4, "tv": 12}), "u", "c", "tv-001", 300,
                   {"phone": 4, "tv": 12})
    assert r["decision"] == Decision.IDEMPOTENT


def test_concurrent_incoming_wins():
    r = ce.resolve(_stored(200, {"tv": 5, "phone": 2}), "u", "c", "phone-001", 350,
                   {"tv": 4, "phone": 3})
    assert r["decision"] == Decision.CONCURRENT_ACCEPTED
    assert r["newState"]["positionSeconds"] == 350
    assert r["newState"]["vectorClock"] == {"tv": 5, "phone": 3}


def test_concurrent_stored_wins():
    r = ce.resolve(_stored(500, {"tv": 5, "phone": 2}), "u", "c", "phone-001", 350,
                   {"tv": 4, "phone": 3})
    assert r["decision"] == Decision.CONCURRENT_REJECTED
    assert Decision.is_durable(r["decision"])  # merged clock persisted
    assert r["newState"]["positionSeconds"] == 500
