"""Request-level idempotency via Redis SET NX."""
from __future__ import annotations

from typing import Optional

from .config import config


class IdempotencyService:
    def __init__(self, redis_client) -> None:
        self.redis = redis_client
        self.ttl = config.IDEMPOTENCY_TTL

    def is_duplicate(self, request_id: Optional[str]) -> bool:
        """True if already seen (duplicate); False if newly claimed."""
        if not request_id:
            return False
        claimed = self.redis.set(self._key(request_id), "PENDING", nx=True, ex=self.ttl)
        return not bool(claimed)

    def complete(self, request_id: Optional[str], decision: str) -> None:
        if not request_id:
            return
        self.redis.set(self._key(request_id), decision, ex=self.ttl)

    def prior_decision(self, request_id: Optional[str]) -> Optional[str]:
        if not request_id:
            return None
        v = self.redis.get(self._key(request_id))
        if v is None:
            return None
        return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)

    @staticmethod
    def _key(request_id: str) -> str:
        return f"sp:idem:{request_id}"
