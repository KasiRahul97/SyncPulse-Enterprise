package com.syncpulse.domain;

import java.time.Instant;

/** Authoritative, durable playhead state for a (user, content) pair. */
public record PlayheadState(
        String userId,
        String contentId,
        String deviceId,
        long positionSeconds,
        VectorClock vectorClock,
        String status,
        long revision,
        String lastRequestId,
        Instant lastUpdatedAt
) {
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_CONFLICTED = "CONFLICTED";
}
