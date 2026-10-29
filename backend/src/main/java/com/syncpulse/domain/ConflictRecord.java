package com.syncpulse.domain;

import java.time.Instant;
import java.util.UUID;

/** An entry in the conflict audit trail. */
public record ConflictRecord(
        String userId,
        Instant resolvedAt,
        UUID conflictId,
        String contentId,
        String incomingDeviceId,
        VectorClock incomingVectorClock,
        VectorClock storedVectorClock,
        Comparison comparison,
        Decision decision,
        String reason,
        long incomingPosition,
        long authoritativePosition,
        String requestId
) {}
