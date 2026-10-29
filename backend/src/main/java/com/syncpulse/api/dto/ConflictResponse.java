package com.syncpulse.api.dto;

import com.syncpulse.domain.ConflictRecord;

import java.time.Instant;
import java.util.Map;

public record ConflictResponse(
        String conflictId,
        Instant resolvedAt,
        String userId,
        String contentId,
        String incomingDeviceId,
        Map<String, Long> incomingVectorClock,
        Map<String, Long> storedVectorClock,
        String comparison,
        String decision,
        String reason,
        long incomingPosition,
        long authoritativePosition,
        String requestId
) {
    public static ConflictResponse from(ConflictRecord c) {
        return new ConflictResponse(
                c.conflictId().toString(), c.resolvedAt(), c.userId(), c.contentId(),
                c.incomingDeviceId(), c.incomingVectorClock().asMap(), c.storedVectorClock().asMap(),
                c.comparison().name(), c.decision().name(), c.reason(),
                c.incomingPosition(), c.authoritativePosition(), c.requestId());
    }
}
