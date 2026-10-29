package com.syncpulse.api.dto;

import com.syncpulse.domain.PlayheadState;

import java.time.Instant;
import java.util.Map;

public record PlayheadStateResponse(
        String userId,
        String contentId,
        String deviceId,
        long positionSeconds,
        Map<String, Long> vectorClock,
        String status,
        long revision,
        String lastRequestId,
        Instant lastUpdatedAt
) {
    public static PlayheadStateResponse from(PlayheadState s) {
        return new PlayheadStateResponse(s.userId(), s.contentId(), s.deviceId(), s.positionSeconds(),
                s.vectorClock().asMap(), s.status(), s.revision(), s.lastRequestId(), s.lastUpdatedAt());
    }
}
