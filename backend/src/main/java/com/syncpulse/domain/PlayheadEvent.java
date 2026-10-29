package com.syncpulse.domain;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Versioned change event published to Kafka after a committed state change.
 * The schema is intentionally extensible — add fields with new {@code version}s.
 */
public record PlayheadEvent(
        UUID eventId,
        String eventType,      // PLAYHEAD_UPDATED
        int version,
        Instant occurredAt,
        String userId,
        String contentId,
        String deviceId,
        long positionSeconds,
        Map<String, Long> vectorClock,
        String decision,
        long revision,
        String cdcSource       // LOCAL_ADAPTER | DEBEZIUM  (honest provenance)
) {
    public static final String TYPE_PLAYHEAD_UPDATED = "PLAYHEAD_UPDATED";
    public static final int CURRENT_VERSION = 1;
}
