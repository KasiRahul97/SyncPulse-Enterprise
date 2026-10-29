package com.syncpulse.consumer;

import java.util.Map;

/** A change event as received and time-stamped by the companion service. */
public record ConsumedEvent(
        long seq,
        String eventId,
        String eventType,
        String userId,
        String contentId,
        String deviceId,
        long positionSeconds,
        Map<String, Long> vectorClock,
        String decision,
        String cdcSource,
        String occurredAt,        // producer timestamp (ISO-8601)
        long receivedAtEpochMs,   // when the consumer received it
        long propagationLatencyMs // receivedAt - occurredAt
) {}
