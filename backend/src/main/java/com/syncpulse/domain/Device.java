package com.syncpulse.domain;

import java.time.Instant;

/** A device registered to a user. */
public record Device(
        String userId,
        String deviceId,
        DeviceType deviceType,
        Instant registeredAt,
        Instant lastSeenAt
) {}
