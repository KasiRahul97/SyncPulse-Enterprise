package com.syncpulse.api.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;

/**
 * A playhead position update from a (simulated) device.
 *
 * <p>{@code vectorClock} is optional: when supplied (the device manages its own clock,
 * as the simulators do) it is used verbatim for causal comparison. When omitted, the
 * server manages the clock by incrementing the device's component over the stored clock
 * — convenient for load tests where every ping is intentionally causally-later.</p>
 */
public record PlayheadUpdateRequest(
        @NotBlank String userId,
        @NotBlank String contentId,
        @NotBlank String deviceId,
        @Min(value = 0, message = "positionSeconds must be >= 0") long positionSeconds,
        Map<String, Long> vectorClock,
        Long clientTimestamp,      // epoch millis, observability only
        String requestId,          // idempotency key (optional but recommended)
        Boolean force,             // force persistence (seek/pause/checkpoint)
        Long contentDurationSeconds // optional; if set, position must not exceed it
) {}
