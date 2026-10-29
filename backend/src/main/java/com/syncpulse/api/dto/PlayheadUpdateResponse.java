package com.syncpulse.api.dto;

import java.util.Map;

/** Structured result of processing a playhead update through the full pipeline. */
public record PlayheadUpdateResponse(
        String decision,               // ACCEPTED | REJECTED_STALE | DEBOUNCED | ...
        String reason,
        String comparison,             // BEFORE | AFTER | EQUAL | CONCURRENT | null (debounced)
        String debounceDecision,       // PERSIST | DEBOUNCED | FORCE_PERSIST
        String debounceReason,
        boolean persisted,             // wrote to Cassandra
        boolean idempotent,            // duplicate requestId / equal clock
        boolean eventEmitted,          // event handed to the CDC/Kafka pipeline
        long authoritativePosition,
        Map<String, Long> authoritativeVectorClock,
        long revision,
        String requestId,
        long processingMs
) {}
