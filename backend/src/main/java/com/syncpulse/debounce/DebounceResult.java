package com.syncpulse.debounce;

/** Structured result returned by the Redis Lua debounce script. */
public record DebounceResult(
        DebounceDecision decision,
        String reason,
        long lastPersistedPosition,
        long lastPersistedTs,
        long positionDelta,
        long elapsedMs
) {}
