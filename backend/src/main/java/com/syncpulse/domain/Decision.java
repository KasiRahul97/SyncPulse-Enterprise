package com.syncpulse.domain;

/**
 * Final decision produced by the pipeline for a single playhead update.
 *
 * <p>The {@code durable} flag indicates whether the decision results in a write to
 * Cassandra; {@code conflict} indicates whether it is recorded in the conflict audit log.</p>
 */
public enum Decision {
    /** First write for a stream — accepted, persisted. */
    ACCEPTED_FIRST(true, false),
    /** Incoming clock is causally AFTER the stored clock — accepted, persisted. */
    ACCEPTED(true, false),
    /** Incoming clock is causally BEFORE the stored clock — rejected, not persisted. */
    REJECTED_STALE(false, true),
    /** Incoming clock EQUALS stored clock (or duplicate requestId) — no state transition. */
    IDEMPOTENT(false, false),
    /** Concurrent clocks; resolution policy selected the INCOMING update — persisted. */
    CONCURRENT_ACCEPTED(true, true),
    /**
     * Concurrent clocks; resolution policy kept the STORED position, but the merged
     * (element-wise max) clock IS persisted so the causal history converges and the
     * losing update cannot re-trigger the same conflict.
     */
    CONCURRENT_REJECTED(true, true),
    /** Debounced at the Redis layer — never reached the conflict engine. */
    DEBOUNCED(false, false);

    private final boolean durable;
    private final boolean conflict;

    Decision(boolean durable, boolean conflict) {
        this.durable = durable;
        this.conflict = conflict;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isConflict() {
        return conflict;
    }
}
