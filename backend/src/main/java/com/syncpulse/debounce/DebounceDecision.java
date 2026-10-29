package com.syncpulse.debounce;

/** Outcome of the atomic Redis debounce gate. */
public enum DebounceDecision {
    /** Significant enough to proceed to the conflict engine / durable store. */
    PERSIST,
    /** Dropped at Redis — not significant enough to reach Cassandra. */
    DEBOUNCED,
    /** Caller-forced persistence (first ping, seek, pause, checkpoint). */
    FORCE_PERSIST;

    public boolean shouldPersist() {
        return this != DEBOUNCED;
    }
}
