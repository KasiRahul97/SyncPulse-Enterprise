package com.syncpulse.domain;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * An immutable distributed <b>vector clock</b>: a map of {@code deviceId -> logical counter}.
 *
 * <p>Vector clocks capture <i>causality</i> ("this update happened after that one",
 * or "these two are concurrent") without relying on wall-clock time, which cannot be
 * trusted across independent devices. This is the correctness backbone of SyncPulse.</p>
 *
 * <p>A missing component is treated as {@code 0} — a device that has never been heard
 * from is at logical time zero.</p>
 */
public final class VectorClock {

    private final Map<String, Long> counters;

    private VectorClock(Map<String, Long> counters) {
        // Store a normalized, immutable copy (drop non-positive entries, sorted keys).
        Map<String, Long> normalized = new TreeMap<>();
        for (Map.Entry<String, Long> e : counters.entrySet()) {
            if (e.getKey() != null && e.getValue() != null && e.getValue() > 0) {
                normalized.put(e.getKey(), e.getValue());
            }
        }
        this.counters = Collections.unmodifiableMap(normalized);
    }

    public static VectorClock empty() {
        return new VectorClock(Map.of());
    }

    public static VectorClock of(Map<String, Long> counters) {
        return new VectorClock(counters == null ? Map.of() : counters);
    }

    /** Logical time of a single device (0 if absent). */
    public long get(String deviceId) {
        return counters.getOrDefault(deviceId, 0L);
    }

    /** Returns a new clock with {@code deviceId}'s counter incremented by one. */
    public VectorClock increment(String deviceId) {
        if (deviceId == null || deviceId.isBlank()) {
            throw new IllegalArgumentException("deviceId must not be blank");
        }
        Map<String, Long> next = new TreeMap<>(counters);
        next.merge(deviceId, 1L, Long::sum);
        return new VectorClock(next);
    }

    /**
     * Element-wise maximum merge — the least upper bound of two clocks in the
     * causal lattice. The merged clock dominates (is AFTER-or-EQUAL to) both inputs,
     * which guarantees convergence after conflict resolution.
     */
    public VectorClock merge(VectorClock other) {
        Map<String, Long> next = new TreeMap<>(counters);
        for (String device : other.counters.keySet()) {
            next.merge(device, other.get(device), Math::max);
        }
        return new VectorClock(next);
    }

    /**
     * Compares this clock (A) against {@code other} (B) under the happened-before order.
     *
     * <p>A is BEFORE B iff every component of A ≤ the corresponding component of B and
     * at least one is strictly smaller. AFTER is the symmetric case. EQUAL when all
     * components match. Otherwise the clocks are CONCURRENT.</p>
     */
    public Comparison compareTo(VectorClock other) {
        boolean aLessSomewhere = false;   // some component of A strictly < B
        boolean aGreaterSomewhere = false; // some component of A strictly > B

        Set<String> devices = new TreeSet<>();
        devices.addAll(this.counters.keySet());
        devices.addAll(other.counters.keySet());

        for (String device : devices) {
            long a = this.get(device);
            long b = other.get(device);
            if (a < b) {
                aLessSomewhere = true;
            } else if (a > b) {
                aGreaterSomewhere = true;
            }
        }

        if (!aLessSomewhere && !aGreaterSomewhere) {
            return Comparison.EQUAL;
        }
        if (aLessSomewhere && !aGreaterSomewhere) {
            return Comparison.BEFORE;
        }
        if (aGreaterSomewhere && !aLessSomewhere) {
            return Comparison.AFTER;
        }
        return Comparison.CONCURRENT;
    }

    /** Immutable view of the underlying counters (ordered by device id). */
    public Map<String, Long> asMap() {
        return new LinkedHashMap<>(counters);
    }

    public boolean isEmpty() {
        return counters.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VectorClock other)) return false;
        return counters.equals(other.counters);
    }

    @Override
    public int hashCode() {
        return counters.hashCode();
    }

    @Override
    public String toString() {
        return counters.toString();
    }
}
