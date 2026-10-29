package com.syncpulse.vectorclock;

import com.syncpulse.domain.Comparison;
import com.syncpulse.domain.VectorClock;
import org.springframework.stereotype.Service;

/**
 * Injectable facade over {@link VectorClock} operations. The math lives in the
 * immutable {@link VectorClock} value type; this service exists for dependency
 * injection and to give a single, testable seam for causal-ordering logic.
 */
@Service
public class VectorClockService {

    public Comparison compare(VectorClock a, VectorClock b) {
        return a.compareTo(b);
    }

    public VectorClock merge(VectorClock a, VectorClock b) {
        return a.merge(b);
    }

    public VectorClock increment(VectorClock clock, String deviceId) {
        return clock.increment(deviceId);
    }
}
