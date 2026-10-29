package com.syncpulse.conflict;

import com.syncpulse.domain.Comparison;
import com.syncpulse.domain.Decision;
import com.syncpulse.domain.PlayheadState;
import com.syncpulse.domain.VectorClock;

/**
 * Outcome of the conflict engine. Pure decision object — it does not perform I/O.
 *
 * @param newState the state to persist when {@code decision.isDurable()}, else null
 */
public record ConflictResolution(
        Decision decision,
        Comparison comparison,
        String reason,
        VectorClock incomingClock,
        VectorClock storedClock,
        long incomingPosition,
        long authoritativePosition,
        PlayheadState newState
) {}
