package com.syncpulse.conflict;

import com.syncpulse.domain.Comparison;
import com.syncpulse.domain.Decision;
import com.syncpulse.domain.PlayheadState;
import com.syncpulse.domain.VectorClock;
import com.syncpulse.vectorclock.VectorClockService;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

/**
 * The causal conflict engine. Given the currently stored authoritative state and an
 * incoming update, it decides the outcome using <b>vector-clock causality</b> — never
 * wall-clock time.
 *
 * <h3>Cases</h3>
 * <ol>
 *   <li>No prior state → {@code ACCEPTED_FIRST}</li>
 *   <li>Incoming AFTER stored → {@code ACCEPTED}</li>
 *   <li>Incoming BEFORE stored → {@code REJECTED_STALE} (authoritative state never regresses)</li>
 *   <li>Incoming EQUAL stored → {@code IDEMPOTENT} (no state transition)</li>
 *   <li>Concurrent → deterministic policy: <b>highest position wins</b>; the merged
 *       (element-wise max) clock is always persisted so history converges.</li>
 * </ol>
 */
@Service
public class ConflictResolver {

    private final VectorClockService vc;

    public ConflictResolver(VectorClockService vc) {
        this.vc = vc;
    }

    public ConflictResolution resolve(Optional<PlayheadState> current,
                                      String userId, String contentId, String deviceId,
                                      long incomingPosition, VectorClock incomingClock) {

        // Case 1 — first update for this stream.
        if (current.isEmpty()) {
            PlayheadState next = new PlayheadState(userId, contentId, deviceId, incomingPosition,
                    incomingClock, PlayheadState.STATUS_ACTIVE, 1L, null, Instant.now());
            return new ConflictResolution(Decision.ACCEPTED_FIRST, Comparison.AFTER,
                    "first update for stream; no prior authoritative state",
                    incomingClock, VectorClock.empty(), incomingPosition, incomingPosition, next);
        }

        PlayheadState stored = current.get();
        VectorClock storedClock = stored.vectorClock();
        Comparison cmp = vc.compare(incomingClock, storedClock);

        return switch (cmp) {
            case AFTER -> {
                VectorClock merged = vc.merge(storedClock, incomingClock);
                PlayheadState next = new PlayheadState(userId, contentId, deviceId, incomingPosition,
                        merged, PlayheadState.STATUS_ACTIVE, stored.revision() + 1, null, Instant.now());
                yield new ConflictResolution(Decision.ACCEPTED, cmp,
                        "incoming vector clock is causally after authoritative state",
                        incomingClock, storedClock, incomingPosition, incomingPosition, next);
            }
            case BEFORE -> new ConflictResolution(Decision.REJECTED_STALE, cmp,
                    "incoming vector clock is causally behind authoritative state; rejected to prevent regression",
                    incomingClock, storedClock, incomingPosition, stored.positionSeconds(), null);

            case EQUAL -> new ConflictResolution(Decision.IDEMPOTENT, cmp,
                    "incoming vector clock equals authoritative state; no state transition",
                    incomingClock, storedClock, incomingPosition, stored.positionSeconds(), null);

            case CONCURRENT -> resolveConcurrent(stored, userId, contentId, deviceId,
                    incomingPosition, incomingClock, storedClock);
        };
    }

    /**
     * Deterministic concurrent resolution: <b>highest position wins</b>. Video progress
     * is monotonic-forward semantically, so the further-along position is the safest
     * choice for "Continue Watching". The merged clock is always stored to converge.
     */
    private ConflictResolution resolveConcurrent(PlayheadState stored, String userId, String contentId,
                                                 String incomingDeviceId, long incomingPosition,
                                                 VectorClock incomingClock, VectorClock storedClock) {
        VectorClock merged = vc.merge(storedClock, incomingClock);
        long revision = stored.revision() + 1;

        if (incomingPosition > stored.positionSeconds()) {
            PlayheadState next = new PlayheadState(userId, contentId, incomingDeviceId, incomingPosition,
                    merged, PlayheadState.STATUS_ACTIVE, revision, null, Instant.now());
            return new ConflictResolution(Decision.CONCURRENT_ACCEPTED, Comparison.CONCURRENT,
                    "concurrent update; highest-position-wins selected INCOMING (" + incomingPosition +
                            "s > " + stored.positionSeconds() + "s); clocks merged",
                    incomingClock, storedClock, incomingPosition, incomingPosition, next);
        }

        // Stored position wins (ties keep the incumbent), but persist the merged clock.
        PlayheadState next = new PlayheadState(userId, contentId, stored.deviceId(), stored.positionSeconds(),
                merged, PlayheadState.STATUS_ACTIVE, revision, null, Instant.now());
        return new ConflictResolution(Decision.CONCURRENT_REJECTED, Comparison.CONCURRENT,
                "concurrent update; highest-position-wins kept STORED (" + stored.positionSeconds() +
                        "s >= " + incomingPosition + "s); clocks merged",
                incomingClock, storedClock, incomingPosition, stored.positionSeconds(), next);
    }
}
