package com.syncpulse;

import com.syncpulse.conflict.ConflictResolution;
import com.syncpulse.conflict.ConflictResolver;
import com.syncpulse.domain.Comparison;
import com.syncpulse.domain.Decision;
import com.syncpulse.domain.PlayheadState;
import com.syncpulse.domain.VectorClock;
import com.syncpulse.vectorclock.VectorClockService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ConflictResolverTest {

    private ConflictResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new ConflictResolver(new VectorClockService());
    }

    private PlayheadState stored(long pos, Map<String, Long> clock) {
        return new PlayheadState("user-001", "movie-001", "tv-001", pos,
                VectorClock.of(clock), PlayheadState.STATUS_ACTIVE, 3L, null, Instant.now());
    }

    @Test
    @DisplayName("Case 1 — first update is ACCEPTED_FIRST and durable")
    void firstUpdate() {
        ConflictResolution r = resolver.resolve(Optional.empty(), "user-001", "movie-001", "phone-001",
                42, VectorClock.of(Map.of("phone", 1L)));
        assertThat(r.decision()).isEqualTo(Decision.ACCEPTED_FIRST);
        assertThat(r.decision().isDurable()).isTrue();
        assertThat(r.newState().positionSeconds()).isEqualTo(42);
    }

    @Test
    @DisplayName("Case 2 — causally newer update is ACCEPTED")
    void causallyNewer() {
        var current = Optional.of(stored(120, Map.of("phone", 4L, "tv", 8L)));
        ConflictResolution r = resolver.resolve(current, "user-001", "movie-001", "tv-001",
                200, VectorClock.of(Map.of("phone", 4L, "tv", 12L)));
        assertThat(r.comparison()).isEqualTo(Comparison.AFTER);
        assertThat(r.decision()).isEqualTo(Decision.ACCEPTED);
        assertThat(r.newState().positionSeconds()).isEqualTo(200);
    }

    @Test
    @DisplayName("Case 3 — causally stale update is REJECTED_STALE; state does not regress")
    void causallyStale() {
        var current = Optional.of(stored(420, Map.of("phone", 4L, "tv", 12L)));
        ConflictResolution r = resolver.resolve(current, "user-001", "movie-001", "phone-001",
                120, VectorClock.of(Map.of("phone", 4L, "tv", 8L)));
        assertThat(r.comparison()).isEqualTo(Comparison.BEFORE);
        assertThat(r.decision()).isEqualTo(Decision.REJECTED_STALE);
        assertThat(r.decision().isDurable()).isFalse();
        assertThat(r.authoritativePosition()).isEqualTo(420); // unchanged
    }

    @Test
    @DisplayName("Case 4 — equal clock is IDEMPOTENT (no state transition)")
    void equalClock() {
        var current = Optional.of(stored(300, Map.of("phone", 4L, "tv", 12L)));
        ConflictResolution r = resolver.resolve(current, "user-001", "movie-001", "tv-001",
                300, VectorClock.of(Map.of("phone", 4L, "tv", 12L)));
        assertThat(r.comparison()).isEqualTo(Comparison.EQUAL);
        assertThat(r.decision()).isEqualTo(Decision.IDEMPOTENT);
        assertThat(r.decision().isDurable()).isFalse();
    }

    @Test
    @DisplayName("Case 5a — concurrent, higher incoming position wins (CONCURRENT_ACCEPTED)")
    void concurrentIncomingWins() {
        var current = Optional.of(stored(200, Map.of("tv", 5L, "phone", 2L)));
        ConflictResolution r = resolver.resolve(current, "user-001", "movie-001", "phone-001",
                350, VectorClock.of(Map.of("tv", 4L, "phone", 3L)));
        assertThat(r.comparison()).isEqualTo(Comparison.CONCURRENT);
        assertThat(r.decision()).isEqualTo(Decision.CONCURRENT_ACCEPTED);
        assertThat(r.newState().positionSeconds()).isEqualTo(350);
        // merged clock must dominate both inputs (convergence)
        assertThat(r.newState().vectorClock().get("tv")).isEqualTo(5L);
        assertThat(r.newState().vectorClock().get("phone")).isEqualTo(3L);
    }

    @Test
    @DisplayName("Case 5b — concurrent, stored position higher keeps STORED but merges clock")
    void concurrentStoredWins() {
        var current = Optional.of(stored(500, Map.of("tv", 5L, "phone", 2L)));
        ConflictResolution r = resolver.resolve(current, "user-001", "movie-001", "phone-001",
                350, VectorClock.of(Map.of("tv", 4L, "phone", 3L)));
        assertThat(r.decision()).isEqualTo(Decision.CONCURRENT_REJECTED);
        assertThat(r.decision().isDurable()).isTrue(); // merged clock persisted
        assertThat(r.newState().positionSeconds()).isEqualTo(500);
        assertThat(r.newState().vectorClock().get("phone")).isEqualTo(3L);
    }
}
