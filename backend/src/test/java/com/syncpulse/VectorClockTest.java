package com.syncpulse;

import com.syncpulse.domain.Comparison;
import com.syncpulse.domain.VectorClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VectorClockTest {

    @Test
    @DisplayName("two empty clocks are EQUAL")
    void emptyClocksAreEqual() {
        assertThat(VectorClock.empty().compareTo(VectorClock.empty())).isEqualTo(Comparison.EQUAL);
    }

    @Test
    @DisplayName("identical clocks are EQUAL")
    void equalClocks() {
        VectorClock a = VectorClock.of(Map.of("phone", 5L, "tv", 8L));
        VectorClock b = VectorClock.of(Map.of("phone", 5L, "tv", 8L));
        assertThat(a.compareTo(b)).isEqualTo(Comparison.EQUAL);
    }

    @Test
    @DisplayName("A BEFORE B when every component <= and one strictly <")
    void before() {
        VectorClock a = VectorClock.of(Map.of("phone", 4L, "tv", 8L));
        VectorClock b = VectorClock.of(Map.of("phone", 4L, "tv", 12L));
        assertThat(a.compareTo(b)).isEqualTo(Comparison.BEFORE);
        assertThat(b.compareTo(a)).isEqualTo(Comparison.AFTER);
    }

    @Test
    @DisplayName("empty clock is BEFORE any non-empty clock")
    void emptyBeforeNonEmpty() {
        assertThat(VectorClock.empty().compareTo(VectorClock.of(Map.of("tv", 1L))))
                .isEqualTo(Comparison.BEFORE);
    }

    @Test
    @DisplayName("CONCURRENT when neither dominates (the stale-device case)")
    void concurrent() {
        VectorClock tv = VectorClock.of(Map.of("tv", 5L, "phone", 2L));
        VectorClock phone = VectorClock.of(Map.of("tv", 4L, "phone", 3L));
        assertThat(tv.compareTo(phone)).isEqualTo(Comparison.CONCURRENT);
        assertThat(phone.compareTo(tv)).isEqualTo(Comparison.CONCURRENT);
    }

    @Test
    @DisplayName("missing device entries are treated as zero")
    void missingEntriesAreZero() {
        VectorClock a = VectorClock.of(Map.of("phone", 1L));            // tv=0 implicitly
        VectorClock b = VectorClock.of(Map.of("phone", 1L, "tv", 3L));
        assertThat(a.compareTo(b)).isEqualTo(Comparison.BEFORE);
    }

    @Test
    @DisplayName("increment advances only the target device")
    void increment() {
        VectorClock a = VectorClock.of(Map.of("phone", 1L, "tv", 2L));
        VectorClock b = a.increment("phone");
        assertThat(b.get("phone")).isEqualTo(2L);
        assertThat(b.get("tv")).isEqualTo(2L);
        assertThat(a.get("phone")).isEqualTo(1L); // original untouched (immutable)
    }

    @Test
    @DisplayName("increment on a new device creates its component at 1")
    void incrementNewDevice() {
        VectorClock a = VectorClock.empty().increment("web");
        assertThat(a.get("web")).isEqualTo(1L);
    }

    @Test
    @DisplayName("merge is element-wise maximum and dominates both inputs")
    void merge() {
        VectorClock tv = VectorClock.of(Map.of("tv", 5L, "phone", 2L));
        VectorClock phone = VectorClock.of(Map.of("tv", 4L, "phone", 3L));
        VectorClock merged = tv.merge(phone);
        assertThat(merged.get("tv")).isEqualTo(5L);
        assertThat(merged.get("phone")).isEqualTo(3L);
        assertThat(merged.compareTo(tv)).isEqualTo(Comparison.AFTER);
        assertThat(merged.compareTo(phone)).isEqualTo(Comparison.AFTER);
    }

    @Test
    @DisplayName("increment rejects a blank device id")
    void incrementRejectsBlank() {
        assertThatThrownBy(() -> VectorClock.empty().increment(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
