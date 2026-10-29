package com.syncpulse.service;

import com.syncpulse.domain.Decision;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Central runtime metrics. Every value here is derived from real pipeline activity —
 * nothing is hardcoded. Also mirrors counters into Micrometer for {@code /actuator/prometheus}.
 */
@Service
public class MetricsService {

    private final MeterRegistry registry;

    private final AtomicLong totalPings = new AtomicLong();
    private final AtomicLong debounced = new AtomicLong();
    private final AtomicLong persistCandidates = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejectedStale = new AtomicLong();
    private final AtomicLong concurrentConflicts = new AtomicLong();
    private final AtomicLong idempotentHits = new AtomicLong();
    private final AtomicLong cassandraWrites = new AtomicLong();
    private final AtomicLong conflictsLogged = new AtomicLong();
    private final AtomicLong kafkaEmitted = new AtomicLong();
    private final AtomicLong kafkaFailed = new AtomicLong();

    // Per-second rates (refreshed by the scheduler below).
    private volatile double pingsPerSec;
    private volatile double writesPerSec;
    private volatile double eventsPerSec;
    private long prevPings, prevWrites, prevEvents;

    private final LatencyReservoir latency = new LatencyReservoir(4096);

    public MetricsService(MeterRegistry registry) {
        this.registry = registry;
    }

    @PostConstruct
    void bindMicrometer() {
        registry.gauge("syncpulse.pings.total", totalPings);
        registry.gauge("syncpulse.debounced.total", debounced);
        registry.gauge("syncpulse.persist.candidates", persistCandidates);
        registry.gauge("syncpulse.accepted.total", accepted);
        registry.gauge("syncpulse.rejected.stale.total", rejectedStale);
        registry.gauge("syncpulse.concurrent.total", concurrentConflicts);
        registry.gauge("syncpulse.cassandra.writes.total", cassandraWrites);
        registry.gauge("syncpulse.kafka.emitted.total", kafkaEmitted);
    }

    // ---- mutations ----
    public void recordPing() { totalPings.incrementAndGet(); }
    public void recordDebounced() { debounced.incrementAndGet(); }
    public void recordPersistCandidate() { persistCandidates.incrementAndGet(); }
    public void recordIdempotent() { idempotentHits.incrementAndGet(); }
    public void recordCassandraWrite() { cassandraWrites.incrementAndGet(); }
    public void recordConflictLogged() { conflictsLogged.incrementAndGet(); }
    public void incrementKafkaEmitted() { kafkaEmitted.incrementAndGet(); }
    public void incrementKafkaFailed() { kafkaFailed.incrementAndGet(); }
    public void recordLatency(long millis) { latency.record(millis); }

    public void recordDecision(Decision d) {
        switch (d) {
            case ACCEPTED, ACCEPTED_FIRST -> accepted.incrementAndGet();
            case CONCURRENT_ACCEPTED -> { accepted.incrementAndGet(); concurrentConflicts.incrementAndGet(); }
            case CONCURRENT_REJECTED -> concurrentConflicts.incrementAndGet();
            case REJECTED_STALE -> rejectedStale.incrementAndGet();
            case IDEMPOTENT -> idempotentHits.incrementAndGet();
            case DEBOUNCED -> { /* counted separately */ }
        }
    }

    @Scheduled(fixedRate = 1000)
    void sampleRates() {
        long p = totalPings.get(), w = cassandraWrites.get(), e = kafkaEmitted.get();
        pingsPerSec = p - prevPings;
        writesPerSec = w - prevWrites;
        eventsPerSec = e - prevEvents;
        prevPings = p; prevWrites = w; prevEvents = e;
    }

    public double writeReductionPercentage() {
        long p = totalPings.get();
        if (p == 0) return 0.0;
        return 100.0 * (1.0 - ((double) cassandraWrites.get() / (double) p));
    }

    /** Immutable snapshot for the dashboard JSON. */
    public Map<String, Object> snapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("totalPings", totalPings.get());
        m.put("debounced", debounced.get());
        m.put("persistCandidates", persistCandidates.get());
        m.put("accepted", accepted.get());
        m.put("rejectedStale", rejectedStale.get());
        m.put("concurrentConflicts", concurrentConflicts.get());
        m.put("idempotentHits", idempotentHits.get());
        m.put("cassandraWrites", cassandraWrites.get());
        m.put("conflictsLogged", conflictsLogged.get());
        m.put("kafkaEmitted", kafkaEmitted.get());
        m.put("kafkaFailed", kafkaFailed.get());
        m.put("writeReductionPercentage", round(writeReductionPercentage()));
        m.put("pingsPerSecond", pingsPerSec);
        m.put("writesPerSecond", writesPerSec);
        m.put("eventsPerSecond", eventsPerSec);
        LatencyReservoir.Percentiles pct = latency.percentiles();
        m.put("latencyMs", Map.of(
                "count", pct.count(),
                "avg", round(pct.avg()),
                "p50", pct.p50(),
                "p95", pct.p95(),
                "p99", pct.p99(),
                "max", pct.max()));
        return m;
    }

    public void reset() {
        totalPings.set(0); debounced.set(0); persistCandidates.set(0); accepted.set(0);
        rejectedStale.set(0); concurrentConflicts.set(0); idempotentHits.set(0);
        cassandraWrites.set(0); conflictsLogged.set(0); kafkaEmitted.set(0); kafkaFailed.set(0);
        prevPings = prevWrites = prevEvents = 0;
        pingsPerSec = writesPerSec = eventsPerSec = 0;
        latency.reset();
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** Small lock-guarded reservoir for latency percentiles. */
    static final class LatencyReservoir {
        private final long[] samples;
        private int size;
        private int idx;
        private long total;

        LatencyReservoir(int capacity) {
            this.samples = new long[capacity];
        }

        synchronized void record(long v) {
            if (size == samples.length) {
                total -= samples[idx];   // evict the value being overwritten
            } else {
                size++;
            }
            total += v;
            samples[idx] = v;
            idx = (idx + 1) % samples.length;
        }

        synchronized void reset() {
            size = 0; idx = 0; total = 0;
        }

        synchronized Percentiles percentiles() {
            if (size == 0) return new Percentiles(0, 0, 0, 0, 0, 0);
            long[] copy = Arrays.copyOf(samples, size);
            Arrays.sort(copy);
            return new Percentiles(size,
                    (double) total / size,
                    copy[(int) Math.floor(0.50 * (size - 1))],
                    copy[(int) Math.floor(0.95 * (size - 1))],
                    copy[(int) Math.floor(0.99 * (size - 1))],
                    copy[size - 1]);
        }

        record Percentiles(int count, double avg, long p50, long p95, long p99, long max) {}
    }
}
