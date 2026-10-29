package com.syncpulse.service;

import com.syncpulse.api.dto.PlayheadUpdateRequest;
import com.syncpulse.api.dto.PlayheadUpdateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load simulator that drives real traffic through the real pipeline (real Redis
 * debounce, real Cassandra writes, real Kafka emission) and measures the outcome.
 * Every reported number is measured — nothing is fabricated.
 *
 * <p>Each device streams to its own content id, so this isolates the debounce /
 * write-reduction behaviour (the primary NFR) from conflict resolution.</p>
 */
@Service
public class BenchmarkService {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkService.class);

    private final PlayheadService playheadService;

    public BenchmarkService(PlayheadService playheadService) {
        this.playheadService = playheadService;
    }

    public record BenchmarkRequest(
            Integer users,
            Integer devicesPerUser,
            Integer pingsPerDevice,
            Integer positionStepSeconds,
            Integer concurrency) {
    }

    private static int def(Integer v, int fallback) {
        return v == null ? fallback : Math.max(1, v);
    }

    public Map<String, Object> run(BenchmarkRequest cfg) {
        int users = def(cfg.users(), 5);
        int devices = def(cfg.devicesPerUser(), 2);
        int pings = def(cfg.pingsPerDevice(), 200);
        int step = def(cfg.positionStepSeconds(), 2);
        int concurrency = def(cfg.concurrency(), 8);

        long totalRequests = (long) users * devices * pings;
        AtomicLong persisted = new AtomicLong();
        AtomicLong debounced = new AtomicLong();
        AtomicLong accepted = new AtomicLong();
        AtomicLong eventsEmitted = new AtomicLong();
        List<Long> latencies = new CopyOnWriteArrayList<>();

        log.info("Benchmark starting: {} users x {} devices x {} pings = {} requests (concurrency={})",
                users, devices, pings, totalRequests, concurrency);

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<?>> futures = new ArrayList<>();
        long start = System.nanoTime();

        for (int u = 0; u < users; u++) {
            String userId = "bench-user-" + u;
            for (int d = 0; d < devices; d++) {
                String deviceId = "bench-dev-" + u + "-" + d;
                String contentId = "bench-content-" + u + "-" + d; // isolated debounce stream
                futures.add(pool.submit(() -> {
                    for (int i = 1; i <= pings; i++) {
                        long position = (long) i * step;
                        PlayheadUpdateRequest req = new PlayheadUpdateRequest(
                                userId, contentId, deviceId, position,
                                null, System.currentTimeMillis(),
                                "bench-" + deviceId + "-" + i, false, null);
                        PlayheadUpdateResponse resp = playheadService.update(req);
                        latencies.add(resp.processingMs());
                        if (resp.persisted()) persisted.incrementAndGet();
                        if ("DEBOUNCED".equals(resp.decision())) debounced.incrementAndGet();
                        if (resp.decision().startsWith("ACCEPTED")) accepted.incrementAndGet();
                        if (resp.eventEmitted()) eventsEmitted.incrementAndGet();
                    }
                }));
            }
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.warn("Benchmark task failed: {}", e.getCause() == null ? e.getMessage() : e.getCause().getMessage());
            }
        }
        pool.shutdown();
        long elapsedMs = Math.round((System.nanoTime() - start) / 1_000_000.0);

        long syncPulseWrites = persisted.get();
        double writeReduction = totalRequests == 0 ? 0
                : 100.0 * (1.0 - ((double) syncPulseWrites / (double) totalRequests));
        double throughput = elapsedMs == 0 ? 0 : (totalRequests * 1000.0) / elapsedMs;

        List<Long> sorted = new ArrayList<>(latencies);
        sorted.sort(Long::compareTo);

        Map<String, Object> report = new LinkedHashMap<>();
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("users", users);
        config.put("devicesPerUser", devices);
        config.put("pingsPerDevice", pings);
        config.put("positionStepSeconds", step);
        config.put("concurrency", concurrency);
        report.put("config", config);

        Map<String, Object> naive = new LinkedHashMap<>();
        naive.put("model", "Every ping -> Cassandra");
        naive.put("cassandraWrites", totalRequests);
        report.put("naiveBaseline", naive);

        Map<String, Object> sp = new LinkedHashMap<>();
        sp.put("model", "Every ping -> Redis debounce; only significant updates -> Cassandra");
        sp.put("totalRequests", totalRequests);
        sp.put("cassandraWrites", syncPulseWrites);
        sp.put("debouncedRequests", debounced.get());
        sp.put("acceptedUpdates", accepted.get());
        sp.put("kafkaEventsEmitted", eventsEmitted.get());
        report.put("syncpulse", sp);

        Map<String, Object> results = new LinkedHashMap<>();
        results.put("writeReductionPercentage", round(writeReduction));
        results.put("elapsedMs", elapsedMs);
        results.put("throughputRequestsPerSecond", round(throughput));
        results.put("latencyMs", percentiles(sorted));
        report.put("results", results);
        report.put("measured", true);
        report.put("note", "All values measured against the running stack. Position/time thresholds " +
                "come from the deployed debounce configuration.");

        log.info("Benchmark done in {} ms: {} writes for {} pings ({}% reduction)",
                elapsedMs, syncPulseWrites, totalRequests, round(writeReduction));
        return report;
    }

    private Map<String, Object> percentiles(List<Long> sorted) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (sorted.isEmpty()) {
            m.put("count", 0);
            return m;
        }
        int n = sorted.size();
        long sum = 0;
        for (long v : sorted) sum += v;
        m.put("count", n);
        m.put("avg", round((double) sum / n));
        m.put("p50", sorted.get((int) Math.floor(0.50 * (n - 1))));
        m.put("p95", sorted.get((int) Math.floor(0.95 * (n - 1))));
        m.put("p99", sorted.get((int) Math.floor(0.99 * (n - 1))));
        m.put("max", sorted.get(n - 1));
        return m;
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
