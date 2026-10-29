package com.syncpulse.consumer;

import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe ring buffer of consumed events + delivery/latency metrics. */
@Component
public class ConsumedEventStore {

    private static final int CAPACITY = 500;
    private static final int LATENCY_WINDOW = 512;

    private final Deque<ConsumedEvent> buffer = new ArrayDeque<>(CAPACITY);
    private final AtomicLong seq = new AtomicLong();
    private final AtomicLong totalConsumed = new AtomicLong();

    private final long[] latencies = new long[LATENCY_WINDOW];
    private int latSize, latIdx;
    private long latSum, latMax;

    public synchronized ConsumedEvent add(ConsumedEvent partial) {
        long s = seq.incrementAndGet();
        ConsumedEvent e = new ConsumedEvent(s, partial.eventId(), partial.eventType(),
                partial.userId(), partial.contentId(), partial.deviceId(), partial.positionSeconds(),
                partial.vectorClock(), partial.decision(), partial.cdcSource(), partial.occurredAt(),
                partial.receivedAtEpochMs(), partial.propagationLatencyMs());
        if (buffer.size() >= CAPACITY) buffer.pollFirst();
        buffer.addLast(e);
        totalConsumed.incrementAndGet();
        recordLatency(Math.max(0, e.propagationLatencyMs()));
        return e;
    }

    private void recordLatency(long v) {
        if (latSize == latencies.length) {
            latSum -= latencies[latIdx];
        } else {
            latSize++;
        }
        latSum += v;
        latencies[latIdx] = v;
        latIdx = (latIdx + 1) % latencies.length;
        if (v > latMax) latMax = v;
    }

    public synchronized List<ConsumedEvent> since(long afterSeq) {
        List<ConsumedEvent> out = new ArrayList<>();
        for (ConsumedEvent e : buffer) {
            if (e.seq() > afterSeq) out.add(e);
        }
        return out;
    }

    public synchronized long currentSeq() {
        return seq.get();
    }

    public synchronized Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("totalConsumed", totalConsumed.get());
        long[] copy = new long[latSize];
        System.arraycopy(latencies, 0, copy, 0, latSize);
        java.util.Arrays.sort(copy);
        Map<String, Object> lat = new LinkedHashMap<>();
        if (latSize == 0) {
            lat.put("count", 0);
        } else {
            lat.put("count", latSize);
            lat.put("avg", Math.round(((double) latSum / latSize) * 100.0) / 100.0);
            lat.put("p50", copy[(int) Math.floor(0.50 * (latSize - 1))]);
            lat.put("p95", copy[(int) Math.floor(0.95 * (latSize - 1))]);
            lat.put("p99", copy[(int) Math.floor(0.99 * (latSize - 1))]);
            lat.put("max", latMax);
        }
        m.put("propagationLatencyMs", lat);
        return m;
    }
}
