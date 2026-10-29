package com.syncpulse.service;

import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded, thread-safe ring buffer of recent pipeline events. Feeds the dashboard's
 * live activity feed and conflict timeline via simple "give me everything since seq N"
 * polling — robust and demo-proof over an Nginx proxy.
 */
@Service
public class LiveEventLog {

    private static final int CAPACITY = 500;

    private final Deque<LiveEvent> buffer = new ArrayDeque<>(CAPACITY);
    private final AtomicLong seq = new AtomicLong();

    public synchronized LiveEvent add(String category, String userId, String contentId, String deviceId,
                                      String decision, String comparison, Long position, String message) {
        LiveEvent e = new LiveEvent(seq.incrementAndGet(), System.currentTimeMillis(),
                category, userId, contentId, deviceId, decision, comparison, position, message);
        if (buffer.size() >= CAPACITY) {
            buffer.pollFirst();
        }
        buffer.addLast(e);
        return e;
    }

    /** Events with seq strictly greater than {@code afterSeq}, oldest first. */
    public synchronized List<LiveEvent> since(long afterSeq) {
        List<LiveEvent> out = new ArrayList<>();
        for (LiveEvent e : buffer) {
            if (e.seq() > afterSeq) out.add(e);
        }
        return out;
    }

    public synchronized List<LiveEvent> recent(int n) {
        List<LiveEvent> all = new ArrayList<>(buffer);
        int from = Math.max(0, all.size() - n);
        return all.subList(from, all.size());
    }

    public synchronized long currentSeq() {
        return seq.get();
    }

    public synchronized void clear() {
        buffer.clear();
    }
}
