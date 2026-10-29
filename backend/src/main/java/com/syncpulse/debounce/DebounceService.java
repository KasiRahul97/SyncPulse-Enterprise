package com.syncpulse.debounce;

import com.syncpulse.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Executes the atomic debounce decision inside Redis.
 *
 * <p>The decision is made by {@code redis/debounce.lua} under a single atomic script
 * execution — never in application memory — so concurrent pings for the same stream
 * cannot race. The application only interprets the structured result.</p>
 */
@Service
public class DebounceService {

    private static final Logger log = LoggerFactory.getLogger(DebounceService.class);

    private final StringRedisTemplate redis;
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> debounceScript;
    private final AppProperties props;

    public DebounceService(StringRedisTemplate redis,
                           @SuppressWarnings("rawtypes") RedisScript<List> debounceScript,
                           AppProperties props) {
        this.redis = redis;
        this.debounceScript = debounceScript;
        this.props = props;
    }

    public DebounceResult evaluate(String userId, String contentId, String deviceId,
                                   long positionSeconds, boolean force) {
        String key = key(userId, contentId, deviceId);
        long now = System.currentTimeMillis();
        AppProperties.Debounce cfg = props.getDebounce();

        @SuppressWarnings("unchecked")
        List<Object> raw = redis.execute(
                debounceScript,
                List.of(key),
                String.valueOf(positionSeconds),
                String.valueOf(now),
                String.valueOf(cfg.getPositionThresholdSeconds()),
                String.valueOf(cfg.getTimeThresholdSeconds()),
                force ? "1" : "0",
                String.valueOf(cfg.getStateTtlSeconds()));

        if (raw == null || raw.size() < 6) {
            // Fail safe: if Redis returns something unexpected, persist rather than lose data.
            log.warn("Unexpected debounce script result: {}", raw);
            return new DebounceResult(DebounceDecision.FORCE_PERSIST,
                    "debounce script returned unexpected result; failing open", 0, now, 0, 0);
        }

        DebounceDecision decision = DebounceDecision.valueOf(String.valueOf(raw.get(0)));
        return new DebounceResult(
                decision,
                String.valueOf(raw.get(1)),
                asLong(raw.get(2)),
                asLong(raw.get(3)),
                asLong(raw.get(4)),
                asLong(raw.get(5)));
    }

    /** Removes debounce state for a stream (used by admin reset). */
    public void clear(String userId, String contentId, String deviceId) {
        redis.delete(key(userId, contentId, deviceId));
    }

    private String key(String userId, String contentId, String deviceId) {
        return "sp:debounce:" + userId + ":" + contentId + ":" + deviceId;
    }

    private static long asLong(Object o) {
        if (o instanceof Number n) return n.longValue();
        try {
            return Long.parseLong(String.valueOf(o));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
