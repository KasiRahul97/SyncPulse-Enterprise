package com.syncpulse.service;

import com.syncpulse.persistence.PlayheadStateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Demo housekeeping: reset counters and clear demo state for a clean run. */
@Service
public class AdminService {

    private static final Logger log = LoggerFactory.getLogger(AdminService.class);

    private final MetricsService metrics;
    private final LiveEventLog liveLog;
    private final StringRedisTemplate redis;
    private final PlayheadStateRepository stateRepo;
    private final SeedService seedService;

    public AdminService(MetricsService metrics, LiveEventLog liveLog, StringRedisTemplate redis,
                        PlayheadStateRepository stateRepo, SeedService seedService) {
        this.metrics = metrics;
        this.liveLog = liveLog;
        this.redis = redis;
        this.stateRepo = stateRepo;
        this.seedService = seedService;
    }

    public Map<String, Object> reset(boolean clearState, boolean reseed) {
        metrics.reset();
        liveLog.clear();

        long redisCleared = clearRedis("sp:debounce:*") + clearRedis("sp:idem:*");

        if (clearState) {
            // Clear the deterministic demo stream so scenarios start fresh.
            stateRepo.delete(DemoData.USER, DemoData.CONTENT);
        }
        if (reseed) {
            seedService.seed();
        }

        log.info("Admin reset: metrics+feed cleared, {} redis keys removed, clearState={}, reseed={}",
                redisCleared, clearState, reseed);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("reset", true);
        out.put("redisKeysCleared", redisCleared);
        out.put("demoStateCleared", clearState);
        out.put("reseeded", reseed);
        return out;
    }

    private long clearRedis(String pattern) {
        Set<String> keys = redis.keys(pattern);
        if (keys == null || keys.isEmpty()) return 0;
        Long n = redis.delete(keys);
        return n == null ? 0 : n;
    }
}
