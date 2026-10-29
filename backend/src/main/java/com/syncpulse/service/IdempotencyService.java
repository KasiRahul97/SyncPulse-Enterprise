package com.syncpulse.service;

import com.syncpulse.config.AppProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

/**
 * Request-level idempotency using Redis. The first time a {@code requestId} is seen it
 * is atomically claimed (SET NX); a re-submission of the same id is recognized as a
 * duplicate and short-circuits before it can cause a second state transition.
 */
@Service
public class IdempotencyService {

    private final StringRedisTemplate redis;
    private final Duration ttl;

    public IdempotencyService(StringRedisTemplate redis, AppProperties props) {
        this.redis = redis;
        this.ttl = Duration.ofSeconds(props.getIdempotency().getTtlSeconds());
    }

    /** @return true if this requestId was already seen (duplicate); false if newly claimed. */
    public boolean isDuplicate(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return false; // no id supplied -> cannot dedupe
        }
        Boolean claimed = redis.opsForValue().setIfAbsent(key(requestId), "PENDING", ttl);
        return Boolean.FALSE.equals(claimed);
    }

    public void complete(String requestId, String decision) {
        if (requestId == null || requestId.isBlank()) return;
        redis.opsForValue().set(key(requestId), decision, ttl);
    }

    public Optional<String> priorDecision(String requestId) {
        if (requestId == null || requestId.isBlank()) return Optional.empty();
        return Optional.ofNullable(redis.opsForValue().get(key(requestId)));
    }

    private String key(String requestId) {
        return "sp:idem:" + requestId;
    }
}
