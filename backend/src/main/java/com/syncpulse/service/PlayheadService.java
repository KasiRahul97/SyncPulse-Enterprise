package com.syncpulse.service;

import com.syncpulse.api.dto.PlayheadUpdateRequest;
import com.syncpulse.api.dto.PlayheadUpdateResponse;
import com.syncpulse.cdc.CdcEventPublisher;
import com.syncpulse.common.ValidationException;
import com.syncpulse.conflict.ConflictResolution;
import com.syncpulse.conflict.ConflictResolver;
import com.syncpulse.debounce.DebounceResult;
import com.syncpulse.debounce.DebounceService;
import com.syncpulse.domain.*;
import com.syncpulse.persistence.ConflictRepository;
import com.syncpulse.persistence.PlayheadStateRepository;
import com.syncpulse.vectorclock.VectorClockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Orchestrates the full ingestion pipeline for a single playhead update:
 * <pre>ping → idempotency → Redis debounce → conflict engine → Cassandra → CDC/Kafka</pre>
 */
@Service
public class PlayheadService {

    private static final Logger log = LoggerFactory.getLogger(PlayheadService.class);

    private final DebounceService debounce;
    private final ConflictResolver conflictResolver;
    private final PlayheadStateRepository stateRepo;
    private final ConflictRepository conflictRepo;
    private final CdcEventPublisher cdc;
    private final IdempotencyService idempotency;
    private final MetricsService metrics;
    private final LiveEventLog liveLog;
    private final VectorClockService vc;

    public PlayheadService(DebounceService debounce, ConflictResolver conflictResolver,
                           PlayheadStateRepository stateRepo, ConflictRepository conflictRepo,
                           CdcEventPublisher cdc, IdempotencyService idempotency,
                           MetricsService metrics, LiveEventLog liveLog, VectorClockService vc) {
        this.debounce = debounce;
        this.conflictResolver = conflictResolver;
        this.stateRepo = stateRepo;
        this.conflictRepo = conflictRepo;
        this.cdc = cdc;
        this.idempotency = idempotency;
        this.metrics = metrics;
        this.liveLog = liveLog;
        this.vc = vc;
    }

    public PlayheadUpdateResponse update(PlayheadUpdateRequest req) {
        long start = System.nanoTime();
        metrics.recordPing();

        // ---- input validation beyond bean constraints ----
        if (req.contentDurationSeconds() != null && req.positionSeconds() > req.contentDurationSeconds()) {
            throw new ValidationException("positionSeconds (" + req.positionSeconds() +
                    ") exceeds contentDurationSeconds (" + req.contentDurationSeconds() + ")");
        }

        // ---- idempotency: claim the requestId; short-circuit duplicates ----
        if (idempotency.isDuplicate(req.requestId())) {
            metrics.recordIdempotent();
            String prior = idempotency.priorDecision(req.requestId()).orElse("IDEMPOTENT");
            if ("PENDING".equals(prior)) prior = "IDEMPOTENT";
            liveLog.add("DECISION", req.userId(), req.contentId(), req.deviceId(),
                    "IDEMPOTENT", null, req.positionSeconds(),
                    "duplicate requestId " + req.requestId() + " ignored");
            Optional<PlayheadState> cur = stateRepo.find(req.userId(), req.contentId());
            return finish(start, response("IDEMPOTENT",
                    "duplicate requestId; no new state transition", null,
                    "SKIPPED", "duplicate request", false, true, false, cur, req));
        }

        liveLog.add("PING", req.userId(), req.contentId(), req.deviceId(),
                null, null, req.positionSeconds(), "ping received");

        boolean force = Boolean.TRUE.equals(req.force());

        // ---- Redis atomic debounce gate ----
        DebounceResult db = debounce.evaluate(req.userId(), req.contentId(), req.deviceId(),
                req.positionSeconds(), force);

        if (!db.decision().shouldPersist()) {
            metrics.recordDebounced();
            metrics.recordDecision(Decision.DEBOUNCED);
            idempotency.complete(req.requestId(), "DEBOUNCED");
            liveLog.add("DEBOUNCE", req.userId(), req.contentId(), req.deviceId(),
                    "DEBOUNCED", null, req.positionSeconds(),
                    "debounced (" + db.reason() + "); Δpos=" + db.positionDelta() +
                            "s elapsed=" + db.elapsedMs() + "ms");
            // Deliberately NO Cassandra read here — that is the whole point of debouncing.
            return finish(start, new PlayheadUpdateResponse("DEBOUNCED", db.reason(), null,
                    db.decision().name(), db.reason(), false, false, false,
                    req.positionSeconds(), Map.of(), 0, req.requestId(), 0));
        }

        metrics.recordPersistCandidate();
        liveLog.add("DEBOUNCE", req.userId(), req.contentId(), req.deviceId(),
                db.decision().name(), null, req.positionSeconds(),
                "passed debounce (" + db.reason() + ")");

        // ---- conflict engine (vector-clock causality) ----
        Optional<PlayheadState> current = stateRepo.find(req.userId(), req.contentId());
        VectorClock incomingClock = effectiveIncomingClock(req, current);

        ConflictResolution res = conflictResolver.resolve(current, req.userId(), req.contentId(),
                req.deviceId(), req.positionSeconds(), incomingClock);

        boolean persisted = false;
        boolean eventEmitted = false;
        long authoritativePosition = res.authoritativePosition();
        VectorClock authoritativeClock = res.newState() != null
                ? res.newState().vectorClock()
                : (current.map(PlayheadState::vectorClock).orElse(VectorClock.empty()));
        long revision = res.newState() != null
                ? res.newState().revision()
                : current.map(PlayheadState::revision).orElse(0L);

        // ---- durable commit ----
        if (res.decision().isDurable() && res.newState() != null) {
            PlayheadState toStore = new PlayheadState(
                    res.newState().userId(), res.newState().contentId(), res.newState().deviceId(),
                    res.newState().positionSeconds(), res.newState().vectorClock(),
                    res.newState().status(), res.newState().revision(), req.requestId(), Instant.now());
            stateRepo.upsert(toStore);
            metrics.recordCassandraWrite();
            persisted = true;

            // ---- committed change → event → Kafka (via the CDC adapter) ----
            PlayheadEvent event = new PlayheadEvent(UUID.randomUUID(),
                    PlayheadEvent.TYPE_PLAYHEAD_UPDATED, PlayheadEvent.CURRENT_VERSION, Instant.now(),
                    toStore.userId(), toStore.contentId(), toStore.deviceId(), toStore.positionSeconds(),
                    toStore.vectorClock().asMap(), res.decision().name(), toStore.revision(),
                    cdc.adapterName());
            cdc.onCommitted(event);
            eventEmitted = true;
        }

        // ---- conflict audit ----
        if (res.decision().isConflict()) {
            conflictRepo.record(new ConflictRecord(req.userId(), Instant.now(), UUID.randomUUID(),
                    req.contentId(), req.deviceId(), res.incomingClock(), res.storedClock(),
                    res.comparison(), res.decision(), res.reason(), res.incomingPosition(),
                    res.authoritativePosition(), req.requestId()));
            metrics.recordConflictLogged();
        }

        metrics.recordDecision(res.decision());
        idempotency.complete(req.requestId(), res.decision().name());
        liveLog.add("DECISION", req.userId(), req.contentId(), req.deviceId(),
                res.decision().name(), res.comparison().name(), req.positionSeconds(), res.reason());

        return finish(start, new PlayheadUpdateResponse(
                res.decision().name(), res.reason(), res.comparison().name(),
                db.decision().name(), db.reason(), persisted, false, eventEmitted,
                authoritativePosition, authoritativeClock.asMap(), revision, req.requestId(), 0));
    }

    /**
     * Determines the vector clock to use for causal comparison. If the client supplied
     * one (the simulators do), it is used verbatim. Otherwise the server advances the
     * stored clock for this device (server-managed mode) so the update is causally-later.
     */
    private VectorClock effectiveIncomingClock(PlayheadUpdateRequest req, Optional<PlayheadState> current) {
        if (req.vectorClock() != null && !req.vectorClock().isEmpty()) {
            return VectorClock.of(req.vectorClock());
        }
        VectorClock base = current.map(PlayheadState::vectorClock).orElse(VectorClock.empty());
        return vc.increment(base, req.deviceId());
    }

    public Optional<PlayheadState> getState(String userId, String contentId) {
        return stateRepo.find(userId, contentId);
    }

    // ---- helpers ----

    private PlayheadUpdateResponse response(String decision, String reason, String comparison,
                                            String debounceDecision, String debounceReason,
                                            boolean persisted, boolean idempotent, boolean eventEmitted,
                                            Optional<PlayheadState> cur, PlayheadUpdateRequest req) {
        long pos = cur.map(PlayheadState::positionSeconds).orElse(req.positionSeconds());
        Map<String, Long> clock = cur.map(s -> s.vectorClock().asMap()).orElse(Map.of());
        long rev = cur.map(PlayheadState::revision).orElse(0L);
        return new PlayheadUpdateResponse(decision, reason, comparison, debounceDecision, debounceReason,
                persisted, idempotent, eventEmitted, pos, clock, rev, req.requestId(), 0);
    }

    private PlayheadUpdateResponse finish(long startNanos, PlayheadUpdateResponse r) {
        long ms = Math.round((System.nanoTime() - startNanos) / 1_000_000.0);
        metrics.recordLatency(ms);
        return new PlayheadUpdateResponse(r.decision(), r.reason(), r.comparison(),
                r.debounceDecision(), r.debounceReason(), r.persisted(), r.idempotent(), r.eventEmitted(),
                r.authoritativePosition(), r.authoritativeVectorClock(), r.revision(), r.requestId(), ms);
    }
}
