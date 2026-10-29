package com.syncpulse.persistence;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.syncpulse.config.AppProperties;
import com.syncpulse.domain.Comparison;
import com.syncpulse.domain.ConflictRecord;
import com.syncpulse.domain.Decision;
import com.syncpulse.domain.VectorClock;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/** Data access for {@code conflict_log} — recent-first conflict audit per user. */
@Repository
@DependsOn("schemaInitializer")
public class ConflictRepository {

    private final CqlSession session;
    private final String keyspace;

    private PreparedStatement insertStmt;
    private PreparedStatement recentStmt;

    public ConflictRepository(CqlSession session, AppProperties props) {
        this.session = session;
        this.keyspace = props.getCassandra().getKeyspace();
    }

    @PostConstruct
    void prepare() {
        insertStmt = session.prepare("INSERT INTO " + keyspace + ".conflict_log (" +
                "user_id, resolved_at, conflict_id, content_id, incoming_device_id, " +
                "incoming_vector_clock, stored_vector_clock, comparison, decision, reason, " +
                "incoming_position, authoritative_position, request_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
        recentStmt = session.prepare("SELECT * FROM " + keyspace + ".conflict_log " +
                "WHERE user_id=? LIMIT ?");
    }

    public void record(ConflictRecord c) {
        session.execute(insertStmt.bind(
                c.userId(), c.resolvedAt(), c.conflictId(), c.contentId(), c.incomingDeviceId(),
                c.incomingVectorClock().asMap(), c.storedVectorClock().asMap(),
                c.comparison().name(), c.decision().name(), c.reason(),
                c.incomingPosition(), c.authoritativePosition(), c.requestId()));
    }

    public List<ConflictRecord> recentForUser(String userId, int limit) {
        List<ConflictRecord> result = new ArrayList<>();
        for (Row row : session.execute(recentStmt.bind(userId, limit))) {
            result.add(new ConflictRecord(
                    row.getString("user_id"),
                    row.getInstant("resolved_at"),
                    row.getUuid("conflict_id"),
                    row.getString("content_id"),
                    row.getString("incoming_device_id"),
                    VectorClock.of(row.getMap("incoming_vector_clock", String.class, Long.class)),
                    VectorClock.of(row.getMap("stored_vector_clock", String.class, Long.class)),
                    Comparison.valueOf(row.getString("comparison")),
                    Decision.valueOf(row.getString("decision")),
                    row.getString("reason"),
                    row.getLong("incoming_position"),
                    row.getLong("authoritative_position"),
                    row.getString("request_id")));
        }
        return result;
    }
}
