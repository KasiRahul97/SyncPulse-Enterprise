package com.syncpulse.persistence;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.syncpulse.config.AppProperties;
import com.syncpulse.domain.PlayheadState;
import com.syncpulse.domain.VectorClock;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;

/** Data access for the {@code playhead_state} table (single-partition upserts/reads). */
@Repository
@DependsOn("schemaInitializer")
public class PlayheadStateRepository {

    private final CqlSession session;
    private final String keyspace;

    private PreparedStatement selectStmt;
    private PreparedStatement upsertStmt;
    private PreparedStatement deleteStmt;

    public PlayheadStateRepository(CqlSession session, AppProperties props) {
        this.session = session;
        this.keyspace = props.getCassandra().getKeyspace();
    }

    @PostConstruct
    void prepare() {
        selectStmt = session.prepare(
                "SELECT * FROM " + keyspace + ".playhead_state WHERE user_id=? AND content_id=?");
        upsertStmt = session.prepare(
                "UPDATE " + keyspace + ".playhead_state SET device_id=?, position_seconds=?, " +
                        "vector_clock=?, status=?, revision=?, last_request_id=?, last_updated_at=? " +
                        "WHERE user_id=? AND content_id=?");
        deleteStmt = session.prepare(
                "DELETE FROM " + keyspace + ".playhead_state WHERE user_id=? AND content_id=?");
    }

    public Optional<PlayheadState> find(String userId, String contentId) {
        Row row = session.execute(selectStmt.bind(userId, contentId)).one();
        return Optional.ofNullable(row).map(this::map);
    }

    public void upsert(PlayheadState s) {
        session.execute(upsertStmt.bind(
                s.deviceId(),
                s.positionSeconds(),
                s.vectorClock().asMap(),
                s.status(),
                s.revision(),
                s.lastRequestId(),
                s.lastUpdatedAt(),
                s.userId(),
                s.contentId()));
    }

    public void delete(String userId, String contentId) {
        session.execute(deleteStmt.bind(userId, contentId));
    }

    private PlayheadState map(Row row) {
        return new PlayheadState(
                row.getString("user_id"),
                row.getString("content_id"),
                row.getString("device_id"),
                row.getLong("position_seconds"),
                VectorClock.of(row.getMap("vector_clock", String.class, Long.class)),
                row.getString("status"),
                row.getLong("revision"),
                row.getString("last_request_id"),
                row.getInstant("last_updated_at"));
    }
}
