package com.syncpulse.persistence;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.syncpulse.config.AppProperties;
import com.syncpulse.domain.PlayheadEvent;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Audit of change events emitted toward the streaming layer ({@code cdc_audit}). */
@Repository
@DependsOn("schemaInitializer")
public class CdcAuditRepository {

    private static final DateTimeFormatter DAY =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private final CqlSession session;
    private final String keyspace;
    private PreparedStatement insertStmt;

    public CdcAuditRepository(CqlSession session, AppProperties props) {
        this.session = session;
        this.keyspace = props.getCassandra().getKeyspace();
    }

    @PostConstruct
    void prepare() {
        insertStmt = session.prepare("INSERT INTO " + keyspace + ".cdc_audit (" +
                "event_date, emitted_at, event_id, source_table, change_type, event_type, " +
                "user_id, content_id, device_id, payload, delivered) VALUES (?,?,?,?,?,?,?,?,?,?,?)");
    }

    public void record(PlayheadEvent event, String payloadJson, boolean delivered) {
        Instant now = event.occurredAt();
        session.execute(insertStmt.bind(
                DAY.format(now), now, event.eventId(), "playhead_state", "UPDATE",
                event.eventType(), event.userId(), event.contentId(), event.deviceId(),
                payloadJson, delivered));
    }
}
