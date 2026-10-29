package com.syncpulse.persistence;

import com.datastax.oss.driver.api.core.CqlSession;
import com.syncpulse.config.AppProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Ensures the keyspace and all tables exist on startup (idempotent {@code CREATE ...
 * IF NOT EXISTS}). This makes the backend self-sufficient even when the
 * {@code cassandra-init} container is not used (e.g. running the jar locally).
 */
@Component
public class SchemaInitializer {

    private static final Logger log = LoggerFactory.getLogger(SchemaInitializer.class);

    private final CqlSession session;
    private final AppProperties props;

    public SchemaInitializer(CqlSession session, AppProperties props) {
        this.session = session;
        this.props = props;
    }

    @PostConstruct
    public void initialize() {
        String ks = props.getCassandra().getKeyspace();
        int rf = props.getCassandra().getReplicationFactor();

        log.info("Ensuring Cassandra keyspace '{}' (RF={}) and tables exist...", ks, rf);

        execWithRetry("CREATE KEYSPACE IF NOT EXISTS " + ks +
                " WITH replication = {'class':'SimpleStrategy','replication_factor':" + rf + "}");

        List<String> tables = List.of(
                "CREATE TABLE IF NOT EXISTS " + ks + ".playhead_state (" +
                        "user_id text, content_id text, device_id text, position_seconds bigint," +
                        "vector_clock map<text,bigint>, status text, revision bigint," +
                        "last_request_id text, last_updated_at timestamp," +
                        "PRIMARY KEY ((user_id, content_id)))",

                "CREATE TABLE IF NOT EXISTS " + ks + ".device_registry (" +
                        "user_id text, device_id text, device_type text," +
                        "registered_at timestamp, last_seen_at timestamp," +
                        "PRIMARY KEY ((user_id), device_id))",

                "CREATE TABLE IF NOT EXISTS " + ks + ".conflict_log (" +
                        "user_id text, resolved_at timestamp, conflict_id uuid, content_id text," +
                        "incoming_device_id text, incoming_vector_clock map<text,bigint>," +
                        "stored_vector_clock map<text,bigint>, comparison text, decision text, reason text," +
                        "incoming_position bigint, authoritative_position bigint, request_id text," +
                        "PRIMARY KEY ((user_id), resolved_at, conflict_id)) " +
                        "WITH CLUSTERING ORDER BY (resolved_at DESC, conflict_id ASC)",

                "CREATE TABLE IF NOT EXISTS " + ks + ".cdc_audit (" +
                        "event_date text, emitted_at timestamp, event_id uuid, source_table text," +
                        "change_type text, event_type text, user_id text, content_id text, device_id text," +
                        "payload text, delivered boolean," +
                        "PRIMARY KEY ((event_date), emitted_at, event_id)) " +
                        "WITH CLUSTERING ORDER BY (emitted_at DESC, event_id ASC)"
        );

        for (String ddl : tables) {
            execWithRetry(ddl);
        }
        log.info("Cassandra schema is ready.");
    }

    private void execWithRetry(String cql) {
        int attempts = 0;
        RuntimeException last = null;
        while (attempts < 10) {
            try {
                session.execute(cql);
                return;
            } catch (RuntimeException e) {
                last = e;
                attempts++;
                log.warn("Schema statement failed (attempt {}/10): {}", attempts, e.getMessage());
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted during schema init", ie);
                }
            }
        }
        throw new IllegalStateException("Failed to apply schema after retries: " + cql, last);
    }
}
