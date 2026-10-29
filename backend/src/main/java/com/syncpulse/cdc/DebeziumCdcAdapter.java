package com.syncpulse.cdc;

import com.syncpulse.domain.PlayheadEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * REAL CDC MODE.
 *
 * <p>When {@code SYNCPULSE_CDC_MODE=debezium}, change propagation is owned entirely by a
 * Debezium Cassandra connector that tails Cassandra's CDC commit log and publishes to
 * Kafka. The backend therefore does <b>not</b> emit events itself — doing so would
 * double-publish. This adapter is intentionally a no-op that documents that contract.</p>
 *
 * <p>See {@code infrastructure/debezium/} for the connector configuration and the
 * {@code cdc_enabled} table requirement.</p>
 */
@Component
@ConditionalOnProperty(name = "syncpulse.cdc.mode", havingValue = "debezium")
public class DebeziumCdcAdapter implements CdcEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DebeziumCdcAdapter.class);

    public DebeziumCdcAdapter() {
        log.info("CDC mode = DEBEZIUM: change events are produced by the Debezium connector, " +
                "not by the backend. Backend emission is disabled.");
    }

    @Override
    public void onCommitted(PlayheadEvent event) {
        // Intentionally no-op: Debezium tails the Cassandra commit log and publishes to Kafka.
    }

    @Override
    public String adapterName() {
        return "DEBEZIUM";
    }
}
