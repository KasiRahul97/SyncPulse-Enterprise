package com.syncpulse.cdc;

import com.syncpulse.domain.PlayheadEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * LOCAL DEVELOPMENT CDC ADAPTER — <b>not</b> Debezium.
 *
 * <p>Emits a Kafka event immediately after a successful Cassandra commit, giving a
 * reproducible event-driven pipeline for local runs and live demos. Events carry
 * {@code cdcSource=LOCAL_ADAPTER} so their provenance is never misrepresented.</p>
 *
 * <p>To use real CDC instead, set {@code SYNCPULSE_CDC_MODE=debezium} and deploy the
 * Debezium Cassandra connector (see {@code infrastructure/debezium}).</p>
 */
@Component
@ConditionalOnProperty(name = "syncpulse.cdc.mode", havingValue = "local", matchIfMissing = true)
public class LocalDevelopmentCdcAdapter implements CdcEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(LocalDevelopmentCdcAdapter.class);

    private final KafkaEventProducer producer;

    public LocalDevelopmentCdcAdapter(KafkaEventProducer producer) {
        this.producer = producer;
        log.info("CDC mode = LOCAL: backend emits Kafka events post-commit (labelled LOCAL_ADAPTER).");
    }

    @Override
    public void onCommitted(PlayheadEvent event) {
        producer.publish(event);
    }

    @Override
    public String adapterName() {
        return "LOCAL_ADAPTER";
    }
}
