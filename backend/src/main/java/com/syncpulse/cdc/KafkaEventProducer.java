package com.syncpulse.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncpulse.config.AppProperties;
import com.syncpulse.domain.PlayheadEvent;
import com.syncpulse.persistence.CdcAuditRepository;
import com.syncpulse.service.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Serializes a {@link PlayheadEvent} and publishes it to the Kafka topic, records the
 * emission in {@code cdc_audit}, and updates metrics. Shared by the CDC adapters.
 */
@Component
public class KafkaEventProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;
    private final AppProperties props;
    private final CdcAuditRepository cdcAudit;
    private final MetricsService metrics;

    public KafkaEventProducer(KafkaTemplate<String, String> kafka, ObjectMapper objectMapper,
                              AppProperties props, CdcAuditRepository cdcAudit, MetricsService metrics) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.props = props;
        this.cdcAudit = cdcAudit;
        this.metrics = metrics;
    }

    public void publish(PlayheadEvent event) {
        final String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            log.error("Failed to serialize event {}: {}", event.eventId(), e.getMessage());
            metrics.incrementKafkaFailed();
            return;
        }

        String key = event.userId() + ":" + event.contentId();
        kafka.send(props.getKafka().getTopic(), key, payload).whenComplete((result, ex) -> {
            boolean delivered = ex == null;
            if (delivered) {
                metrics.incrementKafkaEmitted();
            } else {
                metrics.incrementKafkaFailed();
                log.warn("Kafka publish failed for event {}: {}", event.eventId(), ex.getMessage());
            }
            try {
                cdcAudit.record(event, payload, delivered);
            } catch (RuntimeException auditEx) {
                log.warn("Failed to write cdc_audit for {}: {}", event.eventId(), auditEx.getMessage());
            }
        });
    }
}
