package com.syncpulse.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Consumes {@code PLAYHEAD_UPDATED} events from Kafka and reacts as a second-screen
 * companion would — here, by recording the event and measuring how long it took to
 * propagate from commit to consumption. No polling of Cassandra is involved.
 */
@Component
public class CompanionListener {

    private static final Logger log = LoggerFactory.getLogger(CompanionListener.class);

    private final ObjectMapper mapper;
    private final ConsumedEventStore store;

    public CompanionListener(ObjectMapper mapper, ConsumedEventStore store) {
        this.mapper = mapper;
        this.store = store;
    }

    @KafkaListener(topics = "${syncpulse.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String payload) {
        try {
            JsonNode n = mapper.readTree(payload);
            long receivedAt = System.currentTimeMillis();

            String occurredAt = text(n, "occurredAt");
            long latency = 0;
            if (occurredAt != null) {
                try {
                    latency = receivedAt - Instant.parse(occurredAt).toEpochMilli();
                } catch (Exception ignore) {
                    latency = 0;
                }
            }

            Map<String, Long> clock = new LinkedHashMap<>();
            JsonNode vc = n.get("vectorClock");
            if (vc != null && vc.isObject()) {
                vc.fields().forEachRemaining(e -> clock.put(e.getKey(), e.getValue().asLong()));
            }

            ConsumedEvent event = new ConsumedEvent(0,
                    text(n, "eventId"), text(n, "eventType"), text(n, "userId"),
                    text(n, "contentId"), text(n, "deviceId"),
                    n.path("positionSeconds").asLong(), clock, text(n, "decision"),
                    text(n, "cdcSource"), occurredAt, receivedAt, latency);

            ConsumedEvent stored = store.add(event);
            log.info("Consumed {} user={} device={} pos={}s decision={} latency={}ms",
                    stored.eventType(), stored.userId(), stored.deviceId(),
                    stored.positionSeconds(), stored.decision(), stored.propagationLatencyMs());
        } catch (Exception e) {
            log.error("Failed to process event payload: {}", e.getMessage());
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }
}
