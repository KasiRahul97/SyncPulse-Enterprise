package com.syncpulse.consumer;

import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/consumer")
public class ConsumerController {

    private final ConsumedEventStore store;

    public ConsumerController(ConsumedEventStore store) {
        this.store = store;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", "UP", "service", "syncpulse-companion");
    }

    /** Consumed events with seq &gt; since, for the dashboard's Kafka event stream panel. */
    @GetMapping("/events")
    public Map<String, Object> events(@RequestParam(defaultValue = "0") long since) {
        List<ConsumedEvent> events = store.since(since);
        return Map.of("cursor", store.currentSeq(), "events", events);
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        return store.metrics();
    }
}
