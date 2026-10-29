package com.syncpulse.api;

import com.syncpulse.service.LiveEvent;
import com.syncpulse.service.LiveEventLog;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/events")
@Tag(name = "Live Feed", description = "Pipeline activity feed for the dashboard")
public class EventsController {

    private final LiveEventLog log;

    public EventsController(LiveEventLog log) {
        this.log = log;
    }

    /**
     * Returns pipeline events with seq &gt; {@code since}. The dashboard polls with the
     * highest seq it has seen, so it receives only new events (robust "live" feed).
     */
    @GetMapping
    @Operation(summary = "Live pipeline events since a sequence number")
    public Map<String, Object> events(@RequestParam(defaultValue = "0") long since) {
        List<LiveEvent> events = log.since(since);
        return Map.of("cursor", log.currentSeq(), "events", events);
    }
}
