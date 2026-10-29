package com.syncpulse.api;

import com.syncpulse.service.SystemStatusService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Health", description = "Liveness and infrastructure status")
public class HealthController {

    private final SystemStatusService status;

    public HealthController(SystemStatusService status) {
        this.status = status;
    }

    /** Lightweight liveness probe (used by the container healthcheck). */
    @GetMapping("/health")
    @Operation(summary = "Liveness probe")
    public Map<String, Object> health() {
        return Map.of("status", "UP", "service", "syncpulse-backend");
    }

    /** Full infrastructure status for the dashboard's status indicators. */
    @GetMapping("/status")
    @Operation(summary = "Infrastructure component status (Redis, Cassandra, Kafka, CDC)")
    public Map<String, Object> status() {
        return status.status();
    }
}
