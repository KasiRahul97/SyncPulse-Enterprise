package com.syncpulse.api;

import com.syncpulse.service.MetricsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/metrics")
@Tag(name = "Metrics", description = "Live runtime metrics (all real, none hardcoded)")
public class MetricsController {

    private final MetricsService metrics;

    public MetricsController(MetricsService metrics) {
        this.metrics = metrics;
    }

    @GetMapping
    @Operation(summary = "Snapshot of live pipeline metrics for the dashboard")
    public Map<String, Object> metrics() {
        return metrics.snapshot();
    }
}
