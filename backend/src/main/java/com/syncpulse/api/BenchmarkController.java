package com.syncpulse.api;

import com.syncpulse.service.BenchmarkService;
import com.syncpulse.service.BenchmarkService.BenchmarkRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/benchmark")
@Tag(name = "Benchmark", description = "Load simulator (measures against the running stack)")
public class BenchmarkController {

    private final BenchmarkService benchmark;

    public BenchmarkController(BenchmarkService benchmark) {
        this.benchmark = benchmark;
    }

    @PostMapping("/run")
    @Operation(summary = "Run a load benchmark and return measured naive-vs-SyncPulse results")
    public Map<String, Object> run(@RequestBody(required = false) BenchmarkRequest request) {
        BenchmarkRequest cfg = request == null
                ? new BenchmarkRequest(null, null, null, null, null) : request;
        return benchmark.run(cfg);
    }
}
