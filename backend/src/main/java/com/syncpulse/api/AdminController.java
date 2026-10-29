package com.syncpulse.api;

import com.syncpulse.service.AdminService;
import com.syncpulse.service.DemoData;
import com.syncpulse.service.SeedService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/admin")
@Tag(name = "Admin", description = "Demo housekeeping")
public class AdminController {

    private final AdminService admin;
    private final SeedService seed;

    public AdminController(AdminService admin, SeedService seed) {
        this.admin = admin;
        this.seed = seed;
    }

    @PostMapping("/reset")
    @Operation(summary = "Reset metrics + live feed; optionally clear demo state and reseed")
    public Map<String, Object> reset(@RequestParam(defaultValue = "true") boolean clearState,
                                     @RequestParam(defaultValue = "true") boolean reseed) {
        return admin.reset(clearState, reseed);
    }

    @PostMapping("/seed")
    @Operation(summary = "Seed the deterministic demo devices")
    public Map<String, Object> seed() {
        seed.seed();
        return Map.of("seeded", true, "user", DemoData.USER,
                "devices", DemoData.DEVICES.stream().map(DemoData.SeedDevice::deviceId).toList());
    }

    @GetMapping("/demo-info")
    @Operation(summary = "Deterministic demo identifiers (user, content, devices)")
    public Map<String, Object> demoInfo() {
        List<Map<String, String>> devs = DemoData.DEVICES.stream()
                .map(d -> Map.of("deviceId", d.deviceId(), "type", d.type().name())).toList();
        return Map.of(
                "userId", DemoData.USER,
                "contentId", DemoData.CONTENT,
                "contentDurationSeconds", DemoData.CONTENT_DURATION_SECONDS,
                "devices", devs);
    }
}
