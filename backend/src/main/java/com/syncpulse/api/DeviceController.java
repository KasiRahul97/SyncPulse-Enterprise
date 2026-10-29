package com.syncpulse.api;

import com.syncpulse.api.dto.DeviceRegistrationRequest;
import com.syncpulse.domain.Device;
import com.syncpulse.service.DeviceService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Devices", description = "Device registry")
public class DeviceController {

    private final DeviceService devices;

    public DeviceController(DeviceService devices) {
        this.devices = devices;
    }

    @PostMapping("/devices/register")
    @Operation(summary = "Register (or refresh) a device for a user")
    public Device register(@Valid @RequestBody DeviceRegistrationRequest req) {
        return devices.register(req.userId(), req.deviceId(), req.deviceType());
    }

    @PostMapping("/devices/heartbeat")
    @Operation(summary = "Update a device's last-seen timestamp")
    public Map<String, Object> heartbeat(@RequestParam String userId, @RequestParam String deviceId) {
        devices.heartbeat(userId, deviceId);
        return Map.of("userId", userId, "deviceId", deviceId, "acknowledged", true);
    }

    @GetMapping("/users/{userId}/devices")
    @Operation(summary = "List all devices registered to a user")
    public List<Device> list(@PathVariable String userId) {
        return devices.listForUser(userId);
    }
}
