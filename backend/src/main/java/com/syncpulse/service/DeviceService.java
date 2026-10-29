package com.syncpulse.service;

import com.syncpulse.domain.Device;
import com.syncpulse.domain.DeviceType;
import com.syncpulse.persistence.DeviceRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class DeviceService {

    private final DeviceRepository repo;
    private final LiveEventLog liveLog;

    public DeviceService(DeviceRepository repo, LiveEventLog liveLog) {
        this.repo = repo;
        this.liveLog = liveLog;
    }

    public Device register(String userId, String deviceId, DeviceType type) {
        Instant now = Instant.now();
        Device existing = repo.listForUser(userId).stream()
                .filter(d -> d.deviceId().equals(deviceId)).findFirst().orElse(null);
        Instant registeredAt = existing != null ? existing.registeredAt() : now;
        Device d = new Device(userId, deviceId, type, registeredAt, now);
        repo.register(d);
        liveLog.add("SYSTEM", userId, null, deviceId, null, null, null,
                "device registered (" + type + ")");
        return d;
    }

    public List<Device> listForUser(String userId) {
        return repo.listForUser(userId);
    }

    public void heartbeat(String userId, String deviceId) {
        repo.touch(userId, deviceId, Instant.now());
    }
}
