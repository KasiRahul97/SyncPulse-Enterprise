package com.syncpulse.service;

import com.syncpulse.domain.DeviceType;

import java.util.List;

/** Deterministic demo identifiers used by the seed, demo scenarios and reset. */
public final class DemoData {

    public static final String USER = "user-001";
    public static final String CONTENT = "movie-inception";
    public static final long CONTENT_DURATION_SECONDS = 8880; // 2h28m

    public record SeedDevice(String deviceId, DeviceType type) {}

    public static final List<SeedDevice> DEVICES = List.of(
            new SeedDevice("phone-001", DeviceType.PHONE),
            new SeedDevice("tv-001", DeviceType.TV),
            new SeedDevice("web-001", DeviceType.WEB),
            new SeedDevice("tablet-001", DeviceType.TABLET)
    );

    private DemoData() {}
}
