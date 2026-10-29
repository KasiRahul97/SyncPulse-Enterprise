package com.syncpulse.service;

import com.syncpulse.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** Registers deterministic demo devices on startup (idempotent) so the demo is ready. */
@Component
public class SeedService implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SeedService.class);

    private final AppProperties props;
    private final DeviceService devices;

    public SeedService(AppProperties props, DeviceService devices) {
        this.props = props;
        this.devices = devices;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!props.getSeed().isEnabled()) {
            log.info("Seed disabled (syncpulse.seed.enabled=false).");
            return;
        }
        seed();
    }

    public void seed() {
        for (DemoData.SeedDevice d : DemoData.DEVICES) {
            try {
                devices.register(DemoData.USER, d.deviceId(), d.type());
            } catch (RuntimeException e) {
                log.warn("Failed to seed device {}: {}", d.deviceId(), e.getMessage());
            }
        }
        log.info("Seeded {} demo devices for user '{}'.", DemoData.DEVICES.size(), DemoData.USER);
    }
}
