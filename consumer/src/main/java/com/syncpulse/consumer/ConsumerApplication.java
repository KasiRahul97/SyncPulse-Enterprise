package com.syncpulse.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * SyncPulse Companion Service — a downstream consumer that represents a "second screen"
 * application. It subscribes to the Kafka change topic and reacts to committed playhead
 * changes in near real time, WITHOUT ever polling the primary database.
 */
@SpringBootApplication
public class ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}
