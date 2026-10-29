package com.syncpulse.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/** Strongly-typed binding of all {@code syncpulse.*} configuration. */
@ConfigurationProperties(prefix = "syncpulse")
public class AppProperties {

    private final Cdc cdc = new Cdc();
    private final Kafka kafka = new Kafka();
    private final Debounce debounce = new Debounce();
    private final Idempotency idempotency = new Idempotency();
    private final Seed seed = new Seed();
    private final Cassandra cassandra = new Cassandra();
    private final Cors cors = new Cors();

    public Cdc getCdc() { return cdc; }
    public Kafka getKafka() { return kafka; }
    public Debounce getDebounce() { return debounce; }
    public Idempotency getIdempotency() { return idempotency; }
    public Seed getSeed() { return seed; }
    public Cassandra getCassandra() { return cassandra; }
    public Cors getCors() { return cors; }

    public static class Cdc {
        /** local | debezium */
        private String mode = "local";
        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
    }

    public static class Kafka {
        private String topic = "syncpulse.playhead.changes";
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }

    public static class Debounce {
        private int positionThresholdSeconds = 5;
        private int timeThresholdSeconds = 10;
        private int stateTtlSeconds = 86400;
        public int getPositionThresholdSeconds() { return positionThresholdSeconds; }
        public void setPositionThresholdSeconds(int v) { this.positionThresholdSeconds = v; }
        public int getTimeThresholdSeconds() { return timeThresholdSeconds; }
        public void setTimeThresholdSeconds(int v) { this.timeThresholdSeconds = v; }
        public int getStateTtlSeconds() { return stateTtlSeconds; }
        public void setStateTtlSeconds(int v) { this.stateTtlSeconds = v; }
    }

    public static class Idempotency {
        private int ttlSeconds = 3600;
        public int getTtlSeconds() { return ttlSeconds; }
        public void setTtlSeconds(int v) { this.ttlSeconds = v; }
    }

    public static class Seed {
        private boolean enabled = true;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    public static class Cassandra {
        private String keyspace = "syncpulse";
        private int replicationFactor = 1;
        public String getKeyspace() { return keyspace; }
        public void setKeyspace(String keyspace) { this.keyspace = keyspace; }
        public int getReplicationFactor() { return replicationFactor; }
        public void setReplicationFactor(int v) { this.replicationFactor = v; }
    }

    public static class Cors {
        private List<String> allowedOrigins = List.of("http://localhost:3000");
        public List<String> getAllowedOrigins() { return allowedOrigins; }
        public void setAllowedOrigins(List<String> v) { this.allowedOrigins = v; }
    }
}
