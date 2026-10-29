package com.syncpulse.cdc;

import com.syncpulse.domain.PlayheadEvent;

/**
 * Abstraction over "a committed Cassandra state change becomes a Kafka event".
 *
 * <p>The architectural contract is fixed:
 * <pre>committed state change → event → Kafka → downstream consumer</pre>
 * Only the mechanism that detects the commit varies:</p>
 *
 * <ul>
 *   <li>{@link LocalDevelopmentCdcAdapter} — the backend emits the event right after a
 *       successful Cassandra write. Reproducible and demo-friendly. Clearly labelled
 *       {@code cdcSource=LOCAL_ADAPTER}. It is <b>not</b> Debezium.</li>
 *   <li>{@link DebeziumCdcAdapter} — a real Debezium connector tails Cassandra's CDC
 *       commit log and publishes to Kafka; in this mode the backend emits nothing.</li>
 * </ul>
 */
public interface CdcEventPublisher {

    /** Called by the pipeline immediately after a durable Cassandra commit. */
    void onCommitted(PlayheadEvent event);

    /** Human-readable name of the active adapter (surfaced in the dashboard). */
    String adapterName();
}
