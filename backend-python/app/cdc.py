"""CDC abstraction: committed change -> event -> Kafka. Honest local vs debezium modes."""
from __future__ import annotations

import json
import logging
import threading

from confluent_kafka import Producer

from .config import config

log = logging.getLogger("syncpulse.cdc")


class KafkaEventProducer:
    """Serializes events to Kafka; records cdc_audit + metrics via delivery callbacks."""

    def __init__(self, store, metrics) -> None:
        self.store = store
        self.metrics = metrics
        self.topic = config.KAFKA_TOPIC
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "retries": 3,
            "message.timeout.ms": 8000,
        })
        self._running = True
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        while self._running:
            self.producer.poll(0.2)

    def publish(self, event: dict) -> None:
        public = {k: v for k, v in event.items() if not k.startswith("_")}
        try:
            payload = json.dumps(public)
        except Exception as e:  # noqa: BLE001
            log.error("serialize failed for %s: %s", event.get("eventId"), e)
            self.metrics.inc_kafka_failed()
            return
        key = f"{event['userId']}:{event['contentId']}"

        def cb(err, _msg, _event=event, _payload=payload):
            delivered = err is None
            if delivered:
                self.metrics.inc_kafka_emitted()
            else:
                self.metrics.inc_kafka_failed()
                log.warning("Kafka publish failed for %s: %s", _event.get("eventId"), err)
            try:
                self.store.record_cdc(_event, _payload, delivered)
            except Exception as ae:  # noqa: BLE001
                log.warning("cdc_audit write failed: %s", ae)

        try:
            self.producer.produce(self.topic, key=key.encode(), value=payload.encode(), callback=cb)
            self.producer.poll(0)
        except BufferError:
            self.producer.flush(2)
            self.producer.produce(self.topic, key=key.encode(), value=payload.encode(), callback=cb)


class CdcEventPublisher:
    def __init__(self, kafka_producer: KafkaEventProducer) -> None:
        self.mode = config.CDC_MODE.lower()
        self.producer = kafka_producer
        if self.mode == "debezium":
            log.info("CDC mode = DEBEZIUM: events produced by the Debezium connector, not the backend.")
        else:
            log.info("CDC mode = LOCAL: backend emits Kafka events post-commit (labelled LOCAL_ADAPTER).")

    @property
    def adapter_name(self) -> str:
        return "DEBEZIUM" if self.mode == "debezium" else "LOCAL_ADAPTER"

    def on_committed(self, event: dict) -> None:
        # In debezium mode the connector owns propagation; backend emits nothing.
        if self.mode != "debezium":
            self.producer.publish(event)
