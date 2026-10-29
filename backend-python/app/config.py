import os


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


class Config:
    # Cassandra
    CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
    CASSANDRA_PORT = _int("CASSANDRA_PORT", 9042)
    CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "syncpulse")
    CASSANDRA_DATACENTER = os.getenv("CASSANDRA_DATACENTER", "datacenter1")
    CASSANDRA_REPLICATION_FACTOR = _int("CASSANDRA_REPLICATION_FACTOR", 1)

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = _int("REDIS_PORT", 6379)

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "syncpulse.playhead.changes")

    # Debounce policy
    DEBOUNCE_POSITION_THRESHOLD = _int("DEBOUNCE_POSITION_THRESHOLD", 5)
    DEBOUNCE_TIME_THRESHOLD = _int("DEBOUNCE_TIME_THRESHOLD", 10)
    DEBOUNCE_STATE_TTL = _int("DEBOUNCE_STATE_TTL", 86400)

    # Idempotency
    IDEMPOTENCY_TTL = _int("IDEMPOTENCY_TTL", 3600)

    # CDC: "local" or "debezium"
    CDC_MODE = os.getenv("SYNCPULSE_CDC_MODE", "local")

    # Demo seed
    SEED_ENABLED = os.getenv("SYNCPULSE_SEED_ENABLED", "true").lower() == "true"

    # CORS
    CORS_ORIGINS = [
        o.strip() for o in os.getenv(
            "SYNCPULSE_CORS_ORIGINS", "http://localhost:3000,http://localhost:5173"
        ).split(",") if o.strip()
    ]


config = Config()
