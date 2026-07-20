from __future__ import annotations

import time
from typing import Callable, Any


def create_kafka_producer_with_retry(
    bootstrap_servers: str,
    value_serializer: Callable[[Any], bytes],
    key_serializer: Callable[[str], bytes],
    retries: int = 15,
    retry_interval_seconds: float = 2.0,
):
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks="all",
                enable_idempotence=True,
                linger_ms=5,
                max_in_flight_requests_per_connection=1,
                value_serializer=value_serializer,
                key_serializer=key_serializer,
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(retry_interval_seconds)
    assert last_error is not None
    raise last_error
