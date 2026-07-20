#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fraud_pipeline import (  # noqa: E402
    RECEIVER_STATE_SOURCE_FILENAME,
    RECEIVER_STATE_TOPIC,
    SENDER_STATE_SOURCE_FILENAME,
    SENDER_STATE_TOPIC,
    SourceDataError,
    TRANSACTION_SOURCE_FILENAME,
    TRANSACTION_TOPIC,
    iter_logical_source_triplets,
)
from fraud_pipeline.kafka_client import create_kafka_producer_with_retry  # noqa: E402
from fraud_pipeline.serialization import dumps  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish 3 independent logical source CSVs to Kafka: "
            "transaction, sender_state, receiver_state."
        )
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument(
        "--source-dir",
        default=str(ROOT / "data" / "logical_sources"),
        help=(
            "Thu muc chua 3 CSV da tach: "
            f"{TRANSACTION_SOURCE_FILENAME}, {SENDER_STATE_SOURCE_FILENAME}, {RECEIVER_STATE_SOURCE_FILENAME}"
        ),
    )
    parser.add_argument("--rate", type=float, default=100.0, help="Events per second. Use 0 for max speed.")
    parser.add_argument("--max-events", type=int, default=1000)
    return parser.parse_args()


def main() -> int:
    try:
        from kafka import KafkaProducer
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Thieu dependency 'kafka-python'. Cai bang lenh: "
            "python -m pip install -r requirements-local.txt"
        ) from exc

    args = parse_args()
    producer = create_kafka_producer_with_retry(
        args.bootstrap_servers,
        value_serializer=lambda value: dumps(value),
        key_serializer=lambda value: value.encode("utf-8"),
    )

    delay = 0.0 if args.rate <= 0 else 1.0 / args.rate
    published = 0
    try:
        for transaction_payload, sender_payload, receiver_payload in iter_logical_source_triplets(
            args.source_dir,
            limit=args.max_events,
        ):
            producer.send(
                TRANSACTION_TOPIC,
                key=transaction_payload["event_id"],
                value=transaction_payload,
            )
            producer.send(
                SENDER_STATE_TOPIC,
                key=sender_payload["source_event_id"],
                value=sender_payload,
            )
            producer.send(
                RECEIVER_STATE_TOPIC,
                key=receiver_payload["source_event_id"],
                value=receiver_payload,
            )
            published += 1
            if delay:
                time.sleep(delay)
        producer.flush()
        print(
            f"Published {published} correlated events from 3 independent source CSVs to Kafka."
        )
        return 0
    except SourceDataError as exc:
        raise SystemExit(f"Loi du lieu nguon logic: {exc}") from exc
    finally:
        producer.close()


if __name__ == "__main__":
    raise SystemExit(main())
