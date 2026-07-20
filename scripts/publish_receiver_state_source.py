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
    iter_receiver_state_source_payloads,
)
from fraud_pipeline.kafka_client import create_kafka_producer_with_retry  # noqa: E402
from fraud_pipeline.serialization import dumps  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish receiver state source CSV to Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument(
        "--source-dir",
        default=str(ROOT / "Data" / "logical_sources"),
        help=f"Thu muc chua file {RECEIVER_STATE_SOURCE_FILENAME}",
    )
    parser.add_argument("--rate", type=float, default=100.0, help="Correlated events per second. Use 0 for max speed.")
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
        for payload in iter_receiver_state_source_payloads(args.source_dir, limit=args.max_events):
            producer.send(RECEIVER_STATE_TOPIC, key=payload["source_event_id"], value=payload)
            published += 1
            if published % 100 == 0:
                print(f"Progress: Published {published} events...", flush=True)
            if delay:
                time.sleep(delay)
        producer.flush()
        print(f"FINAL: Published {published} receiver state source events to {RECEIVER_STATE_TOPIC}.", flush=True)
        return 0
    finally:
        producer.close()


if __name__ == "__main__":
    raise SystemExit(main())
