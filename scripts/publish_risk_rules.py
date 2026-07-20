#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fraud_pipeline import RISK_RULES_TOPIC, risk_rule_event  # noqa: E402
from fraud_pipeline.kafka_client import create_kafka_producer_with_retry  # noqa: E402
from fraud_pipeline.serialization import dumps  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish default fraud risk rules to Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
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
    try:
        count = 0
        for rule in risk_rule_event():
            producer.send(RISK_RULES_TOPIC, key=rule["rule_id"], value=rule)
            count += 1
        producer.flush()
        print(f"Published {count} risk rules to {RISK_RULES_TOPIC}.")
        return 0
    finally:
        producer.close()


if __name__ == "__main__":
    raise SystemExit(main())
