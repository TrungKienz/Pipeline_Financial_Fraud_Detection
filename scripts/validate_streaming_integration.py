#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cassandra.cluster import Cluster  # noqa: E402
from kafka import KafkaConsumer  # noqa: E402

from fraud_pipeline import (  # noqa: E402
    PIPELINE_DEAD_LETTER_TOPIC,
    RECEIVER_STATE_TOPIC,
    SENDER_STATE_TOPIC,
    TRANSACTION_TOPIC,
    build_streaming_validation_cases,
    expected_dead_letter_index,
    iter_logical_source_triplets,
)
from fraud_pipeline.kafka_client import create_kafka_producer_with_retry  # noqa: E402
from fraud_pipeline.serialization import dumps  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish controlled validation cases to Kafka and verify Spark integration outputs."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--source-dir", default=str(ROOT / "data" / "logical_sources"))
    parser.add_argument("--cassandra-host", default="localhost")
    parser.add_argument("--cassandra-port", type=int, default=9042)
    parser.add_argument("--cassandra-keyspace", default="fraud_detection")
    parser.add_argument("--timeout-seconds", type=int, default=45)
    parser.add_argument("--json-out", help="Optional path to save validation summary as JSON.")
    return parser.parse_args()


def load_base_triplet(source_dir: str) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    iterator = iter_logical_source_triplets(source_dir, limit=1)
    try:
        return next(iterator)
    except StopIteration as exc:
        raise SystemExit(
            f"Khong tim thay du lieu nguon trong {source_dir}. Hay chay split_logical_sources.py truoc."
        ) from exc


def create_dead_letter_consumer(bootstrap_servers: str) -> KafkaConsumer:
    return KafkaConsumer(
        PIPELINE_DEAD_LETTER_TOPIC,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        group_id=f"validation-{uuid4().hex}",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def publish_validation_cases(bootstrap_servers: str, cases) -> None:
    producer = create_kafka_producer_with_retry(
        bootstrap_servers,
        value_serializer=lambda value: dumps(value),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    try:
        for case in cases:
            if case.transaction is not None:
                producer.send(TRANSACTION_TOPIC, key=str(case.transaction["event_id"]), value=case.transaction)
            if case.sender is not None:
                producer.send(SENDER_STATE_TOPIC, key=str(case.sender["source_event_id"]), value=case.sender)
            if case.receiver is not None:
                producer.send(RECEIVER_STATE_TOPIC, key=str(case.receiver["source_event_id"]), value=case.receiver)
        producer.flush()
    finally:
        producer.close()


def collect_dead_letters(consumer: KafkaConsumer, expected_pairs: set[tuple[str, str]], timeout_seconds: int) -> set[tuple[str, str]]:
    observed: set[tuple[str, str]] = set()
    deadline = time.time() + timeout_seconds
    while time.time() < deadline and not expected_pairs.issubset(observed):
        records = consumer.poll(timeout_ms=1000)
        for batch in records.values():
            for message in batch:
                payload = message.value if isinstance(message.value, dict) else {}
                source_key = str(payload.get("source_key", ""))
                error = str(payload.get("error", ""))
                if source_key and error:
                    observed.add((source_key, error))
    return observed


def wait_for_cassandra_event(
    host: str,
    port: int,
    keyspace: str,
    day_bucket: datetime.date,
    event_id: str,
    timeout_seconds: int,
) -> bool:
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    try:
        deadline = time.time() + timeout_seconds
        prepared = session.prepare("SELECT event_id FROM transactions_by_day WHERE day_bucket = ?")
        while time.time() < deadline:
            rows = session.execute(prepared, (day_bucket,))
            if any(row.event_id == event_id for row in rows):
                return True
            time.sleep(1.5)
        return False
    finally:
        session.shutdown()
        cluster.shutdown()


def summarize_cases(cases, observed_dead_letters: set[tuple[str, str]], clean_event_found: bool) -> dict[str, object]:
    case_summaries: list[dict[str, object]] = []
    overall_ok = True
    for case in cases:
        if case.expected_dead_letter_error is None:
            found = clean_event_found
            unexpected_dead_letter = any(source_key == case.expected_cassandra_event_id for source_key, _ in observed_dead_letters)
            ok = found and not unexpected_dead_letter
            detail = (
                "transaction da duoc ghi vao Cassandra va khong thay dead-letter bat thuong"
                if ok
                else "khong tim thay transaction trong Cassandra hoac phat hien dead-letter bat thuong"
            )
        else:
            signature = (str(case.expected_dead_letter_source_key), str(case.expected_dead_letter_error))
            ok = signature in observed_dead_letters
            detail = (
                f"da nhan dead-letter {case.expected_dead_letter_error}"
                if ok
                else f"chua nhan dead-letter {case.expected_dead_letter_error}"
            )
        overall_ok = overall_ok and ok
        case_summaries.append(
            {
                "case": case.name,
                "ok": ok,
                "expected_dead_letter_error": case.expected_dead_letter_error,
                "expected_dead_letter_source_key": case.expected_dead_letter_source_key,
                "expected_cassandra_event_id": case.expected_cassandra_event_id,
                "detail": detail,
            }
        )

    return {
        "ok": overall_ok,
        "cases": case_summaries,
        "observed_dead_letters": [
            {"source_key": source_key, "error": error} for source_key, error in sorted(observed_dead_letters)
        ],
        "clean_case_persisted_to_cassandra": clean_event_found,
    }


def main() -> int:
    args = parse_args()
    transaction_payload, sender_payload, receiver_payload = load_base_triplet(args.source_dir)
    run_suffix = uuid4().hex[:10]
    cases = build_streaming_validation_cases(transaction_payload, sender_payload, receiver_payload, run_suffix)
    expected_pairs = set(expected_dead_letter_index(cases).values())
    clean_case = next(case for case in cases if case.name == "clean_integration")
    clean_event_id = str(clean_case.expected_cassandra_event_id)
    clean_event_day = datetime.fromisoformat(str(clean_case.transaction["event_time"]).replace("Z", "+00:00")).date()

    consumer = create_dead_letter_consumer(args.bootstrap_servers)
    try:
        time.sleep(1.0)
        publish_validation_cases(args.bootstrap_servers, cases)
        observed_dead_letters = collect_dead_letters(consumer, expected_pairs, args.timeout_seconds)
    finally:
        consumer.close()

    clean_event_found = wait_for_cassandra_event(
        args.cassandra_host,
        args.cassandra_port,
        args.cassandra_keyspace,
        clean_event_day,
        clean_event_id,
        args.timeout_seconds,
    )

    summary = summarize_cases(cases, observed_dead_letters, clean_event_found)
    summary["run_suffix"] = run_suffix
    summary["bootstrap_servers"] = args.bootstrap_servers
    summary["cassandra"] = f"{args.cassandra_host}:{args.cassandra_port}/{args.cassandra_keyspace}"

    text = json.dumps(summary, indent=2, ensure_ascii=False)
    print(text)
    if args.json_out:
        Path(args.json_out).write_text(text, encoding="utf-8")
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
