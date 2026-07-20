#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_PROFILES = (
    "smoke=10x50",
    "medium=50x200",
    "high=100x500",
)

DOCKER_SIZE_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}


@dataclass(frozen=True)
class BenchmarkProfile:
    name: str
    rate: float
    event_count: int


@dataclass
class TrackedEvent:
    event_id: str
    day_bucket: date
    sender_account_id: str
    receiver_account_id: str
    kafka_ack_completed_at_epoch_ns: int
    benchmark_run_id: str
    spark_seen_at_epoch_ns: int | None = None
    cassandra_persisted_at_epoch_ns: int | None = None
    cassandra_visible_at_ns: int | None = None
    spark_seen_at: str | None = None
    cassandra_persisted_at: str | None = None

    def total_latency_ms(self) -> float | None:
        if self.cassandra_persisted_at_epoch_ns is None:
            return None
        return (self.cassandra_persisted_at_epoch_ns - self.kafka_ack_completed_at_epoch_ns) / 1_000_000

    def kafka_to_spark_ms(self) -> float | None:
        if self.spark_seen_at_epoch_ns is None:
            return None
        return (self.spark_seen_at_epoch_ns - self.kafka_ack_completed_at_epoch_ns) / 1_000_000

    def spark_to_cassandra_ms(self) -> float | None:
        if self.spark_seen_at_epoch_ns is None or self.cassandra_persisted_at_epoch_ns is None:
            return None
        return (self.cassandra_persisted_at_epoch_ns - self.spark_seen_at_epoch_ns) / 1_000_000


def datetime_to_epoch_ns(value: datetime) -> int:
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return int(normalized.timestamp() * 1_000_000_000)


class DockerStatsSampler:
    def __init__(self, container_names: list[str], interval_seconds: float) -> None:
        self.container_names = container_names
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="docker-stats-sampler", daemon=True)
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(5.0, self.interval_seconds * 2))

    def _run(self) -> None:
        while not self._stop_event.is_set():
            started = time.time()
            try:
                snapshot = collect_docker_stats(self.container_names)
                self.samples.append(
                    {
                        "captured_at": datetime.now(timezone.utc).isoformat(),
                        "containers": snapshot,
                    }
                )
            except Exception as exc:  # pragma: no cover
                self.errors.append(str(exc))
            elapsed = time.time() - started
            self._stop_event.wait(max(0.0, self.interval_seconds - elapsed))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a standalone end-to-end benchmark from Kafka ingestion to Cassandra visibility."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--source-dir", default=str(ROOT / "Data" / "logical_sources"))
    parser.add_argument("--cassandra-host", default="localhost")
    parser.add_argument("--cassandra-port", type=int, default=9042)
    parser.add_argument("--cassandra-keyspace", default="fraud_detection")
    parser.add_argument(
        "--profiles",
        nargs="*",
        default=list(DEFAULT_PROFILES),
        help="Profiles in name=ratexcount form, for example smoke=10x50 medium=50x200",
    )
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--visibility-timeout", type=float, default=120.0)
    parser.add_argument("--sample-interval", type=float, default=1.0)
    parser.add_argument("--probe-workers", type=int, default=8)
    parser.add_argument(
        "--containers",
        nargs="*",
        help="Optional explicit Docker container names. Defaults to all currently running containers.",
    )
    parser.add_argument(
        "--json-out",
        default=str(ROOT / "end_to_end_benchmark" / "benchmark-results.json"),
        help="Path to save benchmark report as JSON.",
    )
    return parser.parse_args(argv)


def parse_profiles(raw_profiles: Iterable[str]) -> list[BenchmarkProfile]:
    profiles: list[BenchmarkProfile] = []
    for raw in raw_profiles:
        name, payload = raw.split("=", 1)
        rate_text, count_text = payload.lower().split("x", 1)
        profiles.append(BenchmarkProfile(name=name, rate=float(rate_text), event_count=int(count_text)))
    return profiles


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_benchmark_event_id(benchmark_run_id: str, original_event_id: str) -> str:
    return f"bench:{benchmark_run_id}:{original_event_id}"


def run_command(command: list[str]) -> str:
    completed = subprocess.run(command, capture_output=True, text=True, check=True)
    return completed.stdout.strip()


def discover_running_containers(explicit_names: list[str] | None) -> list[str]:
    if explicit_names:
        return explicit_names
    output = run_command(["docker", "ps", "--format", "{{.Names}}"])
    names = [line.strip() for line in output.splitlines() if line.strip()]
    if not names:
        raise RuntimeError("Khong tim thay container nao dang chay de lay resource metrics.")
    return names


def parse_percentage(text: str) -> float:
    return float(text.strip().rstrip("%") or 0.0)


def parse_size_to_bytes(text: str) -> float:
    cleaned = text.strip().replace(" ", "")
    if not cleaned:
        return 0.0
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([A-Za-z]+)", cleaned)
    if not match:
        raise ValueError(f"Khong parse duoc size: {text!r}")
    value = float(match.group(1))
    unit = match.group(2).lower()
    if unit not in DOCKER_SIZE_UNITS:
        raise ValueError(f"Don vi size khong ho tro: {unit}")
    return value * DOCKER_SIZE_UNITS[unit]


def parse_dual_size(text: str) -> tuple[float, float]:
    left, right = [part.strip() for part in text.split("/", 1)]
    return parse_size_to_bytes(left), parse_size_to_bytes(right)


def parse_pids(text: str) -> int:
    cleaned = text.strip()
    return int(cleaned) if cleaned else 0


def collect_docker_stats(container_names: list[str]) -> dict[str, dict[str, Any]]:
    command = ["docker", "stats", "--no-stream", "--format", "{{json .}}", *container_names]
    output = run_command(command)
    stats: dict[str, dict[str, Any]] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        memory_used, memory_limit = parse_dual_size(payload.get("MemUsage", "0B / 0B"))
        network_rx, network_tx = parse_dual_size(payload.get("NetIO", "0B / 0B"))
        block_read, block_write = parse_dual_size(payload.get("BlockIO", "0B / 0B"))
        name = payload.get("Name") or payload.get("Container") or "unknown"
        stats[name] = {
            "cpu_percent": parse_percentage(payload.get("CPUPerc", "0%")),
            "memory_used_bytes": memory_used,
            "memory_limit_bytes": memory_limit,
            "memory_percent": parse_percentage(payload.get("MemPerc", "0%")),
            "network_rx_bytes": network_rx,
            "network_tx_bytes": network_tx,
            "block_read_bytes": block_read,
            "block_write_bytes": block_write,
            "pids": parse_pids(payload.get("PIDs", "0")),
        }
    return stats


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * pct
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def summarize_latencies(latencies_ms: list[float]) -> dict[str, float]:
    if not latencies_ms:
        return {
            "min_ms": 0.0,
            "mean_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_ms": 0.0,
        }
    return {
        "min_ms": min(latencies_ms),
        "mean_ms": statistics.fmean(latencies_ms),
        "p50_ms": percentile(latencies_ms, 0.50),
        "p95_ms": percentile(latencies_ms, 0.95),
        "p99_ms": percentile(latencies_ms, 0.99),
        "max_ms": max(latencies_ms),
    }


def summarize_resource_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {"sample_count": 0, "containers": {}}

    grouped: dict[str, list[dict[str, Any]]] = {}
    for sample in samples:
        for name, stats in sample["containers"].items():
            grouped.setdefault(name, []).append(stats)

    containers: dict[str, Any] = {}
    for name, entries in grouped.items():
        first = entries[0]
        last = entries[-1]
        cpu_values = [entry["cpu_percent"] for entry in entries]
        mem_values = [entry["memory_used_bytes"] for entry in entries]
        mem_pct_values = [entry["memory_percent"] for entry in entries]
        pid_values = [entry["pids"] for entry in entries]
        containers[name] = {
            "sample_count": len(entries),
            "cpu_percent_avg": statistics.fmean(cpu_values),
            "cpu_percent_max": max(cpu_values),
            "memory_used_avg_bytes": statistics.fmean(mem_values),
            "memory_used_max_bytes": max(mem_values),
            "memory_limit_bytes": last["memory_limit_bytes"],
            "memory_percent_avg": statistics.fmean(mem_pct_values),
            "memory_percent_max": max(mem_pct_values),
            "network_rx_delta_bytes": max(0.0, last["network_rx_bytes"] - first["network_rx_bytes"]),
            "network_tx_delta_bytes": max(0.0, last["network_tx_bytes"] - first["network_tx_bytes"]),
            "block_read_delta_bytes": max(0.0, last["block_read_bytes"] - first["block_read_bytes"]),
            "block_write_delta_bytes": max(0.0, last["block_write_bytes"] - first["block_write_bytes"]),
            "pids_max": max(pid_values),
        }

    return {
        "sample_count": len(samples),
        "first_sample_at": samples[0]["captured_at"],
        "last_sample_at": samples[-1]["captured_at"],
        "containers": containers,
    }


def build_cassandra_session(host: str, port: int, keyspace: str):
    from cassandra.cluster import Cluster

    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    session.default_timeout = 10
    return cluster, session


def build_probe_statements(session: Any) -> dict[str, Any]:
    return {
        "benchmark_timing": session.prepare(
            """
            SELECT spark_seen_at, cassandra_persisted_at
            FROM benchmark_stage_timings_by_run
            WHERE benchmark_run_id = ? AND event_id = ?
            """
        ),
    }


def probe_event_visibility(session: Any, statements: dict[str, Any], tracked: TrackedEvent) -> TrackedEvent:
    row = session.execute(statements["benchmark_timing"], (tracked.benchmark_run_id, tracked.event_id)).one()
    if row is not None:
        if tracked.spark_seen_at is None and row.spark_seen_at is not None:
            spark_seen_at = row.spark_seen_at.replace(tzinfo=timezone.utc)
            tracked.spark_seen_at = spark_seen_at.isoformat()
            tracked.spark_seen_at_epoch_ns = datetime_to_epoch_ns(spark_seen_at)
        if tracked.cassandra_persisted_at is None and row.cassandra_persisted_at is not None:
            cassandra_persisted_at = row.cassandra_persisted_at.replace(tzinfo=timezone.utc)
            tracked.cassandra_persisted_at = cassandra_persisted_at.isoformat()
            tracked.cassandra_persisted_at_epoch_ns = datetime_to_epoch_ns(cassandra_persisted_at)
            tracked.cassandra_visible_at_ns = time.perf_counter_ns()
    return tracked


def publish_profile(
    profile: BenchmarkProfile,
    bootstrap_servers: str,
    source_dir: str,
) -> tuple[list[TrackedEvent], float, str, str]:
    from fraud_pipeline.kafka_client import create_kafka_producer_with_retry
    from fraud_pipeline.serialization import dumps
    from fraud_pipeline.source_csv import iter_logical_source_triplets
    from fraud_pipeline.topics import RECEIVER_STATE_TOPIC, SENDER_STATE_TOPIC, TRANSACTION_TOPIC

    producer = create_kafka_producer_with_retry(
        bootstrap_servers,
        value_serializer=lambda value: dumps(value),
        key_serializer=lambda value: value.encode("utf-8"),
    )

    tracked_events: list[TrackedEvent] = []
    benchmark_run_id = f"{profile.name}-{int(time.time() * 1000)}"
    started_at = now_utc_iso()
    started_perf_ns = time.perf_counter_ns()
    try:
        for index, (transaction, sender_state, receiver_state) in enumerate(
            iter_logical_source_triplets(source_dir, limit=profile.event_count)
        ):
            transaction = dict(transaction)
            sender_state = dict(sender_state)
            receiver_state = dict(receiver_state)
            benchmark_event_id = make_benchmark_event_id(benchmark_run_id, str(transaction["event_id"]))
            transaction["event_id"] = benchmark_event_id
            sender_state["source_event_id"] = benchmark_event_id
            receiver_state["source_event_id"] = benchmark_event_id
            sender_state["event_id"] = f"{benchmark_event_id}:sender"
            receiver_state["event_id"] = f"{benchmark_event_id}:receiver"
            if profile.rate > 0:
                target_elapsed = index / profile.rate
                current_elapsed = (time.perf_counter_ns() - started_perf_ns) / 1_000_000_000
                sleep_seconds = target_elapsed - current_elapsed
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

            tx_future = producer.send(TRANSACTION_TOPIC, key=transaction["event_id"], value=transaction)
            sender_future = producer.send(SENDER_STATE_TOPIC, key=sender_state["source_event_id"], value=sender_state)
            receiver_future = producer.send(RECEIVER_STATE_TOPIC, key=receiver_state["source_event_id"], value=receiver_state)

            tx_future.get(timeout=15)
            tx_ack_ns = time.time_ns()
            sender_future.get(timeout=15)
            sender_ack_ns = time.time_ns()
            receiver_future.get(timeout=15)
            receiver_ack_ns = time.time_ns()

            tracked_events.append(
                TrackedEvent(
                    event_id=benchmark_event_id,
                    day_bucket=date.fromisoformat(str(transaction["event_time"]).split("T", 1)[0]),
                    sender_account_id=str(transaction["nameOrig"]),
                    receiver_account_id=str(transaction["nameDest"]),
                    kafka_ack_completed_at_epoch_ns=max(tx_ack_ns, sender_ack_ns, receiver_ack_ns),
                    benchmark_run_id=benchmark_run_id,
                )
            )
        producer.flush()
    finally:
        producer.close()

    finished_perf_ns = time.perf_counter_ns()
    finished_at = now_utc_iso()
    publish_duration_seconds = (finished_perf_ns - started_perf_ns) / 1_000_000_000
    return tracked_events, publish_duration_seconds, started_at, finished_at


def wait_for_cassandra_visibility(
    session: Any,
    statements: dict[str, Any],
    tracked_events: list[TrackedEvent],
    poll_interval: float,
    visibility_timeout: float,
    probe_workers: int,
) -> tuple[list[TrackedEvent], list[str], float]:
    pending = {event.event_id: event for event in tracked_events}
    deadline = time.monotonic() + visibility_timeout
    wait_started_ns = time.perf_counter_ns()

    while pending and time.monotonic() < deadline:
        current_batch = list(pending.values())
        with ThreadPoolExecutor(max_workers=max(1, min(probe_workers, len(current_batch)))) as executor:
            futures = [executor.submit(probe_event_visibility, session, statements, event) for event in current_batch]
            for future in as_completed(futures):
                updated = future.result()
                if updated.cassandra_visible_at_ns is not None:
                    pending.pop(updated.event_id, None)
        if pending:
            time.sleep(poll_interval)

    total_wait_seconds = (time.perf_counter_ns() - wait_started_ns) / 1_000_000_000
    timed_out_event_ids = sorted(pending)
    return tracked_events, timed_out_event_ids, total_wait_seconds


def run_profile(
    profile: BenchmarkProfile,
    bootstrap_servers: str,
    source_dir: str,
    session: Any,
    statements: dict[str, Any],
    poll_interval: float,
    visibility_timeout: float,
    sample_interval: float,
    probe_workers: int,
    container_names: list[str],
) -> dict[str, Any]:
    sampler = DockerStatsSampler(container_names=container_names, interval_seconds=sample_interval)
    sampler.start()
    run_started_at = now_utc_iso()

    try:
        tracked_events, publish_duration_seconds, publish_started_at, publish_finished_at = publish_profile(
            profile=profile,
            bootstrap_servers=bootstrap_servers,
            source_dir=source_dir,
        )
        tracked_events, timed_out_event_ids, wait_duration_seconds = wait_for_cassandra_visibility(
            session=session,
            statements=statements,
            tracked_events=tracked_events,
            poll_interval=poll_interval,
            visibility_timeout=visibility_timeout,
            probe_workers=probe_workers,
        )
    finally:
        sampler.stop()

    run_finished_at = now_utc_iso()
    kafka_to_spark_ms = [event.kafka_to_spark_ms() for event in tracked_events if event.kafka_to_spark_ms() is not None]
    spark_to_cassandra_ms = [event.spark_to_cassandra_ms() for event in tracked_events if event.spark_to_cassandra_ms() is not None]
    total_latencies_ms = [event.total_latency_ms() for event in tracked_events if event.total_latency_ms() is not None]
    successful_events = len(total_latencies_ms)
    timeout_events = len(timed_out_event_ids)
    throughput_eps = (successful_events / publish_duration_seconds) if publish_duration_seconds > 0 else 0.0

    return {
        "benchmark_run_id": tracked_events[0].benchmark_run_id if tracked_events else "",
        "name": profile.name,
        "target_rate_eps": profile.rate,
        "requested_event_count": profile.event_count,
        "published_event_count": len(tracked_events),
        "successful_event_count": successful_events,
        "timed_out_event_count": timeout_events,
        "timed_out_event_ids": timed_out_event_ids,
        "started_at": run_started_at,
        "finished_at": run_finished_at,
        "publish_started_at": publish_started_at,
        "publish_finished_at": publish_finished_at,
        "publish_duration_seconds": publish_duration_seconds,
        "visibility_wait_seconds": wait_duration_seconds,
        "effective_publish_throughput_eps": throughput_eps,
        "stage_latency_summary": {
            "kafka_to_spark_ms": summarize_latencies([value for value in kafka_to_spark_ms if value is not None]),
            "spark_to_cassandra_ms": summarize_latencies([value for value in spark_to_cassandra_ms if value is not None]),
            "kafka_to_cassandra_ms": summarize_latencies([value for value in total_latencies_ms if value is not None]),
        },
        "resource_summary": summarize_resource_samples(sampler.samples),
        "resource_sampler_errors": list(sampler.errors),
        "events": [
            {
                **{
                    **asdict(event),
                    "day_bucket": event.day_bucket.isoformat(),
                },
                "kafka_to_spark_ms": event.kafka_to_spark_ms(),
                "spark_to_cassandra_ms": event.spark_to_cassandra_ms(),
                "kafka_to_cassandra_ms": event.total_latency_ms(),
            }
            for event in tracked_events
        ],
    }


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    profiles = parse_profiles(args.profiles)
    container_names = discover_running_containers(args.containers)
    cluster, session = build_cassandra_session(args.cassandra_host, args.cassandra_port, args.cassandra_keyspace)
    statements = build_probe_statements(session)

    try:
        results = []
        for profile in profiles:
            print(
                f"[benchmark] profile={profile.name} rate={profile.rate} eps events={profile.event_count}",
                flush=True,
            )
            profile_result = run_profile(
                profile=profile,
                bootstrap_servers=args.bootstrap_servers,
                source_dir=args.source_dir,
                session=session,
                statements=statements,
                poll_interval=args.poll_interval,
                visibility_timeout=args.visibility_timeout,
                sample_interval=args.sample_interval,
                probe_workers=args.probe_workers,
                container_names=container_names,
            )
            results.append(profile_result)
            print(
                json.dumps(
                    {
                        "profile": profile.name,
                        "successful_event_count": profile_result["successful_event_count"],
                        "timed_out_event_count": profile_result["timed_out_event_count"],
                        "stage_latency_summary": profile_result["stage_latency_summary"],
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

        report = {
            "generated_at": now_utc_iso(),
            "bootstrap_servers": args.bootstrap_servers,
            "source_dir": str(Path(args.source_dir)),
            "cassandra": {
                "host": args.cassandra_host,
                "port": args.cassandra_port,
                "keyspace": args.cassandra_keyspace,
            },
            "container_names": container_names,
            "profiles": results,
        }
    finally:
        session.shutdown()
        cluster.shutdown()

    output_path = Path(args.json_out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved benchmark report to {output_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
