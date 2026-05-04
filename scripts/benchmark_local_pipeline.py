#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fraud_pipeline import PipelineConfig, iter_transaction_events  # noqa: E402
from fraud_pipeline.benchmark import (  # noqa: E402
    BenchmarkProfile,
    DEFAULT_PROFILES,
    run_benchmark_suite,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local in-memory benchmark profiles before full-stream integration.")
    parser.add_argument("--csv-path", default=str(PipelineConfig().default_csv_path))
    parser.add_argument("--seed-limit", type=int, default=200)
    parser.add_argument(
        "--profiles",
        nargs="*",
        help="Optional custom profiles in name=count form, e.g. smoke=500 medium=3000",
    )
    parser.add_argument("--json-out", default="benchmark-results.json")
    return parser.parse_args()


def parse_profiles(raw_profiles: list[str] | None) -> tuple[BenchmarkProfile, ...]:
    if not raw_profiles:
        return DEFAULT_PROFILES
    profiles: list[BenchmarkProfile] = []
    for item in raw_profiles:
        name, count = item.split("=", 1)
        profiles.append(BenchmarkProfile(name=name, event_count=int(count)))
    return tuple(profiles)


def main() -> int:
    args = parse_args()
    profiles = parse_profiles(args.profiles)
    config = PipelineConfig()
    seed_events = list(iter_transaction_events(args.csv_path, config=config, limit=args.seed_limit))
    results = run_benchmark_suite(seed_events, profiles=profiles, config=config)
    payload = {
        "seed_event_count": len(seed_events),
        "profiles": [result.to_dict() for result in results],
    }
    print(json.dumps(payload, indent=2))
    Path(args.json_out).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nSaved benchmark results to {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
