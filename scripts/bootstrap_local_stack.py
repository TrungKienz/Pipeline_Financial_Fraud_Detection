#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap the local stack with Kafka topics, rules, and sample events.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--csv-path", default=str(ROOT / "Data" / "archive (2)" / "PS_20174392719_1491204439457_log.csv"))
    parser.add_argument("--source-dir", default=str(ROOT / "Data" / "logical_sources"))
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument(
        "--producer-mode",
        choices=("parallel", "combined"),
        default="parallel",
        help="parallel chay 3 producer doc lap song song. combined dung 1 script publish ca 3 nguon.",
    )
    return parser.parse_args()


def ensure_local_dependencies() -> None:
    required_modules = {
        "kafka": "kafka-python",
    }
    missing = [
        package_name
        for module_name, package_name in required_modules.items()
        if importlib.util.find_spec(module_name) is None
    ]
    if missing:
        requirement_file = ROOT / "requirements-local.txt"
        packages = " ".join(sorted(set(missing)))
        raise SystemExit(
            "Moi truong Python local dang thieu dependency de publish vao Kafka.\n"
            f"Python dang dung: {sys.executable}\n"
            f"Cai nhanh: {sys.executable} -m pip install -r {requirement_file}\n"
            f"Hoac cai rieng: {sys.executable} -m pip install {packages}"
        )


def main() -> int:
    args = parse_args()
    ensure_local_dependencies()
    publish_command = (
        [
            sys.executable,
            str(ROOT / "scripts" / "publish_logical_sources_parallel.py"),
            "--bootstrap-servers",
            args.bootstrap_servers,
            "--source-dir",
            args.source_dir,
            "--max-events",
            str(args.max_events),
            "--rate",
            str(args.rate),
        ]
        if args.producer_mode == "parallel"
        else [
            sys.executable,
            str(ROOT / "scripts" / "publish_transactions.py"),
            "--bootstrap-servers",
            args.bootstrap_servers,
            "--source-dir",
            args.source_dir,
            "--max-events",
            str(args.max_events),
            "--rate",
            str(args.rate),
        ]
    )
    commands = [
        [
            sys.executable,
            str(ROOT / "scripts" / "publish_risk_rules.py"),
            "--bootstrap-servers",
            args.bootstrap_servers,
        ],
        publish_command,
    ]
    for command in commands:
        subprocess.run(command, check=True, cwd=ROOT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
