#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run 3 independent source producers in parallel for transaction, sender_state, receiver_state."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--source-dir", default=str(ROOT / "data" / "logical_sources"))
    parser.add_argument("--max-events", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=100.0, help="Correlated events per second per source.")
    parser.add_argument(
        "--startup-stagger-seconds",
        type=float,
        default=0.2,
        help="Small stagger between starting producers to make logs easier to read.",
    )
    return parser.parse_args()


def build_commands(args: argparse.Namespace) -> list[list[str]]:
    common = [
        "--bootstrap-servers",
        args.bootstrap_servers,
        "--source-dir",
        args.source_dir,
        "--max-events",
        str(args.max_events),
        "--rate",
        str(args.rate),
    ]
    return [
        [sys.executable, str(ROOT / "scripts" / "publish_transaction_source.py"), *common],
        [sys.executable, str(ROOT / "scripts" / "publish_sender_state_source.py"), *common],
        [sys.executable, str(ROOT / "scripts" / "publish_receiver_state_source.py"), *common],
    ]


def terminate_processes(processes: list[subprocess.Popen]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        if process.poll() is None:
            process.wait(timeout=10)


def main() -> int:
    args = parse_args()
    processes: list[subprocess.Popen] = []
    try:
        for command in build_commands(args):
            processes.append(subprocess.Popen(command, cwd=ROOT))
            if args.startup_stagger_seconds > 0:
                time.sleep(args.startup_stagger_seconds)

        exit_codes = [process.wait() for process in processes]
        failed = [str(code) for code in exit_codes if code != 0]
        if failed:
            raise SystemExit(f"Co producer that bai. Exit codes: {', '.join(failed)}")
        print("All 3 logical source producers completed successfully.")
        return 0
    except BaseException:
        terminate_processes(processes)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
