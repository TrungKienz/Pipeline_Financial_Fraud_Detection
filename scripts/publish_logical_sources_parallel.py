#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def stream_logs(process, name):
    for line in iter(process.stdout.readline, ""):
        print(f"[{name}] {line.strip()}")
    process.stdout.close()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run 3 independent source producers in parallel.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--source-dir", default=str(ROOT / "Data" / "logical_sources"))
    parser.add_argument("--max-events", type=int, default=10000000)
    parser.add_argument("--rate", type=float, default=100.0)
    return parser.parse_args()

def main() -> int:
    args = parse_args()
    common = [
        "--bootstrap-servers", args.bootstrap_servers,
        "--source-dir", args.source_dir,
        "--max-events", str(args.max_events),
        "--rate", str(args.rate),
    ]
    
    commands = {
        "TX": [sys.executable, "-u", str(ROOT / "scripts" / "publish_transaction_source.py"), *common],
        "SENDER": [sys.executable, "-u", str(ROOT / "scripts" / "publish_sender_state_source.py"), *common],
        "RECEIVER": [sys.executable, "-u", str(ROOT / "scripts" / "publish_receiver_state_source.py"), *common],
    }

    processes = []
    threads = []

    for name, cmd in commands.items():
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        processes.append(p)
        t = threading.Thread(target=stream_logs, args=(p, name))
        t.start()
        threads.append(t)

    try:
        for p in processes:
            p.wait()
        for t in threads:
            t.join()
        print("All producers finished.")
        return 0
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
        return 1

if __name__ == "__main__":
    sys.exit(main())
