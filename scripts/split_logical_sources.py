#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fraud_pipeline import PipelineConfig, split_integrated_csv_to_logical_sources  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split the integrated PaySim CSV into 3 independent logical source CSVs."
    )
    parser.add_argument("--csv-path", default=str(PipelineConfig().default_csv_path))
    parser.add_argument("--output-dir", default=str(ROOT / "data" / "logical_sources"))
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument("--json-out", help="Optional path to save the split summary as JSON.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = split_integrated_csv_to_logical_sources(
        args.csv_path,
        args.output_dir,
        config=PipelineConfig(),
        limit=args.max_events,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nSaved split summary to {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
