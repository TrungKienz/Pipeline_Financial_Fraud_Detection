#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fraud_pipeline import PipelineConfig, RuleEngine, iter_transaction_events, sliding_window_metrics, tumbling_window_metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a small in-memory smoke test over PaySim rows.")
    parser.add_argument("--csv-path", default=str(PipelineConfig().default_csv_path))
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--json-out", help="Optional path to save summary as JSON.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = PipelineConfig()
    engine = RuleEngine(config)
    events = list(iter_transaction_events(args.csv_path, config=config, limit=args.limit))
    sender_index: dict[str, list] = {}
    alerts = []

    for event in events:
        recent = sender_index.get(event.name_orig, [])
        decision = engine.evaluate(event, recent_sender_events=recent)
        if decision.is_alert:
            alerts.append(decision)
        sender_index.setdefault(event.name_orig, []).append(event)

    summary = {
        "csv_path": str(Path(args.csv_path)),
        "events_processed": len(events),
        "alerts_emitted": len(alerts),
        "top_alerts": [
            {
                "event_id": alert.event_id,
                "risk_score": alert.risk_score,
                "severity": alert.severity,
                "triggered_rules": list(alert.triggered_rules),
            }
            for alert in alerts[:10]
        ],
        "tumbling_windows": [
            {
                "window_start": metric.window_start.isoformat(),
                "window_end": metric.window_end.isoformat(),
                "event_count": metric.event_count,
                "fraud_count": metric.fraud_count,
                "total_amount": metric.total_amount,
                "fraud_rate": metric.fraud_rate,
            }
            for metric in tumbling_window_metrics(events, window_seconds=300)[:5]
        ],
        "sliding_windows": [
            {
                "window_start": metric.window_start.isoformat(),
                "window_end": metric.window_end.isoformat(),
                "event_count": metric.event_count,
                "fraud_count": metric.fraud_count,
                "total_amount": metric.total_amount,
                "fraud_rate": metric.fraud_rate,
            }
            for metric in sliding_window_metrics(events, window_seconds=600, slide_seconds=300)[:5]
        ],
    }

    print(json.dumps(summary, indent=2))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
