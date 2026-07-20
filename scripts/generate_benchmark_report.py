#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Benchmark Report</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 32px; color: #111; background: #f6f4ef; }}
    h1, h2 {{ margin-bottom: 8px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 20px 0; background: white; }}
    th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
    th {{ background: #f0dfb5; }}
    .chart {{ display: grid; gap: 10px; margin: 24px 0; }}
    .bar-row {{ display: grid; grid-template-columns: 180px 1fr 120px; align-items: center; gap: 12px; }}
    .bar {{ height: 22px; background: linear-gradient(90deg, #244b5a, #5fa8a0); border-radius: 6px; }}
    .note {{ color: #555; }}
  </style>
</head>
<body>
  <h1>Local Benchmark Report</h1>
  <p class="note">Seed events: {seed_event_count}</p>
  <h2>Summary Table</h2>
  {table_html}
  <h2>Throughput (events/sec)</h2>
  <div class="chart">{throughput_chart}</div>
  <h2>Total Runtime (ms)</h2>
  <div class="chart">{latency_chart}</div>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a lightweight HTML benchmark report from JSON results.")
    parser.add_argument("--input", default="benchmark-results.json")
    parser.add_argument("--output", default="benchmark-report.html")
    return parser.parse_args()


def build_table(rows: list[dict]) -> str:
    header = (
        "<table><thead><tr>"
        "<th>Profile</th><th>Events</th><th>Alerts</th><th>Total ms</th>"
        "<th>Throughput eps</th><th>Rule p50 ms</th><th>Rule p95 ms</th>"
        "</tr></thead><tbody>"
    )
    body = "".join(
        f"<tr><td>{row['profile']}</td><td>{row['event_count']}</td><td>{row['alerts_emitted']}</td>"
        f"<td>{row['total_ms']}</td><td>{row['throughput_eps']}</td>"
        f"<td>{row['rule_p50_ms']}</td><td>{row['rule_p95_ms']}</td></tr>"
        for row in rows
    )
    return header + body + "</tbody></table>"


def build_bar_chart(rows: list[dict], metric: str) -> str:
    max_value = max((float(row[metric]) for row in rows), default=1.0)
    parts: list[str] = []
    for row in rows:
        value = float(row[metric])
        width = 0 if max_value == 0 else (value / max_value) * 100
        parts.append(
            f"<div class='bar-row'><div>{row['profile']}</div><div class='bar' style='width:{width:.2f}%'></div><div>{value:.3f}</div></div>"
        )
    return "".join(parts)


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    rows = payload.get("profiles", [])
    html = HTML_TEMPLATE.format(
        seed_event_count=payload.get("seed_event_count", 0),
        table_html=build_table(rows),
        throughput_chart=build_bar_chart(rows, "throughput_eps"),
        latency_chart=build_bar_chart(rows, "total_ms"),
    )
    output_path.write_text(html, encoding="utf-8")
    print(f"Saved HTML report to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
