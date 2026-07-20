#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class ProfilePoint:
    source_file: str
    family: str
    trigger_label: str
    name: str
    rate_eps: float
    requested_event_count: int
    successful_event_count: int
    timed_out_event_count: int
    success_rate: float
    timeout_rate: float
    kafka_to_spark_mean_ms: float
    kafka_to_spark_p95_ms: float
    spark_to_cassandra_mean_ms: float
    spark_to_cassandra_p95_ms: float
    e2e_mean_ms: float
    e2e_p95_ms: float
    e2e_max_ms: float
    resource_summary: dict[str, Any]


def infer_trigger_label(file_name: str) -> str:
    if "trigger-2s" in file_name:
        return "2s trigger"
    if "20s" in file_name:
        return "20s trigger"
    return "unknown"


def infer_family(file_name: str) -> str:
    lowered = file_name.lower()
    if "low-load" in lowered:
        return "low-load"
    if "high-load" in lowered or "trigger-2s-high" in lowered:
        return "high-load"
    if "extreme" in lowered:
        return "extreme"
    return "other"


def load_profile_points(results_dir: Path) -> list[ProfilePoint]:
    points: list[ProfilePoint] = []
    for path in sorted(results_dir.glob("benchmark-results*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        trigger_label = infer_trigger_label(path.name)
        family = infer_family(path.name)
        for profile in payload.get("profiles", []):
            requested = int(profile["requested_event_count"])
            successful = int(profile["successful_event_count"])
            timed_out = int(profile["timed_out_event_count"])
            stage = profile["stage_latency_summary"]
            points.append(
                ProfilePoint(
                    source_file=path.name,
                    family=family,
                    trigger_label=trigger_label,
                    name=str(profile["name"]),
                    rate_eps=float(profile["target_rate_eps"]),
                    requested_event_count=requested,
                    successful_event_count=successful,
                    timed_out_event_count=timed_out,
                    success_rate=(successful / requested * 100.0) if requested else 0.0,
                    timeout_rate=(timed_out / requested * 100.0) if requested else 0.0,
                    kafka_to_spark_mean_ms=float(stage["kafka_to_spark_ms"]["mean_ms"]),
                    kafka_to_spark_p95_ms=float(stage["kafka_to_spark_ms"]["p95_ms"]),
                    spark_to_cassandra_mean_ms=float(stage["spark_to_cassandra_ms"]["mean_ms"]),
                    spark_to_cassandra_p95_ms=float(stage["spark_to_cassandra_ms"]["p95_ms"]),
                    e2e_mean_ms=float(stage["kafka_to_cassandra_ms"]["mean_ms"]),
                    e2e_p95_ms=float(stage["kafka_to_cassandra_ms"]["p95_ms"]),
                    e2e_max_ms=float(stage["kafka_to_cassandra_ms"]["max_ms"]),
                    resource_summary=dict(profile.get("resource_summary", {})),
                )
            )
    return sorted(points, key=lambda item: (item.rate_eps, item.trigger_label, item.name))


def container_metric(point: ProfilePoint, container_name: str, metric_name: str) -> float:
    containers = point.resource_summary.get("containers", {})
    container = containers.get(container_name, {})
    return float(container.get(metric_name, 0.0) or 0.0)


def metric_series_by_trigger(
    by_trigger: dict[str, list[ProfilePoint]],
    unique_rates: list[int],
    container_name: str,
    metric_name: str,
    scale: float = 1.0,
) -> list[tuple[str, list[float]]]:
    return [
        (
            trigger_label,
            [
                next(
                    (
                        container_metric(item, container_name, metric_name) / scale
                        for item in items
                        if int(item.rate_eps) == rate
                    ),
                    0.0,
                )
                for rate in unique_rates
            ],
        )
        for trigger_label, items in by_trigger.items()
    ]


def has_matplotlib() -> bool:
    try:
        import matplotlib.pyplot as plt  # noqa: F401
        return True
    except ModuleNotFoundError:
        return False


def group_by_trigger(points: list[ProfilePoint]) -> dict[str, list[ProfilePoint]]:
    grouped: dict[str, list[ProfilePoint]] = {}
    for point in points:
        grouped.setdefault(point.trigger_label, []).append(point)
    for trigger_label in grouped:
        grouped[trigger_label] = sorted(grouped[trigger_label], key=lambda item: item.rate_eps)
    return grouped


def style_axes(ax, title: str, xlabel: str, ylabel: str) -> None:
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, linestyle="--", alpha=0.3)


def annotate_points(ax, xs: list[float], ys: list[float], labels: list[str]) -> None:
    for x_value, y_value, label in zip(xs, ys, labels):
        ax.annotate(label, (x_value, y_value), textcoords="offset points", xytext=(0, 6), ha="center", fontsize=8)


def plot_success_rate(points: list[ProfilePoint], output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    grouped = group_by_trigger(points)
    fig, ax = plt.subplots(figsize=(10, 6))
    for trigger_label, items in grouped.items():
        xs = [item.rate_eps for item in items]
        ys = [item.success_rate for item in items]
        labels = [item.name for item in items]
        ax.plot(xs, ys, marker="o", linewidth=2, label=trigger_label)
        annotate_points(ax, xs, ys, labels)

    style_axes(ax, "Success Rate vs Input Rate", "Input rate (events/sec)", "Success rate (%)")
    ax.set_ylim(0, 105)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "success_rate_vs_rate.png", dpi=200)
    plt.close(fig)


def plot_timeout_count(points: list[ProfilePoint], output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    grouped = group_by_trigger(points)
    fig, ax = plt.subplots(figsize=(10, 6))
    for trigger_label, items in grouped.items():
        xs = [item.rate_eps for item in items]
        ys = [item.timed_out_event_count for item in items]
        labels = [item.name for item in items]
        ax.plot(xs, ys, marker="o", linewidth=2, label=trigger_label)
        annotate_points(ax, xs, ys, labels)

    style_axes(ax, "Timed-out Events vs Input Rate", "Input rate (events/sec)", "Timed-out events")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "timeout_count_vs_rate.png", dpi=200)
    plt.close(fig)


def plot_e2e_latency(points: list[ProfilePoint], output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    grouped = group_by_trigger(points)
    fig, ax = plt.subplots(figsize=(10, 6))
    for trigger_label, items in grouped.items():
        xs = [item.rate_eps for item in items]
        mean_values = [item.e2e_mean_ms / 1000.0 for item in items]
        p95_values = [item.e2e_p95_ms / 1000.0 for item in items]
        ax.plot(xs, mean_values, marker="o", linewidth=2, label=f"{trigger_label} mean")
        ax.plot(xs, p95_values, marker="s", linewidth=2, linestyle="--", label=f"{trigger_label} p95")

    style_axes(ax, "End-to-End Latency vs Input Rate", "Input rate (events/sec)", "Latency (seconds)")
    ax.legend(ncol=2)
    fig.tight_layout()
    fig.savefig(output_dir / "e2e_latency_vs_rate.png", dpi=200)
    plt.close(fig)


def plot_stage_latency(points: list[ProfilePoint], output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    grouped = group_by_trigger(points)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharex=True)

    for trigger_label, items in grouped.items():
        xs = [item.rate_eps for item in items]
        kafka_to_spark = [item.kafka_to_spark_mean_ms / 1000.0 for item in items]
        spark_to_cassandra = [item.spark_to_cassandra_mean_ms for item in items]
        axes[0].plot(xs, kafka_to_spark, marker="o", linewidth=2, label=trigger_label)
        axes[1].plot(xs, spark_to_cassandra, marker="o", linewidth=2, label=trigger_label)

    style_axes(axes[0], "Kafka -> Spark Mean Latency", "Input rate (events/sec)", "Latency (seconds)")
    style_axes(axes[1], "Spark -> Cassandra Mean Latency", "Input rate (events/sec)", "Latency (ms)")
    axes[0].legend()
    axes[1].legend()
    fig.tight_layout()
    fig.savefig(output_dir / "stage_latency_vs_rate.png", dpi=200)
    plt.close(fig)


def plot_success_vs_latency(points: list[ProfilePoint], output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 6))
    color_map = {
        "20s trigger": "tab:blue",
        "2s trigger": "tab:orange",
        "unknown": "tab:gray",
    }
    for trigger_label, items in group_by_trigger(points).items():
        xs = [item.success_rate for item in items]
        ys = [item.e2e_p95_ms / 1000.0 for item in items]
        labels = [f"{item.name} ({int(item.rate_eps)} eps)" for item in items]
        ax.scatter(xs, ys, s=70, label=trigger_label, color=color_map.get(trigger_label, "tab:gray"))
        annotate_points(ax, xs, ys, labels)

    style_axes(ax, "Success Rate vs E2E P95 Latency", "Success rate (%)", "E2E P95 latency (seconds)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "success_rate_vs_e2e_p95.png", dpi=200)
    plt.close(fig)


def write_summary_table(points: list[ProfilePoint], output_dir: Path) -> None:
    lines = [
        "# Benchmark Chart Summary",
        "",
        "| Trigger | Profile | Rate (eps) | Events | Success | Timeout | Success Rate | Kafka->Spark Mean (s) | Spark->Cassandra Mean (ms) | E2E Mean (s) | E2E P95 (s) | Spark CPU Avg (%) | Spark Mem Max (MB) | Cassandra CPU Avg (%) | Kafka CPU Avg (%) |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in sorted(points, key=lambda point: (point.trigger_label, point.rate_eps, point.name)):
        lines.append(
            "| {trigger} | {name} | {rate:.0f} | {events} | {success} | {timeout} | {success_rate:.2f}% | {k2s:.2f} | {s2c:.2f} | {e2e_mean:.2f} | {e2e_p95:.2f} | {spark_cpu:.2f} | {spark_mem:.2f} | {cass_cpu:.2f} | {kafka_cpu:.2f} |".format(
                trigger=item.trigger_label,
                name=item.name,
                rate=item.rate_eps,
                events=item.requested_event_count,
                success=item.successful_event_count,
                timeout=item.timed_out_event_count,
                success_rate=item.success_rate,
                k2s=item.kafka_to_spark_mean_ms / 1000.0,
                s2c=item.spark_to_cassandra_mean_ms,
                e2e_mean=item.e2e_mean_ms / 1000.0,
                e2e_p95=item.e2e_p95_ms / 1000.0,
                spark_cpu=container_metric(item, "spark-fraud-detection", "cpu_percent_avg"),
                spark_mem=container_metric(item, "spark-fraud-detection", "memory_used_max_bytes") / (1024 * 1024),
                cass_cpu=container_metric(item, "cassandra", "cpu_percent_avg"),
                kafka_cpu=container_metric(item, "kafka", "cpu_percent_avg"),
            )
        )
    (output_dir / "CHART_SUMMARY.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def mermaid_xychart(title: str, x_label: str, y_label: str, xs: list[str], series: list[tuple[str, list[float]]]) -> str:
    lines = [
        "```mermaid",
        "xychart-beta",
        f'    title "{title}"',
        f'    x-axis "{x_label}" [{", ".join(xs)}]',
        f'    y-axis "{y_label}" 0 --> {max(1, int(max(max(values) for _, values in series) * 1.1))}',
    ]
    for label, values in series:
        rendered = ", ".join(f"{value:.2f}" if isinstance(value, float) and not float(value).is_integer() else f"{int(value)}" for value in values)
        lines.append(f'    line "{label}" [{rendered}]')
    lines.append("```")
    return "\n".join(lines)


def write_mermaid_report(points: list[ProfilePoint], output_dir: Path) -> None:
    by_trigger = group_by_trigger(points)
    sections: list[str] = ["# Benchmark Charts", "", "Bao cao nay duoc sinh tu cac file JSON benchmark trong `end_to_end_benchmark/`."]

    unique_rates = sorted({int(item.rate_eps) for item in points})
    xs = [str(rate) for rate in unique_rates]
    sections.extend(
        [
            "",
            "## Success Rate",
            "",
            mermaid_xychart(
                "Success Rate vs Input Rate",
                "Rate (eps)",
                "Success Rate (%)",
                xs,
                [
                    (
                        trigger_label,
                        [
                            next((item.success_rate for item in items if int(item.rate_eps) == rate), 0.0)
                            for rate in unique_rates
                        ],
                    )
                    for trigger_label, items in by_trigger.items()
                ],
            ),
        ]
    )

    for trigger_label, items in by_trigger.items():
        xs_trigger = [str(int(item.rate_eps)) for item in items]
        sections.extend(
            [
                "",
                f"## {trigger_label}",
                "",
                mermaid_xychart(
                    f"{trigger_label} - E2E Mean vs P95",
                    "Rate (eps)",
                    "Latency (s)",
                    xs_trigger,
                    [
                        ("E2E Mean", [item.e2e_mean_ms / 1000.0 for item in items]),
                        ("E2E P95", [item.e2e_p95_ms / 1000.0 for item in items]),
                    ],
                ),
                "",
                mermaid_xychart(
                    f"{trigger_label} - Stage Mean Latency",
                    "Rate (eps)",
                    "Latency",
                    xs_trigger,
                    [
                        ("Kafka->Spark (s)", [item.kafka_to_spark_mean_ms / 1000.0 for item in items]),
                        ("Spark->Cassandra (ms)", [item.spark_to_cassandra_mean_ms for item in items]),
                    ],
                ),
                "",
                mermaid_xychart(
                    f"{trigger_label} - Timed-out Events",
                    "Rate (eps)",
                    "Timeout count",
                    xs_trigger,
                    [("Timed-out events", [item.timed_out_event_count for item in items])],
                ),
            ]
        )

    sections.extend(
        [
            "",
            "## Resource Usage",
            "",
            mermaid_xychart(
                "Spark App CPU Avg vs Input Rate",
                "Rate (eps)",
                "CPU (%)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "spark-fraud-detection", "cpu_percent_avg"),
            ),
            "",
            mermaid_xychart(
                "Spark App Memory Max vs Input Rate",
                "Rate (eps)",
                "Memory (MB)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "spark-fraud-detection", "memory_used_max_bytes", 1024 * 1024),
            ),
            "",
            mermaid_xychart(
                "Cassandra CPU Avg vs Input Rate",
                "Rate (eps)",
                "CPU (%)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "cassandra", "cpu_percent_avg"),
            ),
            "",
            mermaid_xychart(
                "Kafka CPU Avg vs Input Rate",
                "Rate (eps)",
                "CPU (%)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "kafka", "cpu_percent_avg"),
            ),
            "",
            mermaid_xychart(
                "Spark Worker CPU Avg vs Input Rate",
                "Rate (eps)",
                "CPU (%)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "spark-worker", "cpu_percent_avg"),
            ),
            "",
            mermaid_xychart(
                "Spark Worker Memory Max vs Input Rate",
                "Rate (eps)",
                "Memory (MB)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "spark-worker", "memory_used_max_bytes", 1024 * 1024),
            ),
            "",
            mermaid_xychart(
                "Cassandra Memory Max vs Input Rate",
                "Rate (eps)",
                "Memory (MB)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "cassandra", "memory_used_max_bytes", 1024 * 1024),
            ),
            "",
            mermaid_xychart(
                "Spark App Network RX Delta vs Input Rate",
                "Rate (eps)",
                "Network RX (MB)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "spark-fraud-detection", "network_rx_delta_bytes", 1024 * 1024),
            ),
            "",
            mermaid_xychart(
                "Spark App Network TX Delta vs Input Rate",
                "Rate (eps)",
                "Network TX (MB)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "spark-fraud-detection", "network_tx_delta_bytes", 1024 * 1024),
            ),
            "",
            mermaid_xychart(
                "Spark App Block Write Delta vs Input Rate",
                "Rate (eps)",
                "Block Write (MB)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "spark-fraud-detection", "block_write_delta_bytes", 1024 * 1024),
            ),
            "",
            mermaid_xychart(
                "Spark Worker Block Write Delta vs Input Rate",
                "Rate (eps)",
                "Block Write (MB)",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "spark-worker", "block_write_delta_bytes", 1024 * 1024),
            ),
            "",
            mermaid_xychart(
                "Spark App Max PIDs vs Input Rate",
                "Rate (eps)",
                "PIDs",
                xs,
                metric_series_by_trigger(by_trigger, unique_rates, "spark-fraud-detection", "pids_max"),
            ),
        ]
    )

    sections.extend(["", "## Summary Table", "", (output_dir / "CHART_SUMMARY.md").read_text(encoding="utf-8").strip()])
    (output_dir / "CHARTS.md").write_text("\n".join(sections) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    results_dir = ROOT
    output_dir = results_dir / "charts"
    output_dir.mkdir(parents=True, exist_ok=True)

    points = load_profile_points(results_dir)
    if not points:
        raise SystemExit("Khong tim thay file benchmark-results*.json trong thu muc end_to_end_benchmark")

    write_summary_table(points, output_dir)
    write_mermaid_report(points, output_dir)

    generated_files = ["CHART_SUMMARY.md", "CHARTS.md"]
    if has_matplotlib():
        plot_success_rate(points, output_dir)
        plot_timeout_count(points, output_dir)
        plot_e2e_latency(points, output_dir)
        plot_stage_latency(points, output_dir)
        plot_success_vs_latency(points, output_dir)
        generated_files.extend(
            [
                "success_rate_vs_rate.png",
                "timeout_count_vs_rate.png",
                "e2e_latency_vs_rate.png",
                "stage_latency_vs_rate.png",
                "success_rate_vs_e2e_p95.png",
            ]
        )
    else:
        print("Matplotlib not installed, generated Mermaid/Markdown charts only.")

    print(f"Saved chart outputs to {output_dir}: {', '.join(generated_files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
