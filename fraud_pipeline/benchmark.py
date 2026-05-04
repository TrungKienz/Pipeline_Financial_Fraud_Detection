from __future__ import annotations

import statistics
import time
from dataclasses import asdict, dataclass

from .config import PipelineConfig
from .models import FraudDecision, TransactionEvent
from .rules import RuleEngine
from .synthetic import synthesize_events
from .windows import sliding_window_metrics, tumbling_window_metrics


@dataclass(frozen=True)
class BenchmarkProfile:
    name: str
    event_count: int
    tumbling_window_seconds: int = 300
    sliding_window_seconds: int = 600
    sliding_step_seconds: int = 60


@dataclass(frozen=True)
class BenchmarkResult:
    profile: str
    event_count: int
    alerts_emitted: int
    parse_like_ms: float
    rule_eval_ms: float
    window_eval_ms: float
    total_ms: float
    throughput_eps: float
    rule_p50_ms: float
    rule_p95_ms: float
    tumbling_windows: int
    sliding_windows: int

    def to_dict(self) -> dict:
        return asdict(self)


DEFAULT_PROFILES = (
    BenchmarkProfile(name="small", event_count=1_000),
    BenchmarkProfile(name="medium", event_count=5_000),
    BenchmarkProfile(name="large", event_count=10_000),
)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    values = sorted(values)
    position = (len(values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    if lower == upper:
        return values[lower]
    weight = position - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def run_benchmark(
    seed_events: list[TransactionEvent],
    profile: BenchmarkProfile,
    config: PipelineConfig | None = None,
) -> BenchmarkResult:
    config = config or PipelineConfig()
    engine = RuleEngine(config)

    overall_start = time.perf_counter()

    parse_start = time.perf_counter()
    events = synthesize_events(seed_events, profile.event_count)
    parse_like_ms = (time.perf_counter() - parse_start) * 1000

    sender_index: dict[str, list[TransactionEvent]] = {}
    alerts: list[FraudDecision] = []
    rule_timings_ms: list[float] = []

    rule_start = time.perf_counter()
    for event in events:
        recent = sender_index.get(event.name_orig, [])
        decision_start = time.perf_counter()
        decision = engine.evaluate(event, recent_sender_events=recent)
        rule_timings_ms.append((time.perf_counter() - decision_start) * 1000)
        sender_index.setdefault(event.name_orig, []).append(event)
        if decision.is_alert:
            alerts.append(decision)
    rule_eval_ms = (time.perf_counter() - rule_start) * 1000

    window_start = time.perf_counter()
    tumbling = tumbling_window_metrics(events, window_seconds=profile.tumbling_window_seconds)
    sliding = sliding_window_metrics(
        events,
        window_seconds=profile.sliding_window_seconds,
        slide_seconds=profile.sliding_step_seconds,
    )
    window_eval_ms = (time.perf_counter() - window_start) * 1000

    total_ms = (time.perf_counter() - overall_start) * 1000
    throughput_eps = profile.event_count / (total_ms / 1000) if total_ms else 0.0

    return BenchmarkResult(
        profile=profile.name,
        event_count=profile.event_count,
        alerts_emitted=len(alerts),
        parse_like_ms=round(parse_like_ms, 3),
        rule_eval_ms=round(rule_eval_ms, 3),
        window_eval_ms=round(window_eval_ms, 3),
        total_ms=round(total_ms, 3),
        throughput_eps=round(throughput_eps, 3),
        rule_p50_ms=round(statistics.median(rule_timings_ms) if rule_timings_ms else 0.0, 6),
        rule_p95_ms=round(_percentile(rule_timings_ms, 0.95), 6),
        tumbling_windows=len(tumbling),
        sliding_windows=len(sliding),
    )


def run_benchmark_suite(
    seed_events: list[TransactionEvent],
    profiles: tuple[BenchmarkProfile, ...] = DEFAULT_PROFILES,
    config: PipelineConfig | None = None,
) -> list[BenchmarkResult]:
    return [run_benchmark(seed_events, profile, config=config) for profile in profiles]
