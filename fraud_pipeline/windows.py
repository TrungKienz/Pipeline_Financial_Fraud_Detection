from __future__ import annotations

from datetime import datetime, timedelta

from .models import TransactionEvent, WindowMetric


def tumbling_window_metrics(
    events: list[TransactionEvent],
    window_seconds: int = 60,
) -> list[WindowMetric]:
    if not events:
        return []
    buckets: dict[int, list[TransactionEvent]] = {}
    for event in sorted(events, key=lambda item: item.event_time):
        timestamp = int(event.event_time.timestamp())
        bucket_start = timestamp - (timestamp % window_seconds)
        buckets.setdefault(bucket_start, []).append(event)

    metrics: list[WindowMetric] = []
    for bucket_start in sorted(buckets):
        bucket_events = buckets[bucket_start]
        metrics.append(_build_metric(bucket_events, bucket_start, window_seconds))
    return metrics


def sliding_window_metrics(
    events: list[TransactionEvent],
    window_seconds: int = 600,
    slide_seconds: int = 60,
) -> list[WindowMetric]:
    if not events:
        return []
    ordered = sorted(events, key=lambda item: item.event_time)
    start_ts = int(ordered[0].event_time.timestamp())
    end_ts = int(ordered[-1].event_time.timestamp())
    metrics: list[WindowMetric] = []

    current_start = start_ts - (start_ts % slide_seconds)
    while current_start <= end_ts:
        current_end = current_start + window_seconds
        window_events = [
            event
            for event in ordered
            if current_start <= int(event.event_time.timestamp()) < current_end
        ]
        if window_events:
            metrics.append(_build_metric(window_events, current_start, window_seconds))
        current_start += slide_seconds
    return metrics


def _build_metric(events: list[TransactionEvent], start_ts: int, window_seconds: int) -> WindowMetric:
    event_count = len(events)
    fraud_count = sum(event.is_fraud for event in events)
    total_amount = sum(event.amount for event in events)
    fraud_rate = fraud_count / event_count if event_count else 0.0
    start = datetime.fromtimestamp(start_ts, tz=events[0].event_time.tzinfo)
    end = start + timedelta(seconds=window_seconds)
    return WindowMetric(
        window_start=start,
        window_end=end,
        event_count=event_count,
        fraud_count=fraud_count,
        total_amount=total_amount,
        fraud_rate=fraud_rate,
    )
