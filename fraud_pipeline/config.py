from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    base_event_time: datetime = field(
        default_factory=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    step_seconds: int = 60
    high_amount_transfer_threshold: float = 200_000.0
    high_amount_cash_out_threshold: float = 200_000.0
    rapid_outflow_window_seconds: int = 600
    rapid_outflow_count_threshold: int = 3
    rapid_outflow_amount_threshold: float = 300_000.0
    balance_tolerance: float = 1.0
    schema_version: int = 1
    default_csv_path: Path = Path(
        r"F:\Project\Bigdata\Data\archive (2)\PS_20174392719_1491204439457_log.csv"
    )
