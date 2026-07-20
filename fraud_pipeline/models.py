from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class TransactionEvent:
    event_id: str
    event_time: datetime
    producer_ts: datetime
    step: int
    txn_type: str
    amount: float
    name_orig: str
    oldbalance_org: float
    newbalance_orig: float
    name_dest: str
    oldbalance_dest: float
    newbalance_dest: float
    is_fraud: int
    schema_version: int = 1


@dataclass(frozen=True)
class AccountStateUpdate:
    event_id: str
    source_event_id: str
    account_id: str
    role: str
    step: int
    balance_before: float
    balance_after: float
    event_time: datetime


@dataclass(frozen=True)
class FraudDecision:
    event_id: str
    is_alert: bool
    risk_score: float
    severity: str
    ml_score: float = 0.0
    ml_model_version: str = "v0"
    triggered_rules: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class WindowMetric:
    window_start: datetime
    window_end: datetime
    event_count: int
    fraud_count: int
    total_amount: float
    fraud_rate: float

