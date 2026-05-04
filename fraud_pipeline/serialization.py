from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .models import AccountStateUpdate, FraudDecision, TransactionEvent, WindowMetric


def _dt(value: datetime) -> str:
    return value.isoformat()


def transaction_to_dict(event: TransactionEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "event_time": _dt(event.event_time),
        "producer_ts": _dt(event.producer_ts),
        "step": event.step,
        "type": event.txn_type,
        "amount": event.amount,
        "nameOrig": event.name_orig,
        "nameDest": event.name_dest,
        "isFraud": event.is_fraud,
        "isFlaggedFraud": event.is_flagged_fraud,
        "schema_version": event.schema_version,
    }


def account_state_to_dict(update: AccountStateUpdate) -> dict[str, Any]:
    return {
        "event_id": update.event_id,
        "source_event_id": update.source_event_id,
        "account_id": update.account_id,
        "role": update.role,
        "step": update.step,
        "balance_before": update.balance_before,
        "balance_after": update.balance_after,
        "event_time": _dt(update.event_time),
    }


def sender_state_to_dict(update: AccountStateUpdate) -> dict[str, Any]:
    if update.role != "sender":
        raise ValueError("sender_state_to_dict chi nhan sender update")
    return {
        "event_id": update.event_id,
        "source_event_id": update.source_event_id,
        "event_time": _dt(update.event_time),
        "step": update.step,
        "nameOrig": update.account_id,
        "oldbalanceOrg": update.balance_before,
        "newbalanceOrig": update.balance_after,
    }


def receiver_state_to_dict(update: AccountStateUpdate) -> dict[str, Any]:
    if update.role != "receiver":
        raise ValueError("receiver_state_to_dict chi nhan receiver update")
    return {
        "event_id": update.event_id,
        "source_event_id": update.source_event_id,
        "event_time": _dt(update.event_time),
        "step": update.step,
        "nameDest": update.account_id,
        "oldbalanceDest": update.balance_before,
        "newbalanceDest": update.balance_after,
    }


def fraud_decision_to_dict(event: TransactionEvent, decision: FraudDecision) -> dict[str, Any]:
    return {
        "alert_id": f"alert:{decision.event_id}",
        "event_id": decision.event_id,
        "account_id": event.name_orig,
        "nameDest": event.name_dest,
        "event_time": _dt(event.event_time),
        "txn_type": event.txn_type,
        "amount": event.amount,
        "risk_score": decision.risk_score,
        "severity": decision.severity,
        "triggered_rules": list(decision.triggered_rules),
        "is_alert": decision.is_alert,
    }


def window_metric_to_dict(metric: WindowMetric, window_type: str) -> dict[str, Any]:
    return {
        "window_type": window_type,
        "window_start": _dt(metric.window_start),
        "window_end": _dt(metric.window_end),
        "event_count": metric.event_count,
        "fraud_count": metric.fraud_count,
        "total_amount": metric.total_amount,
        "fraud_rate": metric.fraud_rate,
    }


def risk_rule_event() -> list[dict[str, Any]]:
    return [
        {
            "rule_id": "high_amount_transfer",
            "rule_type": "amount_threshold",
            "txn_type": "TRANSFER",
            "threshold": 200000.0,
            "severity": "high",
        },
        {
            "rule_id": "high_amount_cash_out",
            "rule_type": "amount_threshold",
            "txn_type": "CASH_OUT",
            "threshold": 200000.0,
            "severity": "high",
        },
        {
            "rule_id": "rapid_outflow_default",
            "rule_type": "velocity_threshold",
            "txn_type": "ANY",
            "threshold": 300000.0,
            "count_threshold": 3,
            "severity": "medium",
        },
    ]


def dumps(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")
