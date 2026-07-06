from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from typing import Any

from .models import AccountStateUpdate, FraudDecision, PredictionRecord, TransactionEvent, WindowMetric


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
    record = prediction_record_from_decision(event, decision)
    if record.alert_id is None:
        record = replace(record, alert_id=f"alert:{decision.event_id}")
    return prediction_record_to_dict(record)


def prediction_record_from_decision(event: TransactionEvent, decision: FraudDecision) -> PredictionRecord:
    alert_id = f"alert:{decision.event_id}" if decision.is_alert else None
    return PredictionRecord(
        event_id=decision.event_id,
        account_id=event.name_orig,
        name_dest=event.name_dest,
        event_time=event.event_time,
        txn_type=event.txn_type,
        amount=event.amount,
        risk_score=decision.risk_score,
        severity=decision.severity,
        ml_score=decision.ml_score,
        ml_model_version=decision.ml_model_version,
        triggered_rules=decision.triggered_rules,
        is_alert=decision.is_alert,
        alert_id=alert_id,
    )


def prediction_record_to_dict(record: PredictionRecord) -> dict[str, Any]:
    payload = {
        "event_id": record.event_id,
        "account_id": record.account_id,
        "nameDest": record.name_dest,
        "event_time": _dt(record.event_time),
        "txn_type": record.txn_type,
        "amount": record.amount,
        "risk_score": record.risk_score,
        "severity": record.severity,
        "ml_score": record.ml_score,
        "ml_model_version": record.ml_model_version,
        "triggered_rules": list(record.triggered_rules),
        "is_alert": record.is_alert,
    }
    if record.alert_id is not None:
        payload["alert_id"] = record.alert_id
    return payload


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
            "rule_id": "account_drain_near_zero_v1",
            "rule_type": "account_drain_threshold",
            "min_balance_floor": 50000.0,
            "ratio_threshold": 0.8,
            "near_zero_balance": 1000.0,
            "weight": 0.35,
            "severity": "high",
        },
        {
            "rule_id": "sender_fan_out_burst_v1",
            "rule_type": "fan_out_threshold",
            "window_seconds": 900,
            "distinct_receiver_threshold": 3,
            "total_amount_threshold": 250000.0,
            "weight": 0.25,
            "severity": "medium",
        },
        {
            "rule_id": "receiver_fan_in_burst_v1",
            "rule_type": "fan_in_threshold",
            "window_seconds": 900,
            "distinct_sender_threshold": 3,
            "total_amount_threshold": 250000.0,
            "weight": 0.25,
            "severity": "medium",
        },
        {
            "rule_id": "structured_split_transfer_v1",
            "rule_type": "structuring_threshold",
            "window_seconds": 900,
            "count_threshold": 4,
            "min_amount": 40000.0,
            "max_amount": 90000.0,
            "total_amount_threshold": 250000.0,
            "weight": 0.30,
            "severity": "high",
        },
        {
            "rule_id": "new_counterparty_large_transfer_v1",
            "rule_type": "new_counterparty_threshold",
            "amount_threshold": 150000.0,
            "weight": 0.15,
            "severity": "medium",
        },
        {
            "rule_id": "cashout_after_inbound_chain_v1",
            "rule_type": "cashout_after_inbound_threshold",
            "window_seconds": 1800,
            "ratio_threshold": 0.8,
            "weight": 0.40,
            "severity": "medium",
        },
    ]


def dumps(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")
