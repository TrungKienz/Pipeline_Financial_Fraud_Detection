from __future__ import annotations

from .config import PipelineConfig
from .models import TransactionEvent


SENDER_DEBIT_TYPES = {"PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT"}
RECEIVER_CREDIT_TYPES = {"TRANSFER", "CASH_IN"}


def sender_balance_delta(event: TransactionEvent) -> float:
    return event.oldbalance_org - event.newbalance_orig


def receiver_balance_delta(event: TransactionEvent) -> float:
    return event.newbalance_dest - event.oldbalance_dest


def sender_balance_inconsistent(event: TransactionEvent, config: PipelineConfig | None = None) -> bool:
    config = config or PipelineConfig()
    if event.txn_type not in SENDER_DEBIT_TYPES:
        return False
    expected = event.oldbalance_org - event.amount
    return abs(expected - event.newbalance_orig) > config.balance_tolerance


def receiver_balance_inconsistent(event: TransactionEvent, config: PipelineConfig | None = None) -> bool:
    config = config or PipelineConfig()
    if event.txn_type not in RECEIVER_CREDIT_TYPES:
        return False
    expected = event.oldbalance_dest + event.amount
    return abs(expected - event.newbalance_dest) > config.balance_tolerance


def sender_depletion_ratio(event: TransactionEvent) -> float:
    if event.oldbalance_org <= 0:
        return 1.0 if event.amount > 0 else 0.0
    return min(event.amount / event.oldbalance_org, 1.0)


def build_feature_record(event: TransactionEvent, config: PipelineConfig | None = None) -> dict[str, float | int | str]:
    return {
        "event_id": event.event_id,
        "step": event.step,
        "txn_type": event.txn_type,
        "amount": event.amount,
        "sender_balance_delta": sender_balance_delta(event),
        "receiver_balance_delta": receiver_balance_delta(event),
        "sender_depletion_ratio": sender_depletion_ratio(event),
        "sender_balance_inconsistent": int(sender_balance_inconsistent(event, config)),
        "receiver_balance_inconsistent": int(receiver_balance_inconsistent(event, config)),
        "label_is_fraud": event.is_fraud,
    }

