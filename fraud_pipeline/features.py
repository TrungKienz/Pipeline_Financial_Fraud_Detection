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


def amount_to_balance_ratio(event: TransactionEvent) -> float:
    denom = event.oldbalance_org + event.oldbalance_dest
    if denom <= 0:
        return 1.0 if event.amount > 0 else 0.0
    return min(event.amount / denom, 1.0)


def is_zero_balance_after(event: TransactionEvent) -> bool:
    return int(event.newbalance_orig == 0 and event.txn_type in SENDER_DEBIT_TYPES)


def is_same_sender_receiver(event: TransactionEvent) -> bool:
    return int(event.name_orig == event.name_dest)


def build_feature_record(event: TransactionEvent, config: PipelineConfig | None = None) -> dict[str, float | int | str]:
    return {
        "event_id": event.event_id,
        "step": event.step,
        "txn_type": event.txn_type,
        "amount": event.amount,
        "sender_balance_delta": sender_balance_delta(event),
        "receiver_balance_delta": receiver_balance_delta(event),
        "sender_depletion_ratio": sender_depletion_ratio(event),
        "amount_to_balance_ratio": amount_to_balance_ratio(event),
        "is_zero_balance_after": is_zero_balance_after(event),
        "is_same_sender_receiver": is_same_sender_receiver(event),
        "sender_balance_inconsistent": int(sender_balance_inconsistent(event, config)),
        "receiver_balance_inconsistent": int(receiver_balance_inconsistent(event, config)),
        "label_is_fraud": event.is_fraud,
    }


FEATURE_COLUMNS = [
    "step",
    "amount",
    "sender_balance_delta",
    "receiver_balance_delta",
    "sender_depletion_ratio",
    "amount_to_balance_ratio",
    "is_zero_balance_after",
    "is_same_sender_receiver",
    "sender_balance_inconsistent",
    "receiver_balance_inconsistent",
]

TXN_TYPE_CATEGORIES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]

