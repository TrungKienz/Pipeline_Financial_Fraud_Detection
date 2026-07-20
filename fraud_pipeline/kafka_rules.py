from __future__ import annotations

import json
from dataclasses import dataclass
from collections.abc import Mapping

from .config import PipelineConfig
from .topics import RISK_RULES_TOPIC


@dataclass(frozen=True)
class RuntimeRuleState:
    amount_thresholds: dict[str, float]
    rapid_outflow_amount_threshold: float
    rapid_outflow_count_threshold: int
    watchlisted_accounts: frozenset[str]
    account_drain_min_balance_floor: float
    account_drain_ratio_threshold: float
    account_drain_near_zero_balance: float
    account_drain_weight: float
    fan_out_window_seconds: int
    fan_out_distinct_receiver_threshold: int
    fan_out_total_amount_threshold: float
    sender_fan_out_weight: float
    fan_in_window_seconds: int
    fan_in_distinct_sender_threshold: int
    fan_in_total_amount_threshold: float
    receiver_fan_in_weight: float
    structuring_window_seconds: int
    structuring_count_threshold: int
    structuring_min_amount: float
    structuring_max_amount: float
    structuring_total_amount_threshold: float
    structured_split_weight: float
    new_counterparty_amount_threshold: float
    new_counterparty_weight: float
    cashout_after_inbound_window_seconds: int
    cashout_after_inbound_ratio_threshold: float
    cashout_after_inbound_weight: float


def build_runtime_rule_state(
    payloads: list[Mapping[str, object]],
    config: PipelineConfig | None = None,
) -> RuntimeRuleState:
    config = config or PipelineConfig()
    amount_thresholds = {
        "TRANSFER": config.high_amount_transfer_threshold,
        "CASH_OUT": config.high_amount_cash_out_threshold,
    }
    rapid_outflow_amount_threshold = config.rapid_outflow_amount_threshold
    rapid_outflow_count_threshold = config.rapid_outflow_count_threshold
    watchlisted_accounts: set[str] = set()
    account_drain_min_balance_floor = config.account_drain_min_balance_floor
    account_drain_ratio_threshold = config.account_drain_ratio_threshold
    account_drain_near_zero_balance = config.account_drain_near_zero_balance
    account_drain_weight = config.account_drain_weight
    fan_out_window_seconds = config.fan_out_window_seconds
    fan_out_distinct_receiver_threshold = config.fan_out_distinct_receiver_threshold
    fan_out_total_amount_threshold = config.fan_out_total_amount_threshold
    sender_fan_out_weight = config.sender_fan_out_weight
    fan_in_window_seconds = config.fan_in_window_seconds
    fan_in_distinct_sender_threshold = config.fan_in_distinct_sender_threshold
    fan_in_total_amount_threshold = config.fan_in_total_amount_threshold
    receiver_fan_in_weight = config.receiver_fan_in_weight
    structuring_window_seconds = config.structuring_window_seconds
    structuring_count_threshold = config.structuring_count_threshold
    structuring_min_amount = config.structuring_min_amount
    structuring_max_amount = config.structuring_max_amount
    structuring_total_amount_threshold = config.structuring_total_amount_threshold
    structured_split_weight = config.structured_split_weight
    new_counterparty_amount_threshold = config.new_counterparty_amount_threshold
    new_counterparty_weight = config.new_counterparty_weight
    cashout_after_inbound_window_seconds = config.cashout_after_inbound_window_seconds
    cashout_after_inbound_ratio_threshold = config.cashout_after_inbound_ratio_threshold
    cashout_after_inbound_weight = config.cashout_after_inbound_weight

    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        rule_type = str(payload.get("rule_type", "")).strip().lower()
        if rule_type == "amount_threshold":
            txn_type = str(payload.get("txn_type", "")).upper()
            threshold = float(payload.get("threshold", config.high_amount_transfer_threshold))
            if txn_type in {"TRANSFER", "CASH_OUT"}:
                amount_thresholds[txn_type] = threshold
        elif rule_type == "velocity_threshold":
            threshold = payload.get("threshold")
            count_threshold = payload.get("count_threshold")
            if threshold is not None:
                rapid_outflow_amount_threshold = float(threshold)
            if count_threshold is not None:
                rapid_outflow_count_threshold = int(count_threshold)
        elif rule_type == "watchlist_update":
            account_id = str(payload.get("account_id", "")).strip()
            operation = str(payload.get("operation", "add")).strip().lower()
            if not account_id:
                continue
            if operation == "remove":
                watchlisted_accounts.discard(account_id)
            else:
                watchlisted_accounts.add(account_id)
        elif rule_type == "account_drain_threshold":
            if payload.get("min_balance_floor") is not None:
                account_drain_min_balance_floor = float(payload["min_balance_floor"])
            if payload.get("ratio_threshold") is not None:
                account_drain_ratio_threshold = float(payload["ratio_threshold"])
            if payload.get("near_zero_balance") is not None:
                account_drain_near_zero_balance = float(payload["near_zero_balance"])
            if payload.get("weight") is not None:
                account_drain_weight = float(payload["weight"])
        elif rule_type == "fan_out_threshold":
            if payload.get("window_seconds") is not None:
                fan_out_window_seconds = int(payload["window_seconds"])
            if payload.get("distinct_receiver_threshold") is not None:
                fan_out_distinct_receiver_threshold = int(payload["distinct_receiver_threshold"])
            if payload.get("total_amount_threshold") is not None:
                fan_out_total_amount_threshold = float(payload["total_amount_threshold"])
            if payload.get("weight") is not None:
                sender_fan_out_weight = float(payload["weight"])
        elif rule_type == "fan_in_threshold":
            if payload.get("window_seconds") is not None:
                fan_in_window_seconds = int(payload["window_seconds"])
            if payload.get("distinct_sender_threshold") is not None:
                fan_in_distinct_sender_threshold = int(payload["distinct_sender_threshold"])
            if payload.get("total_amount_threshold") is not None:
                fan_in_total_amount_threshold = float(payload["total_amount_threshold"])
            if payload.get("weight") is not None:
                receiver_fan_in_weight = float(payload["weight"])
        elif rule_type == "structuring_threshold":
            if payload.get("window_seconds") is not None:
                structuring_window_seconds = int(payload["window_seconds"])
            if payload.get("count_threshold") is not None:
                structuring_count_threshold = int(payload["count_threshold"])
            if payload.get("min_amount") is not None:
                structuring_min_amount = float(payload["min_amount"])
            if payload.get("max_amount") is not None:
                structuring_max_amount = float(payload["max_amount"])
            if payload.get("total_amount_threshold") is not None:
                structuring_total_amount_threshold = float(payload["total_amount_threshold"])
            if payload.get("weight") is not None:
                structured_split_weight = float(payload["weight"])
        elif rule_type == "new_counterparty_threshold":
            if payload.get("amount_threshold") is not None:
                new_counterparty_amount_threshold = float(payload["amount_threshold"])
            if payload.get("weight") is not None:
                new_counterparty_weight = float(payload["weight"])
        elif rule_type == "cashout_after_inbound_threshold":
            if payload.get("window_seconds") is not None:
                cashout_after_inbound_window_seconds = int(payload["window_seconds"])
            if payload.get("ratio_threshold") is not None:
                cashout_after_inbound_ratio_threshold = float(payload["ratio_threshold"])
            if payload.get("weight") is not None:
                cashout_after_inbound_weight = float(payload["weight"])

    return RuntimeRuleState(
        amount_thresholds=amount_thresholds,
        rapid_outflow_amount_threshold=rapid_outflow_amount_threshold,
        rapid_outflow_count_threshold=rapid_outflow_count_threshold,
        watchlisted_accounts=frozenset(watchlisted_accounts),
        account_drain_min_balance_floor=account_drain_min_balance_floor,
        account_drain_ratio_threshold=account_drain_ratio_threshold,
        account_drain_near_zero_balance=account_drain_near_zero_balance,
        account_drain_weight=account_drain_weight,
        fan_out_window_seconds=fan_out_window_seconds,
        fan_out_distinct_receiver_threshold=fan_out_distinct_receiver_threshold,
        fan_out_total_amount_threshold=fan_out_total_amount_threshold,
        sender_fan_out_weight=sender_fan_out_weight,
        fan_in_window_seconds=fan_in_window_seconds,
        fan_in_distinct_sender_threshold=fan_in_distinct_sender_threshold,
        fan_in_total_amount_threshold=fan_in_total_amount_threshold,
        receiver_fan_in_weight=receiver_fan_in_weight,
        structuring_window_seconds=structuring_window_seconds,
        structuring_count_threshold=structuring_count_threshold,
        structuring_min_amount=structuring_min_amount,
        structuring_max_amount=structuring_max_amount,
        structuring_total_amount_threshold=structuring_total_amount_threshold,
        structured_split_weight=structured_split_weight,
        new_counterparty_amount_threshold=new_counterparty_amount_threshold,
        new_counterparty_weight=new_counterparty_weight,
        cashout_after_inbound_window_seconds=cashout_after_inbound_window_seconds,
        cashout_after_inbound_ratio_threshold=cashout_after_inbound_ratio_threshold,
        cashout_after_inbound_weight=cashout_after_inbound_weight,
    )


def load_runtime_rule_state(
    bootstrap_servers: str,
    config: PipelineConfig | None = None,
) -> RuntimeRuleState:
    from kafka import KafkaConsumer

    config = config or PipelineConfig()
    payloads: list[Mapping[str, object]] = []
    consumer = KafkaConsumer(
        RISK_RULES_TOPIC,
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=3000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    try:
        for message in consumer:
            if isinstance(message.value, Mapping):
                payloads.append(message.value)
    finally:
        consumer.close()
    return build_runtime_rule_state(payloads, config=config)


def load_amount_thresholds(
    bootstrap_servers: str,
    config: PipelineConfig | None = None,
) -> dict[str, float]:
    return load_runtime_rule_state(bootstrap_servers, config=config).amount_thresholds
