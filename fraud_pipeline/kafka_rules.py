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

    return RuntimeRuleState(
        amount_thresholds=amount_thresholds,
        rapid_outflow_amount_threshold=rapid_outflow_amount_threshold,
        rapid_outflow_count_threshold=rapid_outflow_count_threshold,
        watchlisted_accounts=frozenset(watchlisted_accounts),
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
