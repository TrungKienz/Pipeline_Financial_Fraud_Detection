from __future__ import annotations

from dataclasses import dataclass
import logging
from math import prod

from .config import PipelineConfig
from .models import FraudDecision, TransactionEvent

LOGGER = logging.getLogger(__name__)

try:
    from model.model_utils import predict_proba, model_is_loaded, get_model_version, get_threshold
    _ML_AVAILABLE = True
except ImportError:
    LOGGER.exception(
        "ML runtime unavailable: failed to import model utilities")
    _ML_AVAILABLE = False


OUTBOUND_TYPES = {"TRANSFER", "CASH_OUT"}
INBOUND_TYPES = {"TRANSFER", "CASH_IN"}


@dataclass
class RuleEngine:
    config: PipelineConfig

    def evaluate(
        self,
        event: TransactionEvent,
        recent_sender_events: list[TransactionEvent] | None = None,
        recent_receiver_events: list[TransactionEvent] | None = None,
        recent_inbound_events: list[TransactionEvent] | None = None,
        known_counterparties: set[str] | None = None,
        watchlisted_accounts: set[str] | None = None,
    ) -> FraudDecision:
        recent_sender_events = recent_sender_events or []
        recent_receiver_events = recent_receiver_events or []
        recent_inbound_events = recent_inbound_events or []
        known_counterparties = known_counterparties or set()
        triggered_rules: list[str] = []
        weights: list[float] = []

        if self._is_account_drain_near_zero(event):
            triggered_rules.append("account_drain_near_zero")
            weights.append(self.config.account_drain_weight)

        if self._has_sender_fan_out_burst(event, recent_sender_events):
            triggered_rules.append("sender_fan_out_burst")
            weights.append(self.config.sender_fan_out_weight)

        if self._has_receiver_fan_in_burst(event, recent_receiver_events):
            triggered_rules.append("receiver_fan_in_burst")
            weights.append(self.config.receiver_fan_in_weight)

        if self._has_structured_split_transfer(event, recent_sender_events):
            triggered_rules.append("structured_split_transfer")
            weights.append(self.config.structured_split_weight)

        if self._is_new_counterparty_large_transfer(event, known_counterparties):
            triggered_rules.append("new_counterparty_large_transfer")
            weights.append(self.config.new_counterparty_weight)

        if self._has_cashout_after_inbound_chain(event, recent_inbound_events):
            triggered_rules.append("cashout_after_inbound_chain")
            weights.append(self.config.cashout_after_inbound_weight)

        rule_risk_score = 1 - \
            prod(1 - weight for weight in weights) if weights else 0.0

        # Calculate raw dynamic features to pass to ML
        dynamic_features = self._calculate_raw_dynamic_features(
            event,
            recent_sender_events,
            recent_receiver_events,
            recent_inbound_events,
            known_counterparties
        )

        ml_score = self._predict_ml_score(event, dynamic_features)
        ml_version = get_model_version() if _ML_AVAILABLE else "v0"
        threshold = get_threshold() if _ML_AVAILABLE else 0.85

        combined_risk_score = min(
            (rule_risk_score * 0.6) + (ml_score * 0.4), 1.0)
        is_alert = bool(triggered_rules) or ml_score >= threshold
        severity = "high" if combined_risk_score >= 0.65 else "medium" if combined_risk_score >= 0.35 else "low"

        return FraudDecision(
            event_id=event.event_id,
            is_alert=is_alert,
            risk_score=round(combined_risk_score, 4),
            severity=severity,
            ml_score=ml_score,
            ml_model_version=ml_version,
            triggered_rules=tuple(triggered_rules),
        )

    def _predict_ml_score(self, event: TransactionEvent, dynamic_features: dict[str, float] | None = None) -> float:
        if not _ML_AVAILABLE:
            return 0.0
        try:
            return predict_proba(event, dynamic_features=dynamic_features)
        except Exception:
            LOGGER.exception(
                "ML prediction failed for event_id=%s", event.event_id)
            return 0.0

    def _calculate_raw_dynamic_features(
        self,
        event: TransactionEvent,
        recent_sender_events: list[TransactionEvent],
        recent_receiver_events: list[TransactionEvent],
        recent_inbound_events: list[TransactionEvent],
        known_counterparties: set[str],
    ) -> dict[str, float]:
        # 1. Sender recent activity in fan-out window
        sender_window = self._matching_sender_window(event, recent_sender_events)
        sender_count = len(sender_window)
        sender_amount = sum(item.amount for item in sender_window)

        # 2. Receiver recent activity in fan-in window
        receiver_window = []
        if event.txn_type in INBOUND_TYPES:
            window_start = event.event_time.timestamp() - self.config.fan_in_window_seconds
            receiver_window = [
                item
                for item in recent_receiver_events
                if item.name_dest == event.name_dest and item.event_time.timestamp() >= window_start
            ]
        receiver_count = len(receiver_window)
        receiver_amount = sum(item.amount for item in receiver_window)

        # 3. New counterparty check
        is_new_cp = 1.0 if (event.txn_type == "TRANSFER" and event.name_dest not in known_counterparties) else 0.0

        # 4. Inbound to cash-out ratio
        inbound_ratio = 0.0
        if event.txn_type == "CASH_OUT":
            window_start = event.event_time.timestamp() - self.config.cashout_after_inbound_window_seconds
            matching_inbound = [
                item
                for item in recent_inbound_events
                if item.name_dest == event.name_orig and item.event_time.timestamp() >= window_start
            ]
            inbound_total = sum(item.amount for item in matching_inbound)
            if inbound_total > 0:
                inbound_ratio = event.amount / inbound_total

        # 5. New Features: velocity_transactions_1h
        h1_start = event.event_time.timestamp() - 3600
        sender_h1_window = [
            item for item in recent_sender_events
            if item.name_orig == event.name_orig and item.event_time.timestamp() >= h1_start
        ]
        velocity_1h = len(sender_h1_window)

        # 6. New Features: time_since_last_purchase
        sender_txs = [
            item for item in recent_sender_events
            if item.name_orig == event.name_orig and item.event_time.timestamp() < event.event_time.timestamp()
        ]
        if sender_txs:
            last_ts = max(item.event_time.timestamp() for item in sender_txs)
            time_since_last = event.event_time.timestamp() - last_ts
        else:
            time_since_last = 86400.0

        return {
            "sender_recent_txn_count": float(sender_count),
            "sender_recent_total_amount": float(sender_amount),
            "receiver_recent_txn_count": float(receiver_count),
            "receiver_recent_total_amount": float(receiver_amount),
            "is_new_counterparty": is_new_cp,
            "inbound_to_cashout_ratio": inbound_ratio,
            "velocity_transactions_1h": float(velocity_1h),
            "time_since_last_purchase": float(time_since_last),
        }


    def _is_account_drain_near_zero(self, event: TransactionEvent) -> bool:
        if event.txn_type not in OUTBOUND_TYPES:
            return False
        if event.oldbalance_org < self.config.account_drain_min_balance_floor:
            return False
        if event.oldbalance_org <= 0:
            return False
        drain_ratio = event.amount / event.oldbalance_org
        return (
            drain_ratio >= self.config.account_drain_ratio_threshold
            and event.newbalance_orig <= self.config.account_drain_near_zero_balance
        )

    def _matching_sender_window(self, event: TransactionEvent, recent_sender_events: list[TransactionEvent]) -> list[TransactionEvent]:
        if event.txn_type not in OUTBOUND_TYPES:
            return []
        window_start = event.event_time.timestamp() - self.config.fan_out_window_seconds
        return [
            item
            for item in recent_sender_events
            if item.name_orig == event.name_orig and item.event_time.timestamp() >= window_start
        ]

    def _has_sender_fan_out_burst(self, event: TransactionEvent, recent_sender_events: list[TransactionEvent]) -> bool:
        matching = self._matching_sender_window(event, recent_sender_events)
        if not matching and event.txn_type not in OUTBOUND_TYPES:
            return False
        distinct_receivers = {item.name_dest for item in matching}
        distinct_receivers.add(event.name_dest)
        total_amount = sum(item.amount for item in matching) + event.amount
        return (
            len(distinct_receivers) >= self.config.fan_out_distinct_receiver_threshold
            and total_amount >= self.config.fan_out_total_amount_threshold
        )

    def _has_receiver_fan_in_burst(self, event: TransactionEvent, recent_receiver_events: list[TransactionEvent]) -> bool:
        if event.txn_type not in INBOUND_TYPES:
            return False
        window_start = event.event_time.timestamp() - self.config.fan_in_window_seconds
        matching = [
            item
            for item in recent_receiver_events
            if item.name_dest == event.name_dest and item.event_time.timestamp() >= window_start
        ]
        distinct_senders = {item.name_orig for item in matching}
        distinct_senders.add(event.name_orig)
        total_amount = sum(item.amount for item in matching) + event.amount
        return (
            len(distinct_senders) >= self.config.fan_in_distinct_sender_threshold
            and total_amount >= self.config.fan_in_total_amount_threshold
        )

    def _has_structured_split_transfer(self, event: TransactionEvent, recent_sender_events: list[TransactionEvent]) -> bool:
        if event.txn_type != "TRANSFER":
            return False
        if not (self.config.structuring_min_amount <= event.amount <= self.config.structuring_max_amount):
            return False
        window_start = event.event_time.timestamp() - self.config.structuring_window_seconds
        matching = [
            item
            for item in recent_sender_events
            if (
                item.name_orig == event.name_orig
                and item.txn_type == "TRANSFER"
                and self.config.structuring_min_amount <= item.amount <= self.config.structuring_max_amount
                and item.event_time.timestamp() >= window_start
            )
        ]
        total_amount = sum(item.amount for item in matching) + event.amount
        return (
            len(matching) + 1 >= self.config.structuring_count_threshold
            and total_amount >= self.config.structuring_total_amount_threshold
        )

    def _is_new_counterparty_large_transfer(self, event: TransactionEvent, known_counterparties: set[str]) -> bool:
        return (
            event.txn_type == "TRANSFER"
            and event.amount >= self.config.new_counterparty_amount_threshold
            and event.name_dest not in known_counterparties
        )

    def _has_cashout_after_inbound_chain(self, event: TransactionEvent, recent_inbound_events: list[TransactionEvent]) -> bool:
        if event.txn_type != "CASH_OUT":
            return False
        window_start = event.event_time.timestamp(
        ) - self.config.cashout_after_inbound_window_seconds
        matching = [
            item
            for item in recent_inbound_events
            if item.name_dest == event.name_orig and item.event_time.timestamp() >= window_start
        ]
        if not matching:
            return False
        inbound_total = sum(item.amount for item in matching)
        if inbound_total <= 0:
            return False
        return (event.amount / inbound_total) >= self.config.cashout_after_inbound_ratio_threshold
