from __future__ import annotations

import hashlib
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


def dest_is_merchant(event: TransactionEvent) -> int:
    return int(event.name_dest.startswith("M"))


def hour_of_day(event: TransactionEvent) -> int:
    return event.step % 24


def sender_balance_discrepancy(event: TransactionEvent) -> float:
    if event.txn_type not in SENDER_DEBIT_TYPES:
        return 0.0
    expected = event.oldbalance_org - event.amount
    return expected - event.newbalance_orig


def receiver_balance_discrepancy(event: TransactionEvent) -> float:
    if event.txn_type not in RECEIVER_CREDIT_TYPES:
        return 0.0
    expected = event.oldbalance_dest + event.amount
    return expected - event.newbalance_dest


def is_night_transaction(event: TransactionEvent) -> int:
    hour = event.step % 24
    return int(hour >= 22 or hour <= 6)


def _hash_unit(event_id: str, salt: str) -> float:
    """Deterministic, reproducible uniform draw in [0, 1) keyed by (salt, event_id).

    Independent of the fraud label, so identical at train and serving time.
    """
    h = int(hashlib.md5(f"{salt}:{event_id}".encode("utf-8")).hexdigest(), 16)
    return (h % 10_000) / 10_000.0


def _risk_proxy(event: TransactionEvent) -> float:
    """Latent risk score in [0, 1] derived ONLY from real transaction attributes.

    Never reads ``event.is_fraud`` -> no target leakage and no train/serve skew.
    The synthetic contextual fields are sampled with probabilities that scale with
    this proxy, giving a plausible (non-circular) correlation with fraud drivers.
    """
    type_risk = 1.0 if event.txn_type in {"TRANSFER", "CASH_OUT"} else 0.0
    amount_risk = min(event.amount / 200_000.0, 1.0)
    depletion = sender_depletion_ratio(event)
    night = float(is_night_transaction(event))
    proxy = 0.35 * type_risk + 0.25 * amount_risk + 0.25 * depletion + 0.15 * night
    return max(0.0, min(proxy, 1.0))


def get_browser(event: TransactionEvent) -> str:
    # Browser carries little fraud signal -> keep it (near-)uniform but label-free.
    browsers = ["chrome", "safari", "firefox", "edge"]
    u = _hash_unit(event.event_id, "browser")
    return browsers[min(int(u * 4), 3)]


def get_device_type(event: TransactionEvent) -> str:
    # Higher risk skews toward mobile/tablet (label-independent).
    u = _hash_unit(event.event_id, "device")
    proxy = _risk_proxy(event)
    p_desktop = 0.6 - 0.4 * proxy   # 0.6 -> 0.2 as risk rises
    p_mobile = 0.3 + 0.2 * proxy    # 0.3 -> 0.5 as risk rises
    if u < p_desktop:
        return "desktop"
    if u < p_desktop + p_mobile:
        return "mobile"
    return "tablet"


def get_country(event: TransactionEvent) -> str:
    # Higher risk skews the country distribution (label-independent).
    countries = ["US", "VN", "SG", "PH", "TH"]
    u = _hash_unit(event.event_id, "country")
    if _risk_proxy(event) > 0.5:
        weights = [0.12, 0.12, 0.16, 0.30, 0.30]
    else:
        weights = [0.30, 0.25, 0.20, 0.15, 0.10]
    cum = 0.0
    for c, w in zip(countries, weights):
        cum += w
        if u < cum:
            return c
    return countries[-1]


def new_device_flag(event: TransactionEvent) -> int:
    # Base 5% rising to ~40% with risk proxy; independent of the label.
    prob = 0.05 + 0.35 * _risk_proxy(event)
    return int(_hash_unit(event.event_id, "new_device") < prob)


def shipping_billing_mismatch(event: TransactionEvent) -> int:
    # Base 2% rising to ~30% with risk proxy; independent of the label.
    prob = 0.02 + 0.28 * _risk_proxy(event)
    return int(_hash_unit(event.event_id, "ship_bill") < prob)


def ip_billing_country_mismatch(event: TransactionEvent) -> int:
    # Base 3% rising to ~25% with risk proxy; independent of the label.
    prob = 0.03 + 0.22 * _risk_proxy(event)
    return int(_hash_unit(event.event_id, "ip_bill") < prob)


def failed_payment_attempts_24h(event: TransactionEvent) -> int:
    # 0..3 attempts; likelihood and count grow with risk proxy, independent of label.
    prob_any = 0.05 + 0.35 * _risk_proxy(event)
    u = _hash_unit(event.event_id, "failed_pay")
    if u >= prob_any:
        return 0
    intensity = (prob_any - u) / prob_any  # in (0, 1]
    return 1 + min(int(intensity * 3), 2)  # 1..3


def build_feature_record(
    event: TransactionEvent,
    config: PipelineConfig | None = None,
    dynamic_features: dict[str, float] | None = None,
) -> dict[str, float | int | str]:
    record = {
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
        "dest_is_merchant": dest_is_merchant(event),
        "hour_of_day": hour_of_day(event),
        "sender_balance_discrepancy": sender_balance_discrepancy(event),
        "receiver_balance_discrepancy": receiver_balance_discrepancy(event),
        
        # New static features
        "is_night_transaction": is_night_transaction(event),
        "new_device_flag": new_device_flag(event),
        "shipping_billing_mismatch": shipping_billing_mismatch(event),
        "ip_billing_country_mismatch": ip_billing_country_mismatch(event),
        "failed_payment_attempts_24h": failed_payment_attempts_24h(event),
        "browser": get_browser(event),
        "device_type": get_device_type(event),
        "country": get_country(event),
        
        "label_is_fraud": event.is_fraud,
    }

    # Add dynamic features, defaulting to 0.0 or sensible defaults if not provided
    dynamic_cols = [
        "sender_recent_txn_count",
        "sender_recent_total_amount",
        "receiver_recent_txn_count",
        "receiver_recent_total_amount",
        "is_new_counterparty",
        "inbound_to_cashout_ratio",
        "velocity_transactions_1h",
        "time_since_last_purchase",
    ]
    for col in dynamic_cols:
        if dynamic_features and col in dynamic_features:
            record[col] = float(dynamic_features[col])
        else:
            if col == "time_since_last_purchase":
                record[col] = 86400.0  # Default to 1 day in seconds
            else:
                record[col] = 0.0

    return record


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
    "dest_is_merchant",
    "hour_of_day",
    "sender_balance_discrepancy",
    "receiver_balance_discrepancy",
    "is_night_transaction",
    "new_device_flag",
    "shipping_billing_mismatch",
    "ip_billing_country_mismatch",
    "failed_payment_attempts_24h",
    "sender_recent_txn_count",
    "sender_recent_total_amount",
    "receiver_recent_txn_count",
    "receiver_recent_total_amount",
    "is_new_counterparty",
    "inbound_to_cashout_ratio",
    "velocity_transactions_1h",
    "time_since_last_purchase",
]

TXN_TYPE_CATEGORIES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
BROWSER_CATEGORIES = ["chrome", "safari", "firefox", "edge"]
DEVICE_TYPE_CATEGORIES = ["desktop", "mobile", "tablet"]
COUNTRY_CATEGORIES = ["US", "VN", "SG", "PH", "TH"]



