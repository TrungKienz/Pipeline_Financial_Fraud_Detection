"""Guard against target leakage in engineered/synthetic features.

Every feature must be computable at serving time, where the fraud label is
unknown. This test asserts that flipping ``is_fraud`` on an otherwise identical
event does not change any engineered feature -- i.e. no feature reads the label.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest

from fraud_pipeline.models import TransactionEvent
from fraud_pipeline.features import (
    FEATURE_COLUMNS,
    build_feature_record,
    get_browser,
    get_country,
    get_device_type,
)


def _make_event(is_fraud: int) -> TransactionEvent:
    ts = datetime(2024, 1, 1, 23, 0, 0)
    return TransactionEvent(
        event_id="evt-leakage-check-001",
        event_time=ts,
        producer_ts=ts,
        step=23,
        txn_type="TRANSFER",
        amount=185_000.0,
        name_orig="C123456789",
        oldbalance_org=185_000.0,
        newbalance_orig=0.0,
        name_dest="C987654321",
        oldbalance_dest=0.0,
        newbalance_dest=185_000.0,
        is_fraud=is_fraud,
    )


def test_feature_vector_is_independent_of_label():
    legit = _make_event(is_fraud=0)
    fraud = replace(legit, is_fraud=1)

    rec_legit = build_feature_record(legit)
    rec_fraud = build_feature_record(fraud)

    for col in FEATURE_COLUMNS:
        assert rec_legit[col] == rec_fraud[col], (
            f"Feature {col!r} depends on the fraud label -> target leakage"
        )


def test_categoricals_are_independent_of_label():
    legit = _make_event(is_fraud=0)
    fraud = replace(legit, is_fraud=1)

    assert get_browser(legit) == get_browser(fraud)
    assert get_device_type(legit) == get_device_type(fraud)
    assert get_country(legit) == get_country(fraud)


@pytest.mark.parametrize("event_id", [f"evt-{i:05d}" for i in range(50)])
def test_synthetic_flags_stay_in_range(event_id):
    ts = datetime(2024, 1, 1, 3, 0, 0)
    ev = TransactionEvent(
        event_id=event_id,
        event_time=ts,
        producer_ts=ts,
        step=3,
        txn_type="CASH_OUT",
        amount=50_000.0,
        name_orig="C1",
        oldbalance_org=50_000.0,
        newbalance_orig=0.0,
        name_dest="C2",
        oldbalance_dest=0.0,
        newbalance_dest=50_000.0,
        is_fraud=0,
    )
    rec = build_feature_record(ev)
    assert rec["new_device_flag"] in (0, 1)
    assert rec["shipping_billing_mismatch"] in (0, 1)
    assert rec["ip_billing_country_mismatch"] in (0, 1)
    assert 0 <= rec["failed_payment_attempts_24h"] <= 3
