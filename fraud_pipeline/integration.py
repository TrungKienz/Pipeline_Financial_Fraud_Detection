from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from .models import TransactionEvent


class IntegrationError(ValueError):
    pass


def _value(payload: Mapping[str, Any], key: str) -> Any:
    if key not in payload:
        raise IntegrationError(f"Thieu truong bat buoc: {key}")
    return payload[key]


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise IntegrationError(f"Gia tri thoi gian khong hop le: {value!r}")


def integrate_logical_streams(
    transaction_payload: Mapping[str, Any],
    sender_payload: Mapping[str, Any],
    receiver_payload: Mapping[str, Any],
) -> dict[str, Any]:
    tx_event_id = str(_value(transaction_payload, "event_id"))
    sender_source_event_id = str(_value(sender_payload, "source_event_id"))
    receiver_source_event_id = str(_value(receiver_payload, "source_event_id"))

    if sender_source_event_id != tx_event_id:
        raise IntegrationError("Sender stream khong khop source_event_id voi transaction stream")
    if receiver_source_event_id != tx_event_id:
        raise IntegrationError("Receiver stream khong khop source_event_id voi transaction stream")

    tx_step = int(_value(transaction_payload, "step"))
    sender_step = int(_value(sender_payload, "step"))
    receiver_step = int(_value(receiver_payload, "step"))
    if tx_step != sender_step or tx_step != receiver_step:
        raise IntegrationError("Step giua 3 logical streams khong dong nhat")

    tx_name_orig = str(_value(transaction_payload, "nameOrig"))
    tx_name_dest = str(_value(transaction_payload, "nameDest"))
    sender_name_orig = str(_value(sender_payload, "nameOrig"))
    receiver_name_dest = str(_value(receiver_payload, "nameDest"))
    if tx_name_orig != sender_name_orig:
        raise IntegrationError("nameOrig giua transaction va sender_state khong khop")
    if tx_name_dest != receiver_name_dest:
        raise IntegrationError("nameDest giua transaction va receiver_state khong khop")

    tx_event_time = _parse_datetime(_value(transaction_payload, "event_time"))
    sender_event_time = _parse_datetime(_value(sender_payload, "event_time"))
    receiver_event_time = _parse_datetime(_value(receiver_payload, "event_time"))
    if tx_event_time != sender_event_time or tx_event_time != receiver_event_time:
        raise IntegrationError("event_time giua 3 logical streams khong khop")

    producer_ts = _parse_datetime(transaction_payload.get("producer_ts", tx_event_time))
    return {
        "event_id": tx_event_id,
        "event_time": tx_event_time,
        "producer_ts": producer_ts,
        "step": tx_step,
        "type": str(_value(transaction_payload, "type")),
        "amount": float(_value(transaction_payload, "amount")),
        "nameOrig": tx_name_orig,
        "nameDest": tx_name_dest,
        "oldbalanceOrg": float(_value(sender_payload, "oldbalanceOrg")),
        "newbalanceOrig": float(_value(sender_payload, "newbalanceOrig")),
        "oldbalanceDest": float(_value(receiver_payload, "oldbalanceDest")),
        "newbalanceDest": float(_value(receiver_payload, "newbalanceDest")),
        "isFraud": int(_value(transaction_payload, "isFraud")),
        "schema_version": int(transaction_payload.get("schema_version", 1)),
    }


def integrated_payload_to_transaction_event(payload: Mapping[str, Any]) -> TransactionEvent:
    return TransactionEvent(
        event_id=str(_value(payload, "event_id")),
        event_time=_parse_datetime(_value(payload, "event_time")),
        producer_ts=_parse_datetime(payload.get("producer_ts", _value(payload, "event_time"))),
        step=int(_value(payload, "step")),
        txn_type=str(_value(payload, "type")),
        amount=float(_value(payload, "amount")),
        name_orig=str(_value(payload, "nameOrig")),
        oldbalance_org=float(_value(payload, "oldbalanceOrg")),
        newbalance_orig=float(_value(payload, "newbalanceOrig")),
        name_dest=str(_value(payload, "nameDest")),
        oldbalance_dest=float(_value(payload, "oldbalanceDest")),
        newbalance_dest=float(_value(payload, "newbalanceDest")),
        is_fraud=int(_value(payload, "isFraud")),
        schema_version=int(payload.get("schema_version", 1)),
    )
