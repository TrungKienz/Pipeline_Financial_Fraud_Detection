from __future__ import annotations

import csv
from itertools import zip_longest
from pathlib import Path
from typing import Any, Iterator

from .config import PipelineConfig
from .parsing import derive_account_state_updates, iter_transaction_events
from .serialization import receiver_state_to_dict, sender_state_to_dict, transaction_to_dict


TRANSACTION_SOURCE_FILENAME = "transaction_source.csv"
SENDER_STATE_SOURCE_FILENAME = "sender_state_source.csv"
RECEIVER_STATE_SOURCE_FILENAME = "receiver_state_source.csv"

TRANSACTION_SOURCE_FIELDS = [
    "event_id",
    "event_time",
    "producer_ts",
    "step",
    "type",
    "amount",
    "nameOrig",
    "nameDest",
    "isFraud",
    "schema_version",
]

SENDER_STATE_SOURCE_FIELDS = [
    "event_id",
    "source_event_id",
    "event_time",
    "step",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
]

RECEIVER_STATE_SOURCE_FIELDS = [
    "event_id",
    "source_event_id",
    "event_time",
    "step",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
]


class SourceDataError(ValueError):
    pass


def logical_source_paths(output_dir: str | Path) -> dict[str, Path]:
    root = Path(output_dir)
    return {
        "transaction": root / TRANSACTION_SOURCE_FILENAME,
        "sender_state": root / SENDER_STATE_SOURCE_FILENAME,
        "receiver_state": root / RECEIVER_STATE_SOURCE_FILENAME,
    }


def split_integrated_csv_to_logical_sources(
    csv_path: str | Path,
    output_dir: str | Path,
    config: PipelineConfig | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    config = config or PipelineConfig()
    paths = logical_source_paths(output_dir)
    paths["transaction"].parent.mkdir(parents=True, exist_ok=True)

    with paths["transaction"].open("w", encoding="utf-8", newline="") as transaction_handle, \
        paths["sender_state"].open("w", encoding="utf-8", newline="") as sender_handle, \
        paths["receiver_state"].open("w", encoding="utf-8", newline="") as receiver_handle:
        transaction_writer = csv.DictWriter(transaction_handle, fieldnames=TRANSACTION_SOURCE_FIELDS)
        sender_writer = csv.DictWriter(sender_handle, fieldnames=SENDER_STATE_SOURCE_FIELDS)
        receiver_writer = csv.DictWriter(receiver_handle, fieldnames=RECEIVER_STATE_SOURCE_FIELDS)
        transaction_writer.writeheader()
        sender_writer.writeheader()
        receiver_writer.writeheader()

        event_count = 0
        for event in iter_transaction_events(csv_path, config=config, limit=limit):
            transaction_writer.writerow(transaction_to_dict(event))
            for update in derive_account_state_updates(event):
                if update.role == "sender":
                    sender_writer.writerow(sender_state_to_dict(update))
                elif update.role == "receiver":
                    receiver_writer.writerow(receiver_state_to_dict(update))
                else:
                    raise SourceDataError(f"Khong ho tro role: {update.role}")
            event_count += 1

    return {
        "event_count": event_count,
        "transaction_csv": str(paths["transaction"]),
        "sender_state_csv": str(paths["sender_state"]),
        "receiver_state_csv": str(paths["receiver_state"]),
    }


def _iter_csv_rows(csv_path: str | Path, limit: int | None = None) -> Iterator[dict[str, str]]:
    with Path(csv_path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader):
            if limit is not None and index >= limit:
                break
            yield row


def load_transaction_source_row(row: dict[str, str]) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "event_time": row["event_time"],
        "producer_ts": row["producer_ts"],
        "step": int(row["step"]),
        "type": row["type"],
        "amount": float(row["amount"]),
        "nameOrig": row["nameOrig"],
        "nameDest": row["nameDest"],
        "isFraud": int(row["isFraud"]),
        "schema_version": int(row.get("schema_version", "1")),
    }


def load_sender_state_source_row(row: dict[str, str]) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "source_event_id": row["source_event_id"],
        "event_time": row["event_time"],
        "step": int(row["step"]),
        "nameOrig": row["nameOrig"],
        "oldbalanceOrg": float(row["oldbalanceOrg"]),
        "newbalanceOrig": float(row["newbalanceOrig"]),
    }


def load_receiver_state_source_row(row: dict[str, str]) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "source_event_id": row["source_event_id"],
        "event_time": row["event_time"],
        "step": int(row["step"]),
        "nameDest": row["nameDest"],
        "oldbalanceDest": float(row["oldbalanceDest"]),
        "newbalanceDest": float(row["newbalanceDest"]),
    }


def iter_logical_source_triplets(
    source_dir: str | Path,
    limit: int | None = None,
) -> Iterator[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]]:
    paths = logical_source_paths(source_dir)
    transaction_rows = _iter_csv_rows(paths["transaction"], limit=limit)
    sender_rows = _iter_csv_rows(paths["sender_state"], limit=limit)
    receiver_rows = _iter_csv_rows(paths["receiver_state"], limit=limit)

    for index, triple in enumerate(zip_longest(transaction_rows, sender_rows, receiver_rows), start=1):
        transaction_row, sender_row, receiver_row = triple
        if transaction_row is None or sender_row is None or receiver_row is None:
            raise SourceDataError(
                f"So dong giua 3 file nguon khong dong bo tai vi tri {index}. "
                "Can tach lai 3 CSV tu du lieu goc."
            )

        transaction_payload = load_transaction_source_row(transaction_row)
        sender_payload = load_sender_state_source_row(sender_row)
        receiver_payload = load_receiver_state_source_row(receiver_row)

        tx_event_id = transaction_payload["event_id"]
        if sender_payload["source_event_id"] != tx_event_id:
            raise SourceDataError(f"sender_state source_event_id khong khop transaction event_id tai dong {index}")
        if receiver_payload["source_event_id"] != tx_event_id:
            raise SourceDataError(f"receiver_state source_event_id khong khop transaction event_id tai dong {index}")

        yield transaction_payload, sender_payload, receiver_payload


def iter_transaction_source_payloads(
    source_dir: str | Path,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    paths = logical_source_paths(source_dir)
    for row in _iter_csv_rows(paths["transaction"], limit=limit):
        yield load_transaction_source_row(row)


def iter_sender_state_source_payloads(
    source_dir: str | Path,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    paths = logical_source_paths(source_dir)
    for row in _iter_csv_rows(paths["sender_state"], limit=limit):
        yield load_sender_state_source_row(row)


def iter_receiver_state_source_payloads(
    source_dir: str | Path,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    paths = logical_source_paths(source_dir)
    for row in _iter_csv_rows(paths["receiver_state"], limit=limit):
        yield load_receiver_state_source_row(row)
