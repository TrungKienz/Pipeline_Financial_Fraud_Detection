from __future__ import annotations

import csv
import hashlib
from datetime import timedelta
from pathlib import Path
from typing import Iterator

from .config import PipelineConfig
from .models import AccountStateUpdate, TransactionEvent


class ParseError(ValueError):
    pass


def build_event_id(row: dict[str, str]) -> str:
    raw = "|".join(
        [
            row["step"].strip(),
            row["type"].strip(),
            row["amount"].strip(),
            row["nameOrig"].strip(),
            row["nameDest"].strip(),
            row["oldbalanceOrg"].strip(),
            row["newbalanceOrig"].strip(),
            row["oldbalanceDest"].strip(),
            row["newbalanceDest"].strip(),
            row["isFraud"].strip(),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def parse_csv_row(row: dict[str, str], config: PipelineConfig | None = None) -> TransactionEvent:
    config = config or PipelineConfig()
    try:
        step = int(row["step"])
        amount = float(row["amount"])
        oldbalance_org = float(row["oldbalanceOrg"])
        newbalance_orig = float(row["newbalanceOrig"])
        oldbalance_dest = float(row["oldbalanceDest"])
        newbalance_dest = float(row["newbalanceDest"])
        is_fraud = int(row["isFraud"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"Invalid PaySim row: {exc}") from exc

    event_time = config.base_event_time + timedelta(seconds=step * config.step_seconds)
    event_id = build_event_id(row)
    return TransactionEvent(
        event_id=event_id,
        event_time=event_time,
        producer_ts=event_time,
        step=step,
        txn_type=row["type"].strip(),
        amount=amount,
        name_orig=row["nameOrig"].strip(),
        oldbalance_org=oldbalance_org,
        newbalance_orig=newbalance_orig,
        name_dest=row["nameDest"].strip(),
        oldbalance_dest=oldbalance_dest,
        newbalance_dest=newbalance_dest,
        is_fraud=is_fraud,
        schema_version=config.schema_version,
    )


def derive_account_state_updates(event: TransactionEvent) -> list[AccountStateUpdate]:
    return [
        AccountStateUpdate(
            event_id=f"{event.event_id}:sender",
            source_event_id=event.event_id,
            account_id=event.name_orig,
            role="sender",
            step=event.step,
            balance_before=event.oldbalance_org,
            balance_after=event.newbalance_orig,
            event_time=event.event_time,
        ),
        AccountStateUpdate(
            event_id=f"{event.event_id}:receiver",
            source_event_id=event.event_id,
            account_id=event.name_dest,
            role="receiver",
            step=event.step,
            balance_before=event.oldbalance_dest,
            balance_after=event.newbalance_dest,
            event_time=event.event_time,
        ),
    ]


def iter_transaction_events(
    csv_path: str | Path,
    config: PipelineConfig | None = None,
    limit: int | None = None,
) -> Iterator[TransactionEvent]:
    config = config or PipelineConfig()
    path = Path(csv_path)
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader):
            if limit is not None and index >= limit:
                break
            yield parse_csv_row(row, config=config)
