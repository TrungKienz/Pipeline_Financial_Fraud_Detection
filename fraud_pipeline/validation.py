from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class ValidationCase:
    name: str
    transaction: dict[str, Any] | None
    sender: dict[str, Any] | None
    receiver: dict[str, Any] | None
    expected_dead_letter_error: str | None
    expected_dead_letter_source_key: str | None
    expected_cassandra_event_id: str | None


def _copy_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return dict(payload)


def _retarget_triplet(
    transaction_payload: Mapping[str, Any],
    sender_payload: Mapping[str, Any],
    receiver_payload: Mapping[str, Any],
    event_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    transaction = _copy_payload(transaction_payload)
    sender = _copy_payload(sender_payload)
    receiver = _copy_payload(receiver_payload)

    transaction["event_id"] = event_id
    sender["event_id"] = f"{event_id}:sender"
    sender["source_event_id"] = event_id
    receiver["event_id"] = f"{event_id}:receiver"
    receiver["source_event_id"] = event_id
    return transaction, sender, receiver


def build_streaming_validation_cases(
    transaction_payload: Mapping[str, Any],
    sender_payload: Mapping[str, Any],
    receiver_payload: Mapping[str, Any],
    run_suffix: str,
) -> tuple[ValidationCase, ...]:
    clean_tx, clean_sender, clean_receiver = _retarget_triplet(
        transaction_payload,
        sender_payload,
        receiver_payload,
        f"validation-clean-{run_suffix}",
    )

    missing_sender_tx, _, missing_sender_receiver = _retarget_triplet(
        transaction_payload,
        sender_payload,
        receiver_payload,
        f"validation-missing-sender-{run_suffix}",
    )

    missing_receiver_tx, missing_receiver_sender, _ = _retarget_triplet(
        transaction_payload,
        sender_payload,
        receiver_payload,
        f"validation-missing-receiver-{run_suffix}",
    )

    mismatch_tx, mismatch_sender, mismatch_receiver = _retarget_triplet(
        transaction_payload,
        sender_payload,
        receiver_payload,
        f"validation-mismatch-{run_suffix}",
    )
    mismatch_receiver["nameDest"] = f"{mismatch_receiver['nameDest']}-MISMATCH"

    orphan_sender_tx, orphan_sender, _ = _retarget_triplet(
        transaction_payload,
        sender_payload,
        receiver_payload,
        f"validation-orphan-sender-{run_suffix}",
    )
    orphan_sender["source_event_id"] = f"{orphan_sender_tx['event_id']}-missing"

    orphan_receiver_tx, _, orphan_receiver = _retarget_triplet(
        transaction_payload,
        sender_payload,
        receiver_payload,
        f"validation-orphan-receiver-{run_suffix}",
    )
    orphan_receiver["source_event_id"] = f"{orphan_receiver_tx['event_id']}-missing"

    return (
        ValidationCase(
            name="clean_integration",
            transaction=clean_tx,
            sender=clean_sender,
            receiver=clean_receiver,
            expected_dead_letter_error=None,
            expected_dead_letter_source_key=None,
            expected_cassandra_event_id=str(clean_tx["event_id"]),
        ),
        ValidationCase(
            name="missing_sender_state",
            transaction=missing_sender_tx,
            sender=None,
            receiver=missing_sender_receiver,
            expected_dead_letter_error="missing_sender_state",
            expected_dead_letter_source_key=str(missing_sender_tx["event_id"]),
            expected_cassandra_event_id=None,
        ),
        ValidationCase(
            name="missing_receiver_state",
            transaction=missing_receiver_tx,
            sender=missing_receiver_sender,
            receiver=None,
            expected_dead_letter_error="missing_receiver_state",
            expected_dead_letter_source_key=str(missing_receiver_tx["event_id"]),
            expected_cassandra_event_id=None,
        ),
        ValidationCase(
            name="semantic_mismatch",
            transaction=mismatch_tx,
            sender=mismatch_sender,
            receiver=mismatch_receiver,
            expected_dead_letter_error="semantic_key_mismatch",
            expected_dead_letter_source_key=str(mismatch_tx["event_id"]),
            expected_cassandra_event_id=None,
        ),
        ValidationCase(
            name="orphan_sender_state",
            transaction=None,
            sender=orphan_sender,
            receiver=None,
            expected_dead_letter_error="orphan_sender_state",
            expected_dead_letter_source_key=str(orphan_sender["event_id"]),
            expected_cassandra_event_id=None,
        ),
        ValidationCase(
            name="orphan_receiver_state",
            transaction=None,
            sender=None,
            receiver=orphan_receiver,
            expected_dead_letter_error="orphan_receiver_state",
            expected_dead_letter_source_key=str(orphan_receiver["event_id"]),
            expected_cassandra_event_id=None,
        ),
    )


def expected_dead_letter_index(cases: tuple[ValidationCase, ...]) -> dict[str, tuple[str, str]]:
    return {
        case.name: (str(case.expected_dead_letter_source_key), str(case.expected_dead_letter_error))
        for case in cases
        if case.expected_dead_letter_error and case.expected_dead_letter_source_key
    }
