import unittest

from fraud_pipeline.validation import build_streaming_validation_cases, expected_dead_letter_index


def sample_transaction() -> dict[str, object]:
    return {
        "event_id": "evt-1",
        "event_time": "2026-01-01T00:01:00+00:00",
        "producer_ts": "2026-01-01T00:01:00+00:00",
        "step": 1,
        "type": "TRANSFER",
        "amount": 1000.0,
        "nameOrig": "C123",
        "nameDest": "C456",
        "isFraud": 0,
        "schema_version": 1,
    }


def sample_sender() -> dict[str, object]:
    return {
        "event_id": "evt-1:sender",
        "source_event_id": "evt-1",
        "event_time": "2026-01-01T00:01:00+00:00",
        "step": 1,
        "nameOrig": "C123",
        "oldbalanceOrg": 5000.0,
        "newbalanceOrig": 4000.0,
    }


def sample_receiver() -> dict[str, object]:
    return {
        "event_id": "evt-1:receiver",
        "source_event_id": "evt-1",
        "event_time": "2026-01-01T00:01:00+00:00",
        "step": 1,
        "nameDest": "C456",
        "oldbalanceDest": 100.0,
        "newbalanceDest": 1100.0,
    }


class ValidationCaseTests(unittest.TestCase):
    def test_build_streaming_validation_cases_creates_expected_patterns(self) -> None:
        cases = build_streaming_validation_cases(sample_transaction(), sample_sender(), sample_receiver(), "run123")
        case_by_name = {case.name: case for case in cases}

        clean = case_by_name["clean_integration"]
        self.assertEqual(clean.transaction["event_id"], "validation-clean-run123")
        self.assertEqual(clean.sender["source_event_id"], "validation-clean-run123")
        self.assertEqual(clean.receiver["source_event_id"], "validation-clean-run123")

        mismatch = case_by_name["semantic_mismatch"]
        self.assertNotEqual(mismatch.transaction["nameDest"], mismatch.receiver["nameDest"])

        orphan_sender = case_by_name["orphan_sender_state"]
        self.assertTrue(str(orphan_sender.sender["source_event_id"]).endswith("-missing"))

        orphan_receiver = case_by_name["orphan_receiver_state"]
        self.assertTrue(str(orphan_receiver.receiver["source_event_id"]).endswith("-missing"))

    def test_expected_dead_letter_index_returns_only_negative_cases(self) -> None:
        cases = build_streaming_validation_cases(sample_transaction(), sample_sender(), sample_receiver(), "run123")
        index = expected_dead_letter_index(cases)

        self.assertNotIn("clean_integration", index)
        self.assertEqual(index["missing_sender_state"][1], "missing_sender_state")
        self.assertEqual(index["semantic_mismatch"][1], "semantic_key_mismatch")


if __name__ == "__main__":
    unittest.main()
