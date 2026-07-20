import unittest

from fraud_pipeline import (
    IntegrationError,
    integrated_payload_to_transaction_event,
    integrate_logical_streams,
)


def sample_transaction(**overrides):
    payload = {
        "event_id": "evt-1",
        "event_time": "2026-01-01T00:01:00+00:00",
        "producer_ts": "2026-01-01T00:01:00+00:00",
        "step": 1,
        "type": "TRANSFER",
        "amount": 250000.0,
        "nameOrig": "C123",
        "nameDest": "C456",
        "isFraud": 1,
        "schema_version": 1,
    }
    payload.update(overrides)
    return payload


def sample_sender(**overrides):
    payload = {
        "event_id": "evt-1:sender",
        "source_event_id": "evt-1",
        "event_time": "2026-01-01T00:01:00+00:00",
        "step": 1,
        "nameOrig": "C123",
        "oldbalanceOrg": 300000.0,
        "newbalanceOrig": 50000.0,
    }
    payload.update(overrides)
    return payload


def sample_receiver(**overrides):
    payload = {
        "event_id": "evt-1:receiver",
        "source_event_id": "evt-1",
        "event_time": "2026-01-01T00:01:00+00:00",
        "step": 1,
        "nameDest": "C456",
        "oldbalanceDest": 1000.0,
        "newbalanceDest": 251000.0,
    }
    payload.update(overrides)
    return payload


class IntegrationTests(unittest.TestCase):
    def test_integrate_logical_streams_merges_three_payloads(self):
        merged = integrate_logical_streams(
            sample_transaction(),
            sample_sender(),
            sample_receiver(),
        )

        self.assertEqual(merged["event_id"], "evt-1")
        self.assertEqual(merged["oldbalanceOrg"], 300000.0)
        self.assertEqual(merged["newbalanceDest"], 251000.0)

    def test_integrate_logical_streams_rejects_mismatched_semantic_keys(self):
        with self.assertRaises(IntegrationError):
            integrate_logical_streams(
                sample_transaction(nameDest="C999"),
                sample_sender(),
                sample_receiver(),
            )

    def test_integrated_payload_to_transaction_event_restores_domain_model(self):
        merged = integrate_logical_streams(
            sample_transaction(),
            sample_sender(),
            sample_receiver(),
        )
        event = integrated_payload_to_transaction_event(merged)

        self.assertEqual(event.name_orig, "C123")
        self.assertEqual(event.name_dest, "C456")
        self.assertEqual(event.oldbalance_org, 300000.0)
        self.assertEqual(event.newbalance_dest, 251000.0)


if __name__ == "__main__":
    unittest.main()
