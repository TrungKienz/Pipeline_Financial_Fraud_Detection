import unittest

from fraud_pipeline import derive_account_state_updates, parse_csv_row, receiver_state_to_dict, sender_state_to_dict


def sample_row(**overrides: str) -> dict[str, str]:
    row = {
        "step": "5",
        "type": "TRANSFER",
        "amount": "700.0",
        "nameOrig": "C1",
        "oldbalanceOrg": "1000.0",
        "newbalanceOrig": "300.0",
        "nameDest": "C2",
        "oldbalanceDest": "10.0",
        "newbalanceDest": "710.0",
        "isFraud": "0",
        "isFlaggedFraud": "0",
    }
    row.update(overrides)
    return row


class SerializationTests(unittest.TestCase):
    def test_sender_state_payload_contains_correlation_key(self) -> None:
        event = parse_csv_row(sample_row())
        sender_update = derive_account_state_updates(event)[0]

        payload = sender_state_to_dict(sender_update)

        self.assertEqual(payload["source_event_id"], event.event_id)
        self.assertEqual(payload["nameOrig"], event.name_orig)
        self.assertEqual(payload["oldbalanceOrg"], event.oldbalance_org)

    def test_receiver_state_payload_contains_correlation_key(self) -> None:
        event = parse_csv_row(sample_row())
        receiver_update = derive_account_state_updates(event)[1]

        payload = receiver_state_to_dict(receiver_update)

        self.assertEqual(payload["source_event_id"], event.event_id)
        self.assertEqual(payload["nameDest"], event.name_dest)
        self.assertEqual(payload["newbalanceDest"], event.newbalance_dest)


if __name__ == "__main__":
    unittest.main()
