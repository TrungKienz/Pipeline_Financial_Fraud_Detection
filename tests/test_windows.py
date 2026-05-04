import unittest

from fraud_pipeline import parse_csv_row, sliding_window_metrics, tumbling_window_metrics


def sample_event(step: int, amount: float, is_fraud: int) -> dict[str, str]:
    return {
        "step": str(step),
        "type": "TRANSFER",
        "amount": str(amount),
        "nameOrig": f"C{step}",
        "oldbalanceOrg": "1000.0",
        "newbalanceOrig": "500.0",
        "nameDest": f"D{step}",
        "oldbalanceDest": "0.0",
        "newbalanceDest": "500.0",
        "isFraud": str(is_fraud),
        "isFlaggedFraud": "0",
    }


class WindowMetricTests(unittest.TestCase):
    def test_tumbling_window_metrics_groups_events(self) -> None:
        events = [
            parse_csv_row(sample_event(1, 100.0, 0)),
            parse_csv_row(sample_event(1, 200.0, 1)),
            parse_csv_row(sample_event(2, 300.0, 0)),
        ]

        metrics = tumbling_window_metrics(events, window_seconds=60)

        self.assertEqual(len(metrics), 2)
        self.assertEqual(metrics[0].event_count, 2)
        self.assertEqual(metrics[0].fraud_count, 1)
        self.assertEqual(metrics[1].total_amount, 300.0)

    def test_sliding_window_metrics_accumulates_overlapping_windows(self) -> None:
        events = [
            parse_csv_row(sample_event(1, 100.0, 0)),
            parse_csv_row(sample_event(2, 150.0, 0)),
            parse_csv_row(sample_event(3, 200.0, 1)),
        ]

        metrics = sliding_window_metrics(events, window_seconds=180, slide_seconds=60)

        self.assertTrue(metrics)
        self.assertGreaterEqual(metrics[0].event_count, 2)
        self.assertTrue(any(metric.fraud_count == 1 for metric in metrics))


if __name__ == "__main__":
    unittest.main()
