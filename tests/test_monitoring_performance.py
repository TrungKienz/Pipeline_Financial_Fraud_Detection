import unittest

import pandas as pd

from monitoring.model.metrics_store import prepare_labeled_predictions
from monitoring.model.performance_report import build_performance_summary, compute_binary_metrics


class MonitoringPerformanceTests(unittest.TestCase):
    def test_prepare_labeled_predictions_merges_review_labels(self) -> None:
        predictions = pd.DataFrame(
            [
                {
                    "event_ts": "2026-01-01T00:00:00+00:00",
                    "event_id": "evt-1",
                    "alert_id": "alert:evt-1",
                    "is_alert": True,
                    "actual_label": None,
                },
                {
                    "event_ts": "2026-01-01T00:05:00+00:00",
                    "event_id": "evt-2",
                    "alert_id": "alert:evt-2",
                    "is_alert": False,
                    "actual_label": None,
                },
            ]
        )
        reviews = pd.DataFrame(
            [
                {"alert_id": "alert:evt-1", "event_id": "evt-1", "review_label": "fraud", "review_status": "confirmed_fraud", "reviewed_at": "2026-01-02T00:00:00+00:00"},
                {"alert_id": "alert:evt-2", "event_id": "evt-2", "review_label": "legit", "review_status": "false_positive", "reviewed_at": "2026-01-02T00:05:00+00:00"},
            ]
        )

        merged = prepare_labeled_predictions(predictions, reviews)

        self.assertEqual(int(merged["label_available"].sum()), 2)
        self.assertEqual(merged.loc[merged["event_id"] == "evt-1", "effective_label"].iloc[0], "fraud")
        self.assertEqual(merged.loc[merged["event_id"] == "evt-2", "effective_label"].iloc[0], "legit")

    def test_compute_binary_metrics_counts_tp_fp_tn_fn(self) -> None:
        frame = pd.DataFrame(
            [
                {"label_available": True, "predicted_positive": True, "actual_positive": True, "actual_negative": False},
                {"label_available": True, "predicted_positive": True, "actual_positive": False, "actual_negative": True},
                {"label_available": True, "predicted_positive": False, "actual_positive": True, "actual_negative": False},
                {"label_available": True, "predicted_positive": False, "actual_positive": False, "actual_negative": True},
            ]
        )

        metrics = compute_binary_metrics(frame)

        self.assertEqual(metrics["tp"], 1)
        self.assertEqual(metrics["fp"], 1)
        self.assertEqual(metrics["tn"], 1)
        self.assertEqual(metrics["fn"], 1)
        self.assertEqual(metrics["precision"], 0.5)
        self.assertEqual(metrics["recall"], 0.5)
        self.assertEqual(metrics["f1"], 0.5)
        self.assertEqual(metrics["false_positive_rate"], 0.5)

    def test_build_performance_summary_reports_coverage(self) -> None:
        predictions = pd.DataFrame(
            [
                {"event_ts": "2026-01-01T00:00:00+00:00", "event_id": "evt-1", "alert_id": "alert:evt-1", "is_alert": True, "actual_label": None},
                {"event_ts": "2026-01-01T00:10:00+00:00", "event_id": "evt-2", "alert_id": None, "is_alert": False, "actual_label": None},
            ]
        )
        reviews = pd.DataFrame(
            [
                {"alert_id": "alert:evt-1", "event_id": "evt-1", "review_label": "fraud", "review_status": "confirmed_fraud", "reviewed_at": "2026-01-02T00:00:00+00:00"}
            ]
        )

        summary = build_performance_summary(predictions, reviews)

        self.assertEqual(summary["prediction_rows"], 2)
        self.assertEqual(summary["labeled_rows"], 1)
        self.assertEqual(summary["unlabeled_rows"], 1)
        self.assertEqual(summary["label_coverage"], 0.5)
        self.assertIn("rolling_windows", summary)


if __name__ == "__main__":
    unittest.main()
