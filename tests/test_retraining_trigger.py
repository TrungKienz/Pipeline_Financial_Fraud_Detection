import unittest

from monitoring.model.check_retraining_trigger import evaluate_retraining_need


class RetrainingTriggerTests(unittest.TestCase):
    def test_retraining_trigger_fires_for_low_precision(self) -> None:
        drift_report = {
            "monitored_features": ["amount", "risk_score", "ml_score"],
            "drifted_feature_count": 0,
            "features": {
                "amount": {"ks_statistic": 0.05},
                "is_alert": {"distribution": {"True": {"reference": 0.1, "serving": 0.11}}},
            },
        }
        performance_report = {
            "generated_at": "2026-01-05T00:00:00+00:00",
            "label_coverage": 0.9,
            "warnings": [],
            "rolling_windows": {
                "7d": {
                    "labeled_rows": 50,
                    "precision": 0.5,
                    "recall": 0.8,
                    "f1": 0.62,
                }
            },
        }
        policy = {
            "feature_drift_ratio_threshold": 0.3,
            "amount_ks_stat_threshold": 0.2,
            "alert_rate_change_threshold": 0.1,
            "label_coverage_min": 0.5,
            "minimum_labeled_rows_7d": 25,
            "require_minimum_sample_for_trigger": True,
            "precision_7d_min": 0.8,
            "recall_7d_min": 0.7,
            "f1_7d_min": 0.75,
        }

        decision = evaluate_retraining_need(drift_report, performance_report, policy)

        self.assertTrue(decision["retrain_required"])
        self.assertTrue(any(item["type"] == "precision_7d" for item in decision["reasons"]))

    def test_retraining_trigger_stays_conservative_with_small_sample(self) -> None:
        drift_report = {
            "monitored_features": ["amount", "risk_score"],
            "drifted_feature_count": 0,
            "features": {
                "amount": {"ks_statistic": 0.05},
                "is_alert": {"distribution": {"True": {"reference": 0.1, "serving": 0.1}}},
            },
        }
        performance_report = {
            "generated_at": "2026-01-05T00:00:00+00:00",
            "label_coverage": 0.9,
            "warnings": [],
            "rolling_windows": {
                "7d": {
                    "labeled_rows": 3,
                    "precision": 0.1,
                    "recall": 0.1,
                    "f1": 0.1,
                }
            },
        }
        policy = {
            "feature_drift_ratio_threshold": 0.3,
            "amount_ks_stat_threshold": 0.2,
            "alert_rate_change_threshold": 0.1,
            "label_coverage_min": 0.5,
            "minimum_labeled_rows_7d": 25,
            "require_minimum_sample_for_trigger": True,
            "precision_7d_min": 0.8,
            "recall_7d_min": 0.7,
            "f1_7d_min": 0.75,
        }

        decision = evaluate_retraining_need(drift_report, performance_report, policy)

        self.assertFalse(any(item["type"] == "precision_7d" for item in decision["reasons"]))
        self.assertTrue(any("below the minimum 25" in warning for warning in decision["warnings"]))


if __name__ == "__main__":
    unittest.main()
