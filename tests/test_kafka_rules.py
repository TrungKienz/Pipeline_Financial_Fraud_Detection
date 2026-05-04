import unittest

from fraud_pipeline import PipelineConfig
from fraud_pipeline.kafka_rules import build_runtime_rule_state


class KafkaRuleStateTests(unittest.TestCase):
    def test_build_runtime_rule_state_applies_amount_and_velocity_overrides(self) -> None:
        state = build_runtime_rule_state(
            [
                {
                    "rule_id": "transfer-threshold",
                    "rule_type": "amount_threshold",
                    "txn_type": "TRANSFER",
                    "threshold": 125000.0,
                },
                {
                    "rule_id": "velocity-default",
                    "rule_type": "velocity_threshold",
                    "txn_type": "ANY",
                    "threshold": 456000.0,
                    "count_threshold": 5,
                },
            ],
            config=PipelineConfig(),
        )

        self.assertEqual(state.amount_thresholds["TRANSFER"], 125000.0)
        self.assertEqual(state.rapid_outflow_amount_threshold, 456000.0)
        self.assertEqual(state.rapid_outflow_count_threshold, 5)

    def test_build_runtime_rule_state_supports_watchlist_mutations(self) -> None:
        state = build_runtime_rule_state(
            [
                {
                    "rule_id": "watchlist-add-1",
                    "rule_type": "watchlist_update",
                    "account_id": "C100",
                    "operation": "add",
                },
                {
                    "rule_id": "watchlist-remove-1",
                    "rule_type": "watchlist_update",
                    "account_id": "C100",
                    "operation": "remove",
                },
                {
                    "rule_id": "watchlist-add-2",
                    "rule_type": "watchlist_update",
                    "account_id": "C200",
                    "operation": "add",
                },
            ]
        )

        self.assertEqual(state.watchlisted_accounts, frozenset({"C200"}))


if __name__ == "__main__":
    unittest.main()
