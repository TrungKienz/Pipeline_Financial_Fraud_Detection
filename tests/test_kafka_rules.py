import unittest

from fraud_pipeline import PipelineConfig
from fraud_pipeline.kafka_rules import build_runtime_rule_state


class KafkaRuleStateTests(unittest.TestCase):
    def test_build_runtime_rule_state_applies_behavioral_rule_overrides(self) -> None:
        state = build_runtime_rule_state(
            [
                {
                    "rule_id": "account-drain",
                    "rule_type": "account_drain_threshold",
                    "min_balance_floor": 75000.0,
                    "ratio_threshold": 0.9,
                    "near_zero_balance": 500.0,
                    "weight": 0.55,
                },
                {
                    "rule_id": "fan-out",
                    "rule_type": "fan_out_threshold",
                    "window_seconds": 1200,
                    "distinct_receiver_threshold": 4,
                    "total_amount_threshold": 450000.0,
                    "weight": 0.33,
                },
                {
                    "rule_id": "fan-in",
                    "rule_type": "fan_in_threshold",
                    "window_seconds": 600,
                    "distinct_sender_threshold": 5,
                    "total_amount_threshold": 420000.0,
                    "weight": 0.31,
                },
                {
                    "rule_id": "structuring",
                    "rule_type": "structuring_threshold",
                    "window_seconds": 1500,
                    "count_threshold": 6,
                    "min_amount": 30000.0,
                    "max_amount": 85000.0,
                    "total_amount_threshold": 280000.0,
                    "weight": 0.28,
                },
                {
                    "rule_id": "new-counterparty",
                    "rule_type": "new_counterparty_threshold",
                    "amount_threshold": 210000.0,
                    "weight": 0.2,
                },
                {
                    "rule_id": "cashout-chain",
                    "rule_type": "cashout_after_inbound_threshold",
                    "window_seconds": 2700,
                    "ratio_threshold": 0.92,
                    "weight": 0.48,
                },
            ],
            config=PipelineConfig(),
        )

        self.assertEqual(state.account_drain_min_balance_floor, 75000.0)
        self.assertEqual(state.account_drain_ratio_threshold, 0.9)
        self.assertEqual(state.account_drain_near_zero_balance, 500.0)
        self.assertEqual(state.account_drain_weight, 0.55)
        self.assertEqual(state.fan_out_window_seconds, 1200)
        self.assertEqual(state.fan_out_distinct_receiver_threshold, 4)
        self.assertEqual(state.fan_out_total_amount_threshold, 450000.0)
        self.assertEqual(state.sender_fan_out_weight, 0.33)
        self.assertEqual(state.fan_in_window_seconds, 600)
        self.assertEqual(state.fan_in_distinct_sender_threshold, 5)
        self.assertEqual(state.fan_in_total_amount_threshold, 420000.0)
        self.assertEqual(state.receiver_fan_in_weight, 0.31)
        self.assertEqual(state.structuring_window_seconds, 1500)
        self.assertEqual(state.structuring_count_threshold, 6)
        self.assertEqual(state.structuring_min_amount, 30000.0)
        self.assertEqual(state.structuring_max_amount, 85000.0)
        self.assertEqual(state.structuring_total_amount_threshold, 280000.0)
        self.assertEqual(state.structured_split_weight, 0.28)
        self.assertEqual(state.new_counterparty_amount_threshold, 210000.0)
        self.assertEqual(state.new_counterparty_weight, 0.2)
        self.assertEqual(state.cashout_after_inbound_window_seconds, 2700)
        self.assertEqual(state.cashout_after_inbound_ratio_threshold, 0.92)
        self.assertEqual(state.cashout_after_inbound_weight, 0.48)

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
