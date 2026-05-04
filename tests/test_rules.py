import unittest

from fraud_pipeline import PipelineConfig, RuleEngine, parse_csv_row


def sample_row(**overrides: str) -> dict[str, str]:
    row = {
        "step": "1",
        "type": "TRANSFER",
        "amount": "250000.0",
        "nameOrig": "C1",
        "oldbalanceOrg": "300000.0",
        "newbalanceOrig": "50000.0",
        "nameDest": "C2",
        "oldbalanceDest": "1000.0",
        "newbalanceDest": "251000.0",
        "isFraud": "1",
        "isFlaggedFraud": "0",
    }
    row.update(overrides)
    return row


class RuleEngineTests(unittest.TestCase):
    def test_high_amount_transfer_triggers_alert(self) -> None:
        engine = RuleEngine(PipelineConfig(high_amount_transfer_threshold=200000.0))
        event = parse_csv_row(sample_row())

        decision = engine.evaluate(event)

        self.assertTrue(decision.is_alert)
        self.assertIn("high_amount_transfer", decision.triggered_rules)

    def test_sender_balance_inconsistency_triggers_alert(self) -> None:
        engine = RuleEngine(PipelineConfig(balance_tolerance=0.1))
        event = parse_csv_row(sample_row(newbalanceOrig="12345.0"))

        decision = engine.evaluate(event)

        self.assertIn("sender_balance_inconsistency", decision.triggered_rules)

    def test_watchlist_and_rapid_outflow_increase_risk(self) -> None:
        config = PipelineConfig(
            high_amount_transfer_threshold=9999999.0,
            rapid_outflow_count_threshold=3,
            rapid_outflow_amount_threshold=500.0,
        )
        engine = RuleEngine(config)
        first = parse_csv_row(sample_row(step="1", amount="100.0", oldbalanceOrg="1000.0", newbalanceOrig="900.0"))
        second = parse_csv_row(sample_row(step="2", amount="150.0", oldbalanceOrg="900.0", newbalanceOrig="750.0"))
        current = parse_csv_row(sample_row(step="3", amount="300.0", oldbalanceOrg="750.0", newbalanceOrig="450.0"))

        decision = engine.evaluate(
            current,
            recent_sender_events=[first, second],
            watchlisted_accounts={"C1"},
        )

        self.assertIn("rapid_outflow_pattern", decision.triggered_rules)
        self.assertIn("watchlist_hit", decision.triggered_rules)
        self.assertGreater(decision.risk_score, 0.5)


if __name__ == "__main__":
    unittest.main()
