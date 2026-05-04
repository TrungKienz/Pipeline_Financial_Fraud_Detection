import unittest

from fraud_pipeline import PipelineConfig, parse_csv_row
from fraud_pipeline.benchmark import BenchmarkProfile, run_benchmark
from fraud_pipeline.synthetic import synthesize_events


def sample_event(step: int = 1, amount: str = "100.0") -> dict[str, str]:
    return {
        "step": str(step),
        "type": "TRANSFER",
        "amount": amount,
        "nameOrig": "C123",
        "oldbalanceOrg": "1000.0",
        "newbalanceOrig": "900.0",
        "nameDest": "D999",
        "oldbalanceDest": "0.0",
        "newbalanceDest": "100.0",
        "isFraud": "0",
        "isFlaggedFraud": "0",
    }


class SyntheticAndBenchmarkTests(unittest.TestCase):
    def test_synthesize_events_expands_seed(self) -> None:
        seed = [parse_csv_row(sample_event(step=1)), parse_csv_row(sample_event(step=2))]
        generated = synthesize_events(seed, target_count=10)

        self.assertEqual(len(generated), 10)
        self.assertNotEqual(generated[0].event_id, generated[1].event_id)
        self.assertNotEqual(generated[0].name_orig, generated[1].name_orig)

    def test_run_benchmark_returns_positive_metrics(self) -> None:
        seed = [parse_csv_row(sample_event(step=1)), parse_csv_row(sample_event(step=2, amount="250000.0"))]
        result = run_benchmark(
            seed,
            BenchmarkProfile(name="test", event_count=100),
            config=PipelineConfig(high_amount_transfer_threshold=200000.0),
        )

        self.assertEqual(result.profile, "test")
        self.assertEqual(result.event_count, 100)
        self.assertGreater(result.total_ms, 0)
        self.assertGreater(result.throughput_eps, 0)


if __name__ == "__main__":
    unittest.main()
