import tempfile
import unittest
from pathlib import Path

from fraud_pipeline import (
    PipelineConfig,
    iter_logical_source_triplets,
    logical_source_paths,
    split_integrated_csv_to_logical_sources,
)


def sample_rows() -> str:
        return "\n".join(
        [
            "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud",
            "1,TRANSFER,100.0,C1,1000.0,900.0,C2,10.0,110.0,0",
            "2,CASH_OUT,250000.0,C3,260000.0,10000.0,C4,0.0,0.0,1",
        ]
    )


class SourceCsvTests(unittest.TestCase):
    def test_split_integrated_csv_to_logical_sources_creates_three_independent_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            raw_csv = Path(temp_dir) / "raw.csv"
            output_dir = Path(temp_dir) / "logical_sources"
            raw_csv.write_text(sample_rows(), encoding="utf-8")

            summary = split_integrated_csv_to_logical_sources(
                raw_csv,
                output_dir,
                config=PipelineConfig(),
            )

            paths = logical_source_paths(output_dir)
            self.assertEqual(summary["event_count"], 2)
            self.assertTrue(paths["transaction"].exists())
            self.assertTrue(paths["sender_state"].exists())
            self.assertTrue(paths["receiver_state"].exists())

    def test_iter_logical_source_triplets_preserves_event_correlation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            raw_csv = Path(temp_dir) / "raw.csv"
            output_dir = Path(temp_dir) / "logical_sources"
            raw_csv.write_text(sample_rows(), encoding="utf-8")
            split_integrated_csv_to_logical_sources(raw_csv, output_dir, config=PipelineConfig())

            triples = list(iter_logical_source_triplets(output_dir))

            self.assertEqual(len(triples), 2)
            first_tx, first_sender, first_receiver = triples[0]
            self.assertEqual(first_sender["source_event_id"], first_tx["event_id"])
            self.assertEqual(first_receiver["source_event_id"], first_tx["event_id"])
            self.assertEqual(first_sender["nameOrig"], first_tx["nameOrig"])
            self.assertEqual(first_receiver["nameDest"], first_tx["nameDest"])


if __name__ == "__main__":
    unittest.main()
