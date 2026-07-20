import tempfile
import unittest
from pathlib import Path

from fraud_pipeline.runtime_state import (
    PIPELINE_RUN_ID_FILENAME,
    read_or_create_pipeline_run_id,
    scoped_query_name,
)


class RuntimeStateTests(unittest.TestCase):
    def test_read_or_create_pipeline_run_id_reuses_marker_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint_root = Path(temp_dir) / "checkpoints"
            first = read_or_create_pipeline_run_id(str(checkpoint_root), env_run_id="")
            second = read_or_create_pipeline_run_id(str(checkpoint_root), env_run_id="")

            self.assertEqual(first, second)
            self.assertTrue((checkpoint_root / PIPELINE_RUN_ID_FILENAME).exists())

    def test_read_or_create_pipeline_run_id_changes_after_marker_reset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint_root = Path(temp_dir) / "checkpoints"
            first = read_or_create_pipeline_run_id(str(checkpoint_root), env_run_id="")
            (checkpoint_root / PIPELINE_RUN_ID_FILENAME).unlink()
            second = read_or_create_pipeline_run_id(str(checkpoint_root), env_run_id="")

            self.assertNotEqual(first, second)

    def test_scoped_query_name_includes_run_id(self) -> None:
        self.assertEqual(scoped_query_name("integrated_fraud_pipeline", "run-1"), "integrated_fraud_pipeline:run-1")


if __name__ == "__main__":
    unittest.main()
