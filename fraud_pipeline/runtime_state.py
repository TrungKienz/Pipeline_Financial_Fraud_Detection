from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4


PIPELINE_RUN_ID_FILENAME = ".pipeline_run_id"


def read_or_create_pipeline_run_id(checkpoint_root: str, env_run_id: str | None = None) -> str:
    explicit_run_id = (env_run_id if env_run_id is not None else os.getenv("PIPELINE_RUN_ID", "")).strip()
    if explicit_run_id:
        return explicit_run_id

    root = Path(checkpoint_root)
    root.mkdir(parents=True, exist_ok=True)
    marker_path = root / PIPELINE_RUN_ID_FILENAME
    if marker_path.exists():
        run_id = marker_path.read_text(encoding="utf-8").strip()
        if run_id:
            return run_id
    run_id = uuid4().hex
    marker_path.write_text(run_id, encoding="utf-8")
    return run_id


def scoped_query_name(base_name: str, run_id: str) -> str:
    return f"{base_name}:{run_id}"
