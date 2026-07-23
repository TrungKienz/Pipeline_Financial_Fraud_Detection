from __future__ import annotations

import json
import importlib.metadata
import logging
import os
from pathlib import Path
from typing import Any, Mapping

import joblib
import numpy as np
import pandas as pd

from fraud_pipeline.config import RULE_SCORING_CONFIG_FIELDS
from fraud_pipeline.features import (
    INFERENCE_AVAILABLE_FEATURES,
    assert_no_forbidden_features,
    build_feature_record,
)
from fraud_pipeline.models import TransactionEvent


MODEL_DIR = Path(__file__).resolve().parent
DEFAULT_ARTIFACT = MODEL_DIR / "artifacts" / "fraud_pipeline_selected.joblib"
SELECTED_MODEL_POINTER = MODEL_DIR / "selected_model.json"
LOGGER = logging.getLogger(__name__)
MAX_THRESHOLD_SENTINEL = float(np.nextafter(1.0, np.inf))
REQUIRED_ARTIFACT_KEYS = frozenset(
    {
        "artifact_format_version",
        "feature_configuration",
        "preprocessor",
        "model",
        "feature_columns",
        "ml_threshold",
        "hybrid_threshold",
        "rule_weight",
        "ml_weight",
        "model_version",
        "model_tag",
        "dataset_version",
        "split_manifest_version",
        "cost_config",
        "rule_config",
        "dependency_versions",
    }
)

_artifact_cache: dict[str, Any] = {
    "path": None,
    "mtime_ns": None,
    "artifact": None,
    "attempted": False,
}


def _artifact_path() -> Path:
    configured = os.environ.get("FRAUD_MODEL_ARTIFACT")
    if configured:
        return Path(configured).expanduser().resolve()
    if SELECTED_MODEL_POINTER.exists():
        try:
            pointer = json.loads(SELECTED_MODEL_POINTER.read_text(encoding="utf-8"))
            relative = pointer.get("artifact_path")
            if relative:
                path = Path(str(relative))
                return (path if path.is_absolute() else MODEL_DIR / path).resolve()
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            LOGGER.exception(
                "Failed to read selected model pointer: %s", SELECTED_MODEL_POINTER
            )
    return DEFAULT_ARTIFACT.resolve()


def validate_model_artifact(artifact: Mapping[str, Any]) -> None:
    missing = sorted(REQUIRED_ARTIFACT_KEYS.difference(artifact))
    if missing:
        raise ValueError(f"Selected model artifact is missing keys: {missing}")
    if artifact["feature_configuration"] != "deployment_safe":
        raise ValueError(
            "Runtime model artifact must use feature_configuration='deployment_safe'"
        )
    features = [str(value) for value in artifact["feature_columns"]]
    if not features:
        raise ValueError("Selected model artifact contains no feature columns")
    assert_no_forbidden_features(features)
    unavailable = sorted(set(features).difference(INFERENCE_AVAILABLE_FEATURES))
    if unavailable:
        raise ValueError(
            f"Selected model uses features unavailable at inference time: {unavailable}"
        )
    for name in ("ml_threshold", "hybrid_threshold"):
        threshold = float(artifact[name])
        if not np.isfinite(threshold) or not 0.0 <= threshold <= MAX_THRESHOLD_SENTINEL:
            raise ValueError(
                f"Artifact {name} must be finite and in [0, {MAX_THRESHOLD_SENTINEL}]"
            )
    rule_weight = float(artifact["rule_weight"])
    ml_weight = float(artifact["ml_weight"])
    if rule_weight < 0 or ml_weight < 0 or rule_weight + ml_weight <= 0:
        raise ValueError("Artifact hybrid weights must be non-negative with positive sum")
    if not hasattr(artifact["preprocessor"], "transform"):
        raise ValueError("Artifact preprocessor is not fitted/transformable")
    if not hasattr(artifact["model"], "predict_proba"):
        raise ValueError("Artifact model does not implement predict_proba")
    if not isinstance(artifact["rule_config"], Mapping):
        raise ValueError("Artifact rule_config must be a mapping")
    rule_keys = set(artifact["rule_config"])
    expected_rule_keys = set(RULE_SCORING_CONFIG_FIELDS)
    if rule_keys != expected_rule_keys:
        raise ValueError(
            "Artifact rule_config keys do not match the runtime scoring contract: "
            f"missing={sorted(expected_rule_keys - rule_keys)}, "
            f"unknown={sorted(rule_keys - expected_rule_keys)}"
        )
    recorded_versions = dict(artifact["dependency_versions"])
    packages = ["scikit-learn"]
    if artifact["model_tag"] == "xgb":
        packages.append("xgboost")
    elif artifact["model_tag"] == "lgbm":
        packages.append("lightgbm")
    for package in packages:
        expected_version = recorded_versions.get(package)
        if expected_version is None:
            raise ValueError(f"Artifact does not record required dependency {package}")
        try:
            runtime_version = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError as exc:
            raise ValueError(f"Runtime dependency is not installed: {package}") from exc
        if runtime_version != expected_version:
            raise ValueError(
                f"Artifact/runtime dependency mismatch for {package}: "
                f"trained={expected_version}, runtime={runtime_version}"
            )


def load_model_artifact(
    path: str | Path | None = None,
    *,
    strict: bool = False,
    force_reload: bool = False,
) -> dict[str, Any] | None:
    artifact_path = Path(path).resolve() if path is not None else _artifact_path()
    if (
        not force_reload
        and _artifact_cache["attempted"]
        and _artifact_cache["path"] == artifact_path
    ):
        return _artifact_cache["artifact"]
    try:
        mtime_ns = artifact_path.stat().st_mtime_ns
    except OSError:
        if strict:
            raise FileNotFoundError(
                f"Selected fraud model artifact not found: {artifact_path}. "
                "Run 'python -m model.train_models' after feature preparation."
            )
        if not _artifact_cache["attempted"] or _artifact_cache["path"] != artifact_path:
            LOGGER.warning("Selected fraud model artifact not found: %s", artifact_path)
        _artifact_cache.update(
            {"path": artifact_path, "mtime_ns": None, "artifact": None, "attempted": True}
        )
        return None

    try:
        loaded = joblib.load(artifact_path)
        if not isinstance(loaded, dict):
            raise ValueError("Selected model artifact must contain a dictionary bundle")
        validate_model_artifact(loaded)
    except Exception:
        if strict:
            raise
        LOGGER.exception("Failed to load selected fraud model artifact: %s", artifact_path)
        loaded = None
    _artifact_cache.update(
        {
            "path": artifact_path,
            "mtime_ns": mtime_ns,
            "artifact": loaded,
            "attempted": True,
        }
    )
    return loaded


def reset_artifact_cache() -> None:
    _artifact_cache.update(
        {"path": None, "mtime_ns": None, "artifact": None, "attempted": False}
    )


def refresh_model_artifact(*, strict: bool = False) -> dict[str, Any] | None:
    """Reload an atomically replaced bundle at an explicit batch boundary."""

    path = _artifact_path()
    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        mtime_ns = None
    if (
        _artifact_cache["attempted"]
        and _artifact_cache["path"] == path
        and _artifact_cache["mtime_ns"] == mtime_ns
    ):
        return _artifact_cache["artifact"]
    return load_model_artifact(path, strict=strict, force_reload=True)


def predict_frame(artifact: Mapping[str, Any], frame: pd.DataFrame) -> np.ndarray:
    if REQUIRED_ARTIFACT_KEYS.issubset(artifact):
        validate_model_artifact(artifact)
    else:
        missing_components = sorted(
            {"preprocessor", "model", "feature_columns"}.difference(artifact)
        )
        if missing_components:
            raise ValueError(
                f"Prediction bundle is missing components: {missing_components}"
            )
        assert_no_forbidden_features(artifact["feature_columns"])
    feature_columns = [str(name) for name in artifact["feature_columns"]]
    missing = [name for name in feature_columns if name not in frame.columns]
    if missing:
        raise ValueError(f"Inference frame is missing selected features: {missing}")
    ordered = frame.loc[:, feature_columns]
    transformed = artifact["preprocessor"].transform(ordered)
    probabilities = np.asarray(artifact["model"].predict_proba(transformed))
    if probabilities.ndim != 2 or probabilities.shape[1] != 2:
        raise ValueError(
            f"Expected binary predict_proba output with two columns, got {probabilities.shape}"
        )
    classes = list(getattr(artifact["model"], "classes_", [0, 1]))
    if 1 not in classes:
        raise ValueError(f"Selected model classes do not contain fraud class 1: {classes}")
    return probabilities[:, classes.index(1)].astype(np.float64, copy=False)


def transform_event(
    event: TransactionEvent,
    dynamic_features: Mapping[str, float] | None = None,
    artifact: Mapping[str, Any] | None = None,
):
    bundle = dict(artifact) if artifact is not None else load_model_artifact()
    if bundle is None:
        return None
    record = build_feature_record(event, dynamic_features=dynamic_features)
    feature_columns = [str(name) for name in bundle["feature_columns"]]
    frame = pd.DataFrame([{name: record[name] for name in feature_columns}])
    return bundle["preprocessor"].transform(frame.loc[:, feature_columns])


def predict_proba(
    event: TransactionEvent,
    dynamic_features: Mapping[str, float] | None = None,
) -> float:
    artifact = load_model_artifact()
    if artifact is None:
        return 0.0
    record = build_feature_record(event, dynamic_features=dynamic_features)
    score = predict_frame(artifact, pd.DataFrame([record]))[0]
    return float(score)


def model_is_loaded() -> bool:
    return load_model_artifact() is not None


def get_model_version() -> str:
    artifact = load_model_artifact()
    return str(artifact["model_version"]) if artifact else "v0"


def get_ml_threshold() -> float:
    artifact = load_model_artifact()
    return float(artifact["ml_threshold"]) if artifact else 0.5


def get_threshold() -> float:
    """Compatibility API; deployed decisions use the hybrid threshold."""

    artifact = load_model_artifact()
    return float(artifact["hybrid_threshold"]) if artifact else 0.5


def get_scoring_config() -> dict[str, float]:
    artifact = load_model_artifact()
    if artifact is None:
        return {"rule_weight": 0.6, "ml_weight": 0.4, "hybrid_threshold": 0.5}
    return {
        "rule_weight": float(artifact["rule_weight"]),
        "ml_weight": float(artifact["ml_weight"]),
        "hybrid_threshold": float(artifact["hybrid_threshold"]),
    }


def get_rule_config() -> dict[str, float | int] | None:
    artifact = load_model_artifact()
    if artifact is None:
        return None
    return {
        str(name): value for name, value in dict(artifact["rule_config"]).items()
    }
