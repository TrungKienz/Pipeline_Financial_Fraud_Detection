from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from sklearn.preprocessing import StandardScaler

from fraud_pipeline.features import (
    FEATURE_COLUMNS,
    TXN_TYPE_CATEGORIES,
    BROWSER_CATEGORIES,
    DEVICE_TYPE_CATEGORIES,
    COUNTRY_CATEGORIES,
    build_feature_record,
)
from fraud_pipeline.models import TransactionEvent

MODEL_DIR = Path(__file__).resolve().parent
LOGGER = logging.getLogger(__name__)


def _model_path(name: str) -> Path:
    return MODEL_DIR / name


def _load_artifact(name: str):
    path = _model_path(name)
    if not path.exists():
        LOGGER.warning("ML artifact not found: %s", path)
        return None
    try:
        return joblib.load(str(path))
    except Exception:
        LOGGER.exception("Failed to load ML artifact: %s", path)
        return None


_model_cache: dict[str, Any] = {}


def _get_model_type() -> str:
    return os.environ.get("FRAUD_MODEL_TYPE", "xgb")


def _model_filename(tag: str | None = None) -> str:
    t = tag or _get_model_type()
    return f"fraud_model_{t}.pkl"


def _metadata_filename(tag: str | None = None) -> str:
    t = tag or _get_model_type()
    return f"model_metadata_{t}.json"


def _get_model():
    key = f"model_{_get_model_type()}"
    if key not in _model_cache:
        _model_cache[key] = _load_artifact(_model_filename())
    return _model_cache[key]


def _get_scaler():
    if "scaler" not in _model_cache:
        _model_cache["scaler"] = _load_artifact("scaler.pkl")
    return _model_cache["scaler"]


def _get_feature_columns():
    if "feature_columns" not in _model_cache:
        path = _model_path("feature_columns.json")
        if path.exists():
            try:
                _model_cache["feature_columns"] = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                LOGGER.exception("Failed to read feature columns: %s", path)
                _model_cache["feature_columns"] = None
        else:
            LOGGER.warning("Feature columns file not found: %s", path)
            _model_cache["feature_columns"] = None
    return _model_cache["feature_columns"]


def _get_metadata():
    key = f"metadata_{_get_model_type()}"
    if key not in _model_cache:
        path = _model_path(_metadata_filename())
        if path.exists():
            try:
                _model_cache[key] = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                LOGGER.exception("Failed to read model metadata: %s", path)
                _model_cache[key] = None
        else:
            LOGGER.warning("Model metadata file not found: %s", path)
            _model_cache[key] = None
    return _model_cache[key]


def _one_hot_txn_type(txn_type: str) -> dict[str, int]:
    return {f"type_{cat}": int(txn_type == cat) for cat in TXN_TYPE_CATEGORIES}


def _one_hot_browser(browser: str) -> dict[str, int]:
    return {f"browser_{cat}": int(browser == cat) for cat in BROWSER_CATEGORIES}


def _one_hot_device_type(device_type: str) -> dict[str, int]:
    return {f"device_type_{cat}": int(device_type == cat) for cat in DEVICE_TYPE_CATEGORIES}


def _one_hot_country(country: str) -> dict[str, int]:
    return {f"country_{cat}": int(country == cat) for cat in COUNTRY_CATEGORIES}


def transform_event(event: TransactionEvent, dynamic_features: dict[str, float] | None = None) -> np.ndarray | None:
    feature_columns = _get_feature_columns()
    if feature_columns is None:
        return None

    record = build_feature_record(event, dynamic_features=dynamic_features)
    raw: dict[str, float] = {col: float(record[col]) for col in FEATURE_COLUMNS}
    raw.update(_one_hot_txn_type(record["txn_type"]))
    raw.update(_one_hot_browser(record["browser"]))
    raw.update(_one_hot_device_type(record["device_type"]))
    raw.update(_one_hot_country(record["country"]))

    vec = np.array([raw.get(col, 0.0) for col in feature_columns], dtype=np.float64).reshape(1, -1)

    scaler = _get_scaler()
    if scaler is not None:
        vec = scaler.transform(vec)

    return vec


def predict_proba(event: TransactionEvent, dynamic_features: dict[str, float] | None = None) -> float:
    model = _get_model()
    if model is None:
        return 0.0

    vec = transform_event(event, dynamic_features=dynamic_features)
    if vec is None:
        return 0.0

    proba = model.predict_proba(vec)[0, 1]
    return round(float(proba), 4)



def model_is_loaded() -> bool:
    return _get_model() is not None


def get_model_version() -> str:
    metadata = _get_metadata()
    if metadata:
        return metadata.get("model_version", "v0")
    return "v0"


def get_threshold() -> float:
    metadata = _get_metadata()
    if metadata:
        return metadata.get("optimal_threshold", 0.5)
    return 0.5
