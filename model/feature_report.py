"""Feature importance + selection summary for the fraud model (Module 4 deliverable).

Loads the currently selected trained model plus ``feature_columns.json`` and
produces a ranked importance table and a list of low-signal features that are
candidates for removal. Tree models expose ``feature_importances_``; linear
models (logreg) fall back to the absolute value of their coefficients.

Run AFTER training so the artifacts reflect the current feature set:

    python model/feature_report.py
    python model/feature_report.py --drop-threshold 0.005

Outputs ``model/feature_importance_summary.json`` and prints a short summary.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

MODEL_DIR = Path(__file__).resolve().parent
ROOT = MODEL_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from model import model_utils  # noqa: E402  (reuse loading helpers)


def _extract_importances(model) -> tuple[np.ndarray, str]:
    if hasattr(model, "feature_importances_"):
        return np.asarray(model.feature_importances_, dtype=np.float64), "tree_importance"
    if hasattr(model, "coef_"):
        return np.abs(np.asarray(model.coef_, dtype=np.float64)).ravel(), "abs_coefficient"
    raise SystemExit(
        "[ERROR] Selected model exposes neither feature_importances_ nor coef_"
    )


def build_summary(drop_threshold: float) -> dict:
    model = model_utils._get_model()
    if model is None:
        raise SystemExit(
            "[ERROR] No trained model found. Run model/train_model.py first."
        )
    feature_cols = model_utils._get_feature_columns()
    if not feature_cols:
        raise SystemExit("[ERROR] feature_columns.json not found. Train the model first.")

    importances, importance_kind = _extract_importances(model)
    if len(importances) != len(feature_cols):
        raise SystemExit(
            f"[ERROR] Importance length {len(importances)} != feature count "
            f"{len(feature_cols)}. Retrain so artifacts match the feature set."
        )

    total = float(importances.sum()) or 1.0
    normalized = importances / total

    order = np.argsort(normalized)[::-1]
    ranked = [
        {
            "rank": rank + 1,
            "feature": feature_cols[idx],
            "importance": round(float(importances[idx]), 6),
            "importance_share": round(float(normalized[idx]), 6),
        }
        for rank, idx in enumerate(order)
    ]

    drop_candidates = [r["feature"] for r in ranked if r["importance_share"] < drop_threshold]
    keep = [r["feature"] for r in ranked if r["importance_share"] >= drop_threshold]

    return {
        "model_type": type(model).__name__,
        "model_version": model_utils.get_model_version(),
        "importance_kind": importance_kind,
        "n_features": len(feature_cols),
        "drop_threshold_share": drop_threshold,
        "ranked_features": ranked,
        "recommended_keep": keep,
        "drop_candidates": drop_candidates,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Feature importance & selection summary")
    parser.add_argument(
        "--drop-threshold",
        type=float,
        default=0.005,
        help="Importance share below which a feature is flagged for removal (default: 0.005)",
    )
    args = parser.parse_args()

    summary = build_summary(args.drop_threshold)

    out_path = MODEL_DIR / "feature_importance_summary.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("=" * 60)
    print(f"FEATURE IMPORTANCE SUMMARY  ({summary['model_type']}, {summary['importance_kind']})")
    print("=" * 60)
    print(f"Features: {summary['n_features']}  |  drop-threshold share: {args.drop_threshold}")
    print("\nTop 15 features:")
    for r in summary["ranked_features"][:15]:
        print(f"  {r['rank']:>2}. {r['feature']:<32} {r['importance_share']:.4f}")
    print(f"\nDrop candidates (< {args.drop_threshold} share): {len(summary['drop_candidates'])}")
    for f in summary["drop_candidates"]:
        print(f"  - {f}")
    print(f"\n[EXPORT] Written to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
