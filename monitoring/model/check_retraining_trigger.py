from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = ROOT / "monitoring" / "reports"
MODEL_DIR = ROOT / "monitoring" / "model"

DEFAULT_DRIFT_REPORT = REPORT_DIR / "drift_report.json"
DEFAULT_PERFORMANCE_REPORT = REPORT_DIR / "performance_report.json"
DEFAULT_POLICY_JSON = MODEL_DIR / "retraining_policy.json"
DEFAULT_OUTPUT_JSON = REPORT_DIR / "retraining_decision.json"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def safe_get_metric(report: dict[str, Any], feature: str, key: str) -> float | None:
    try:
        value = report["features"][feature][key]
    except KeyError:
        return None
    return float(value) if value is not None else None


def evaluate_retraining_need(
    drift_report: dict[str, Any],
    performance_report: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    reasons: list[dict[str, Any]] = []

    monitored_feature_count = max(len(drift_report.get("monitored_features", [])), 1)
    drifted_feature_count = int(drift_report.get("drifted_feature_count", 0))
    drift_ratio = drifted_feature_count / monitored_feature_count
    if drift_ratio >= float(policy["feature_drift_ratio_threshold"]):
        reasons.append(
            {
                "type": "drift_ratio",
                "message": "Too many monitored features are drifting.",
                "observed": round(drift_ratio, 6),
                "threshold": float(policy["feature_drift_ratio_threshold"]),
            }
        )

    amount_ks = safe_get_metric(drift_report, "amount", "ks_statistic")
    if amount_ks is not None and amount_ks >= float(policy["amount_ks_stat_threshold"]):
        reasons.append(
            {
                "type": "amount_drift",
                "message": "Amount distribution drift exceeds threshold.",
                "observed": round(amount_ks, 6),
                "threshold": float(policy["amount_ks_stat_threshold"]),
            }
        )

    is_alert_distribution = drift_report.get("features", {}).get("is_alert", {}).get("distribution", {})
    reference_alert_rate = float(is_alert_distribution.get("True", {}).get("reference", 0.0))
    serving_alert_rate = float(is_alert_distribution.get("True", {}).get("serving", 0.0))
    alert_rate_delta = abs(serving_alert_rate - reference_alert_rate)
    if alert_rate_delta >= float(policy["alert_rate_change_threshold"]):
        reasons.append(
            {
                "type": "alert_rate_change",
                "message": "Alert rate changed significantly from the reference baseline.",
                "observed": round(alert_rate_delta, 6),
                "threshold": float(policy["alert_rate_change_threshold"]),
            }
        )

    coverage = float(performance_report.get("label_coverage", 0.0))
    if coverage < float(policy["label_coverage_min"]):
        reasons.append(
            {
                "type": "label_coverage",
                "message": "Label coverage is too low to trust recent monitoring fully.",
                "observed": round(coverage, 6),
                "threshold": float(policy["label_coverage_min"]),
            }
        )

    rolling_7d = performance_report.get("rolling_windows", {}).get("7d", {})
    labeled_rows_7d = int(rolling_7d.get("labeled_rows", 0) or 0)
    minimum_sample = int(policy["minimum_labeled_rows_7d"])
    enforce_min_sample = bool(policy.get("require_minimum_sample_for_trigger", True))
    enough_sample = labeled_rows_7d >= minimum_sample

    precision_7d = rolling_7d.get("precision")
    recall_7d = rolling_7d.get("recall")
    f1_7d = rolling_7d.get("f1")

    if precision_7d is not None and (enough_sample or not enforce_min_sample) and float(precision_7d) < float(policy["precision_7d_min"]):
        reasons.append(
            {
                "type": "precision_7d",
                "message": "7-day precision is below the minimum target.",
                "observed": float(precision_7d),
                "threshold": float(policy["precision_7d_min"]),
            }
        )

    if recall_7d is not None and (enough_sample or not enforce_min_sample) and float(recall_7d) < float(policy["recall_7d_min"]):
        reasons.append(
            {
                "type": "recall_7d",
                "message": "7-day recall is below the minimum target.",
                "observed": float(recall_7d),
                "threshold": float(policy["recall_7d_min"]),
            }
        )

    if f1_7d is not None and (enough_sample or not enforce_min_sample) and float(f1_7d) < float(policy["f1_7d_min"]):
        reasons.append(
            {
                "type": "f1_7d",
                "message": "7-day F1 is below the minimum target.",
                "observed": float(f1_7d),
                "threshold": float(policy["f1_7d_min"]),
            }
        )

    warnings = list(performance_report.get("warnings", []))
    if enforce_min_sample and not enough_sample:
        warnings.append(
            f"7-day labeled sample is only {labeled_rows_7d}, below the minimum {minimum_sample}; performance triggers were evaluated conservatively."
        )

    return {
        "generated_at": performance_report.get("generated_at") or drift_report.get("generated_at"),
        "retrain_required": bool(reasons),
        "reason_count": len(reasons),
        "reasons": reasons,
        "warnings": warnings,
        "policy_snapshot": policy,
        "observations": {
            "drifted_feature_count": drifted_feature_count,
            "monitored_feature_count": monitored_feature_count,
            "drift_ratio": round(drift_ratio, 6),
            "reference_alert_rate": round(reference_alert_rate, 6),
            "serving_alert_rate": round(serving_alert_rate, 6),
            "alert_rate_delta": round(alert_rate_delta, 6),
            "label_coverage": round(coverage, 6),
            "rolling_7d_labeled_rows": labeled_rows_7d,
            "rolling_7d_precision": precision_7d,
            "rolling_7d_recall": recall_7d,
            "rolling_7d_f1": f1_7d,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate retraining triggers from drift and performance reports.")
    parser.add_argument("--drift-report", default=str(DEFAULT_DRIFT_REPORT), help="Path to drift_report.json")
    parser.add_argument("--performance-report", default=str(DEFAULT_PERFORMANCE_REPORT), help="Path to performance_report.json")
    parser.add_argument("--policy-json", default=str(DEFAULT_POLICY_JSON), help="Path to retraining policy JSON")
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON), help="Path to write retraining_decision.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    drift_report = load_json(Path(args.drift_report))
    performance_report = load_json(Path(args.performance_report))
    policy = load_json(Path(args.policy_json))

    decision = evaluate_retraining_need(drift_report, performance_report, policy)
    output_json = Path(args.output_json)
    ensure_parent(output_json)
    output_json.write_text(json.dumps(decision, indent=2), encoding="utf-8")
    print(f"Wrote retraining decision to {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
