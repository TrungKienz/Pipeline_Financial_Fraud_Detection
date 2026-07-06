from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import joblib
import numpy as np
from imblearn.over_sampling import SMOTE


def _to_native(obj):
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_native(v) for v in obj]
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj
from sklearn.metrics import (
    auc,
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

MODEL_REGISTRY: dict[str, type] = {}
try:
    from sklearn.ensemble import RandomForestClassifier
    MODEL_REGISTRY["rf"] = RandomForestClassifier
except ImportError:
    pass
try:
    from xgboost import XGBClassifier
    MODEL_REGISTRY["xgb"] = XGBClassifier
except ImportError:
    pass

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fraud_pipeline import PipelineConfig, iter_transaction_events, transaction_to_dict
from fraud_pipeline.features import FEATURE_COLUMNS, TXN_TYPE_CATEGORIES, build_feature_record
from fraud_pipeline.parsing import parse_csv_row

MODEL_DIR = Path(__file__).resolve().parent
PLOTS_DIR = MODEL_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def load_events(csv_path: str, limit: int | None) -> list:
    print(f"[INFO] Loading data from: {csv_path}")
    if limit:
        print(f"[INFO] Limit: {limit} rows")
    config = PipelineConfig()
    events = list(iter_transaction_events(csv_path, config=config, limit=limit))
    print(f"[INFO] Loaded {len(events)} transactions")
    return events


def build_features(events: list) -> tuple[np.ndarray, np.ndarray, list[str], list[str]]:
    records = [build_feature_record(ev) for ev in events]
    txn_types = [r["txn_type"] for r in records]
    labels = np.array([r["label_is_fraud"] for r in records], dtype=np.int32)

    feature_dicts = []
    for r in records:
        fv = {col: float(r[col]) for col in FEATURE_COLUMNS}
        fv.update({f"type_{cat}": int(r["txn_type"] == cat) for cat in TXN_TYPE_CATEGORIES})
        feature_dicts.append(fv)

    all_cols = FEATURE_COLUMNS + [f"type_{cat}" for cat in TXN_TYPE_CATEGORIES]
    X = np.array([[d[col] for col in all_cols] for d in feature_dicts], dtype=np.float64)

    return X, labels, txn_types, all_cols


def run_eda(X: np.ndarray, y: np.ndarray, txn_types: list[str], feature_cols: list[str]):
    print("\n" + "=" * 60)
    print("EDA SUMMARY")
    print("=" * 60)

    n_total = len(y)
    n_fraud = int(y.sum())
    n_legit = n_total - n_fraud
    fraud_rate = n_fraud / n_total * 100

    print(f"Total transactions: {n_total}")
    print(f"Fraud: {n_fraud} ({fraud_rate:.4f}%)")
    print(f"Legitimate: {n_legit} ({100 - fraud_rate:.4f}%)")
    print(f"Imbalance ratio: 1:{n_legit // max(n_fraud, 1):,}")

    from collections import Counter
    type_counts = Counter(txn_types)
    print(f"\nTransaction type distribution:")
    for t, c in type_counts.most_common():
        fraud_in_type = sum(1 for i, tt in enumerate(txn_types) if tt == t and y[i] == 1)
        print(f"  {t:>10s}: {c:>8,} ({c/n_total*100:5.2f}%)  | Fraud: {fraud_in_type} ({fraud_in_type/max(c,1)*100:.2f}%)")

    step_vals = X[:, feature_cols.index("step")]
    amount_vals = X[:, feature_cols.index("amount")]
    print(f"\nNumerical features summary:")
    print(f"  step   - min={step_vals.min():.0f}, max={step_vals.max():.0f}, mean={step_vals.mean():.1f}")
    print(f"  amount - min={amount_vals.min():.2f}, max={amount_vals.max():.2f}, mean={amount_vals.mean():.2f}")

    fraud_indices = y == 1
    legit_indices = y == 0
    if fraud_indices.sum() > 0:
        print(f"\n  Fraud amount stats:   min={amount_vals[fraud_indices].min():.2f}, "
              f"max={amount_vals[fraud_indices].max():.2f}, "
              f"mean={amount_vals[fraud_indices].mean():.2f}")
        print(f"  Legit amount stats:   min={amount_vals[legit_indices].min():.2f}, "
              f"max={amount_vals[legit_indices].max():.2f}, "
              f"mean={amount_vals[legit_indices].mean():.2f}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import seaborn as sns

        plt.figure(figsize=(10, 5))
        type_list = list(type_counts.keys())
        fraud_counts = []
        for t in type_list:
            fraud_in_type = sum(1 for i, tt in enumerate(txn_types) if tt == t and y[i] == 1)
            fraud_counts.append(fraud_in_type)
        colors = ["#ff6b6b" if f > 0 else "#51cf66" for f in fraud_counts]
        plt.bar(type_list, fraud_counts, color=colors)
        plt.title("Fraud Count by Transaction Type", fontsize=14, fontweight="bold")
        plt.xlabel("Transaction Type")
        plt.ylabel("Fraud Count")
        for i, v in enumerate(fraud_counts):
            if v > 0:
                plt.text(i, v + max(fraud_counts)*0.01, str(v), ha="center", fontweight="bold")
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "fraud_by_type.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved fraud_by_type.png")

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        axes[0].hist(amount_vals[legit_indices], bins=50, alpha=0.7, color="#51cf66", label="Legitimate")
        axes[0].set_title("Amount Distribution - Legitimate")
        axes[0].set_xlabel("Amount")
        axes[0].set_ylabel("Frequency")
        axes[1].hist(amount_vals[fraud_indices], bins=50, alpha=0.7, color="#ff6b6b", label="Fraud")
        axes[1].set_title("Amount Distribution - Fraud")
        axes[1].set_xlabel("Amount")
        axes[1].set_ylabel("Frequency")
        for ax in axes:
            ax.legend()
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "amount_dist.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved amount_dist.png")

        corr_cols = [c for c in feature_cols if not c.startswith("type_")]
        corr_indices = [feature_cols.index(c) for c in corr_cols]
        corr_X = X[:, corr_indices]
        corr_df = np.column_stack([corr_X, y])
        corr_labels = corr_cols + ["is_fraud"]
        corr_matrix = np.corrcoef(corr_df.T)
        plt.figure(figsize=(12, 10))
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        sns.heatmap(corr_matrix, mask=mask, annot=True, fmt=".2f", cmap="RdBu_r",
                    xticklabels=corr_labels, yticklabels=corr_labels, center=0,
                    square=True, linewidths=0.5)
        plt.title("Feature Correlation Matrix", fontsize=14, fontweight="bold")
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "correlation_matrix.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved correlation_matrix.png")

    except ImportError:
        print("[WARN] matplotlib/seaborn not available, skipping EDA plots")

    eda_summary = {
        "total_transactions": n_total,
        "fraud_count": n_fraud,
        "legitimate_count": n_legit,
        "fraud_rate_percent": round(fraud_rate, 4),
        "imbalance_ratio": f"1:{n_legit // max(n_fraud, 1)}",
        "transaction_types": {t: c for t, c in type_counts.most_common()},
    }
    eda_path = MODEL_DIR / "eda_summary.json"
    eda_path.write_text(json.dumps(eda_summary, indent=2), encoding="utf-8")
    print(f"[INFO] EDA summary saved to {eda_path}")


def tune_threshold(y_val: np.ndarray, y_prob_val: np.ndarray) -> tuple[float, float, float, float, float]:
    precisions, recalls, thresholds = precision_recall_curve(y_val, y_prob_val)
    f1_scores = 2 * (precisions[:-1] * recalls[:-1]) / (precisions[:-1] + recalls[:-1] + 1e-8)
    best_idx = int(np.argmax(f1_scores))
    return thresholds[best_idx], f1_scores[best_idx], precisions[best_idx], recalls[best_idx], auc(recalls, precisions)


def _build_model(model_type: str, n_samples: int, n_pos: int) -> tuple:
    if model_type == "rf":
        print(f"\n[STEP] Training Random Forest...")
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=15,
            min_samples_leaf=5,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
            verbose=0,
        )
        hyperparams = {
            "n_estimators": 100,
            "max_depth": 15,
            "min_samples_leaf": 5,
            "class_weight": "balanced",
        }
        model_name = "RandomForestClassifier"
        model_version_tag = "rf"
    elif model_type == "xgb":
        print(f"\n[STEP] Training XGBoost...")
        scale_pos_weight = (n_samples - n_pos) / max(n_pos, 1)
        model = XGBClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            random_state=42,
            n_jobs=-1,
            verbosity=0,
            eval_metric="logloss",
        )
        hyperparams = {
            "n_estimators": 100,
            "max_depth": 6,
            "learning_rate": 0.1,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "scale_pos_weight": round(scale_pos_weight, 2),
        }
        model_name = "XGBClassifier"
        model_version_tag = "xgb"
    else:
        raise ValueError(f"Unknown model_type: {model_type!r}")
    return model, hyperparams, model_name, model_version_tag


def train_model(X_train, y_train, X_val, y_val, X_test, y_test, feature_cols, model_type="rf"):
    print("\n" + "=" * 60)
    print("TRAINING")
    print("=" * 60)

    n_fraud_train = int(y_train.sum())
    n_fraud_val = int(y_val.sum())
    n_fraud_test = int(y_test.sum())
    print(f"Train: {len(y_train):,} samples ({n_fraud_train:,} fraud, {n_fraud_train/len(y_train)*100:.3f}%)")
    print(f"Val:   {len(y_val):,} samples ({n_fraud_val:,} fraud, {n_fraud_val/len(y_val)*100:.3f}%)")
    print(f"Test:  {len(y_test):,} samples ({n_fraud_test:,} fraud, {n_fraud_test/len(y_test)*100:.3f}%)")

    print(f"\n[STEP] Applying SMOTE on training set...")
    smote = SMOTE(sampling_strategy=0.1, random_state=42)
    X_train_res, y_train_res = smote.fit_resample(X_train, y_train)
    n_resampled = int(y_train_res.sum())
    print(f"  Train before SMOTE: {len(X_train):,} ({n_fraud_train:,} fraud)")
    print(f"  Train after SMOTE:  {len(X_train_res):,} ({n_resampled:,} fraud, {n_resampled/len(X_train_res)*100:.1f}%)")

    model, hyperparams, model_name, model_version_tag = _build_model(
        model_type, len(X_train), n_fraud_train
    )

    start = time.time()
    model.fit(X_train_res, y_train_res)
    elapsed = time.time() - start
    print(f"  Training completed in {elapsed:.2f}s")

    print(f"\n[STEP] Tuning threshold on validation set ({len(y_val):,} samples)...")
    y_prob_val = model.predict_proba(X_val)[:, 1]
    best_threshold, val_f1, val_precision, val_recall, val_auc_pr = tune_threshold(y_val, y_prob_val)
    print(f"  Validation AUC-PR: {val_auc_pr:.4f}")
    print(f"  Optimal threshold:  {best_threshold:.4f}")
    print(f"  Val F1:             {val_f1:.4f}")
    print(f"  Val Precision:      {val_precision:.4f}")
    print(f"  Val Recall:         {val_recall:.4f}")

    print(f"\n[STEP] Final evaluation on held-out test set ({len(y_test):,} samples)...")
    y_prob_test = model.predict_proba(X_test)[:, 1]
    auc_roc = roc_auc_score(y_test, y_prob_test)
    auc_pr = auc(*(precision_recall_curve(y_test, y_prob_test)[:2][::-1]))

    y_pred_opt = (y_prob_test >= best_threshold).astype(int)
    cm = confusion_matrix(y_test, y_pred_opt)
    test_f1 = 2 * cm[1, 1] / (2 * cm[1, 1] + cm[0, 1] + cm[1, 0] + 1e-8)

    print(f"\n  Test AUC-ROC: {auc_roc:.4f}")
    print(f"  Test AUC-PR:  {auc_pr:.4f}")
    print(f"  Test F1:      {test_f1:.4f}")
    print(f"\n  Confusion Matrix (threshold={best_threshold:.4f}):")
    print(f"    TN={cm[0, 0]:,}  FP={cm[0, 1]:,}")
    print(f"    FN={cm[1, 0]:,}  TP={cm[1, 1]:,}")
    print(f"\n  Classification Report:")
    print(classification_report(y_test, y_pred_opt, target_names=["Legit", "Fraud"], digits=4))

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        plt.figure(figsize=(8, 6))
        precisions, recalls, _ = precision_recall_curve(y_test, y_prob_test)
        plt.plot(recalls, precisions, color="#4dabf7", linewidth=2, label=f"PR curve (AUC={auc_pr:.3f})")
        plt.scatter([recalls[np.argmax(2*precisions[:-1]*recalls[:-1]/(precisions[:-1]+recalls[:-1]+1e-8))]],
                    [precisions[np.argmax(2*precisions[:-1]*recalls[:-1]/(precisions[:-1]+recalls[:-1]+1e-8))]],
                    color="#ff6b6b", s=100, zorder=5, label=f"Optimal threshold={best_threshold:.3f}")
        plt.xlabel("Recall", fontsize=12)
        plt.ylabel("Precision", fontsize=12)
        plt.title("Precision-Recall Curve (Test Set)", fontsize=14, fontweight="bold")
        plt.legend()
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "pr_curve.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved pr_curve.png")

        fpr, tpr, _ = roc_curve(y_test, y_prob_test)
        plt.figure(figsize=(8, 6))
        plt.plot(fpr, tpr, color="#4dabf7", linewidth=2, label=f"ROC curve (AUC={auc_roc:.3f})")
        plt.plot([0, 1], [0, 1], "k--", alpha=0.5)
        plt.xlabel("False Positive Rate", fontsize=12)
        plt.ylabel("True Positive Rate", fontsize=12)
        plt.title("ROC Curve (Test Set)", fontsize=14, fontweight="bold")
        plt.legend()
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "roc_curve.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved roc_curve.png")

        import seaborn as sns
        plt.figure(figsize=(6, 5))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", xticklabels=["Legit", "Fraud"],
                    yticklabels=["Legit", "Fraud"])
        plt.title(f"Confusion Matrix (threshold={best_threshold:.3f})", fontsize=14, fontweight="bold")
        plt.ylabel("Actual")
        plt.xlabel("Predicted")
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "confusion_matrix.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved confusion_matrix.png")

        importances = model.feature_importances_
        indices = np.argsort(importances)[::-1]
        plt.figure(figsize=(12, 8))
        colors = plt.cm.Blues(np.linspace(0.4, 1, len(indices)))
        plt.barh(range(len(indices)), importances[indices][::-1], color=colors[::-1])
        plt.yticks(range(len(indices)), [feature_cols[i] for i in indices[::-1]])
        plt.xlabel("Feature Importance", fontsize=12)
        plt.title(f"{model_name} Feature Importance", fontsize=14, fontweight="bold")
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "feature_importance.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved feature_importance.png")

    except ImportError:
        print("[WARN] matplotlib not available, skipping evaluation plots")

    metrics = {
        "model_type": model_name,
        "hyperparameters": hyperparams,
        "auc_roc": round(float(auc_roc), 4),
        "auc_pr": round(float(auc_pr), 4),
        "optimal_threshold": round(float(best_threshold), 4),
        "f1_score": round(float(test_f1), 4),
        "precision": round(float(cm[1, 1] / max(cm[1, 1] + cm[0, 1], 1)), 4),
        "recall": round(float(cm[1, 1] / max(cm[1, 1] + cm[1, 0], 1)), 4),
        "train_time_seconds": round(elapsed, 2),
        "confusion_matrix": {
            "tn": int(cm[0, 0]), "fp": int(cm[0, 1]),
            "fn": int(cm[1, 0]), "tp": int(cm[1, 1]),
        },
        "validation_metrics": {
            "auc_pr": round(float(val_auc_pr), 4),
            "f1": round(float(val_f1), 4),
            "precision": round(float(val_precision), 4),
            "recall": round(float(val_recall), 4),
        },
    }

    return model, best_threshold, metrics, model_version_tag


def export_model(model, scaler, feature_cols, metrics, threshold, model_version_tag="rf"):
    model_name = metrics.get("model_type", "Unknown")
    print("\n" + "=" * 60)
    print("EXPORTING MODEL ARTIFACTS")
    print("=" * 60)

    model_filename = f"fraud_model_{model_version_tag}.pkl"
    model_path = MODEL_DIR / model_filename
    joblib.dump(model, str(model_path))
    print(f"[EXPORT] Model saved to {model_path}")

    scaler_path = MODEL_DIR / "scaler.pkl"
    joblib.dump(scaler, str(scaler_path))
    print(f"[EXPORT] Scaler saved to {scaler_path}")

    cols_path = MODEL_DIR / "feature_columns.json"
    cols_path.write_text(json.dumps(feature_cols, indent=2), encoding="utf-8")
    print(f"[EXPORT] Feature columns saved to {cols_path}")

    metadata = _to_native({
        "model_version": f"v1_paysim_{model_version_tag}",
        "model_type": model_name,
        "feature_columns": feature_cols,
        "optimal_threshold": threshold,
        "metrics": metrics,
    })
    meta_filename = f"model_metadata_{model_version_tag}.json"
    meta_path = MODEL_DIR / meta_filename
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"[EXPORT] Metadata saved to {meta_path}")

    results = _to_native({
        "metrics": metrics,
        "optimal_threshold": threshold,
        "model_version": f"v1_paysim_{model_version_tag}",
        "plots": [p.name for p in PLOTS_DIR.glob("*.png")],
    })
    eval_path = MODEL_DIR / "eval_results.json"
    eval_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"[EXPORT] Evaluation results saved to {eval_path}")

    files = [model_path, scaler_path, cols_path, meta_path, eval_path]
    for f in files:
        print(f"  {f.name}: {f.stat().st_size / 1024:.1f} KB")


def export_test_csv(events: list, output_path: Path):
    print(f"\n[EXPORT] Saving test set ({len(events)} rows) to {output_path}")
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud",
        ])
        writer.writeheader()
        for ev in events:
            writer.writerow({
                "step": str(ev.step),
                "type": ev.txn_type,
                "amount": f"{ev.amount:.2f}",
                "nameOrig": ev.name_orig,
                "oldbalanceOrg": f"{ev.oldbalance_org:.2f}",
                "newbalanceOrig": f"{ev.newbalance_orig:.2f}",
                "nameDest": ev.name_dest,
                "oldbalanceDest": f"{ev.oldbalance_dest:.2f}",
                "newbalanceDest": f"{ev.newbalance_dest:.2f}",
                "isFraud": str(ev.is_fraud),
            })
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")


def parse_args():
    parser = argparse.ArgumentParser(description="Train Fraud Detection ML Model")
    parser.add_argument("--csv-path", required=True, help="Path to PaySim CSV dataset")
    parser.add_argument("--limit", type=int, default=None, help="Number of rows to use (default: all)")
    parser.add_argument("--train-ratio", type=float, default=0.60, help="Train set ratio (default: 0.60)")
    parser.add_argument("--val-ratio", type=float, default=0.20, help="Validation set ratio (default: 0.20)")
    parser.add_argument("--test-ratio", type=float, default=0.20, help="Test set ratio (default: 0.20)")
    parser.add_argument("--skip-eda", action="store_true", help="Skip EDA phase")
    parser.add_argument(
        "--model-type",
        choices=list(MODEL_REGISTRY.keys()),
        default="xgb",
        help=f"Model type to train (default: xgb). Available: {', '.join(MODEL_REGISTRY)}",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    ratios = [args.train_ratio, args.val_ratio, args.test_ratio]
    total = sum(ratios)
    if abs(total - 1.0) > 0.001:
        print(f"[ERROR] Ratios must sum to 1.0 (got {total:.4f})")
        raise SystemExit(1)

    events = load_events(args.csv_path, args.limit)

    X, y, txn_types, feature_cols = build_features(events)

    if not args.skip_eda:
        run_eda(X, y, txn_types, feature_cols)

    print(f"\n[STEP] Stratified random split ({args.train_ratio:.0%}/{args.val_ratio:.0%}/{args.test_ratio:.0%})...")

    val_test_ratio = args.val_ratio + args.test_ratio
    events_train, events_temp, X_train, X_temp, y_train, y_temp = train_test_split(
        events, X, y, test_size=val_test_ratio, random_state=42, stratify=y
    )
    test_size_adjusted = args.test_ratio / val_test_ratio
    events_val, events_test, X_val, X_test, y_val, y_test = train_test_split(
        events_temp, X_temp, y_temp, test_size=test_size_adjusted, random_state=42, stratify=y_temp
    )

    n_fraud_train = int(y_train.sum())
    n_fraud_val = int(y_val.sum())
    n_fraud_test = int(y_test.sum())
    print(f"  Train: {len(y_train):,} samples ({n_fraud_train:,} fraud, {n_fraud_train/len(y_train)*100:.3f}%)")
    print(f"  Val:   {len(y_val):,} samples ({n_fraud_val:,} fraud, {n_fraud_val/len(y_val)*100:.3f}%)")
    print(f"  Test:  {len(y_test):,} samples ({n_fraud_test:,} fraud, {n_fraud_test/len(y_test)*100:.3f}%)")

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)

    model, best_threshold, metrics, model_version_tag = train_model(
        X_train_scaled, y_train, X_val_scaled, y_val, X_test_scaled, y_test,
        feature_cols, model_type=args.model_type,
    )

    export_model(model, scaler, feature_cols, metrics, best_threshold, model_version_tag)

    test_csv_path = MODEL_DIR / "test_set.csv"
    export_test_csv(events_test, test_csv_path)

    print("\n" + "=" * 60)
    print("TRAINING PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    model_version = f"v1_paysim_{model_version_tag}"
    print(f"\nModel type:     {metrics['model_type']}")
    print(f"Model version:  {model_version}")
    print(f"Optimal threshold: {best_threshold:.4f} (tuned on validation set)")
    print(f"Test AUC-ROC: {metrics['auc_roc']:.4f}")
    print(f"Test AUC-PR:  {metrics['auc_pr']:.4f}")
    print(f"Test F1:      {metrics['f1_score']:.4f}")
    print(f"\nPlots saved to: {PLOTS_DIR}")
    print(f"Model artifacts: {MODEL_DIR}")
    print(f"Test set: {test_csv_path}")


if __name__ == "__main__":
    raise SystemExit(main())
