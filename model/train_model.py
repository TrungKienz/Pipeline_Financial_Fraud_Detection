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
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    precision_recall_curve,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

MODEL_REGISTRY: dict[str, type] = {}
MODEL_ORDER = ["logreg", "rf", "xgb", "lgbm"]
MODEL_REGISTRY["logreg"] = LogisticRegression
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
try:
    from lightgbm import LGBMClassifier
    MODEL_REGISTRY["lgbm"] = LGBMClassifier
except ImportError:
    pass

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fraud_pipeline import PipelineConfig, iter_transaction_events, transaction_to_dict
from fraud_pipeline.features import (
    FEATURE_COLUMNS,
    TXN_TYPE_CATEGORIES,
    BROWSER_CATEGORIES,
    DEVICE_TYPE_CATEGORIES,
    COUNTRY_CATEGORIES,
    build_feature_record,
)
from fraud_pipeline.parsing import parse_csv_row

MODEL_DIR = Path(__file__).resolve().parent
PLOTS_DIR = MODEL_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


from collections import defaultdict

class StateSimulator:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.sender_history = defaultdict(list)
        self.receiver_history = defaultdict(list)
        self.inbound_history = defaultdict(list)
        self.counterparties = defaultdict(set)

    def process_event(self, event) -> dict[str, float]:
        ts = event.event_time.timestamp()
        
        # Sender history expiration (ensure we have at least 1 hour of history)
        sender_hist = self.sender_history[event.name_orig]
        sender_hist = [ev for ev in sender_hist if ts - ev.event_time.timestamp() <= max(self.config.rapid_outflow_window_seconds, 3600)]
        self.sender_history[event.name_orig] = sender_hist
        
        # Receiver history expiration
        receiver_hist = self.receiver_history[event.name_dest]
        receiver_hist = [ev for ev in receiver_hist if ts - ev.event_time.timestamp() <= self.config.fan_in_window_seconds]
        self.receiver_history[event.name_dest] = receiver_hist

        # Inbound history expiration
        inbound_hist = self.inbound_history[event.name_orig]
        inbound_hist = [ev for ev in inbound_hist if ts - ev.event_time.timestamp() <= self.config.cashout_after_inbound_window_seconds]
        self.inbound_history[event.name_orig] = inbound_hist

        # Compute sender recent count & amount (fan-out window)
        sender_window = []
        if event.txn_type in {"TRANSFER", "CASH_OUT"}:
            sender_window = [
                ev for ev in sender_hist
                if ev.name_orig == event.name_orig and ts - ev.event_time.timestamp() <= self.config.fan_out_window_seconds
            ]
        sender_count = len(sender_window)
        sender_amount = sum(ev.amount for ev in sender_window)

        # Compute receiver recent count & amount (fan-in window)
        receiver_window = []
        if event.txn_type in {"TRANSFER", "CASH_IN"}:
            receiver_window = [
                ev for ev in receiver_hist
                if ev.name_dest == event.name_dest and ts - ev.event_time.timestamp() <= self.config.fan_in_window_seconds
            ]
        receiver_count = len(receiver_window)
        receiver_amount = sum(ev.amount for ev in receiver_window)

        # New counterparty check
        is_new_cp = 1.0 if (event.txn_type == "TRANSFER" and event.name_dest not in self.counterparties[event.name_orig]) else 0.0

        # Inbound to cashout ratio
        inbound_ratio = 0.0
        if event.txn_type == "CASH_OUT":
            matching_inbound = [
                ev for ev in inbound_hist
                if ev.name_dest == event.name_orig and ts - ev.event_time.timestamp() <= self.config.cashout_after_inbound_window_seconds
            ]
            inbound_total = sum(ev.amount for ev in matching_inbound)
            if inbound_total > 0:
                inbound_ratio = event.amount / inbound_total

        # 5. New features: velocity_transactions_1h
        h1_start = ts - 3600
        sender_h1_window = [
            ev for ev in sender_hist
            if ev.name_orig == event.name_orig and ts - ev.event_time.timestamp() <= 3600
        ]
        velocity_1h = len(sender_h1_window)

        # 6. New features: time_since_last_purchase
        sender_txs = [
            ev for ev in sender_hist
            if ev.name_orig == event.name_orig and ev.event_time.timestamp() < ts
        ]
        if sender_txs:
            last_ts = max(ev.event_time.timestamp() for ev in sender_txs)
            time_since_last = ts - last_ts
        else:
            time_since_last = 86400.0

        # Update histories
        self.sender_history[event.name_orig].append(event)
        self.receiver_history[event.name_dest].append(event)
        if event.txn_type in {"TRANSFER", "CASH_IN"}:
            self.inbound_history[event.name_dest].append(event)
        self.counterparties[event.name_orig].add(event.name_dest)

        return {
            "sender_recent_txn_count": float(sender_count),
            "sender_recent_total_amount": float(sender_amount),
            "receiver_recent_txn_count": float(receiver_count),
            "receiver_recent_total_amount": float(receiver_amount),
            "is_new_counterparty": is_new_cp,
            "inbound_to_cashout_ratio": inbound_ratio,
            "velocity_transactions_1h": float(velocity_1h),
            "time_since_last_purchase": float(time_since_last),
        }


def load_events(csv_path: str, limit: int | None) -> list:
    print(f"[INFO] Loading data from: {csv_path}")
    if limit:
        print(f"[INFO] Limit: {limit} rows")
    config = PipelineConfig()
    events = list(iter_transaction_events(csv_path, config=config, limit=limit))
    events.sort(key=lambda ev: ev.event_time)
    print(f"[INFO] Loaded and sorted {len(events)} transactions")
    return events


def build_features(events: list) -> tuple[np.ndarray, np.ndarray, list[str], list[str]]:
    config = PipelineConfig()
    simulator = StateSimulator(config)
    
    feature_dicts = []
    txn_types = []
    labels = []
    
    for ev in events:
        dyn_feats = simulator.process_event(ev)
        r = build_feature_record(ev, config=config, dynamic_features=dyn_feats)
        txn_types.append(r["txn_type"])
        labels.append(r["label_is_fraud"])
        
        fv = {col: float(r[col]) for col in FEATURE_COLUMNS}
        fv.update({f"type_{cat}": int(r["txn_type"] == cat) for cat in TXN_TYPE_CATEGORIES})
        fv.update({f"browser_{cat}": int(r["browser"] == cat) for cat in BROWSER_CATEGORIES})
        fv.update({f"device_type_{cat}": int(r["device_type"] == cat) for cat in DEVICE_TYPE_CATEGORIES})
        fv.update({f"country_{cat}": int(r["country"] == cat) for cat in COUNTRY_CATEGORIES})
        feature_dicts.append(fv)

    labels = np.array(labels, dtype=np.int32)
    all_cols = (
        FEATURE_COLUMNS
        + [f"type_{cat}" for cat in TXN_TYPE_CATEGORIES]
        + [f"browser_{cat}" for cat in BROWSER_CATEGORIES]
        + [f"device_type_{cat}" for cat in DEVICE_TYPE_CATEGORIES]
        + [f"country_{cat}" for cat in COUNTRY_CATEGORIES]
    )
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


def _round_metric(value: float | None, digits: int = 4):
    if value is None:
        return None
    if not np.isfinite(value):
        return None
    return round(float(value), digits)


def _format_metric(value: float | None, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def business_cost_breakdown(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    amounts: np.ndarray,
    false_alarm_unit_cost: float = 5.0,
) -> dict[str, float | int]:
    y_true = np.asarray(y_true, dtype=np.int32)
    y_pred = np.asarray(y_pred, dtype=np.int32)
    amounts = np.asarray(amounts, dtype=np.float64)

    fn_mask = (y_true == 1) & (y_pred == 0)
    fp_mask = (y_true == 0) & (y_pred == 1)

    missed_fraud_cost = float(amounts[fn_mask].sum())
    false_positives = int(fp_mask.sum())
    false_alarm_total_cost = float(false_alarm_unit_cost * false_positives)
    business_cost = missed_fraud_cost + false_alarm_total_cost

    no_model_baseline_cost = float(amounts[y_true == 1].sum())
    net_cost_savings = no_model_baseline_cost - business_cost
    savings_rate = net_cost_savings / no_model_baseline_cost if no_model_baseline_cost > 0 else 0.0

    return {
        "business_cost": round(business_cost, 2),
        "missed_fraud_cost": round(missed_fraud_cost, 2),
        "false_alarm_total_cost": round(false_alarm_total_cost, 2),
        "false_alarm_unit_cost": round(float(false_alarm_unit_cost), 2),
        "no_model_baseline_cost": round(no_model_baseline_cost, 2),
        "net_cost_savings": round(net_cost_savings, 2),
        "savings_rate": round(float(savings_rate), 4),
        "false_positives": false_positives,
        "false_negatives": int(fn_mask.sum()),
    }


def tune_threshold_by_f1(y_val: np.ndarray, y_prob_val: np.ndarray) -> dict[str, float | None]:
    precisions, recalls, thresholds = precision_recall_curve(y_val, y_prob_val)
    auc_pr = average_precision_score(y_val, y_prob_val) if int(np.sum(y_val)) > 0 else 0.0
    if len(thresholds) == 0:
        return {
            "threshold": 0.5,
            "f1": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "auc_pr": _round_metric(auc_pr),
        }

    f1_scores = 2 * (precisions[:-1] * recalls[:-1]) / (precisions[:-1] + recalls[:-1] + 1e-8)
    best_idx = int(np.argmax(f1_scores))
    return {
        "threshold": float(thresholds[best_idx]),
        "f1": float(f1_scores[best_idx]),
        "precision": float(precisions[best_idx]),
        "recall": float(recalls[best_idx]),
        "auc_pr": _round_metric(auc_pr),
    }


def tune_threshold_by_business_cost(
    y_val: np.ndarray,
    y_prob_val: np.ndarray,
    amounts_val: np.ndarray,
    false_alarm_unit_cost: float = 5.0,
) -> dict[str, float | int]:
    y_val = np.asarray(y_val, dtype=np.int32)
    y_prob_val = np.asarray(y_prob_val, dtype=np.float64)
    amounts_val = np.asarray(amounts_val, dtype=np.float64)

    baseline_cost = float(amounts_val[y_val == 1].sum())
    if len(y_prob_val) == 0:
        return {
            "threshold": 1.000001,
            "evaluated_thresholds": 1,
            **business_cost_breakdown(y_val, np.zeros_like(y_val), amounts_val, false_alarm_unit_cost),
        }

    order = np.argsort(y_prob_val)[::-1]
    scores = y_prob_val[order]
    labels = y_val[order]
    amounts = amounts_val[order]

    fraud_amounts = np.where(labels == 1, amounts, 0.0)
    false_positive_flags = (labels == 0).astype(np.int64)
    cumulative_detected_fraud_amount = np.cumsum(fraud_amounts)
    cumulative_false_positives = np.cumsum(false_positive_flags)

    change_indices = np.flatnonzero(scores[:-1] != scores[1:])
    threshold_end_indices = np.r_[change_indices, len(scores) - 1]
    candidate_thresholds = scores[threshold_end_indices]
    candidate_costs = (
        baseline_cost
        - cumulative_detected_fraud_amount[threshold_end_indices]
        + false_alarm_unit_cost * cumulative_false_positives[threshold_end_indices]
    )

    no_alert_threshold = float(np.nextafter(float(scores[0]), np.inf))
    all_thresholds = np.r_[no_alert_threshold, candidate_thresholds]
    all_costs = np.r_[baseline_cost, candidate_costs]

    best_idx = int(np.argmin(all_costs))
    best_threshold = float(all_thresholds[best_idx])
    y_pred = (y_prob_val >= best_threshold).astype(np.int32)
    return {
        "threshold": best_threshold,
        "evaluated_thresholds": int(len(all_thresholds)),
        **business_cost_breakdown(y_val, y_pred, amounts_val, false_alarm_unit_cost),
    }


def evaluate_at_threshold(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    amounts: np.ndarray,
    threshold: float,
    false_alarm_unit_cost: float = 5.0,
) -> dict[str, object]:
    y_pred = (y_prob >= threshold).astype(np.int32)
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    auc_roc = roc_auc_score(y_true, y_prob) if len(np.unique(y_true)) == 2 else None
    auc_pr = average_precision_score(y_true, y_prob) if int(np.sum(y_true)) > 0 else 0.0

    metrics = {
        "threshold": round(float(threshold), 6),
        "auc_roc": _round_metric(auc_roc),
        "auc_pr": _round_metric(auc_pr),
        "precision": _round_metric(precision_score(y_true, y_pred, zero_division=0)),
        "recall": _round_metric(recall_score(y_true, y_pred, zero_division=0)),
        "f1_score": _round_metric(f1_score(y_true, y_pred, zero_division=0)),
        "confusion_matrix": {
            "tn": int(cm[0, 0]), "fp": int(cm[0, 1]),
            "fn": int(cm[1, 0]), "tp": int(cm[1, 1]),
        },
    }
    metrics.update(business_cost_breakdown(y_true, y_pred, amounts, false_alarm_unit_cost))
    return metrics


def _build_model(model_type: str, n_samples: int, n_pos: int) -> tuple:
    if model_type == "logreg":
        print(f"\n[STEP] Training Logistic Regression...")
        model = LogisticRegression(
            max_iter=1000,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        )
        hyperparams = {
            "max_iter": 1000,
            "class_weight": "balanced",
        }
        model_name = "LogisticRegression"
        model_version_tag = "logreg"
    elif model_type == "rf":
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
    elif model_type == "lgbm":
        print(f"\n[STEP] Training LightGBM...")
        scale_pos_weight = (n_samples - n_pos) / max(n_pos, 1)
        model = LGBMClassifier(
            n_estimators=200,
            max_depth=-1,
            num_leaves=31,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            random_state=42,
            n_jobs=-1,
            verbosity=-1,
        )
        hyperparams = {
            "n_estimators": 200,
            "max_depth": -1,
            "num_leaves": 31,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "scale_pos_weight": round(scale_pos_weight, 2),
        }
        model_name = "LGBMClassifier"
        model_version_tag = "lgbm"
    else:
        raise ValueError(f"Unknown model_type: {model_type!r}")
    return model, hyperparams, model_name, model_version_tag


def _resample_training_data(X_train: np.ndarray, y_train: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    n_fraud_train = int(y_train.sum())
    n_legit_train = int(len(y_train) - n_fraud_train)
    if n_fraud_train < 2 or n_legit_train == 0:
        print("[WARN] Skipping SMOTE: not enough minority/majority samples")
        return X_train, y_train

    current_ratio = n_fraud_train / max(n_legit_train, 1)
    if current_ratio >= 0.1:
        print(f"[INFO] Skipping SMOTE: minority/majority ratio already {current_ratio:.3f}")
        return X_train, y_train

    print(f"\n[STEP] Applying SMOTE on training set...")
    smote = SMOTE(sampling_strategy=0.1, random_state=42, k_neighbors=min(5, n_fraud_train - 1))
    X_train_res, y_train_res = smote.fit_resample(X_train, y_train)
    n_resampled = int(y_train_res.sum())
    print(f"  Train before SMOTE: {len(X_train):,} ({n_fraud_train:,} fraud)")
    print(f"  Train after SMOTE:  {len(X_train_res):,} ({n_resampled:,} fraud, {n_resampled/len(X_train_res)*100:.1f}%)")
    return X_train_res, y_train_res


def train_model(
    X_train,
    y_train,
    X_val,
    y_val,
    X_test,
    y_test,
    amounts_val,
    amounts_test,
    feature_cols,
    model_type="rf",
    false_alarm_unit_cost: float = 5.0,
    resampled_train: tuple[np.ndarray, np.ndarray] | None = None,
):
    print("\n" + "=" * 60)
    print(f"TRAINING: {model_type}")
    print("=" * 60)

    n_fraud_train = int(y_train.sum())
    n_fraud_val = int(y_val.sum())
    n_fraud_test = int(y_test.sum())
    print(f"Train: {len(y_train):,} samples ({n_fraud_train:,} fraud, {n_fraud_train/len(y_train)*100:.3f}%)")
    print(f"Val:   {len(y_val):,} samples ({n_fraud_val:,} fraud, {n_fraud_val/len(y_val)*100:.3f}%)")
    print(f"Test:  {len(y_test):,} samples ({n_fraud_test:,} fraud, {n_fraud_test/len(y_test)*100:.3f}%)")

    if resampled_train is None:
        X_train_res, y_train_res = _resample_training_data(X_train, y_train)
    else:
        X_train_res, y_train_res = resampled_train
        n_resampled = int(y_train_res.sum())
        print(f"[INFO] Using shared training sample: {len(X_train_res):,} rows ({n_resampled:,} fraud)")

    model, hyperparams, model_name, model_version_tag = _build_model(
        model_type, len(X_train), n_fraud_train
    )

    start = time.time()
    model.fit(X_train_res, y_train_res)
    elapsed = time.time() - start
    print(f"  Training completed in {elapsed:.2f}s")

    print(f"\n[STEP] Tuning threshold by business cost on validation set ({len(y_val):,} samples)...")
    y_prob_val = model.predict_proba(X_val)[:, 1]
    cost_threshold = tune_threshold_by_business_cost(
        y_val,
        y_prob_val,
        amounts_val,
        false_alarm_unit_cost=false_alarm_unit_cost,
    )
    f1_threshold = tune_threshold_by_f1(y_val, y_prob_val)
    best_threshold = float(cost_threshold["threshold"])
    val_metrics = evaluate_at_threshold(
        y_val,
        y_prob_val,
        amounts_val,
        best_threshold,
        false_alarm_unit_cost=false_alarm_unit_cost,
    )
    print(f"  Validation AUC-PR:       {val_metrics['auc_pr']:.4f}")
    print(f"  Business-cost threshold: {best_threshold:.6f}")
    print(f"  Validation business cost: {val_metrics['business_cost']:.2f}")
    print(f"  No-model baseline cost:   {val_metrics['no_model_baseline_cost']:.2f}")
    print(f"  Net cost savings:         {val_metrics['net_cost_savings']:.2f} ({val_metrics['savings_rate']:.2%})")
    print(f"  Val Precision/Recall/F1:  {val_metrics['precision']:.4f} / {val_metrics['recall']:.4f} / {val_metrics['f1_score']:.4f}")
    print(f"  Best-F1 threshold:        {float(f1_threshold['threshold']):.6f} (F1={float(f1_threshold['f1']):.4f})")

    print(f"\n[STEP] Final evaluation on held-out test set ({len(y_test):,} samples)...")
    y_prob_test = model.predict_proba(X_test)[:, 1]
    y_pred_opt = (y_prob_test >= best_threshold).astype(int)
    test_metrics = evaluate_at_threshold(
        y_test,
        y_prob_test,
        amounts_test,
        best_threshold,
        false_alarm_unit_cost=false_alarm_unit_cost,
    )
    cm = test_metrics["confusion_matrix"]

    print(f"\n  Test AUC-ROC:       {_format_metric(test_metrics['auc_roc'])}")
    print(f"  Test AUC-PR:        {_format_metric(test_metrics['auc_pr'])}")
    print(f"  Test Precision:     {test_metrics['precision']:.4f}")
    print(f"  Test Recall:        {test_metrics['recall']:.4f}")
    print(f"  Test F1:            {test_metrics['f1_score']:.4f}")
    print(f"  Test Business Cost: {test_metrics['business_cost']:.2f}")
    print(f"  Test Cost Savings:  {test_metrics['net_cost_savings']:.2f} ({test_metrics['savings_rate']:.2%})")
    print(f"\n  Confusion Matrix (threshold={best_threshold:.4f}):")
    print(f"    TN={cm['tn']:,}  FP={cm['fp']:,}")
    print(f"    FN={cm['fn']:,}  TP={cm['tp']:,}")
    print(f"\n  Classification Report:")
    print(classification_report(
        y_test,
        y_pred_opt,
        labels=[0, 1],
        target_names=["Legit", "Fraud"],
        digits=4,
        zero_division=0,
    ))

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        plt.figure(figsize=(8, 6))
        precisions, recalls, _ = precision_recall_curve(y_test, y_prob_test)
        plt.plot(recalls, precisions, color="#4dabf7", linewidth=2, label=f"PR curve (AUC={test_metrics['auc_pr']:.3f})")
        plt.scatter([test_metrics["recall"]], [test_metrics["precision"]],
                    color="#ff6b6b", s=100, zorder=5, label=f"Business threshold={best_threshold:.3f}")
        plt.xlabel("Recall", fontsize=12)
        plt.ylabel("Precision", fontsize=12)
        plt.title(f"Precision-Recall Curve - {model_name} (Test Set)", fontsize=14, fontweight="bold")
        plt.legend()
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / f"pr_curve_{model_version_tag}.png"), dpi=100)
        plt.close()
        print(f"[PLOT] Saved pr_curve_{model_version_tag}.png")

        if test_metrics["auc_roc"] is not None:
            fpr, tpr, _ = roc_curve(y_test, y_prob_test)
            plt.figure(figsize=(8, 6))
            plt.plot(fpr, tpr, color="#4dabf7", linewidth=2, label=f"ROC curve (AUC={test_metrics['auc_roc']:.3f})")
            plt.plot([0, 1], [0, 1], "k--", alpha=0.5)
            plt.xlabel("False Positive Rate", fontsize=12)
            plt.ylabel("True Positive Rate", fontsize=12)
            plt.title(f"ROC Curve - {model_name} (Test Set)", fontsize=14, fontweight="bold")
            plt.legend()
            plt.grid(alpha=0.3)
            plt.tight_layout()
            plt.savefig(str(PLOTS_DIR / f"roc_curve_{model_version_tag}.png"), dpi=100)
            plt.close()
            print(f"[PLOT] Saved roc_curve_{model_version_tag}.png")

        import seaborn as sns
        plt.figure(figsize=(6, 5))
        cm_array = np.array([[cm["tn"], cm["fp"]], [cm["fn"], cm["tp"]]])
        sns.heatmap(cm_array, annot=True, fmt="d", cmap="Blues", xticklabels=["Legit", "Fraud"],
                    yticklabels=["Legit", "Fraud"])
        plt.title(f"{model_name} Confusion Matrix (threshold={best_threshold:.3f})", fontsize=14, fontweight="bold")
        plt.ylabel("Actual")
        plt.xlabel("Predicted")
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / f"confusion_matrix_{model_version_tag}.png"), dpi=100)
        plt.close()
        print(f"[PLOT] Saved confusion_matrix_{model_version_tag}.png")

        importances = None
        importance_label = "Feature Importance"
        if hasattr(model, "feature_importances_"):
            importances = model.feature_importances_
        elif hasattr(model, "coef_"):
            importances = np.abs(model.coef_[0])
            importance_label = "Absolute Coefficient"
        if importances is not None:
            indices = np.argsort(importances)[::-1][:25]
            plt.figure(figsize=(12, 8))
            colors = plt.cm.Blues(np.linspace(0.4, 1, len(indices)))
            plt.barh(range(len(indices)), importances[indices][::-1], color=colors[::-1])
            plt.yticks(range(len(indices)), [feature_cols[i] for i in indices[::-1]])
            plt.xlabel(importance_label, fontsize=12)
            plt.title(f"{model_name} Top Features", fontsize=14, fontweight="bold")
            plt.tight_layout()
            plt.savefig(str(PLOTS_DIR / f"feature_importance_{model_version_tag}.png"), dpi=100)
            plt.close()
            print(f"[PLOT] Saved feature_importance_{model_version_tag}.png")

    except ImportError:
        print("[WARN] matplotlib/seaborn not available, skipping evaluation plots")
    except Exception as exc:
        print(f"[WARN] Failed to generate evaluation plots for {model_name}: {exc}")

    metrics = {
        "model_tag": model_version_tag,
        "model_type": model_name,
        "hyperparameters": hyperparams,
        "threshold_selection_metric": "minimum_validation_business_cost",
        "cost_formula": "sum(amount_i for false negatives) + false_alarm_unit_cost * false_positives",
        "false_alarm_unit_cost": round(float(false_alarm_unit_cost), 2),
        "optimal_threshold": float(best_threshold),
        "best_f1_threshold": {
            "threshold": float(f1_threshold["threshold"]),
            "f1": _round_metric(float(f1_threshold["f1"])),
            "precision": _round_metric(float(f1_threshold["precision"])),
            "recall": _round_metric(float(f1_threshold["recall"])),
        },
        "auc_roc": test_metrics["auc_roc"],
        "auc_pr": test_metrics["auc_pr"],
        "f1_score": test_metrics["f1_score"],
        "precision": test_metrics["precision"],
        "recall": test_metrics["recall"],
        "business_cost": test_metrics["business_cost"],
        "no_model_baseline_cost": test_metrics["no_model_baseline_cost"],
        "net_cost_savings": test_metrics["net_cost_savings"],
        "savings_rate": test_metrics["savings_rate"],
        "train_time_seconds": round(elapsed, 2),
        "confusion_matrix": test_metrics["confusion_matrix"],
        "validation_threshold_cost": cost_threshold,
        "validation_metrics": val_metrics,
        "test_metrics": test_metrics,
    }

    return model, best_threshold, metrics, model_version_tag


def export_model(model, scaler, feature_cols, metrics, threshold, model_version_tag="rf"):
    model_name = metrics.get("model_type", "Unknown")
    print("\n" + "=" * 60)
    print(f"EXPORTING MODEL ARTIFACTS: {model_version_tag}")
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
        "model_tag": model_version_tag,
        "model_type": model_name,
        "feature_columns": feature_cols,
        "optimal_threshold": threshold,
        "threshold_selection_metric": "minimum_validation_business_cost",
        "cost_assumptions": {
            "missed_fraud_cost": "transaction amount for each false negative",
            "false_alarm_unit_cost": metrics.get("false_alarm_unit_cost", 5.0),
            "business_cost_formula": "sum(amount_i for false negatives) + false_alarm_unit_cost * false_positives",
            "no_model_baseline_cost": "sum(amount_i for all actual fraud transactions)",
        },
        "metrics": metrics,
    })
    meta_filename = f"model_metadata_{model_version_tag}.json"
    meta_path = MODEL_DIR / meta_filename
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"[EXPORT] Metadata saved to {meta_path}")

    files = [model_path, scaler_path, cols_path, meta_path]
    for f in files:
        print(f"  {f.name}: {f.stat().st_size / 1024:.1f} KB")

    return {
        "model_path": str(model_path),
        "scaler_path": str(scaler_path),
        "feature_columns_path": str(cols_path),
        "metadata_path": str(meta_path),
    }


def export_comparison_outputs(results: list[dict], selected_result: dict, false_alarm_unit_cost: float) -> None:
    selected_metrics = selected_result["metrics"]
    selected_tag = selected_result["model_tag"]
    selected_version = f"v1_paysim_{selected_tag}"

    comparison = _to_native({
        "selection_metric": "minimum_validation_business_cost",
        "tie_breaker": "higher_validation_auc_pr",
        "selected_model_tag": selected_tag,
        "selected_model_version": selected_version,
        "selected_threshold": selected_result["threshold"],
        "cost_assumptions": {
            "missed_fraud_cost": "transaction amount for each false negative",
            "false_alarm_unit_cost": false_alarm_unit_cost,
            "business_cost_formula": "sum(amount_i for false negatives) + false_alarm_unit_cost * false_positives",
            "no_model_baseline_cost": "sum(amount_i for all actual fraud transactions)",
        },
        "models": [
            {
                "model_tag": item["model_tag"],
                "model_type": item["metrics"]["model_type"],
                "optimal_threshold": item["threshold"],
                "validation_business_cost": item["metrics"]["validation_metrics"]["business_cost"],
                "validation_auc_pr": item["metrics"]["validation_metrics"]["auc_pr"],
                "test_business_cost": item["metrics"]["test_metrics"]["business_cost"],
                "test_auc_pr": item["metrics"]["test_metrics"]["auc_pr"],
                "test_precision": item["metrics"]["test_metrics"]["precision"],
                "test_recall": item["metrics"]["test_metrics"]["recall"],
                "test_f1_score": item["metrics"]["test_metrics"]["f1_score"],
                "test_no_model_baseline_cost": item["metrics"]["test_metrics"]["no_model_baseline_cost"],
                "test_net_cost_savings": item["metrics"]["test_metrics"]["net_cost_savings"],
                "test_savings_rate": item["metrics"]["test_metrics"]["savings_rate"],
                "confusion_matrix": item["metrics"]["test_metrics"]["confusion_matrix"],
                "metrics": item["metrics"],
            }
            for item in results
        ],
    })
    comparison_path = MODEL_DIR / "model_comparison.json"
    comparison_path.write_text(json.dumps(comparison, indent=2), encoding="utf-8")
    print(f"[EXPORT] Model comparison saved to {comparison_path}")

    selected_model = _to_native({
        "model_tag": selected_tag,
        "model_version": selected_version,
        "model_type": selected_metrics["model_type"],
        "optimal_threshold": selected_result["threshold"],
        "selection_metric": "minimum_validation_business_cost",
    })
    selected_path = MODEL_DIR / "selected_model.json"
    selected_path.write_text(json.dumps(selected_model, indent=2), encoding="utf-8")
    print(f"[EXPORT] Selected model pointer saved to {selected_path}")

    eval_results = _to_native({
        "selected_model_tag": selected_tag,
        "model_version": selected_version,
        "optimal_threshold": selected_result["threshold"],
        "metrics": selected_metrics,
        "model_comparison_path": str(comparison_path),
        "plots": [p.name for p in PLOTS_DIR.glob("*.png")],
    })
    eval_path = MODEL_DIR / "eval_results.json"
    eval_path.write_text(json.dumps(eval_results, indent=2), encoding="utf-8")
    print(f"[EXPORT] Evaluation results saved to {eval_path}")


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


def export_final_feature_table(events: list, feature_cols: list[str], output_path: Path, limit: int = 20000):
    print(f"\n[EXPORT] Exporting final feature table ({limit} rows) to {output_path}")
    config = PipelineConfig()
    simulator = StateSimulator(config)
    
    rows_to_export = events[:limit]
    
    with output_path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = ["event_id", "label_is_fraud", "txn_type", "browser", "device_type", "country"] + feature_cols
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for ev in rows_to_export:
            dyn_feats = simulator.process_event(ev)
            r = build_feature_record(ev, config=config, dynamic_features=dyn_feats)
            
            row_dict = {
                "event_id": ev.event_id,
                "label_is_fraud": r["label_is_fraud"],
                "txn_type": r["txn_type"],
                "browser": r["browser"],
                "device_type": r["device_type"],
                "country": r["country"],
            }
            
            fv = {col: float(r[col]) for col in FEATURE_COLUMNS}
            fv.update({f"type_{cat}": int(r["txn_type"] == cat) for cat in TXN_TYPE_CATEGORIES})
            fv.update({f"browser_{cat}": int(r["browser"] == cat) for cat in BROWSER_CATEGORIES})
            fv.update({f"device_type_{cat}": int(r["device_type"] == cat) for cat in DEVICE_TYPE_CATEGORIES})
            fv.update({f"country_{cat}": int(r["country"] == cat) for cat in COUNTRY_CATEGORIES})
            
            row_dict.update({col: fv[col] for col in feature_cols})
            writer.writerow(row_dict)
            
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")


def parse_model_types(raw: str) -> list[str]:
    requested = MODEL_ORDER if raw.strip().lower() == "all" else [item.strip().lower() for item in raw.split(",")]
    selected: list[str] = []
    seen: set[str] = set()
    for model_type in requested:
        if not model_type:
            continue
        if model_type not in MODEL_ORDER:
            raise ValueError(f"Unknown model type: {model_type!r}. Valid values: all, {', '.join(MODEL_ORDER)}")
        if model_type not in MODEL_REGISTRY:
            print(f"[WARN] Skipping {model_type}: dependency is not installed")
            continue
        if model_type not in seen:
            selected.append(model_type)
            seen.add(model_type)
    if not selected:
        raise ValueError("No requested model types are available in this environment")
    return selected


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
        default=None,
        help="Single model type to train; kept for compatibility. Overrides --model-types when set.",
    )
    parser.add_argument(
        "--model-types",
        default="all",
        help=f"Comma-separated model types or 'all' (default: all). Valid: {', '.join(MODEL_ORDER)}",
    )
    parser.add_argument(
        "--false-alarm-cost",
        type=float,
        default=5.0,
        help="Fixed customer-friction/manual-review cost per false positive (default: 5.0)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.false_alarm_cost < 0:
        print(f"[ERROR] --false-alarm-cost must be non-negative (got {args.false_alarm_cost})")
        raise SystemExit(1)

    try:
        model_types = parse_model_types(args.model_type or args.model_types)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1)

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
    print(f"  Models: {', '.join(model_types)}")
    print(f"  False alarm cost: {args.false_alarm_cost:.2f}")

    amounts_val = np.array([ev.amount for ev in events_val], dtype=np.float64)
    amounts_test = np.array([ev.amount for ev in events_test], dtype=np.float64)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)
    resampled_train = _resample_training_data(X_train_scaled, y_train)

    results: list[dict] = []
    for model_type in model_types:
        model, best_threshold, metrics, model_version_tag = train_model(
            X_train_scaled,
            y_train,
            X_val_scaled,
            y_val,
            X_test_scaled,
            y_test,
            amounts_val,
            amounts_test,
            feature_cols,
            model_type=model_type,
            false_alarm_unit_cost=args.false_alarm_cost,
            resampled_train=resampled_train,
        )
        artifacts = export_model(model, scaler, feature_cols, metrics, best_threshold, model_version_tag)
        results.append({
            "model": model,
            "threshold": best_threshold,
            "metrics": metrics,
            "model_tag": model_version_tag,
            "artifacts": artifacts,
        })

    def _selection_key(item: dict) -> tuple[float, float]:
        validation_metrics = item["metrics"]["validation_metrics"]
        validation_cost = float(validation_metrics["business_cost"])
        validation_auc_pr = float(validation_metrics["auc_pr"] or 0.0)
        return validation_cost, -validation_auc_pr

    selected_result = min(results, key=_selection_key)
    export_comparison_outputs(results, selected_result, false_alarm_unit_cost=args.false_alarm_cost)

    test_csv_path = MODEL_DIR / "test_set.csv"
    export_test_csv(events_test, test_csv_path)

    feature_table_path = MODEL_DIR / "final_feature_table.csv"
    export_final_feature_table(events_test, feature_cols, feature_table_path)

    print("\n" + "=" * 60)
    print("TRAINING PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    selected_metrics = selected_result["metrics"]
    model_version = f"v1_paysim_{selected_result['model_tag']}"
    print("\nModel comparison (ranked by validation business cost):")
    for item in sorted(results, key=_selection_key):
        val = item["metrics"]["validation_metrics"]
        test = item["metrics"]["test_metrics"]
        print(
            f"  {item['model_tag']:>6s} | "
            f"val_cost={val['business_cost']:.2f} | "
            f"test_cost={test['business_cost']:.2f} | "
            f"test_auc_pr={test['auc_pr']:.4f} | "
            f"test_f1={test['f1_score']:.4f} | "
            f"savings={test['savings_rate']:.2%}"
        )

    print(f"\nSelected model type:     {selected_metrics['model_type']}")
    print(f"Selected model version:  {model_version}")
    print(f"Business threshold:      {selected_result['threshold']:.6f} (minimum validation business cost)")
    print(f"Test AUC-ROC:            {_format_metric(selected_metrics['auc_roc'])}")
    print(f"Test AUC-PR:             {_format_metric(selected_metrics['auc_pr'])}")
    print(f"Test Precision:          {_format_metric(selected_metrics['precision'])}")
    print(f"Test Recall:             {_format_metric(selected_metrics['recall'])}")
    print(f"Test F1:                 {_format_metric(selected_metrics['f1_score'])}")
    print(f"Test Business Cost:      {selected_metrics['business_cost']:.2f}")
    print(f"No-Model Baseline Cost:  {selected_metrics['no_model_baseline_cost']:.2f}")
    print(f"Net Cost Savings:        {selected_metrics['net_cost_savings']:.2f} ({selected_metrics['savings_rate']:.2%})")
    print(f"\nPlots saved to: {PLOTS_DIR}")
    print(f"Model artifacts: {MODEL_DIR}")
    print(f"Test set: {test_csv_path}")
    print(f"Feature table: {feature_table_path}")


if __name__ == "__main__":
    raise SystemExit(main())
