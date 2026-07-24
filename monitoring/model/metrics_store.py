from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from cassandra.cluster import Cluster


def parse_day_bucket(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def discover_prediction_days(session, lookback_days: int, limit: int) -> list[date]:
    discovered: list[date] = []
    today = datetime.utcnow().date()
    probe_dates = [today - timedelta(days=offset) for offset in range(lookback_days)]
    if date(2026, 1, 1) not in probe_dates:
        probe_dates.append(date(2026, 1, 1))

    for day_bucket in probe_dates:
        rows = session.execute(
            "SELECT event_id FROM model_predictions_by_day WHERE day_bucket = %s LIMIT %s",
            (day_bucket, 1),
        )
        if list(rows):
            discovered.append(day_bucket)
            if len(discovered) >= limit:
                break
    return discovered


def load_predictions_from_cassandra(host: str, port: int, keyspace: str, day_buckets: list[date], per_day_limit: int) -> pd.DataFrame:
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    try:
        buckets = day_buckets or discover_prediction_days(session, lookback_days=400, limit=30)
        records: list[dict[str, Any]] = []
        for day_bucket in buckets:
            rows = session.execute(
                """
                SELECT day_bucket, event_ts, event_id, account_id, name_dest, txn_type, amount,
                       risk_score, severity, ml_score, ml_model_version, triggered_rules,
                       is_alert, alert_id, actual_label
                FROM model_predictions_by_day WHERE day_bucket = %s LIMIT %s
                """,
                (day_bucket, per_day_limit),
            )
            for row in rows:
                records.append(
                    {
                        "day_bucket": row.day_bucket,
                        "event_ts": row.event_ts,
                        "event_id": row.event_id,
                        "account_id": row.account_id,
                        "name_dest": row.name_dest,
                        "txn_type": row.txn_type,
                        "amount": row.amount,
                        "risk_score": row.risk_score,
                        "severity": row.severity,
                        "ml_score": row.ml_score,
                        "ml_model_version": row.ml_model_version,
                        "triggered_rules": list(row.triggered_rules) if row.triggered_rules is not None else [],
                        "is_alert": row.is_alert,
                        "alert_id": row.alert_id,
                        "actual_label": row.actual_label,
                    }
                )
        df = pd.DataFrame(records)
        if not df.empty:
            df["event_ts"] = pd.to_datetime(df["event_ts"])
        return df
    finally:
        session.shutdown()
        cluster.shutdown()


def load_reviews_from_cassandra(host: str, port: int, keyspace: str) -> pd.DataFrame:
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    try:
        rows = session.execute(
            "SELECT alert_id, event_id, review_status, review_label, reviewer, notes, reviewed_at FROM alert_reviews"
        )
        df = pd.DataFrame(list(rows))
        if not df.empty:
            df["reviewed_at"] = pd.to_datetime(df["reviewed_at"])
        return df
    finally:
        session.shutdown()
        cluster.shutdown()


def normalize_actual_label(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    normalized = str(value).strip().lower()
    if normalized in {"fraud", "confirmed_fraud", "positive", "1", "true"}:
        return "fraud"
    if normalized in {"legit", "false_positive", "negative", "0", "false", "not_fraud"}:
        return "legit"
    return None


def normalize_is_alert(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().lower() in {"true", "1", "yes"}


def prepare_labeled_predictions(predictions_df: pd.DataFrame, reviews_df: pd.DataFrame) -> pd.DataFrame:
    merged = predictions_df.copy()
    if merged.empty:
        return merged

    if reviews_df.empty:
        merged["review_label"] = None
        merged["review_status"] = None
        merged["reviewed_at"] = pd.NaT
    else:
        review_subset = reviews_df[["alert_id", "event_id", "review_status", "review_label", "reviewed_at"]].copy()
        merged = merged.merge(review_subset, on=["alert_id", "event_id"], how="left")

    merged["event_ts"] = pd.to_datetime(merged["event_ts"])
    merged["reviewed_at"] = pd.to_datetime(merged.get("reviewed_at"), errors="coerce")
    merged["predicted_positive"] = merged["is_alert"].apply(normalize_is_alert)
    merged["normalized_actual_label"] = merged["actual_label"].apply(normalize_actual_label)
    merged["normalized_review_label"] = merged["review_label"].apply(normalize_actual_label)
    merged["effective_label"] = merged["normalized_actual_label"].combine_first(merged["normalized_review_label"])
    merged["label_source"] = merged.apply(
        lambda row: "actual_label" if pd.notna(row["normalized_actual_label"]) else ("review_label" if pd.notna(row["normalized_review_label"]) else "unlabeled"),
        axis=1,
    )
    merged["label_available"] = merged["effective_label"].notna()
    merged["actual_positive"] = merged["effective_label"].eq("fraud")
    merged["actual_negative"] = merged["effective_label"].eq("legit")
    merged["evaluation_ts"] = merged["reviewed_at"].combine_first(merged["event_ts"])
    return merged
