from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import redis
import streamlit as st
from cassandra.cluster import Cluster


st.set_page_config(page_title="Fraud Pipeline Dashboard", layout="wide")

CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
KEYSPACE = "fraud_detection"
REDIS_HOST = "redis"
REDIS_PORT = 6379


@st.cache_resource(show_spinner=False)
def cassandra_session():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    try:
        session = cluster.connect(KEYSPACE)
    except Exception:
        cluster.shutdown()
        return None, None
    return cluster, session


@st.cache_resource(show_spinner=False)
def redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def load_alerts(limit: int = 50) -> pd.DataFrame:
    _, session = cassandra_session()
    if session is None:
        return pd.DataFrame()
    rows = session.execute(
        """
        SELECT account_id, alert_date, alert_ts, alert_id, event_id, name_dest, txn_type,
               amount, risk_score, severity, triggered_rules
        FROM alerts_by_account
        LIMIT %s
        """,
        (limit,),
    )
    return pd.DataFrame(list(rows))


def load_metrics(limit: int = 100) -> pd.DataFrame:
    _, session = cassandra_session()
    if session is None:
        return pd.DataFrame()
    rows = session.execute(
        """
        SELECT window_type, window_start, window_end, event_count, fraud_count, total_amount, fraud_rate
        FROM metrics_by_window
        LIMIT %s
        """,
        (limit,),
    )
    return pd.DataFrame(list(rows))


def load_recent_alert_cache(limit: int = 20) -> list[dict]:
    client = redis_client()
    values = client.lrange("latest_alerts", 0, limit - 1)
    return [json.loads(item) for item in values]


st.title("Real-time Fraud Detection Dashboard")
st.caption("Local dashboard reading durable data from Cassandra and hot alerts from Redis.")

left, right = st.columns(2)
alerts_df = load_alerts()
metrics_df = load_metrics()
redis_alerts = load_recent_alert_cache()

with left:
    st.subheader("Alert Summary")
    if alerts_df.empty:
        st.info("No alerts stored in Cassandra yet.")
    else:
        st.metric("Alerts in Cassandra", len(alerts_df))
        st.metric("High Severity Alerts", int((alerts_df["severity"] == "high").sum()))
        st.dataframe(alerts_df, use_container_width=True)

with right:
    st.subheader("Window Metrics")
    if metrics_df.empty:
        st.info("No metrics stored yet.")
    else:
        st.metric("Metric Rows", len(metrics_df))
        if "fraud_rate" in metrics_df:
            st.line_chart(metrics_df[["fraud_rate"]])
        st.dataframe(metrics_df, use_container_width=True)

st.subheader("Hot Alerts from Redis")
if not redis_alerts:
    st.info("Redis cache is empty.")
else:
    st.json(redis_alerts)

st.caption(f"Refreshed at {datetime.utcnow().isoformat()}Z")
