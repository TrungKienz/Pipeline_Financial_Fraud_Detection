import streamlit as st
import pandas as pd
import plotly.express as px
from cassandra.cluster import Cluster
from datetime import datetime
import time

# --- CONFIGURATION & STYLING ---
st.set_page_config(
    page_title="Fraud Detection Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Premium CSS for Glassmorphism & Modern UI
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    .stApp {
        background: radial-gradient(circle at 10% 20%, rgb(10, 20, 30) 0%, rgb(0, 0, 0) 90%);
        color: #E0E0E0;
    }
    
    /* Metric Card Styling */
    div[data-testid="stMetricValue"] {
        font-size: 2.2rem !important;
        font-weight: 800 !important;
        color: #00D2FF !important;
    }
    
    /* DataFrame Styling */
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #FFFFFF !important;
        letter-spacing: -1px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- DATA ACCESS LAYER ---
@st.cache_resource
def get_cassandra_session():
    try:
        # Note: 'cassandra' is the hostname in docker-compose, but from local host it's 'localhost'
        # We try localhost first, then fallback to 'cassandra' for in-docker execution
        host = 'localhost'
        cluster = Cluster([host], port=9042)
        session = cluster.connect('fraud_detection')
        return session
    except Exception:
        try:
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect('fraud_detection')
            return session
        except Exception as e:
            st.error(f"Failed to connect to Cassandra: {e}")
            return None

def load_alerts():
    session = get_cassandra_session()
    if not session: return pd.DataFrame()
    
    query = "SELECT * FROM alerts_by_account LIMIT 1000"
    try:
        rows = session.execute(query)
        df = pd.DataFrame(list(rows))
        if not df.empty:
            df['alert_ts'] = pd.to_datetime(df['alert_ts'])
            if 'triggered_rules' in df.columns:
                df['triggered_rules'] = df['triggered_rules'].apply(
                    lambda value: list(value) if isinstance(value, (list, tuple)) else ([] if pd.isna(value) else [str(value)])
                )
                df['primary_rule'] = df['triggered_rules'].apply(lambda rules: rules[0] if rules else 'ml_only')
        return df
    except Exception:
        return pd.DataFrame()

# --- MAIN DASHBOARD ---
def main():
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/shield.png", width=80)
        st.title("Control Panel")
        refresh_rate = st.slider("Auto-refresh (seconds)", 5, 60, 10)
        severity_filter = st.multiselect("Filter Severity", ["high", "medium", "low"], default=["high", "medium"])
        st.divider()
        st.info("System Status: **ACTIVE** 🟢")

    # Header
    col_header, col_status = st.columns([4, 1])
    with col_header:
        st.title("🛡️ Fraud Detection Command Center")
        st.markdown("*Real-time Hybrid Intelligence Pipeline Monitoring*")
    
    with col_status:
        st.write(f"**Last Sync:** {datetime.now().strftime('%H:%M:%S')}")

    # Load Data
    alerts_df = load_alerts()

    if alerts_df.empty:
        st.warning("📡 Waiting for live stream data... Please ensure Spark Job and Ingestion are running.")
        time.sleep(5)
        st.rerun()
        return

    # Filter
    filtered_df = alerts_df[alerts_df['severity'].isin(severity_filter)] if not alerts_df.empty else alerts_df

    # --- METRICS ---
    m1, m2, m3, m4 = st.columns(4)
    with m1: st.metric("Total Alerts", len(alerts_df))
    with m2: 
        high_risk = len(alerts_df[alerts_df['severity'] == 'high'])
        st.metric("High Severity", high_risk, delta=f"{high_risk/len(alerts_df)*100:.1f}%" if len(alerts_df)>0 else "0%")
    with m3: st.metric("Avg Risk Score", f"{alerts_df['risk_score'].mean():.2f}")
    with m4: st.metric("At-Risk Volume", f"${alerts_df['amount'].sum():,.0f}")

    st.divider()

    # --- CHARTS ---
    c1, c2 = st.columns([3, 2])
    
    with c1:
        st.subheader("📈 Risk Timeline")
        if not filtered_df.empty:
            fig_timeline = px.scatter(
                filtered_df, x="alert_ts", y="risk_score", color="severity", 
                size="amount", hover_data=["account_id"],
                color_discrete_map={"high": "#FF4B4B", "medium": "#FFA500", "low": "#00D2FF"},
                template="plotly_dark"
            )
            fig_timeline.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=0,r=0,t=20,b=0))
            st.plotly_chart(fig_timeline, use_container_width=True)
    
    with c2:
        st.subheader("📊 Transaction Types")
        if not filtered_df.empty:
            fig_pie = px.pie(filtered_df, names='txn_type', values='amount', hole=0.4, template="plotly_dark")
            fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=0,r=0,t=20,b=0))
            st.plotly_chart(fig_pie, use_container_width=True)

    c3, c4 = st.columns([2, 3])

    with c3:
        st.subheader("🧩 Alert Distribution By Rule")
        if not filtered_df.empty and 'triggered_rules' in filtered_df.columns:
            exploded_rules = filtered_df[['triggered_rules', 'amount']].explode('triggered_rules')
            exploded_rules['triggered_rules'] = exploded_rules['triggered_rules'].fillna('ml_only')
            rule_counts = (
                exploded_rules.groupby('triggered_rules', as_index=False)
                .agg(alert_count=('triggered_rules', 'size'), total_amount=('amount', 'sum'))
                .sort_values(['alert_count', 'total_amount'], ascending=[False, False])
            )
            fig_rules = px.bar(
                rule_counts.head(10),
                x='triggered_rules',
                y='alert_count',
                color='total_amount',
                template='plotly_dark',
                color_continuous_scale='Bluered',
                labels={'triggered_rules': 'Rule', 'alert_count': 'Alerts', 'total_amount': 'Amount'},
            )
            fig_rules.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=0,r=0,t=20,b=0))
            st.plotly_chart(fig_rules, use_container_width=True)

    with c4:
        st.subheader("🎯 Primary Rule Mix")
        if not filtered_df.empty and 'primary_rule' in filtered_df.columns:
            primary_mix = (
                filtered_df.groupby('primary_rule', as_index=False)
                .agg(alert_count=('primary_rule', 'size'))
                .sort_values('alert_count', ascending=False)
            )
            fig_primary = px.pie(
                primary_mix,
                names='primary_rule',
                values='alert_count',
                hole=0.45,
                template='plotly_dark',
            )
            fig_primary.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=0,r=0,t=20,b=0))
            st.plotly_chart(fig_primary, use_container_width=True)

    # --- DATA TABLE ---
    st.subheader("🚨 Live Alert Feed")
    if not filtered_df.empty:
        # Highlighting logic
        def highlight_fraud(row):
            if row['severity'] == 'high':
                return ['background-color: rgba(255, 75, 75, 0.2)'] * len(row)
            elif row['severity'] == 'medium':
                return ['background-color: rgba(255, 165, 0, 0.1)'] * len(row)
            return [''] * len(row)

        display_df = filtered_df.copy()
        display_df['status'] = display_df['severity'].apply(lambda x: "🔴 CRITICAL" if x == 'high' else ("🟠 WARNING" if x == 'medium' else "🔵 INFO"))
        if 'triggered_rules' in display_df.columns:
            display_df['triggered_rules_display'] = display_df['triggered_rules'].apply(lambda rules: ', '.join(rules) if rules else 'ml_only')
        else:
            display_df['triggered_rules_display'] = 'n/a'
        
        styled_df = (
            display_df[['status', 'severity', 'alert_ts', 'account_id', 'txn_type', 'amount', 'risk_score', 'ml_score', 'triggered_rules_display']]
            .sort_values('alert_ts', ascending=False)
            .style.apply(highlight_fraud, axis=1)
            .hide(axis='columns', subset=['severity'])
        )

        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "status": "Status",
                "risk_score": st.column_config.ProgressColumn("Rule Score", min_value=0, max_value=1),
                "ml_score": st.column_config.ProgressColumn("ML Score", min_value=0, max_value=1),
                "amount": st.column_config.NumberColumn("Amount ($)", format="$ %d"),
                "alert_ts": "Time",
                "triggered_rules_display": "Triggered Rules",
            }
        )

    # Refresh
    time.sleep(refresh_rate)
    st.rerun()

if __name__ == "__main__":
    main()
