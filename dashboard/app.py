import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import datetime

# Page config
st.set_page_config(page_title="Bank Fraud Dashboard", layout="wide")
st.title("🏦 Bank Fraud Detection Dashboard")
st.markdown("Real-time analytics from gold layer")

# Connect to database
@st.cache_resource
def get_engine():
    return create_engine("postgresql://postgres:password@localhost:5432/bank_analytics")

engine = get_engine()

# Helper function to run queries (now accepts params)
@st.cache_data(ttl=60)
def run_query(query, params=None):
    if params:
        return pd.read_sql(query, engine, params=params)
    else:
        return pd.read_sql(query, engine)

# Sidebar filters
st.sidebar.header("Filters")
today = datetime.date.today()
date_range = st.sidebar.date_input(
    "Date range",
    [today - datetime.timedelta(days=30), today]
)

# Convert date objects to strings for SQL (optional, but safe)
start_date = date_range[0].isoformat()
end_date = date_range[1].isoformat()

# --- KPI CARDS ---
col1, col2, col3, col4 = st.columns(4)

summary = run_query("""
    SELECT 
        COUNT(*) as total_txns,
        SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_txns,
        AVG(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100 as fraud_rate,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM gold_fraud_features
    WHERE transaction_timestamp::date BETWEEN %(start)s AND %(end)s
""", params={"start": start_date, "end": end_date})

with col1:
    st.metric("Total Transactions", f"{summary['total_txns'].iloc[0]:,}")
with col2:
    st.metric("Fraudulent Transactions", f"{summary['fraud_txns'].iloc[0]:,}")
with col3:
    st.metric("Fraud Rate", f"{summary['fraud_rate'].iloc[0]:.2f}%")
with col4:
    st.metric("Unique Customers", f"{summary['unique_customers'].iloc[0]:,}")

# --- CHARTS ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Fraud by Hour of Day")
    hourly = run_query("""
        SELECT 
            transaction_hour,
            COUNT(*) as total,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud,
            AVG(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100 as fraud_rate
        FROM gold_fraud_features
        GROUP BY transaction_hour
        ORDER BY transaction_hour
    """)
    fig = px.bar(hourly, x='transaction_hour', y='fraud_rate',
                 title='Fraud Rate by Hour', labels={'fraud_rate': 'Fraud Rate (%)'})
    st.plotly_chart(fig, width='stretch')

with col_right:
    st.subheader("Top Risky Merchant Categories")
    merchant = run_query("""
        SELECT 
            merchant_category,
            COUNT(*) as total,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud,
            AVG(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100 as fraud_rate
        FROM gold_fraud_features
        GROUP BY merchant_category
        HAVING COUNT(*) > 10
        ORDER BY fraud_rate DESC
        LIMIT 10
    """)
    fig = px.bar(merchant, x='merchant_category', y='fraud_rate',
                 title='Fraud Rate by Merchant', labels={'fraud_rate': 'Fraud Rate (%)'})
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, width='stretch')

# --- TIME SERIES ---
st.subheader("Daily Fraud Trend")
daily = run_query("""
    SELECT 
        summary_date,
        total_transactions,
        fraudulent_transactions,
        fraud_rate
    FROM gold_daily_fraud_summary
    ORDER BY summary_date
""")
fig = px.line(daily, x='summary_date', y='fraud_rate',
              title='Fraud Rate Over Time', labels={'fraud_rate': 'Fraud Rate (%)'})
st.plotly_chart(fig, width='stretch')

# --- RECENT FRAUD ALERTS ---
st.subheader("Recent Fraud Alerts")
alerts = run_query("""
    SELECT 
        transaction_id,
        customer_id,
        transaction_timestamp,
        amount,
        merchant_category,
        fraud_risk_tier,
        amount_z_score
    FROM gold_fraud_features
    WHERE is_fraud = true
    ORDER BY transaction_timestamp DESC
    LIMIT 20
""")
st.dataframe(alerts, use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.info("Dashboard refreshes every 60 seconds.")