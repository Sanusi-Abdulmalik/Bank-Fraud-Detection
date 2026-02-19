"""
Verify gold layer data.
"""

from sqlalchemy import create_engine
import pandas as pd

DB_URL = "postgresql://postgres:password@localhost:5432/bank_analytics"
engine = create_engine(DB_URL)

print("🔍 GOLD LAYER VERIFICATION")
print("=" * 60)

# 1. Check row counts
for table in ['gold_customer_360', 'gold_fraud_features', 'gold_daily_fraud_summary']:
    count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", engine).iloc[0,0]
    print(f"{table}: {count} rows")

# 2. Sample gold_customer_360
print("\n👥 Sample gold_customer_360 (first 3 rows):")
print(pd.read_sql("SELECT customer_id, full_name, demographic_segment, total_relationship_balance, is_high_value FROM gold_customer_360 LIMIT 3", engine))

# 3. Sample gold_fraud_features (focus on fraud)
print("\n🚨 Sample gold_fraud_features (fraudulent transactions):")
fraud_sample = pd.read_sql("""
    SELECT transaction_id, customer_id, amount, merchant_category, is_fraud, fraud_risk_tier
    FROM gold_fraud_features
    WHERE is_fraud = true
    LIMIT 5
""", engine)
print(fraud_sample)

# 4. Daily summary
print("\n📊 Daily fraud summary (last 5 days):")
daily = pd.read_sql("""
    SELECT summary_date, total_transactions, fraudulent_transactions, fraud_rate
    FROM gold_daily_fraud_summary
    ORDER BY summary_date DESC
    LIMIT 5
""", engine)
print(daily)

print("\n✅ Gold layer verification complete!")