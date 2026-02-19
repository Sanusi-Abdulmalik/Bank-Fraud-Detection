"""
Quick verification of bronze layer data.
"""

from sqlalchemy import create_engine, text
import pandas as pd

DB_URL = "postgresql://postgres:password@localhost:5432/bank_analytics"
engine = create_engine(DB_URL)

def run_query(query, description):
    print(f"\n{description}")
    print("-" * 40)
    df = pd.read_sql(query, engine)
    print(df)
    return df

print("🔍 BRONZE LAYER VERIFICATION")
print("=" * 60)

# 1. Row counts
run_query("SELECT 'bronze_customers' as table_name, COUNT(*) as row_count FROM bronze_customers UNION ALL "
          "SELECT 'bronze_interactions', COUNT(*) FROM bronze_interactions UNION ALL "
          "SELECT 'bronze_fraud_reports', COUNT(*) FROM bronze_fraud_reports UNION ALL "
          "SELECT 'bronze_accounts', COUNT(*) FROM bronze_accounts UNION ALL "
          "SELECT 'bronze_transactions', COUNT(*) FROM bronze_transactions",
          "📊 Table row counts:")

# 2. Sample data from each table
run_query("SELECT customer_id, first_name, last_name, income, credit_score FROM bronze_customers LIMIT 5",
          "👥 Sample customers:")

run_query("SELECT transaction_id, customer_id, amount, merchant_name, is_fraud FROM bronze_transactions LIMIT 5",
          "💳 Sample transactions:")

# 3. Check fraud count in transactions
fraud_count = pd.read_sql("SELECT COUNT(*) as fraud_count FROM bronze_transactions WHERE is_fraud = true", engine)
print(f"\n🚨 Fraudulent transactions: {fraud_count.iloc[0,0]}")

# 4. Check batch_id and loaded_at metadata
run_query("SELECT batch_id, MIN(loaded_at) as earliest_load, MAX(loaded_at) as latest_load FROM bronze_transactions GROUP BY batch_id",
          "📦 Batch info:")

print("\n✅ Verification complete!")