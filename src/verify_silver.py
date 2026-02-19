"""
Verify silver layer data quality and completeness.
"""

from sqlalchemy import create_engine, text
import pandas as pd

DB_URL = "postgresql://postgres:password@localhost:5432/bank_analytics"
engine = create_engine(DB_URL)

def run_query(query, description):
    print(f"\n{description}")
    print("-" * 60)
    df = pd.read_sql(query, engine)
    print(df.to_string(index=False))
    return df

print("🔍 SILVER LAYER DATA QUALITY CHECK")
print("=" * 60)

# 1. Row counts
run_query("""
    SELECT 'silver_customers' as table_name, COUNT(*) as row_count FROM silver_customers
    UNION ALL
    SELECT 'silver_accounts', COUNT(*) FROM silver_accounts
    UNION ALL
    SELECT 'silver_transactions', COUNT(*) FROM silver_transactions
""", "📊 Table row counts:")

# 2. Customer distribution by segment and risk
run_query("""
    SELECT 
        segment,
        risk_score,
        COUNT(*) as customer_count,
        AVG(total_balance) as avg_balance,
        AVG(credit_score) as avg_credit_score
    FROM silver_customers
    GROUP BY segment, risk_score
    ORDER BY segment, risk_score
""", "👥 Customer segments by risk:")

# 3. Account types distribution
run_query("""
    SELECT 
        account_type,
        COUNT(*) as account_count,
        AVG(current_balance) as avg_balance,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_count,
        AVG(account_age_days)/365 as avg_age_years
    FROM silver_accounts
    GROUP BY account_type
    ORDER BY account_count DESC
""", "💰 Account types:")

# 4. Fraud statistics
run_query("""
    SELECT 
        is_fraud,
        COUNT(*) as transaction_count,
        AVG(amount_abs) as avg_amount,
        SUM(amount_abs) as total_amount,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM silver_transactions
    GROUP BY is_fraud
""", "🚨 Fraud statistics:")

# 5. Fraud by hour of day
run_query("""
    SELECT 
        hour_group,
        COUNT(*) as total_txns,
        SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_txns,
        ROUND(100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate
    FROM silver_transactions
    GROUP BY hour_group
    ORDER BY 
        CASE hour_group
            WHEN 'Late Night' THEN 1
            WHEN 'Morning' THEN 2
            WHEN 'Afternoon' THEN 3
            WHEN 'Evening' THEN 4
            WHEN 'Night' THEN 5
        END
""", "⏰ Fraud by time of day:")

# 6. Data quality checks
print("\n✅ DATA QUALITY CHECKS")
print("-" * 60)

checks = [
    ("silver_customers", "customer_id", "NULL customer IDs"),
    ("silver_customers", "risk_score", "risk_score is NULL"),
    ("silver_accounts", "account_id", "NULL account IDs"),
    ("silver_accounts", "customer_id", "NULL customer IDs"),
    ("silver_transactions", "transaction_id", "NULL transaction IDs"),
    ("silver_transactions", "account_id", "NULL account IDs"),
]

for table, column, description in checks:
    query = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
    count = pd.read_sql(query, engine).iloc[0,0]
    status = "✅" if count == 0 else "⚠️"
    print(f"  {status} {table}: {count} {description}")

print("\n" + "=" * 60)
print("✅ Silver layer verification complete!")