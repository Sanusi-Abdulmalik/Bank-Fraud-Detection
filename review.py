from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("postgresql://postgres:password@localhost:5432/bank_analytics")

# List all tables
tables = pd.read_sql("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    ORDER BY table_name
""", engine)
print("Tables in database:")
print(tables)

# Check row counts for bronze tables
for table in ['bronze_customers', 'bronze_interactions', 'bronze_fraud_reports', 
              'bronze_accounts', 'bronze_transactions']:
    count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", engine).iloc[0,0]
    print(f"{table}: {count} rows")