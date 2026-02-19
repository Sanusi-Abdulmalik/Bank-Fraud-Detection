"""
Load CSV files into bronze layer tables.
Adds metadata: loaded_at, source_file, batch_id.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import uuid

# Database connection
DB_URL = "postgresql://postgres:password@localhost:5432/bank_analytics"
engine = create_engine(DB_URL)

# Mapping: CSV filename -> bronze table name
FILE_TO_TABLE = {
    "crm_customers.csv": "bronze_customers",
    "crm_interactions.csv": "bronze_interactions",
    "crm_fraud_reports.csv": "bronze_fraud_reports",
    "erp_accounts.csv": "bronze_accounts",
    "erp_transactions.csv": "bronze_transactions"
}

# Generate a unique batch ID for this run (timestamp + random suffix)
BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]

def load_csv_to_bronze(csv_path, table_name):
    """Read CSV, add metadata, and load into bronze table."""
    print(f"\n📄 Processing: {os.path.basename(csv_path)} -> {table_name}")
    
    try:
        # Read CSV
        df = pd.read_csv(csv_path)
        print(f"   Read {len(df)} rows from CSV")
        
        # Add metadata columns
        df['loaded_at'] = datetime.now()
        df['source_file'] = os.path.basename(csv_path)
        df['batch_id'] = BATCH_ID
        
        # Handle date/time parsing for specific columns
        # (pandas may auto-detect, but we can force if needed)
        if 'date_of_birth' in df.columns:
            df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        if 'customer_since' in df.columns:
            df['customer_since'] = pd.to_datetime(df['customer_since'])
        if 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated'])
        if 'opening_date' in df.columns:
            df['opening_date'] = pd.to_datetime(df['opening_date'])
        if 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'])
        if 'date_time' in df.columns:
            df['date_time'] = pd.to_datetime(df['date_time'])
        
        # Insert into database (append to existing table)
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✅ Loaded {len(df)} rows into {table_name}")
        
        return len(df)
        
    except Exception as e:
        print(f"❌ Error loading {csv_path}: {e}")
        return 0

def main():
    print("=" * 60)
    print("🏦 BRONZE LAYER DATA LOADER")
    print("=" * 60)
    print(f"Batch ID: {BATCH_ID}")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    total_rows = 0
    successful_files = 0
    
    for csv_filename, table_name in FILE_TO_TABLE.items():
        csv_path = os.path.join("data/raw", csv_filename)
        
        if not os.path.exists(csv_path):
            print(f"\n⚠️  File not found: {csv_path}")
            continue
        
        rows = load_csv_to_bronze(csv_path, table_name)
        if rows > 0:
            total_rows += rows
            successful_files += 1
    
    print("\n" + "=" * 60)
    print("📊 LOAD SUMMARY")
    print("=" * 60)
    print(f"Batch ID: {BATCH_ID}")
    print(f"Files processed successfully: {successful_files} / {len(FILE_TO_TABLE)}")
    print(f"Total rows inserted: {total_rows}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Verify counts in database
    verify_counts()
    
def verify_counts():
    """Print row counts for all bronze tables."""
    print("\n🔍 VERIFYING BRONZE TABLE COUNTS")
    print("-" * 40)
    
    with engine.connect() as conn:
        for table_name in FILE_TO_TABLE.values():
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            print(f"{table_name}: {count} rows")
    
    print("\n✅ Bronze layer load complete!")

if __name__ == "__main__":
    main()