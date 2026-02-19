"""
Transform bronze layer data into clean silver layer tables.
Adds business logic, derived columns, and data quality checks.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Database connection
DB_URL = "postgresql://postgres:password@localhost:5432/bank_analytics"
engine = create_engine(DB_URL)

class SilverTransformer:
    def __init__(self):
        self.batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def transform_customers(self):
        """Transform bronze_customers to silver_customers"""
        print("\n👥 Transforming customers...")
        
        # Read from bronze
        query = "SELECT * FROM bronze_customers"
        df = pd.read_sql(query, engine)
        print(f"   Read {len(df)} customers from bronze")
        
        # Create full name
        df['full_name'] = df['first_name'] + ' ' + df['last_name']
        
        # Clean phone numbers (remove non-digits)
        df['phone_cleaned'] = df['phone'].str.replace(r'\D', '', regex=True)
        
        # Calculate age from date of birth
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        today = datetime.now()
        df['age'] = (today - df['date_of_birth']).dt.days // 365
        
        # Create age groups
        bins = [0, 25, 35, 50, 65, 100]
        labels = ['18-25', '26-35', '36-50', '51-65', '65+']
        df['age_group'] = pd.cut(df['age'], bins=bins, labels=labels)
        
        # Categorize occupations
        def categorize_occupation(job):
            job = str(job).lower()
            if any(word in job for word in ['engineer', 'developer', 'technician', 'analyst']):
                return 'Tech'
            elif any(word in job for word in ['doctor', 'nurse', 'therapist', 'health']):
                return 'Healthcare'
            elif any(word in job for word in ['teacher', 'professor', 'instructor']):
                return 'Education'
            elif any(word in job for word in ['manager', 'director', 'executive']):
                return 'Management'
            elif any(word in job for word in ['sales', 'marketing', 'advertising']):
                return 'Sales/Marketing'
            else:
                return 'Other'
        
        df['occupation_category'] = df['occupation'].apply(categorize_occupation)
        
        # Create income buckets
        bins = [0, 30000, 60000, 100000, 150000, float('inf')]
        labels = ['Low', 'Lower-Middle', 'Middle', 'Upper-Middle', 'High']
        df['income_bucket'] = pd.cut(df['income'], bins=bins, labels=labels)
        
        # Create credit score tiers
        def credit_tier(score):
            if score >= 800:
                return 'Excellent'
            elif score >= 740:
                return 'Very Good'
            elif score >= 670:
                return 'Good'
            elif score >= 580:
                return 'Fair'
            else:
                return 'Poor'
        
        df['credit_score_tier'] = df['credit_score'].apply(credit_tier)
        
        # Calculate customer tenure
        df['customer_since'] = pd.to_datetime(df['customer_since'])
        df['customer_tenure_days'] = (today - df['customer_since']).dt.days
        df['tenure_group'] = pd.cut(
            df['customer_tenure_days'] / 365, 
            bins=[0, 1, 3, 5, 10, float('inf')],
            labels=['<1 year', '1-3 years', '3-5 years', '5-10 years', '10+ years']
        )
        
        # Simplify KYC status
        df['kyc_verified'] = df['kyc_status'] == 'Verified'
        
        # Map risk level to numeric score
        risk_map = {'Low': 1, 'Medium': 2, 'High': 3}
        df['risk_score'] = df['risk_level'].map(risk_map)
        
        # Get account metrics from silver_accounts (will be populated later)
        # For now, set defaults
        df['total_accounts'] = 0
        df['total_balance'] = 0
        df['avg_account_balance'] = 0
        
        # Get interaction metrics from bronze_interactions
        interactions_query = """
            SELECT 
                customer_id,
                MAX(date_time) as last_interaction_date,
                COUNT(*) as total_interactions,
                SUM(CASE WHEN interaction_type = 'complaint' THEN 1 ELSE 0 END) as complaint_count
            FROM bronze_interactions
            GROUP BY customer_id
        """
        interactions = pd.read_sql(interactions_query, engine)
        
        # Merge interactions data
        df = df.merge(interactions, on='customer_id', how='left')
        df['last_interaction_date'] = pd.to_datetime(df['last_interaction_date'])
        
        # Get fraud history
        fraud_query = """
            SELECT 
                customer_id,
                COUNT(*) as fraud_report_count,
                SUM(amount_lost) as total_fraud_loss
            FROM bronze_fraud_reports
            GROUP BY customer_id
        """
        fraud_history = pd.read_sql(fraud_query, engine)
        
        # Merge fraud data
        df = df.merge(fraud_history, on='customer_id', how='left')
        df['has_fraud_history'] = df['fraud_report_count'] > 0
        
        # Fill NaN values
        df['total_interactions'] = df['total_interactions'].fillna(0).astype(int)
        df['complaint_count'] = df['complaint_count'].fillna(0).astype(int)
        df['fraud_report_count'] = df['fraud_report_count'].fillna(0).astype(int)
        df['total_fraud_loss'] = df['total_fraud_loss'].fillna(0)
        df['has_fraud_history'] = df['has_fraud_history'].fillna(False)
        
        # Select and order columns for silver_customers
        # Rename columns to match silver_customers table
        df.rename(columns={'state': 'location_state', 'city': 'location_city'}, inplace=True)

        # Add timestamps and source reference
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        df['source_customer_id'] = df['customer_id']

        # Define the exact columns that exist in silver_customers table
        silver_columns = [
            'customer_id', 'full_name', 'email', 'phone_cleaned',
            'location_state', 'location_city', 'age', 'age_group', 'occupation_category',
            'income_bucket', 'credit_score', 'credit_score_tier',
            'customer_tenure_days', 'tenure_group', 'segment',
            'risk_score', 'kyc_verified', 'total_accounts',
            'total_balance', 'avg_account_balance', 'last_interaction_date',
            'total_interactions', 'complaint_count', 'has_fraud_history',
            'fraud_report_count', 'total_fraud_loss',
            'created_at', 'updated_at', 'source_customer_id'
        ]

        # Select only those columns
        result_df = df[silver_columns].copy()
        
        print(f"✅ Transformed to {len(result_df)} silver customer records")
        print(f"   Features created: {len(result_df.columns)} columns")
        
        return result_df
    
    def transform_accounts(self):
        """Transform bronze_accounts to silver_accounts"""
        print("\n💰 Transforming accounts...")
        
        # Read from bronze
        query = "SELECT * FROM bronze_accounts"
        df = pd.read_sql(query, engine)
        print(f"   Read {len(df)} accounts from bronze")
        
        # Mask account number (show only last 4 digits)
        df['account_number_masked'] = '****' + df['account_number'].str[-4:]
        
        # Calculate account age
        df['opening_date'] = pd.to_datetime(df['opening_date'])
        today = datetime.now()
        df['account_age_days'] = (today - df['opening_date']).dt.days
        
        # Create age groups
        bins = [0, 30, 90, 365, 1095, float('inf')]
        labels = ['<1 month', '1-3 months', '3-12 months', '1-3 years', '3+ years']
        df['age_group'] = pd.cut(df['account_age_days'], bins=bins, labels=labels)
        
        # Determine if account has overdraft
        df['has_overdraft'] = df['overdraft_limit'] > 0
        
        # Calculate overdraft utilization (if overdraft exists)
        df['overdraft_utilization'] = 0
        mask = (df['has_overdraft']) & (df['overdraft_limit'] > 0)
        df.loc[mask, 'overdraft_utilization'] = (
            (df.loc[mask, 'overdraft_limit'] - df.loc[mask, 'available_balance']) / 
            df.loc[mask, 'overdraft_limit']
        ).clip(0, 1)
        
        # Create interest rate tiers
        bins = [0, 0.5, 1.5, 3.0, float('inf')]
        labels = ['Low', 'Medium', 'High', 'Premium']
        df['interest_rate_tier'] = pd.cut(df['interest_rate'], bins=bins, labels=labels)
        
        # Determine account status boolean
        df['is_active'] = df['status'] == 'Active'
        
        # Get transaction metrics from bronze_transactions
        txn_query = """
            SELECT 
                account_id,
                COUNT(*) as total_transactions_30d,
                MAX(transaction_date) as last_transaction_date,
                AVG(ABS(amount)) as avg_daily_transactions
            FROM bronze_transactions
            WHERE transaction_date > CURRENT_DATE - INTERVAL '30 days'
            GROUP BY account_id
        """
        txn_metrics = pd.read_sql(txn_query, engine)
        
        # Merge transaction metrics
        df = df.merge(txn_metrics, on='account_id', how='left')
        df['last_transaction_date'] = pd.to_datetime(df['last_transaction_date'])
        
        # Calculate days since last transaction
        df['days_since_last_transaction'] = (
            today - df['last_transaction_date']
        ).dt.days.fillna(999)  # 999 for accounts with no transactions
        
        # Get fraud metrics
        fraud_query = """
            SELECT 
                account_id,
                COUNT(*) as fraud_transaction_count,
                SUM(ABS(amount)) as fraud_amount_total
            FROM bronze_transactions
            WHERE is_fraud = true
            GROUP BY account_id
        """
        fraud_metrics = pd.read_sql(fraud_query, engine)
        
        # Merge fraud metrics
        df = df.merge(fraud_metrics, on='account_id', how='left')
        df['fraud_transaction_count'] = df['fraud_transaction_count'].fillna(0).astype(int)
        df['fraud_amount_total'] = df['fraud_amount_total'].fillna(0)
        
        # Fill NaN values
        df['total_transactions_30d'] = df['total_transactions_30d'].fillna(0).astype(int)
        df['avg_daily_transactions'] = df['avg_daily_transactions'].fillna(0)
        
        # Add timestamps
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Rename status to account_status
        df.rename(columns={'status': 'account_status'}, inplace=True)
        
        # Drop the raw account_number column (table only expects account_number_masked)
        df.drop(columns=['account_number'], inplace=True)
        
        # Drop the currency column (not in the silver_accounts table schema)
        df.drop(columns=['currency'], inplace=True)
        
        df.drop(columns=['overdraft_limit'], inplace=True)
        
        df.drop(columns=['interest_rate'], inplace=True)
        
        df.drop(columns=['last_updated'], inplace=True)
        
        df.drop(columns=['loaded_at'], inplace=True)
        
        df.drop(columns=['source_file'], inplace=True)
        
        df.drop(columns=['batch_id'], inplace=True)


        # Set last_activity_date (using last_transaction_date)
        df['last_activity_date'] = df['last_transaction_date']

        # Add timestamps
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        print(f"✅ Transformed to {len(df)} silver account records")
        
        return df
    
    def transform_transactions(self):
        """Transform bronze_transactions to silver_transactions"""
        print("\n💳 Transforming transactions...")
        
        # Read from bronze
        query = "SELECT * FROM bronze_transactions"
        df = pd.read_sql(query, engine)
        print(f"   Read {len(df)} transactions from bronze")
        
        # Rename status to transaction_status
        df.rename(columns={'status': 'transaction_status'}, inplace=True)
        
        # Parse dates
        df['transaction_timestamp'] = pd.to_datetime(df['transaction_date'])
        df['transaction_date'] = df['transaction_timestamp'].dt.date
        df['transaction_time'] = df['transaction_timestamp'].dt.time
        
        # Calculate absolute amount
        df['amount_abs'] = df['amount'].abs()
        
        # Clean merchant name
        df['merchant_name_clean'] = df['merchant_name'].str.strip()
        
        # Determine merchant risk level
        high_risk_keywords = ['casino', 'crypto', 'offshore', 'foreign', 'unknown']
        def get_merchant_risk(merchant):
            merchant = str(merchant).lower()
            if any(keyword in merchant for keyword in high_risk_keywords):
                return 'High'
            return 'Low'
        
        df['merchant_risk_level'] = df['merchant_name'].apply(get_merchant_risk)
        
        # Extract state from merchant location (if available)
        def extract_state(location):
            try:
                return str(location).split(', ')[-1]
            except:
                return None
        
        df['merchant_state'] = df['merchant_location'].apply(extract_state)
        
        # Time-based features
        df['transaction_hour'] = df['transaction_timestamp'].dt.hour
        
        # Hour groups
        bins = [0, 6, 12, 17, 20, 24]
        labels = ['Late Night', 'Morning', 'Afternoon', 'Evening', 'Night']
        df['hour_group'] = pd.cut(df['transaction_hour'], bins=bins, labels=labels, right=False)
        
        # Boolean flags
        df['is_weekend'] = df['transaction_timestamp'].dt.dayofweek >= 5
        df['is_night'] = (df['transaction_hour'] < 6) | (df['transaction_hour'] > 22)
        df['is_business_hours'] = (df['transaction_hour'] >= 9) & (df['transaction_hour'] <= 17) & (~df['is_weekend'])
        
        # Get customer data for enrichment
        customer_query = """
            SELECT customer_id, age, risk_score 
            FROM silver_customers
        """
        customers = pd.read_sql(customer_query, engine)
        
        # Get account data for enrichment
        account_query = """
            SELECT account_id, current_balance 
            FROM silver_accounts
        """
        accounts = pd.read_sql(account_query, engine)
        
        # Merge customer and account data
        df = df.merge(customers, on='customer_id', how='left')
        df = df.merge(accounts, on='account_id', how='left')
        
        # Rename columns to avoid confusion
        df.rename(columns={
            'age': 'customer_age',
            'risk_score': 'customer_risk_score',
            'current_balance': 'account_balance'
        }, inplace=True)
        
        # Add created_at
        df['created_at'] = datetime.now()
        
        # Drop the description column (not in the silver_transactions table schema)
        df.drop(columns=['description'], inplace=True)
        
        # Select columns for silver_transactions
        silver_columns = [
            'transaction_id', 'account_id', 'customer_id',
            'transaction_timestamp', 'transaction_date', 'transaction_time',
            'transaction_type', 'amount', 'amount_abs', 'currency',
            'merchant_name_clean', 'merchant_category', 'merchant_risk_level',
            'merchant_location', 'merchant_state', 'transaction_channel',
            'device_id', 'ip_address', 'transaction_hour', 'hour_group',
            'is_weekend', 'is_night', 'is_business_hours',
            'customer_age', 'customer_risk_score', 'account_balance',
            'is_fraud', 'transaction_status',
            'created_at'
        ]
        
        result_df = df[silver_columns].copy()
        
        
        print(f"✅ Transformed to {len(result_df)} silver transaction records")
        print(f"   Features created: {len(result_df.columns)} columns")
        
        
        return result_df
    
    def load_silver_table(self, df, table_name):
        """Load dataframe into silver table"""
        print(f"\n📤 Loading {len(df)} rows into {table_name}...")
        
        # Clear existing data (replace strategy)
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
            conn.commit()
        
        # Load new data
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✅ Loaded into {table_name}")
    
    def update_customers_with_account_metrics(self):
        """Update silver_customers with actual account data after accounts are loaded"""
        print("\n🔄 Updating customers with account metrics...")
        
        update_query = """
            UPDATE silver_customers sc
            SET 
                total_accounts = sub.account_count,
                total_balance = sub.total_balance,
                avg_account_balance = sub.avg_balance
            FROM (
                SELECT 
                    customer_id,
                    COUNT(*) as account_count,
                    SUM(current_balance) as total_balance,
                    AVG(current_balance) as avg_balance
                FROM silver_accounts
                GROUP BY customer_id
            ) sub
            WHERE sc.customer_id = sub.customer_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(update_query))
            conn.commit()
            print(f"✅ Updated {result.rowcount} customer records with account metrics")
    
    def run_all(self):
        """Run all transformations in correct order"""
        print("=" * 60)
        print(f"🏦 SILVER LAYER TRANSFORMATION - Batch: {self.batch_id}")
        print("=" * 60)
        
        try:
            # Step 1: Transform customers (needs data from interactions and fraud reports)
            print("\n📋 STEP 1: Transforming customers")
            customers_df = self.transform_customers()
            self.load_silver_table(customers_df, 'silver_customers')
            
            # Step 2: Transform accounts (independent)
            print("\n📋 STEP 2: Transforming accounts")
            accounts_df = self.transform_accounts()
            self.load_silver_table(accounts_df, 'silver_accounts')
            
            # Step 3: Update customers with account metrics
            print("\n📋 STEP 3: Updating customers with account data")
            self.update_customers_with_account_metrics()
            
            # Step 4: Transform transactions (needs customer and account data)
            print("\n📋 STEP 4: Transforming transactions")
            transactions_df = self.transform_transactions()
            self.load_silver_table(transactions_df, 'silver_transactions')
            
            # Final verification
            self.verify_silver_layer()
            
            print("\n" + "=" * 60)
            print("✅ SILVER LAYER TRANSFORMATION COMPLETE!")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n❌ Transformation failed: {e}")
            raise
    
    def verify_silver_layer(self):
        """Verify silver layer tables"""
        print("\n🔍 VERIFYING SILVER LAYER")
        print("-" * 40)
        
        tables = ['silver_customers', 'silver_accounts', 'silver_transactions']
        
        with engine.connect() as conn:
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"{table}: {count} rows")
                
                # Show sample columns for each table
                if count > 0:
                    sample = pd.read_sql(f"SELECT * FROM {table} LIMIT 1", engine)
                    print(f"   Columns: {', '.join(sample.columns[:5])}...")

if __name__ == "__main__":
    transformer = SilverTransformer()
    transformer.run_all()