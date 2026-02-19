"""
Transform silver layer data into gold layer analytics tables.
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

class GoldTransformer:
    def __init__(self):
        self.batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.today = datetime.now().date()

    def build_customer_360(self):
        """Create gold_customer_360: complete customer profile."""
        print("\n👥 Building gold_customer_360...")

        # Read silver customers
        query = "SELECT * FROM silver_customers"
        customers = pd.read_sql(query, engine)
        print(f"   Read {len(customers)} customers from silver")

        # Read transaction aggregates from silver_transactions
        txn_agg_query = """
            SELECT 
                customer_id,
                COUNT(*) as transaction_count_30d,
                SUM(CASE WHEN amount < 0 THEN amount_abs ELSE 0 END) as total_spent_30d,
                AVG(amount_abs) as avg_transaction_value,
                MAX(transaction_timestamp) as last_transaction_date
            FROM silver_transactions
            WHERE transaction_timestamp > CURRENT_DATE - INTERVAL '30 days'
            GROUP BY customer_id
        """
        txn_agg = pd.read_sql(txn_agg_query, engine)

        # Read fraud history from silver_transactions
        fraud_agg_query = """
            SELECT 
                customer_id,
                COUNT(*) as fraud_count_90d
            FROM silver_transactions
            WHERE is_fraud = true
              AND transaction_timestamp > CURRENT_DATE - INTERVAL '90 days'
            GROUP BY customer_id
        """
        fraud_agg = pd.read_sql(fraud_agg_query, engine)

        # Merge everything into customers
        df = customers.merge(txn_agg, on='customer_id', how='left')
        df = df.merge(fraud_agg, on='customer_id', how='left')

        # Fill NaN
        df['transaction_count_30d'] = df['transaction_count_30d'].fillna(0).astype(int)
        df['total_spent_30d'] = df['total_spent_30d'].fillna(0)
        df['avg_transaction_value'] = df['avg_transaction_value'].fillna(0)
        df['fraud_count_90d'] = df['fraud_count_90d'].fillna(0).astype(int)

        # Compute derived flags
        df['is_high_value'] = df['total_balance'] > 50000
        df['is_at_risk'] = (df['risk_score'] >= 2) & (df['fraud_count_90d'] > 0)
        df['needs_kyc_review'] = ~df['kyc_verified']

        # Map fields to gold_customer_360 column names (as defined in create_tables.sql)
        gold_df = pd.DataFrame({
            'customer_id': df['customer_id'],
            'full_name': df['full_name'],
            'demographic_segment': df['segment'],
            'location_tier': df['location_state'],  # simplified, could be region mapping
            'total_relationship_balance': df['total_balance'],
            'avg_account_balance': df['avg_account_balance'],
            'credit_score_tier': df['credit_score_tier'],
            'income_bucket': df['income_bucket'],
            'transaction_count_30d': df['transaction_count_30d'],
            'total_spent_30d': df['total_spent_30d'],
            'avg_transaction_value': df['avg_transaction_value'],
            'overall_risk_score': df['risk_score'],
            'risk_tier': df['tenure_group'],   # placeholder, could be more sophisticated
            'fraud_probability': df['fraud_report_count'] / 10.0,  # dummy value
            'last_support_call': df['last_interaction_date'],
            'complaint_count_90d': df['complaint_count'],
            'satisfaction_score_avg': np.nan,  # not available, keep as None
            'is_high_value': df['is_high_value'],
            'is_at_risk': df['is_at_risk'],
            'needs_kyc_review': df['needs_kyc_review'],
            'snapshot_date': self.today,
            'created_at': datetime.now()
        })

        print(f"✅ gold_customer_360 built with {len(gold_df)} rows, {len(gold_df.columns)} columns")
        return gold_df

    def build_fraud_features(self):
        """
        Create gold_fraud_features: transaction-level ML features.
        This is the most important table for fraud detection.
        """
        print("\n🚨 Building gold_fraud_features...")

        # Read transactions with all necessary joins
        query = """
            SELECT 
                t.transaction_id,
                t.account_id,
                t.customer_id,
                t.transaction_timestamp,
                t.amount,
                t.amount_abs,
                t.merchant_category,
                t.transaction_channel,
                t.is_fraud,
                t.customer_age,
                t.customer_risk_score,
                t.account_balance,
                t.transaction_hour,
                t.is_weekend,
                t.is_night,
                t.is_business_hours,
                t.hour_group,
                c.income_bucket,
                c.credit_score_tier,
                c.segment,
                a.account_type,
                a.account_age_days
            FROM silver_transactions t
            LEFT JOIN silver_customers c ON t.customer_id = c.customer_id
            LEFT JOIN silver_accounts a ON t.account_id = a.account_id
        """
        df = pd.read_sql(query, engine)
        print(f"   Read {len(df)} transactions with joined customer/account data")

        # Sort by customer and timestamp to compute customer-level features
        df = df.sort_values(['customer_id', 'transaction_timestamp'])

        # --- Feature engineering ---
        # 1. Amount Z‑score per customer (how unusual is this amount)
        df['amount_z_score'] = df.groupby('customer_id')['amount_abs'].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0
        )

        # 2. Time since last transaction (in hours)
        df['prev_transaction'] = df.groupby('customer_id')['transaction_timestamp'].shift(1)
        df['hours_since_last'] = (
            (df['transaction_timestamp'] - df['prev_transaction']).dt.total_seconds() / 3600
        ).fillna(24)  # assume 24h if no previous

        # 3. Rolling counts: transactions in last 1h, 24h, same merchant etc.
        #    For simplicity, we'll use the already computed fields from silver
        #    But we can add more if needed. For now, we'll include:
        df['is_new_merchant_category'] = ~df.groupby('customer_id')['merchant_category'].apply(
            lambda x: x.duplicated()
        ).values

        # 4. Distance from home – not available, placeholder
        df['distance_from_home_km'] = np.random.uniform(0, 100, size=len(df))

        # 5. Velocity – also placeholder
        df['velocity_kmh'] = np.random.uniform(0, 120, size=len(df))

        # 6. Time features as sin/cos
        df['hour_of_day_sin'] = np.sin(2 * np.pi * df['transaction_hour'] / 24)
        df['hour_of_day_cos'] = np.cos(2 * np.pi * df['transaction_hour'] / 24)

        # 7. Amount deviation from average
        df['amount_deviation_from_avg'] = df['amount_abs'] - df.groupby('customer_id')['amount_abs'].transform('mean')

        # 8. Risk tier (derived from risk score)
        def risk_tier(score):
            if score >= 2.5:
                return 'HIGH'
            elif score >= 1.5:
                return 'MEDIUM'
            else:
                return 'LOW'
        df['fraud_risk_tier'] = df['customer_risk_score'].apply(risk_tier)

        # Map columns to gold_fraud_features schema
        gold_df = pd.DataFrame({
            'transaction_id': df['transaction_id'],
            'customer_id': df['customer_id'],
            'account_id': df['account_id'],
            'transaction_timestamp': df['transaction_timestamp'],
            'amount': df['amount'],
            'amount_category': pd.cut(df['amount_abs'], bins=[0, 50, 200, 1000, 10000], labels=['tiny', 'small', 'medium', 'large']),
            'customer_age': df['customer_age'],
            'customer_tenure_days': df['account_age_days'],  # using account age as proxy
            'customer_risk_score': df['customer_risk_score'],
            'customer_avg_daily_spend': df.groupby('customer_id')['amount_abs'].transform('mean') / 30,  # dummy
            'customer_transaction_frequency': df.groupby('customer_id')['amount_abs'].transform('count') / 30,
            'account_age_days': df['account_age_days'],
            'account_balance': df['account_balance'],
            'balance_change_24h': 0,  # not implemented
            'account_transaction_count_7d': 0,  # not implemented
            'merchant_category': df['merchant_category'],
            'merchant_risk_score': np.random.randint(1, 10, size=len(df)),  # dummy
            'is_new_merchant_for_customer': df['is_new_merchant_category'],
            'transaction_hour': df['transaction_hour'],
            'is_night_time': df['is_night'],
            'is_weekend': df['is_weekend'],
            'is_holiday': False,  # not implemented
            'days_since_last_transaction': (df['hours_since_last'] / 24).fillna(1),
            'hour_of_day_sin': df['hour_of_day_sin'],
            'hour_of_day_cos': df['hour_of_day_cos'],
            'transaction_count_1h': 0,  # would need window function
            'transaction_count_24h': 0,
            'transaction_amount_24h': 0,
            'unique_merchants_24h': 0,
            'is_new_device': False,  # not available
            'is_new_location': False,
            'distance_from_home_km': df['distance_from_home_km'],
            'velocity_kmh': df['velocity_kmh'],
            'amount_z_score': df['amount_z_score'],
            'time_since_avg': df['hours_since_last'] / df.groupby('customer_id')['hours_since_last'].transform('mean'),
            'amount_deviation_from_avg': df['amount_deviation_from_avg'],
            'is_fraud': df['is_fraud'],
            'fraud_probability': np.where(df['is_fraud'], 1.0, 0.0),  # ground truth
            'fraud_risk_tier': df['fraud_risk_tier'],
            'model_version': 'v1.0',
            'prediction_timestamp': datetime.now(),
            'created_at': datetime.now()
        })

        print(f"✅ gold_fraud_features built with {len(gold_df)} rows, {len(gold_df.columns)} columns")
        return gold_df

    def build_daily_fraud_summary(self):
        """Create gold_daily_fraud_summary: daily KPIs."""
        print("\n📊 Building gold_daily_fraud_summary...")

        query = """
            SELECT 
                DATE(transaction_timestamp) as txn_date,
                COUNT(*) as total_transactions,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraudulent_transactions,
                SUM(amount_abs) as total_amount,
                SUM(CASE WHEN is_fraud THEN amount_abs ELSE 0 END) as fraud_amount,
                COUNT(DISTINCT customer_id) as affected_customers
            FROM silver_transactions
            GROUP BY DATE(transaction_timestamp)
            ORDER BY txn_date
        """
        daily = pd.read_sql(query, engine)

        if daily.empty:
            print("   No daily data found.")
            return pd.DataFrame()

        # Compute derived metrics
        daily['fraud_rate'] = daily['fraudulent_transactions'] / daily['total_transactions']
        daily['avg_fraud_amount'] = daily['fraud_amount'] / daily['fraudulent_transactions'].replace(0, np.nan)

        # For simplicity, set other JSON columns as empty
        daily['fraud_by_channel'] = '{}'
        daily['fraud_by_merchant_category'] = '{}'
        daily['fraud_by_hour'] = '{}'

        # Repeat victims not computed here

        gold_df = pd.DataFrame({
            'summary_date': daily['txn_date'],
            'total_transactions': daily['total_transactions'],
            'fraudulent_transactions': daily['fraudulent_transactions'],
            'fraud_rate': daily['fraud_rate'],
            'total_amount': daily['total_amount'],
            'fraud_amount': daily['fraud_amount'],
            'avg_fraud_amount': daily['avg_fraud_amount'],
            'fraud_by_channel': daily['fraud_by_channel'],
            'fraud_by_merchant_category': daily['fraud_by_merchant_category'],
            'fraud_by_hour': daily['fraud_by_hour'],
            'affected_customers': daily['affected_customers'],
            'repeat_victims': 0,
            'auto_detected_count': daily['fraudulent_transactions'] // 2,  # dummy
            'reported_count': daily['fraudulent_transactions'] // 2,
            'detection_lag_hours': 0,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        })

        print(f"✅ gold_daily_fraud_summary built with {len(gold_df)} rows")
        return gold_df

    def load_gold_table(self, df, table_name):
        """Load dataframe into gold table (replace strategy)."""
        if df.empty:
            print(f"   ⚠️ No data to load into {table_name}")
            return

        print(f"\n📤 Loading {len(df)} rows into {table_name}...")
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
            conn.commit()
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✅ Loaded into {table_name}")

    def verify_gold_layer(self):
        """Quick verification of gold tables."""
        print("\n🔍 VERIFYING GOLD LAYER")
        print("-" * 40)
        tables = ['gold_customer_360', 'gold_fraud_features', 'gold_daily_fraud_summary']
        with engine.connect() as conn:
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"{table}: {count} rows")
                if count > 0:
                    # Show sample columns
                    sample = pd.read_sql(f"SELECT * FROM {table} LIMIT 1", engine)
                    print(f"   Columns: {', '.join(sample.columns[:5])}...")

    def run_all(self):
        """Execute all gold transformations in order."""
        print("=" * 60)
        print(f"🏦 GOLD LAYER TRANSFORMATION - Batch: {self.batch_id}")
        print("=" * 60)

        try:
            # Step 1: Customer 360
            print("\n📋 STEP 1: Building gold_customer_360")
            cust_df = self.build_customer_360()
            self.load_gold_table(cust_df, 'gold_customer_360')

            # Step 2: Fraud features
            print("\n📋 STEP 2: Building gold_fraud_features")
            fraud_df = self.build_fraud_features()
            self.load_gold_table(fraud_df, 'gold_fraud_features')

            # Step 3: Daily summary
            print("\n📋 STEP 3: Building gold_daily_fraud_summary")
            daily_df = self.build_daily_fraud_summary()
            self.load_gold_table(daily_df, 'gold_daily_fraud_summary')

            # Final verification
            self.verify_gold_layer()

            print("\n" + "=" * 60)
            print("✅ GOLD LAYER TRANSFORMATION COMPLETE!")
            print("=" * 60)

        except Exception as e:
            print(f"\n❌ Gold layer transformation failed: {e}")
            raise

if __name__ == "__main__":
    transformer = GoldTransformer()
    transformer.run_all()