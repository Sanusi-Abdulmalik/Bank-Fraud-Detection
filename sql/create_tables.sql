-- ============================================
-- BANK ANALYTICS DATABASE SCHEMA
-- BRONZE → SILVER → GOLD ARCHITECTURE
-- ============================================

-- First, let's check current state
SELECT 'Current database: ' || current_database() AS info;

-- ============================================
-- BRONZE LAYER: Raw data as it comes from sources
-- ============================================

-- BRONZE: CRM Customers (from CRM system)
CREATE TABLE IF NOT EXISTS bronze_customers (
    -- Original data from CRM
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    date_of_birth DATE,
    occupation VARCHAR(100),
    income NUMERIC(12,2),
    credit_score INTEGER,
    customer_since DATE,
    account_manager VARCHAR(100),
    segment VARCHAR(50),
    risk_level VARCHAR(20),
    kyc_status VARCHAR(20),
    last_updated TIMESTAMP,
    
    -- ETL metadata (our additions)
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(200),
    batch_id VARCHAR(50)
);

-- BRONZE: CRM Interactions (customer service calls, etc.)
CREATE TABLE IF NOT EXISTS bronze_interactions (
    interaction_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    interaction_type VARCHAR(50),
    channel VARCHAR(50),
    agent_id INTEGER,
    date_time TIMESTAMP,
    duration_minutes INTEGER,
    notes TEXT,
    outcome VARCHAR(50),
    satisfaction_score INTEGER,
    
    -- ETL metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(200),
    batch_id VARCHAR(50),
    
    -- Foreign key (not enforced in bronze, but good for reference)
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) 
        REFERENCES bronze_customers(customer_id) ON DELETE CASCADE
);

-- BRONZE: CRM Fraud Reports (customer-reported fraud)
CREATE TABLE IF NOT EXISTS bronze_fraud_reports (
    report_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    report_date DATE,
    report_type VARCHAR(100),
    description TEXT,
    status VARCHAR(50),
    amount_lost NUMERIC(12,2),
    recovery_amount NUMERIC(12,2),
    investigator VARCHAR(100),
    notes TEXT,
    
    -- ETL metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(200),
    batch_id VARCHAR(50),
    
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) 
        REFERENCES bronze_customers(customer_id) ON DELETE CASCADE
);

-- BRONZE: ERP Accounts (bank accounts from core banking system)
CREATE TABLE IF NOT EXISTS bronze_accounts (
    account_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    account_number VARCHAR(50),
    account_type VARCHAR(50),
    opening_date DATE,
    current_balance NUMERIC(12,2),
    available_balance NUMERIC(12,2),
    currency VARCHAR(3),
    status VARCHAR(50),
    overdraft_limit NUMERIC(12,2),
    interest_rate NUMERIC(5,3),
    last_updated TIMESTAMP,
    
    -- ETL metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(200),
    batch_id VARCHAR(50),
    
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) 
        REFERENCES bronze_customers(customer_id) ON DELETE CASCADE
);

-- BRONZE: ERP Transactions (financial transactions - MOST IMPORTANT!)
CREATE TABLE IF NOT EXISTS bronze_transactions (
    transaction_id INTEGER PRIMARY KEY,
    account_id INTEGER,
    customer_id INTEGER,
    transaction_date TIMESTAMP,
    transaction_type VARCHAR(50),
    amount NUMERIC(12,2),
    currency VARCHAR(3),
    merchant_name VARCHAR(200),
    merchant_category VARCHAR(100),
    merchant_location VARCHAR(200),
    transaction_channel VARCHAR(50),
    device_id VARCHAR(100),
    ip_address VARCHAR(50),
    is_fraud BOOLEAN,
    status VARCHAR(50),
    description TEXT,
    
    -- ETL metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(200),
    batch_id VARCHAR(50),
    
    -- Foreign keys
    CONSTRAINT fk_account FOREIGN KEY (account_id) 
        REFERENCES bronze_accounts(account_id) ON DELETE CASCADE,
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) 
        REFERENCES bronze_customers(customer_id) ON DELETE CASCADE
);

-- ============================================
-- SILVER LAYER: Cleaned and standardized data
-- ============================================

-- SILVER: Customers (cleaned and enhanced)
CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id INTEGER PRIMARY KEY,
    
    -- Cleaned personal info
    full_name VARCHAR(200),
    email VARCHAR(200),
    phone_cleaned VARCHAR(20),
    location_state VARCHAR(2),
    location_city VARCHAR(100),
    
    -- Demographic info
    age INTEGER,
    age_group VARCHAR(20),
    occupation_category VARCHAR(50),
    income_bucket VARCHAR(20),
    
    -- Financial profile
    credit_score INTEGER,
    credit_score_tier VARCHAR(20),
    customer_tenure_days INTEGER,
    tenure_group VARCHAR(20),
    
    -- Bank relationship
    segment VARCHAR(50),
    risk_score INTEGER,
    risk_tier VARCHAR(20),
    kyc_verified BOOLEAN,
    
    -- Derived metrics (from other tables)
    total_accounts INTEGER DEFAULT 0,
    total_balance NUMERIC(12,2) DEFAULT 0,
    avg_account_balance NUMERIC(12,2) DEFAULT 0,
    
    -- Interaction metrics
    last_interaction_date DATE,
    total_interactions INTEGER DEFAULT 0,
    complaint_count INTEGER DEFAULT 0,
    
    -- Fraud history
    has_fraud_history BOOLEAN DEFAULT FALSE,
    fraud_report_count INTEGER DEFAULT 0,
    total_fraud_loss NUMERIC(12,2) DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Source reference
    source_customer_id INTEGER
);

-- SILVER: Accounts (cleaned and enhanced)
CREATE TABLE IF NOT EXISTS silver_accounts (
    account_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    
    -- Account info
    account_number_masked VARCHAR(50),
    account_type VARCHAR(50),
    account_subtype VARCHAR(50),
    
    -- Dates
    opening_date DATE,
    account_age_days INTEGER,
    age_group VARCHAR(20),
    last_activity_date DATE,
    
    -- Balances
    current_balance NUMERIC(12,2),
    available_balance NUMERIC(12,2),
    average_balance_30d NUMERIC(12,2),
    balance_trend VARCHAR(20),
    
    -- Limits and rates
    has_overdraft BOOLEAN,
    overdraft_utilization NUMERIC(5,3),
    interest_rate_tier VARCHAR(20),
    
    -- Status
    account_status VARCHAR(50),
    is_active BOOLEAN,
    
    -- Transaction metrics
    avg_daily_transactions NUMERIC(10,2),
    total_transactions_30d INTEGER,
    last_transaction_date DATE,
    days_since_last_transaction INTEGER,
    
    -- Fraud metrics
    fraud_transaction_count INTEGER DEFAULT 0,
    fraud_amount_total NUMERIC(12,2) DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) 
        REFERENCES silver_customers(customer_id) ON DELETE CASCADE
);

-- SILVER: Transactions (cleaned and enhanced)
CREATE TABLE IF NOT EXISTS silver_transactions (
    transaction_id INTEGER PRIMARY KEY,
    account_id INTEGER,
    customer_id INTEGER,
    
    -- Core transaction info
    transaction_timestamp TIMESTAMP,
    transaction_date DATE,
    transaction_time TIME,
    transaction_type VARCHAR(50),
    transaction_subtype VARCHAR(50),
    
    -- Amount info
    amount NUMERIC(12,2),
    amount_abs NUMERIC(12,2),
    currency VARCHAR(3),
    
    -- Merchant info
    merchant_name VARCHAR(200),
    merchant_name_clean VARCHAR(200),
    merchant_category VARCHAR(100),
    merchant_risk_level VARCHAR(20),
    
    -- Location info
    merchant_location VARCHAR(200),
    merchant_state VARCHAR(2),
    merchant_country VARCHAR(2),
    
    -- Channel info
    transaction_channel VARCHAR(50),
    device_id VARCHAR(100),
    ip_address VARCHAR(50),
    
    -- Time features
    transaction_hour INTEGER,
    hour_group VARCHAR(20),
    is_weekend BOOLEAN,
    is_night BOOLEAN,
    is_holiday BOOLEAN,
    is_business_hours BOOLEAN,
    
    -- Customer context at time of transaction
    customer_age INTEGER,
    customer_risk_score INTEGER,
    account_balance NUMERIC(12,2),
    
    -- Fraud info
    is_fraud BOOLEAN,
    fraud_type VARCHAR(50),
    
    -- Status
    transaction_status VARCHAR(50),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign keys
    CONSTRAINT fk_account FOREIGN KEY (account_id) 
        REFERENCES silver_accounts(account_id) ON DELETE CASCADE,
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) 
        REFERENCES silver_customers(customer_id) ON DELETE CASCADE
);

-- ============================================
-- GOLD LAYER: Analytics-ready data
-- ============================================

-- GOLD: Customer 360 View (complete customer profile)
CREATE TABLE IF NOT EXISTS gold_customer_360 (
    customer_id INTEGER PRIMARY KEY,
    
    -- Personal info
    full_name VARCHAR(200),
    demographic_segment VARCHAR(50),
    location_tier VARCHAR(20),
    
    -- Financial profile
    total_relationship_balance NUMERIC(12,2),
    avg_account_balance NUMERIC(12,2),
    credit_score_tier VARCHAR(20),
    income_bucket VARCHAR(20),
    
    -- Activity metrics
    transaction_count_30d INTEGER,
    total_spent_30d NUMERIC(12,2),
    avg_transaction_value NUMERIC(10,2),
    
    -- Risk metrics
    overall_risk_score INTEGER,
    risk_tier VARCHAR(20),
    fraud_probability NUMERIC(5,4),
    
    -- Interaction metrics
    last_support_call DATE,
    complaint_count_90d INTEGER,
    satisfaction_score_avg NUMERIC(3,2),
    
    -- Derived flags
    is_high_value BOOLEAN,
    is_at_risk BOOLEAN,
    needs_kyc_review BOOLEAN,
    
    -- Timestamps
    snapshot_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- GOLD: Fraud Features (ready for machine learning)
CREATE TABLE IF NOT EXISTS gold_fraud_features (
    transaction_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    account_id INTEGER,
    
    -- Transaction details
    transaction_timestamp TIMESTAMP,
    amount NUMERIC(12,2),
    amount_category VARCHAR(20),
    
    -- Customer features
    customer_age INTEGER,
    customer_tenure_days INTEGER,
    customer_risk_score INTEGER,
    customer_avg_daily_spend NUMERIC(12,2),
    customer_transaction_frequency NUMERIC(10,2),
    
    -- Account features
    account_age_days INTEGER,
    account_balance NUMERIC(12,2),
    balance_change_24h NUMERIC(12,2),
    account_transaction_count_7d INTEGER,
    
    -- Merchant features
    merchant_category VARCHAR(100),
    merchant_risk_score INTEGER,
    is_new_merchant_for_customer BOOLEAN,
    
    -- Time features
    transaction_hour INTEGER,
    is_night_time BOOLEAN,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    days_since_last_transaction INTEGER,
    hour_of_day_sin NUMERIC(10,8),
    hour_of_day_cos NUMERIC(10,8),
    
    -- Behavioral features
    transaction_count_1h INTEGER,
    transaction_count_24h INTEGER,
    transaction_amount_24h NUMERIC(12,2),
    unique_merchants_24h INTEGER,
    
    -- Location/device features
    is_new_device BOOLEAN,
    is_new_location BOOLEAN,
    distance_from_home_km NUMERIC(10,2),
    velocity_kmh NUMERIC(10,2),
    
    -- Statistical features
    amount_z_score NUMERIC(10,4),
    time_since_avg NUMERIC(10,2),
    amount_deviation_from_avg NUMERIC(12,2),
    
    -- Target variable
    is_fraud BOOLEAN,
    fraud_probability NUMERIC(5,4),
    fraud_risk_tier VARCHAR(20),
    
    -- Model metadata
    model_version VARCHAR(50),
    prediction_timestamp TIMESTAMP,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign keys
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) 
        REFERENCES gold_customer_360(customer_id) ON DELETE CASCADE,
    CONSTRAINT fk_account FOREIGN KEY (account_id) 
        REFERENCES silver_accounts(account_id) ON DELETE CASCADE
);

-- GOLD: Daily Fraud Summary (for reporting)
CREATE TABLE IF NOT EXISTS gold_daily_fraud_summary (
    summary_date DATE PRIMARY KEY,
    
    -- Transaction counts
    total_transactions INTEGER,
    fraudulent_transactions INTEGER,
    fraud_rate NUMERIC(5,4),
    
    -- Amount totals
    total_amount NUMERIC(15,2),
    fraud_amount NUMERIC(15,2),
    avg_fraud_amount NUMERIC(12,2),
    
    -- By category
    fraud_by_channel JSONB,
    fraud_by_merchant_category JSONB,
    fraud_by_hour JSONB,
    
    -- Customer impact
    affected_customers INTEGER,
    repeat_victims INTEGER,
    
    -- Detection metrics
    auto_detected_count INTEGER,
    reported_count INTEGER,
    detection_lag_hours NUMERIC(10,2),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES for Performance
-- ============================================

-- Bronze layer indexes
CREATE INDEX IF NOT EXISTS idx_bronze_customers_loaded ON bronze_customers(loaded_at);
CREATE INDEX IF NOT EXISTS idx_bronze_transactions_date ON bronze_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_bronze_transactions_customer ON bronze_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_bronze_transactions_fraud ON bronze_transactions(is_fraud);

-- Silver layer indexes
CREATE INDEX IF NOT EXISTS idx_silver_customers_risk ON silver_customers(risk_score);
CREATE INDEX IF NOT EXISTS idx_silver_transactions_timestamp ON silver_transactions(transaction_timestamp);
CREATE INDEX IF NOT EXISTS idx_silver_transactions_customer_date ON silver_transactions(customer_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_silver_transactions_fraud ON silver_transactions(is_fraud);

-- Gold layer indexes (for analytics)
CREATE INDEX IF NOT EXISTS idx_gold_fraud_features_fraud ON gold_fraud_features(is_fraud);
CREATE INDEX IF NOT EXISTS idx_gold_fraud_features_timestamp ON gold_fraud_features(transaction_timestamp);
CREATE INDEX IF NOT EXISTS idx_gold_fraud_features_risk ON gold_fraud_features(fraud_risk_tier);
CREATE INDEX IF NOT EXISTS idx_gold_customer_360_risk ON gold_customer_360(risk_tier);

-- ============================================
-- VIEWS for easier querying
-- ============================================

-- View: Current fraud alerts
CREATE OR REPLACE VIEW vw_current_fraud_alerts AS
SELECT 
    f.transaction_id,
    f.transaction_timestamp,
    c.full_name,
    a.account_number_masked,
    f.amount,
    f.merchant_category,
    f.fraud_risk_tier,
    f.fraud_probability,
    CASE 
        WHEN f.fraud_probability > 0.9 THEN 'CRITICAL'
        WHEN f.fraud_probability > 0.7 THEN 'HIGH'
        WHEN f.fraud_probability > 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END as alert_level
FROM gold_fraud_features f
JOIN gold_customer_360 c ON f.customer_id = c.customer_id
JOIN silver_accounts a ON f.account_id = a.account_id
WHERE f.is_fraud = true
    AND f.transaction_timestamp > CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY f.fraud_probability DESC;

-- View: Customer risk dashboard
CREATE OR REPLACE VIEW vw_customer_risk_dashboard AS
SELECT 
    c.customer_id,
    c.full_name,
    c.demographic_segment,
    c.total_relationship_balance,
    c.overall_risk_score,
    c.risk_tier,
    c.fraud_probability,
    COUNT(DISTINCT a.account_id) as account_count,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) as fraud_count_90d,
    MAX(f.transaction_timestamp) as last_transaction_date
FROM gold_customer_360 c
LEFT JOIN silver_accounts a ON c.customer_id = a.customer_id
LEFT JOIN silver_transactions f ON c.customer_id = f.customer_id 
    AND f.transaction_timestamp > CURRENT_TIMESTAMP - INTERVAL '90 days'
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- ============================================
-- REPORT: Schema summary
-- ============================================

DO $$
DECLARE
    table_count INTEGER;
    total_columns INTEGER;
BEGIN
    -- Count tables
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE';
    
    -- Count columns
    SELECT COUNT(*) INTO total_columns
    FROM information_schema.columns 
    WHERE table_schema = 'public';
    
    RAISE NOTICE '=========================================';
    RAISE NOTICE 'SCHEMA CREATION COMPLETE';
    RAISE NOTICE '=========================================';
    RAISE NOTICE 'Tables created: %', table_count;
    RAISE NOTICE 'Total columns: %', total_columns;
    RAISE NOTICE '=========================================';
    RAISE NOTICE 'BRONZE LAYER: 5 tables (raw data)';
    RAISE NOTICE 'SILVER LAYER: 3 tables (cleaned data)';
    RAISE NOTICE 'GOLD LAYER: 3 tables (analytics-ready)';
    RAISE NOTICE 'VIEWS: 2 views (for reporting)';
    RAISE NOTICE '=========================================';
END $$;