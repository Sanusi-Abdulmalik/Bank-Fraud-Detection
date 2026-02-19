"""
Stage 2: Create Sample Bank Data
This script creates realistic CRM and ERP files for our fraud detection project
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

# Create a Faker instance to generate fake data
fake = Faker()

def create_folder_structure():
    """Make sure all our data folders exist"""
    folders = ['data/raw', 'data/bronze', 'data/silver', 'data/gold']
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
    print("📁 Created all data folders")

def create_crm_customers(num_customers=500):
    """
    Create CRM Customers file
    This is like what you'd get from Salesforce or a bank's CRM system
    """
    print("👥 Creating CRM Customers data...")
    
    customers = []
    
    for customer_id in range(1, num_customers + 1):
        # Create a realistic customer profile
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@example.com"
        
        # Income follows a realistic distribution
        income = int(np.random.lognormal(mean=10.5, sigma=0.6))
        if income > 200000:
            income = random.randint(180000, 250000)
        
        # Credit score (most people have average scores)
        credit_score = int(np.random.normal(loc=700, scale=50))
        credit_score = max(300, min(850, credit_score))  # Keep within valid range
        
        customers.append({
            'customer_id': customer_id,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'occupation': fake.job(),
            'income': income,
            'credit_score': credit_score,
            'customer_since': fake.date_between(start_date='-5y', end_date='today'),
            'account_manager': fake.name(),
            'segment': random.choice(['Retail', 'Wealth', 'Business', 'Corporate', 'Student']),
            'risk_level': random.choice(['Low', 'Medium', 'High']),
            'kyc_status': random.choice(['Verified', 'Pending', 'Expired', 'Verified', 'Verified']),  # More verified
            'last_updated': datetime.now()
        })
    
    df = pd.DataFrame(customers)
    
    # Save to CSV
    df.to_csv('data/raw/crm_customers.csv', index=False)
    print(f"✅ Created {len(df)} customer records")
    
    # Show a sample
    print("\n📋 Sample customer data:")
    print(df[['customer_id', 'first_name', 'last_name', 'income', 'credit_score', 'segment']].head(3))
    
    return df

def create_crm_interactions(customers_df, num_interactions=2000):
    """
    Create CRM Interactions file
    This tracks customer calls, emails, support tickets, etc.
    """
    print("\n📞 Creating CRM Interactions data...")
    
    interactions = []
    
    # Create more interactions for high-risk customers
    high_risk_customers = customers_df[customers_df['risk_level'] == 'High']['customer_id'].tolist()
    
    for interaction_id in range(1, num_interactions + 1):
        # High-risk customers have more interactions
        if random.random() < 0.3 and high_risk_customers:
            customer_id = random.choice(high_risk_customers)
        else:
            customer_id = random.randint(1, len(customers_df))
        
        interaction_date = fake.date_time_between(start_date='-180d', end_date='now')
        
        interactions.append({
            'interaction_id': interaction_id,
            'customer_id': customer_id,
            'interaction_type': random.choice([
                'support_call', 'complaint', 'product_inquiry', 
                'KYC_verification', 'account_issue', 'technical_support'
            ]),
            'channel': random.choice(['Phone', 'Email', 'Branch', 'Mobile App', 'Website', 'Chat']),
            'agent_id': random.randint(1000, 1100),
            'date_time': interaction_date,
            'duration_minutes': random.randint(1, 45),
            'notes': fake.sentence(nb_words=10),
            'outcome': random.choice(['Resolved', 'Escalated', 'Pending', 'Referred', 'Follow-up Required']),
            'satisfaction_score': random.randint(1, 5) if random.random() > 0.3 else None
        })
    
    df = pd.DataFrame(interactions)
    df.to_csv('data/raw/crm_interactions.csv', index=False)
    print(f"✅ Created {len(df)} interaction records")
    
    return df

def create_crm_fraud_reports(customers_df, num_reports=100):
    """
    Create Fraud Reports file
    These are fraud cases reported by customers
    """
    print("\n🚨 Creating CRM Fraud Reports data...")
    
    fraud_reports = []
    
    # Some customers are more likely to report fraud
    fraud_prone_customers = customers_df[
        (customers_df['risk_level'] == 'High') | 
        (customers_df['segment'].isin(['Wealth', 'Corporate']))
    ]['customer_id'].tolist()
    
    for report_id in range(1, num_reports + 1):
        # Fraud-prone customers report more
        if random.random() < 0.4 and fraud_prone_customers:
            customer_id = random.choice(fraud_prone_customers)
        else:
            customer_id = random.randint(1, len(customers_df))
        
        report_date = fake.date_between(start_date='-90d', end_date='today')
        
        fraud_reports.append({
            'report_id': report_id,
            'customer_id': customer_id,
            'report_date': report_date,
            'report_type': random.choice([
                'Stolen Card', 'Unauthorized Transaction', 'Identity Theft', 
                'Phishing', 'Account Takeover', 'Card Skimming', 'Online Fraud'
            ]),
            'description': fake.paragraph(nb_sentences=2),
            'status': random.choice(['Open', 'Investigating', 'Closed', 'Resolved']),
            'amount_lost': random.randint(100, 10000) if random.random() > 0.3 else 0,
            'recovery_amount': random.randint(0, 5000) if random.random() > 0.7 else 0,
            'investigator': fake.name(),
            'notes': fake.paragraph(nb_sentences=1)
        })
    
    df = pd.DataFrame(fraud_reports)
    df.to_csv('data/raw/crm_fraud_reports.csv', index=False)
    print(f"✅ Created {len(df)} fraud report records")
    
    # Show fraud statistics
    print(f"   - Average amount lost: ${df['amount_lost'].mean():.2f}")
    print(f"   - Total reports by type:")
    print(df['report_type'].value_counts().head(3))
    
    return df

def create_erp_accounts(customers_df):
    """
    Create ERP Accounts file
    This is the core banking data - customer accounts
    """
    print("\n💰 Creating ERP Accounts data...")
    
    accounts = []
    account_id = 1
    
    for _, customer in customers_df.iterrows():
        customer_id = customer['customer_id']
        
        # Each customer gets 1-3 accounts based on their segment
        if customer['segment'] == 'Wealth':
            num_accounts = random.randint(2, 5)
        elif customer['segment'] == 'Business':
            num_accounts = random.randint(1, 3)
        else:
            num_accounts = random.randint(1, 2)
        
        for _ in range(num_accounts):
            account_type = random.choice(['Checking', 'Savings', 'Money Market', 'Certificate of Deposit'])
            
            # Balance based on income and account type
            if customer['segment'] == 'Wealth':
                balance = random.randint(5000, 500000)
            elif customer['segment'] == 'Business':
                balance = random.randint(1000, 100000)
            else:
                balance = random.randint(100, 50000)
            
            accounts.append({
                'account_id': account_id,
                'customer_id': customer_id,
                'account_number': fake.bban(),  # Basic Bank Account Number
                'account_type': account_type,
                'opening_date': fake.date_between(
                    start_date=customer['customer_since'], 
                    end_date='today'
                ),
                'current_balance': balance,
                'available_balance': balance - random.randint(0, 500),
                'currency': 'USD',
                'status': random.choice(['Active', 'Active', 'Active', 'Dormant', 'Restricted']),  # Mostly active
                'overdraft_limit': random.randint(0, 5000) if account_type == 'Checking' else 0,
                'interest_rate': round(random.uniform(0.01, 4.5), 3),
                'last_updated': datetime.now()
            })
            account_id += 1
    
    df = pd.DataFrame(accounts)
    df.to_csv('data/raw/erp_accounts.csv', index=False)
    print(f"✅ Created {len(df)} account records")
    print(f"   - Average balance: ${df['current_balance'].mean():.2f}")
    print(f"   - Account types: {df['account_type'].value_counts().to_dict()}")
    
    return df

def create_erp_transactions(accounts_df, num_transactions=10000):
    """
    Create ERP Transactions file
    This is the HEART of our data - all financial transactions
    """
    print("\n💳 Creating ERP Transactions data...")
    
    transactions = []
    transaction_id = 1
    
    # Convert to list for faster access
    accounts_list = accounts_df.to_dict('records')
    
    # Create some patterns for fraud
    high_risk_merchants = ['International Casino', 'Crypto Exchange', 'Offshore Merchant', 
                          'Unregistered Online Store', 'Foreign Travel Agency']
    
    normal_merchants = ['Walmart', 'Amazon', 'Starbucks', 'Netflix', 'Uber', 
                       'Apple Store', 'Exxon', 'Target', 'Costco', 'Home Depot']
    
    for i in range(num_transactions):
        # Pick a random account
        account = random.choice(accounts_list)
        account_id = account['account_id']
        customer_id = account['customer_id']
        
        # Decide if this transaction is fraudulent (1% fraud rate)
        is_fraud = random.random() < 0.01
        
        # Generate transaction date (last 30 days, but some patterns)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        
        if is_fraud:
            # Fraud transactions often happen at unusual times
            hour = random.choice([0, 1, 2, 3, 4, 23])  # Middle of night
            merchant = random.choice(high_risk_merchants)
            amount = random.randint(500, 10000)  # Larger amounts for fraud
            transaction_type = random.choice(['Online Payment', 'POS Purchase', 'Wire Transfer'])
        else:
            # Normal transactions
            hour = random.randint(6, 22)  # Normal business hours
            merchant = random.choice(normal_merchants)
            amount = random.randint(10, 500)
            transaction_type = random.choice(['ATM Withdrawal', 'POS Purchase', 'Online Payment', 
                                            'Transfer', 'Deposit', 'Bill Payment'])
        
        # Create transaction date (mostly recent)
        days_ago = random.randint(0, 30)
        if random.random() < 0.2:  # 20% are older
            days_ago = random.randint(31, 90)
        
        transaction_date = datetime.now() - timedelta(days=days_ago, hours=hour, minutes=minute)
        
        # For withdrawals/expenses, make amount negative
        if transaction_type in ['ATM Withdrawal', 'POS Purchase', 'Online Payment', 'Bill Payment']:
            amount = -abs(amount)
        
        # Create merchant category based on merchant name
        merchant_categories = {
            'Walmart': 'Retail', 'Target': 'Retail', 'Costco': 'Retail',
            'Amazon': 'Online Retail', 'Netflix': 'Entertainment',
            'Starbucks': 'Food & Beverage', 'Uber': 'Transportation',
            'Exxon': 'Fuel', 'Apple Store': 'Electronics',
            'International Casino': 'Gambling', 'Crypto Exchange': 'Cryptocurrency'
        }
        
        merchant_category = merchant_categories.get(merchant, 'Other')
        
        transactions.append({
            'transaction_id': transaction_id,
            'account_id': account_id,
            'customer_id': customer_id,
            'transaction_date': transaction_date,
            'transaction_type': transaction_type,
            'amount': amount,
            'currency': 'USD',
            'merchant_name': merchant,
            'merchant_category': merchant_category,
            'merchant_location': fake.city() + ', ' + fake.state_abbr(),
            'transaction_channel': random.choice(['Branch', 'ATM', 'Online', 'Mobile', 'POS Terminal']),
            'device_id': fake.uuid4() if random.random() > 0.3 else None,
            'ip_address': fake.ipv4() if random.random() > 0.5 else None,
            'is_fraud': is_fraud,
            'status': random.choice(['Completed', 'Completed', 'Completed', 'Pending', 'Failed']),
            'description': fake.sentence(nb_words=6)
        })
        
        transaction_id += 1
    
    df = pd.DataFrame(transactions)
    df.to_csv('data/raw/erp_transactions.csv', index=False)
    
    # Calculate some statistics
    fraud_count = df['is_fraud'].sum()
    fraud_rate = fraud_count / len(df) * 100
    
    print(f"✅ Created {len(df)} transaction records")
    print(f"   - Fraudulent transactions: {fraud_count} ({fraud_rate:.2f}%)")
    print(f"   - Total amount: ${df['amount'].sum():,.2f}")
    print(f"   - Average transaction: ${df['amount'].abs().mean():.2f}")
    
    # Show sample of fraud transactions
    fraud_samples = df[df['is_fraud'] == True].head(3)
    if not fraud_samples.empty:
        print("\n🔍 Sample fraud transactions:")
        for _, row in fraud_samples.iterrows():
            print(f"   - ${abs(row['amount']):,.2f} at {row['merchant_name']} on {row['transaction_date'].strftime('%Y-%m-%d %H:%M')}")
    
    return df

def main():
    """
    Main function to run all data creation steps
    """
    print("=" * 60)
    print("🏦 BANK DATA GENERATOR - STAGE 2")
    print("=" * 60)
    
    # Step 1: Create folders
    create_folder_structure()
    
    # Step 2: Create CRM data
    customers_df = create_crm_customers(500)  # 500 customers
    interactions_df = create_crm_interactions(customers_df, 2000)  # 2000 interactions
    fraud_reports_df = create_crm_fraud_reports(customers_df, 100)  # 100 fraud reports
    
    # Step 3: Create ERP data
    accounts_df = create_erp_accounts(customers_df)
    transactions_df = create_erp_transactions(accounts_df, 10000)  # 10,000 transactions
    
    # Step 4: Summary report
    print("\n" + "=" * 60)
    print("📊 DATA GENERATION COMPLETE - SUMMARY")
    print("=" * 60)
    
    total_size = 0
    for filename in os.listdir('data/raw'):
        if filename.endswith('.csv'):
            filepath = os.path.join('data/raw', filename)
            size = os.path.getsize(filepath) / 1024  # KB
            total_size += size
            print(f"📄 {filename}: {size:.1f} KB")
    
    print(f"\n📦 Total data generated: {total_size:.1f} KB")
    print(f"👥 Total customers: {len(customers_df)}")
    print(f"💰 Total accounts: {len(accounts_df)}")
    print(f"💳 Total transactions: {len(transactions_df)}")
    print(f"🚨 Fraud rate: {transactions_df['is_fraud'].mean() * 100:.2f}%")
    
    print("\n✅ Stage 2 complete! Your data files are in: data/raw/")
    print("\nNext step: Run 'python test_data.py' to verify your data")

# Run everything
if __name__ == "__main__":
    main()