"""
Create all database tables from the SQL file
"""

import psycopg2
from psycopg2 import sql
import os

def read_sql_file(filepath):
    """Read SQL file content"""
    with open(filepath, 'r') as file:
        return file.read()

def create_tables():
    """Create all tables from SQL file"""
    print("🗄️  Creating database tables...")
    print("=" * 50)
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host="localhost",
            database="bank_analytics",
            user="postgres",
            password="password",
            port="5432"
        )
        conn.autocommit = False
        cursor = conn.cursor()
        
        # Read SQL file
        sql_content = read_sql_file('sql/create_tables.sql')
        
        # Split by semicolons to execute statements one by one
        # (This handles the DO $$ block which has semicolons inside)
        statements = []
        current_statement = ""
        in_dollar_quote = False
        
        for line in sql_content.split('\n'):
            # Check if we're entering or leaving a dollar-quoted string
            if 'DO $$' in line or '$$' in line:
                in_dollar_quote = not in_dollar_quote
            
            current_statement += line + '\n'
            
            # Only split on semicolons if not inside dollar quotes
            if ';' in line and not in_dollar_quote:
                statements.append(current_statement.strip())
                current_statement = ""
        
        # Execute each statement
        for i, statement in enumerate(statements, 1):
            if statement.strip():  # Skip empty statements
                try:
                    cursor.execute(statement)
                    # Show progress for major sections
                    if 'CREATE TABLE' in statement and 'bronze_' in statement:
                        table_name = statement.split('bronze_')[1].split(' ')[0]
                        print(f"  ✅ Created bronze_{table_name}")
                    elif 'CREATE TABLE' in statement and 'silver_' in statement:
                        table_name = statement.split('silver_')[1].split(' ')[0]
                        print(f"  ✅ Created silver_{table_name}")
                    elif 'CREATE TABLE' in statement and 'gold_' in statement:
                        table_name = statement.split('gold_')[1].split(' ')[0]
                        print(f"  ✅ Created gold_{table_name}")
                except Exception as e:
                    # Skip errors for tables that already exist
                    if 'already exists' in str(e):
                        pass  # Table already exists, that's fine
                    else:
                        print(f"  ⚠️  Could not execute statement {i}: {e}")
        
        # Commit changes
        conn.commit()
        
        # Count tables created
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        table_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        
        print("\n" + "=" * 50)
        print(f"✅ Database schema created successfully!")
        print(f"📊 Total tables: {table_count}")
        
        # Show table summary
        print("\n📋 TABLES CREATED:")
        print("-" * 30)
        
        bronze_tables = [t[0] for t in tables if t[0].startswith('bronze_')]
        silver_tables = [t[0] for t in tables if t[0].startswith('silver_')]
        gold_tables = [t[0] for t in tables if t[0].startswith('gold_')]
        views = [t[0] for t in tables if t[0].startswith('vw_')]
        
        print(f"BRONZE LAYER ({len(bronze_tables)} tables):")
        for table in sorted(bronze_tables):
            print(f"  • {table}")
        
        print(f"\nSILVER LAYER ({len(silver_tables)} tables):")
        for table in sorted(silver_tables):
            print(f"  • {table}")
        
        print(f"\nGOLD LAYER ({len(gold_tables)} tables):")
        for table in sorted(gold_tables):
            print(f"  • {table}")
        
        print(f"\nVIEWS ({len(views)} views):")
        for view in sorted(views):
            print(f"  • {view}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 50)
        print("🎉 Database setup complete!")
        
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        print("\n🔧 Make sure:")
        print("1. Database is running: docker ps")
        print("2. bank_analytics database exists")
        print("3. You ran: python src/test_connection.py first")
        raise

def verify_schema():
    """Verify the schema was created correctly"""
    print("\n🔍 Verifying database schema...")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="bank_analytics",
            user="postgres",
            password="password",
            port="5432"
        )
        cursor = conn.cursor()
        
        # Check each layer
        layers = {
            'bronze': ['customers', 'interactions', 'fraud_reports', 'accounts', 'transactions'],
            'silver': ['customers', 'accounts', 'transactions'],
            'gold': ['customer_360', 'fraud_features', 'daily_fraud_summary']
        }
        
        for layer, tables in layers.items():
            print(f"\n📊 {layer.upper()} LAYER:")
            for table in tables:
                table_name = f"{layer}_{table}"
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    print(f"  • {table_name}: {count} rows")
                except:
                    print(f"  • {table_name}: Table exists ✓")
        
        # Show column counts
        print("\n📈 TABLE DETAILS:")
        cursor.execute("""
            SELECT 
                table_name,
                COUNT(*) as column_count,
                pg_size_pretty(pg_total_relation_size(table_name::regclass)) as size
            FROM information_schema.columns 
            WHERE table_schema = 'public'
            GROUP BY table_name
            ORDER BY table_name
        """)
        
        for table_name, column_count, size in cursor.fetchall():
            print(f"  • {table_name}: {column_count} columns, {size}")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Schema verification complete!")
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")

if __name__ == "__main__":
    print("🏦 BANK ANALYTICS - DATABASE SETUP")
    print("=" * 50)
    
    # Create tables
    create_tables()
    
    # Verify
    verify_schema()
    
    print("\n" + "=" * 50)
    print("🎯 NEXT STEPS:")
    print("1. Database schema is ready")
    print("2. Tables are empty - ready for data")
    print("3. Next: Load data into bronze layer")
    print("=" * 50)