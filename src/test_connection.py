"""
Test database connection and create our database
"""

import psycopg2
from psycopg2 import sql

def test_connection():
    """Test if we can connect to PostgreSQL"""
    print("🔌 Testing database connection...")
    
    try:
        # First connect to default 'postgres' database
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",  # Connect to default database first
            user="postgres",
            password="password",
            port="5432"
        )
        conn.autocommit = True  # Need this to create a new database
        cursor = conn.cursor()
        
        print("✅ Connected to PostgreSQL server!")
        
        # Check if our database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'bank_analytics'")
        exists = cursor.fetchone()
        
        if exists:
            print("📊 Database 'bank_analytics' already exists")
        else:
            print("🗄️ Creating 'bank_analytics' database...")
            cursor.execute(sql.SQL("CREATE DATABASE bank_analytics"))
            print("✅ Database created successfully!")
        
        cursor.close()
        conn.close()
        
        # Now test connection to our new database
        conn = psycopg2.connect(
            host="localhost",
            database="bank_analytics",
            user="postgres",
            password="password",
            port="5432"
        )
        cursor = conn.cursor()
        
        # Get PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"📦 PostgreSQL Version: {version.split(',')[0]}")
        
        # List all tables (should be empty at first)
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        
        if tables:
            print(f"📋 Existing tables: {[t[0] for t in tables]}")
        else:
            print("📋 No tables yet - ready to create!")
        
        cursor.close()
        conn.close()
        
        print("\n🎉 Database connection successful and ready!")
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Is Docker running? Run: docker ps")
        print("2. Is PostgreSQL container running? Run: docker start bank-db")
        print("3. Wait 30 seconds after starting container")
        print("4. Try: docker run --name bank-db -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres")
        return False

if __name__ == "__main__":
    test_connection()