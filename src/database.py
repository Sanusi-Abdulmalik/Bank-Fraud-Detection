"""
Database connection helper
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables (optional, for future use)
load_dotenv()

class DatabaseConnection:
    """Handles all database connections"""
    
    def __init__(self):
        # Connection parameters - these match what we set up in Stage 1
        self.params = {
            'host': 'localhost',
            'database': 'postgres',  # Default database
            'user': 'postgres',
            'password': 'password',
            'port': '5432'
        }
        
        # Create SQLAlchemy engine for pandas
        self.engine = create_engine(
            f"postgresql://{self.params['user']}:{self.params['password']}@{self.params['host']}:{self.params['port']}/{self.params['database']}"
        )
        
    def get_connection(self):
        """Get a raw PostgreSQL connection"""
        try:
            conn = psycopg2.connect(**self.params)
            return conn
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise
    
    def execute_query(self, query, params=None):
        """Execute a SQL query and return results"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                if query.strip().upper().startswith('SELECT'):
                    result = cursor.fetchall()
                else:
                    conn.commit()
                    result = cursor.rowcount  # Return number of rows affected
                return result
        except Exception as e:
            conn.rollback()
            print(f"❌ Query failed: {e}")
            print(f"   Query: {query}")
            raise
        finally:
            conn.close()
    
    def create_database(self, db_name='bank_analytics'):
        """Create a new database if it doesn't exist"""
        # First connect to default postgres database
        temp_params = self.params.copy()
        conn = psycopg2.connect(**temp_params)
        conn.autocommit = True
        
        try:
            with conn.cursor() as cursor:
                # Check if database exists
                cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
                exists = cursor.fetchone()
                
                if not exists:
                    cursor.execute(f'CREATE DATABASE {db_name}')
                    print(f"✅ Created database: {db_name}")
                else:
                    print(f"📊 Database {db_name} already exists")
                    
                # Update connection to use new database
                self.params['database'] = db_name
                self.engine = create_engine(
                    f"postgresql://{self.params['user']}:{self.params['password']}@{self.params['host']}:{self.params['port']}/{db_name}"
                )
        finally:
            conn.close()
    
    def test_connection(self):
        """Test if database connection works"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute('SELECT version();')
                version = cursor.fetchone()
                print(f"✅ Database connection successful!")
                print(f"   PostgreSQL version: {version[0]}")
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False

# Create a global instance
db = DatabaseConnection()

# Test function
if __name__ == "__main__":
    print("🧪 Testing database connection...")
    if db.test_connection():
        print("\n🎉 Database is ready!")
    else:
        print("\n⚠️  Please check:")
        print("   1. Is Docker running? Run: docker ps")
        print("   2. Is PostgreSQL container running? Run: docker start bank-db")
        print("   3. Wait 30 seconds after starting container")