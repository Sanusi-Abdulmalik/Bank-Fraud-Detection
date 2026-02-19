import psycopg2

try:
    # Try to connect to database
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="password",
        port="5432"
    )
    print("✅ CONGRATULATIONS! Database connection SUCCESSFUL!")
    
    # Test a simple query
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"📦 PostgreSQL version: {version[0]}")
    
    # Clean up
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Oops! Something went wrong: {e}")
    print("\n🔧 Troubleshooting tips:")
    print("1. Is Docker running? Check: docker ps")
    print("2. Did you run: docker run --name bank-db -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres")
    print("3. Wait 30 seconds after starting Docker")