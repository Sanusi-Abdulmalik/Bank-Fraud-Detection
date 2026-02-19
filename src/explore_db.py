"""
Explore the database structure
"""

import psycopg2
import pandas as pd

def explore_database():
    """Explore the database structure"""
    print("🔍 EXPLORING DATABASE STRUCTURE")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="bank_analytics",
            user="postgres",
            password="password",
            port="5432"
        )
        
        # Get all tables
        query = """
            SELECT 
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """
        
        tables_df = pd.read_sql(query, conn)
        
        print("📋 DATABASE TABLES:")
        print("-" * 40)
        
        for _, row in tables_df.iterrows():
            print(f"  {row['table_name']} ({row['table_type']})")
            
            # Get column details for this table
            col_query = f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable
                FROM information_schema.columns 
                WHERE table_name = '{row['table_name']}'
                ORDER BY ordinal_position
            """
            
            cols_df = pd.read_sql(col_query, conn)
            
            # Show first few columns
            sample_cols = cols_df.head(5)['column_name'].tolist()
            if len(cols_df) > 5:
                sample_cols.append(f"... and {len(cols_df) - 5} more")
            
            print(f"    Columns: {', '.join(sample_cols)}")
            print()
        
        # Show layer summary
        print("\n🎯 DATABASE LAYERS SUMMARY:")
        print("-" * 40)
        
        bronze_tables = [t for t in tables_df['table_name'] if t.startswith('bronze_')]
        silver_tables = [t for t in tables_df['table_name'] if t.startswith('silver_')]
        gold_tables = [t for t in tables_df['table_name'] if t.startswith('gold_')]
        views = [t for t in tables_df['table_name'] if t.startswith('vw_')]
        
        print(f"BRONZE LAYER (Raw Data): {len(bronze_tables)} tables")
        print(f"SILVER LAYER (Cleaned Data): {len(silver_tables)} tables")
        print(f"GOLD LAYER (Analytics Ready): {len(gold_tables)} tables")
        print(f"VIEWS (Pre-built Queries): {len(views)} views")
        
        # Show foreign key relationships
        print("\n🔗 FOREIGN KEY RELATIONSHIPS:")
        print("-" * 40)
        
        fk_query = """
            SELECT
                tc.table_name, 
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.table_name;
        """
        
        fk_df = pd.read_sql(fk_query, conn)
        
        if not fk_df.empty:
            for _, row in fk_df.iterrows():
                print(f"  {row['table_name']}.{row['column_name']} → {row['foreign_table_name']}.{row['foreign_column_name']}")
        else:
            print("  No foreign keys defined yet")
        
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ Database exploration complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def test_sample_queries():
    """Test some sample queries"""
    print("\n🧪 TESTING SAMPLE QUERIES")
    print("-" * 40)
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="bank_analytics",
            user="postgres",
            password="password",
            port="5432"
        )
        
        # Query 1: Count tables
        print("1. Counting tables...")
        count_query = "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = 'public'"
        result = pd.read_sql(count_query, conn)
        print(f"   Total tables: {result['table_count'].iloc[0]}")
        
        # Query 2: Check view definitions
        print("\n2. Checking views...")
        view_query = "SELECT table_name FROM information_schema.views WHERE table_schema = 'public'"
        views = pd.read_sql(view_query, conn)
        print(f"   Available views: {', '.join(views['table_name'].tolist())}")
        
        # Query 3: Show table sizes (will be 0 since empty)
        print("\n3. Checking table sizes...")
        size_query = """
            SELECT 
                table_name,
                pg_size_pretty(pg_total_relation_size(table_name)) as size
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY pg_total_relation_size(table_name) DESC
        """
        sizes = pd.read_sql(size_query, conn)
        for _, row in sizes.iterrows():
            print(f"   {row['table_name']}: {row['size']}")
        
        conn.close()
        
        print("\n✅ Sample queries successful!")
        
    except Exception as e:
        print(f"❌ Query error: {e}")

if __name__ == "__main__":
    explore_database()
    test_sample_queries()
    
    print("\n" + "=" * 60)
    print("🎯 WHAT WE'VE BUILT:")
    print("=" * 60)
    print("""
    1. BRONZE LAYER (5 tables)
       • Stores raw data exactly as it comes from CRM/ERP
       • Includes ETL metadata (loaded_at, source_file)
       • No transformations applied
    
    2. SILVER LAYER (3 tables)
       • Cleaned and standardized data
       • Business logic applied
       • Enhanced with derived columns
    
    3. GOLD LAYER (3 tables)
       • Analytics-ready tables
       • Optimized for queries
       • Machine learning features
    
    4. VIEWS (2 views)
       • Pre-built queries for common reports
       • Virtual tables for easy access
    """)
    
    print("\n📁 Your database is EMPTY right now.")
    print("   In Stage 4, we'll LOAD data from CSV files!")