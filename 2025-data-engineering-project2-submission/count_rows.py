# 
# This script connects to a Redshift cluster, counts the number of rows in specified tables,
# and prints the row counts. It reads database connection details from a configuration file (dwh.cfg).

# 
import configparser
import psycopg2

def count_table_rows(cur):
    """Count rows in all tables and print results"""
    table_queries = [
        "SELECT COUNT(*) FROM staging_events",
        "SELECT COUNT(*) FROM staging_songs",
        "SELECT COUNT(*) FROM songplays",
        "SELECT COUNT(*) FROM users",
        "SELECT COUNT(*) FROM songs",
        "SELECT COUNT(*) FROM artists",
        "SELECT COUNT(*) FROM time"
    ]
    
    print("\nRow counts for all tables:")
    print("-" * 40)
    
    for query in table_queries:
        table_name = query.split()[-1]  # Get table name from query
        try:
            cur.execute(query)
            count = cur.fetchone()[0]
            print(f"{table_name}: {count:,} rows")
        except Exception as e:
            print(f"Error counting {table_name}: {e}")

def main():
    # Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Initialize connection variables
    conn = None
    cur = None
    
    try:
        # Connect to Redshift cluster
        conn = psycopg2.connect(
            host=config['CLUSTER']['HOST'],
            dbname=config['CLUSTER']['DB_NAME'],
            user=config['CLUSTER']['DB_USER'],
            password=config['CLUSTER']['DB_PASSWORD'],
            port=int(config['CLUSTER']['DB_PORT'])
        )
        cur = conn.cursor()
        
        print("Connected to Redshift successfully")
        
        # Count rows in all tables
        count_table_rows(cur)

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        print(f"Error details: {e.pgerror}")
        print(f"Error code: {e.pgcode}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
            print("\nDatabase connection closed")

if __name__ == "__main__":
    main()