import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    try:
        for query in copy_table_queries:
            print(f"Executing query: {query[:100]}...")  # Print first 100 chars for debugging
            cur.execute(query)
            conn.commit()
        
        # Validate staging data
        cur.execute("SELECT COUNT(*) FROM staging_events")
        events_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM staging_songs")
        songs_count = cur.fetchone()[0]
        print(f"Staging tables loaded successfully:")
        print(f"staging_events: {events_count} rows")
        print(f"staging_songs: {songs_count} rows")
        
    except Exception as e:
        print(f"Error loading staging tables: {e}")
        conn.rollback()

def insert_tables(cur, conn):
    try:
        for query in insert_table_queries:
            print(f"Executing query: {query[:100]}...")  # Print first 100 chars for debugging
            cur.execute(query)
            conn.commit()
        
        # Validate analytics tables
        validation_queries = [
            ("songplays", "SELECT COUNT(*) FROM songplays"),
            ("users", "SELECT COUNT(*) FROM users"),
            ("songs", "SELECT COUNT(*) FROM songs"),
            ("artists", "SELECT COUNT(*) FROM artists"),
            ("time", "SELECT COUNT(*) FROM time")
        ]
        
        print("Analytics tables loaded successfully:")
        for table, query in validation_queries:
            cur.execute(query)
            count = cur.fetchone()[0]
            print(f"{table}: {count} rows")
            
    except Exception as e:
        print(f"Error inserting into tables: {e}")
        conn.rollback()

def main():
    # Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Main ETL execution
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
        
        # Execute ETL process
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

    except psycopg2.Error as e:
        print(f"Database error during ETL process: {e}")
        print(f"Error details: {e.pgerror}")
        print(f"Error code: {e.pgcode}")
    except Exception as e:
        print(f"Non-database error during ETL process: {e}")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
            print("Database connection closed")


if __name__ == "__main__":
    main()