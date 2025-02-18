import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Please add docstrings to each function 

def load_staging_tables(cur, conn):
    """
    Loads data into staging tables from S3 using the provided cursor and connection.
    This function iterates over a list of SQL COPY queries to load data into staging tables.
    It also validates the data load by counting the number of rows in each staging table.
    Args:
        cur (psycopg2.cursor): Cursor object to execute PostgreSQL commands.
        conn (psycopg2.connection): Connection object to commit transactions.
    Raises:
        Exception: If there is an error during the execution of the queries, it prints the error message and rolls back the transaction.
    Prints:
        The first 100 characters of each query being executed for debugging purposes.
        The number of rows loaded into each staging table after successful execution.
    """
    
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
    """
    Inserts data into the tables and validates the insertion by counting the rows in each table.
    Parameters:
    cur (psycopg2.cursor): Cursor of the database connection.
    conn (psycopg2.connection): Connection to the database.
    The function iterates over the list of insert queries, executes each query, and commits the transaction.
    After inserting the data, it validates the insertion by counting the rows in each analytics table and printing the count.
    Raises:
    Exception: If there is an error during the insertion process, it prints the error message and rolls back the transaction.
    """
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
    """
    Main function to execute the ETL process.
    This function reads the configuration file, connects to the Redshift cluster,
    and executes the ETL process by loading staging tables and inserting data into
    the final tables. It handles database and non-database errors and ensures that
    the database connection is closed after the process.
    Raises:
        psycopg2.Error: If there is a database error during the ETL process.
        Exception: If there is a non-database error during the ETL process.
    """
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