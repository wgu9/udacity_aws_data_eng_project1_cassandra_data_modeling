import psycopg2
from psycopg2 import Error
from sql_queries import create_table_queries, drop_table_queries, config

def drop_tables(cur, conn):
    """
    Drops tables in the database using the provided cursor and connection.
    Parameters:
    cur (psycopg2.extensions.cursor): Cursor to execute database queries.
    conn (psycopg2.extensions.connection): Connection to the database.
    Raises:
    Error: If there is an error executing any of the drop table queries.
    Returns:
    None
    """

    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Error as e:
            print(f"Error executing query: {query}\nError message: {e}")
            conn.rollback()
            raise
    print("Tables dropped successfully")

def create_tables(cur, conn):
    """
    Creates tables in the database using the provided cursor and connection.

    Parameters:
    cur (psycopg2.extensions.cursor): The cursor object to execute database queries.
    conn (psycopg2.extensions.connection): The connection object to commit transactions.

    Raises:
    Error: If there is an error executing any of the queries, the error is printed, 
           the transaction is rolled back, and the error is raised again.

    Returns:
    None
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Error as e:
            print(f"Error executing query: {query}\nError message: {e}")
            conn.rollback()
            raise
    print("Tables created successfully")

def main():
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
        
        # Drop and create tables
        drop_tables(cur, conn)
        create_tables(cur, conn)

    except Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Application error: {e}")
        raise
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
            print("Database connection closed")


if __name__ == "__main__":
    main()