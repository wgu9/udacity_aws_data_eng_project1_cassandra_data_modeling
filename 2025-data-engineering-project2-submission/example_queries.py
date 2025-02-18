# Description: This script connects to a Redshift cluster and runs example queries to demonstrate the data analysis capabilities of the Star Schema.
import configparser
import psycopg2

def run_example_queries(conn):
    """
    Executes example queries on the provided database connection and prints the results.
    Args:
        conn (psycopg2.extensions.connection): The database connection object.
    Queries:
        1. Most played songs: Retrieves the top 5 most played songs along with their play counts.
        2. Peak usage hours: Retrieves the top 5 hours with the highest number of song plays.
    Prints:
        The results of the queries to the console.
    """
    cur = conn.cursor()
    
    # Query 1: Most played songs
    cur.execute("""
        SELECT s.title, COUNT(*) AS play_count
        FROM songplays sp
        JOIN songs s ON sp.song_id = s.song_id
        GROUP BY s.title
        ORDER BY play_count DESC
        LIMIT 5;
    """)
    print("Most Played Songs:")
    for row in cur.fetchall():
        print(row)
    
    # Query 2: Peak usage hours
    cur.execute("""
        SELECT t.hour, COUNT(*) AS play_count
        FROM songplays sp
        JOIN time t ON sp.start_time = t.start_time
        GROUP BY t.hour
        ORDER BY play_count DESC
        LIMIT 5;
    """)
    print("\nPeak Usage Hours:")
    for row in cur.fetchall():
        print(row)

def run_user_activity_analysis(conn):
    """
    Executes a query to analyze user activity by counting the number of users 
    at each subscription level (free vs paid) and prints the results.
    Parameters:
    conn (psycopg2.extensions.connection): The connection object to the database.
    Returns:
    None
    """
    cur = conn.cursor()
    
    # Query: User Activity Analysis
    cur.execute("""
        SELECT u.level, COUNT(*)
        FROM songplays s
        JOIN users u ON s.user_id = u.user_id
        
        GROUP BY u.level;
    """)
    
    print("User Activity Analysis (Free vs Paid Users):")
    for row in cur.fetchall():
        print(row)

        
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
        
        # Run example queries
        run_example_queries(conn)

        run_user_activity_analysis(conn)
    
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