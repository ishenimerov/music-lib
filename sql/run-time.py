import time
import mysql.connector
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Establish a connection to the MySQL database
    connection = mysql.connector.connect(host='localhost', user='root', password='', db='music_library')
    logger.info("Connected to MySQL database!")

    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Define an array of queries
    queries = [
        # Query 1: Simple query to select all songs
        """
        SELECT * FROM songs;
        """,
        # Query 2: Intermediate query to select songs released in 2021
        """
        SELECT song_title, artist_id, release_date
        FROM songs
        WHERE release_date LIKE '2021%';
        """,
        # Query 3: Complex query involving multiple joins
        """
        SELECT s.song_title, a.artist_name, b.album_name
        FROM songs s
        JOIN artists a ON s.artist_id = a.artist_id
        JOIN albums b ON s.album_id = b.album_id
        WHERE YEAR(s.release_date) = 2023;

        """,
        # Query 4: Advanced query with aggregation and subqueries
        """
        SELECT s.song_title AS song_title, a.artist_name AS artist_name, s.release_date
        FROM songs s
        JOIN artists a ON s.artist_id = a.artist_id
        WHERE YEAR(s.release_date) = 2022
        GROUP BY s.song_title, a.artist_name, s.release_date
        HAVING COUNT(*) > (
            SELECT AVG(songs_per_artist)
            FROM (
                SELECT COUNT(*) AS songs_per_artist
                FROM songs
                WHERE YEAR(release_date) = 2022
                GROUP BY artist_id
            ) AS subquery
        )
        ORDER BY s.release_date DESC;

        """
    ]
    # Create a folder named "run-time-results" if it doesn't exist
    folder_name = "run-time-results-100"
    folder_name = os.path.join("sql", "run-time-results-100")
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        logger.info("Created folder 'run-time-results-100' inside 'sql' folder")

    # Define the number of times to execute each query
    num_executions = 31

    # Iterate through the queries and execute each one
    for i, query in enumerate(queries):
        logger.info(f"Executing query {i+1}/{len(queries)}...")

        # Open a new file for writing with a different name for each query
        with open(os.path.join(folder_name, f"runtime_q{i+1}.txt"), "w") as file:
            # Execute the query and record the runtime for each execution
            for j in range(num_executions):
                start_time = time.time()
                cursor.execute(query)
                result = cursor.fetchall()
                end_time = time.time()
                runtime = end_time - start_time

                # Write the runtime to the file
                file.write(f"{j+1}. {runtime:.4f} sec\n")

        logger.info(f"Query {i+1} done!")

except mysql.connector.Error as err:
    logger.error(f"MySQL error: {err}")

finally:
    # Close the cursor and connection
    cursor.close()
    logger.info("Cursor closed!")
    connection.close()
    logger.info("Connection closed!")