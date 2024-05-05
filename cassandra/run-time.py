from cassandra.cluster import Cluster
import time
import os

# Connect to Cassandra cluster
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('music_library')

# Define the queries
queries = [
    """
    SELECT * FROM songs;
    """,
    """
    SELECT song_title, duration, release_date
    FROM songs
    WHERE release_date >= '2023-01-01' AND release_date < '2024-01-01'
    ALLOW FILTERING;
    """
    # # Advanced query
# advanced_query = """
#     SELECT artist_name, COUNT(*) AS song_count
#     FROM songs
#     JOIN artists ON songs.artist_id = artists.artist_id
#     GROUP BY artist_name
#     ORDER BY song_count DESC
#     LIMIT 10;
# """
]

# Define the number of executions for each query
num_executions = 31

# Define the folder name
folder_name = os.path.join("cassandra", "run-time-results-25")

# Check if the folder exists, if not, create it
if not os.path.exists(folder_name):
    os.makedirs(folder_name)
    print("Created folder 'run-time-results-25' inside 'cassandra' folder")

# Iterate through the queries and execute each one
for i, query in enumerate(queries, 1):
    print(f"Running query {i}")
    # Open a new file for writing with a different name for each query
    with open(os.path.join(folder_name, f"runtime_q{i}.txt"), "w") as file:
        # Execute the query and record the runtime for each execution
        for j in range(num_executions):
            start_time = time.time()
            session.execute(query)
            end_time = time.time()
            runtime = end_time - start_time

            # Write the runtime to the file
            file.write(f"{j+1}: {runtime:.4f} sec\n")
    print(f"Query {i} done")

# Close the connection
session.shutdown()
cluster.shutdown()
print('Disconnected from the Cassandra cluster.')
