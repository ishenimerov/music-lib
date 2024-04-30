from cassandra.cluster import Cluster
import time
import os

# Connect to Cassandra cluster
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('music_library')


# Simple query
simple_query = """
    SELECT * FROM songs;
"""

# Intermediate query with ALLOW FILTERING
intermediate_query = """
    SELECT song_title, duration, release_date
    FROM songs
    WHERE release_date >= '2023-01-01' AND release_date < '2024-01-01'
    ALLOW FILTERING;
"""

# Advanced query
advanced_query = """
    SELECT artist_name, COUNT(*) AS song_count
    FROM songs
    JOIN artists ON songs.artist_id = artists.artist_id
    GROUP BY artist_name
    ORDER BY song_count DESC
    LIMIT 10;
"""

# Run each query 31 times and record execution times
execution_times = []
queries = [simple_query, intermediate_query]
for i, query in enumerate(queries, 1):
    print(f"Running query {i}")
    print("Executing query:", query)
    session.execute(query)
    times = []
    for j in range(31):
        start_time = time.time()
        session.execute(query)
        end_time = time.time()
        execution_time = end_time - start_time
        times.append(execution_time)

    execution_times.append(times)
    print(f"Query {i} executed {len(times)} times.")

# Define the folder name
folder_name = os.path.join("sql", "run-time-results-100")

# Check if the folder exists, if not, create it
if not os.path.exists(folder_name):
    os.makedirs(folder_name)
    print("Created folder 'run-time-results-100' inside 'sql' folder")

# Save execution times in a text file inside the folder
file_path = os.path.join(folder_name, "execution_times.txt")
with open(file_path, "w") as file:
    for i, time in enumerate(execution_times, 1):
        file.write(f'{i}. {time:.4f} sec\n')

print("Execution times saved in 'execution_times.txt' inside 'run-time-results-100' folder.")
# Save execution times to a file
with open("cassandra_query_execution_times.txt", "w") as file:
    for i, times in enumerate(execution_times, 1):
        file.write(f"Query {i} execution times:\n")
        for j, time in enumerate(times, 1):
            file.write(f"Iteration {j}: {time:.4f} sec\n")
        file.write("\n")

print("Execution times saved in cassandra_query_execution_times.txt.")

# Close the connection
session.shutdown()
cluster.shutdown()
print('Disconnected from the Cassandra cluster.')
