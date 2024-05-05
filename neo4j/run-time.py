from neo4j import GraphDatabase
import time
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the Neo4j connection URI, username, and password
uri = "bolt://localhost:7687"
username = "neo4j"
password = "Demo123!"

# Create a Neo4j session
driver = GraphDatabase.driver(uri, auth=(username, password))

# Define the directory for results
results_dir = "run-time-results-100"

# Create results directory if it doesn't exist
folder_name = os.path.join("neo4j", "run-time-results-100")
if not os.path.exists(folder_name):
    os.makedirs(folder_name)
    logger.info("Created folder 'run-time-results-100' inside 'neo4j' folder")

# Define Cypher queries
queries = [
    "MATCH (s:Song) RETURN s",
    "MATCH (s:Song) WHERE s.release_date STARTS WITH '2021' RETURN s.song_title, s.artist_id, s.release_date",
    "MATCH (s:Song)-[:BY_ARTIST]->(a:Artist), (s)-[:IN_ALBUM]->(b:Album) WHERE datetime(s.release_date).year = 2023 RETURN s.song_title, a.artist_name, b.album_name",
    "MATCH (s:Song)-[:BY_ARTIST]->(a:Artist) WHERE datetime(s.release_date).year = 2022 WITH s, a ORDER BY s.release_date DESC WITH s.song_title AS song_title, a.artist_name AS artist_name, s.release_date AS release_date WITH song_title, artist_name, release_date, COUNT(*) AS songs_count WITH song_title, artist_name, release_date, songs_count, avg(songs_count) AS avg_songs_per_artist WHERE songs_count > avg_songs_per_artist RETURN song_title, artist_name, release_date ORDER BY release_date DESC"
]

# Run each query 31 times and measure execution time
for index, query in enumerate(queries, start=1):
    print(f"Running Query {index}")
    execution_times = []
    for i in range(31):
        start_time = time.time()
        with driver.session() as session:
            session.run(query)
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)
    # Save results to text file
    with open(os.path.join(folder_name, f"runtime_q{index}.txt"), "w") as file:
        for i, time_value in enumerate(execution_times, start=1):
            file.write(f" {i}: {time_value:.4f} sec\n")
    print("Results saved to file.\n")

# Close the driver
driver.close()
