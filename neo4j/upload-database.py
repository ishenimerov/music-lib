from neo4j import GraphDatabase
import csv

# Define the Neo4j connection URI, username, and password
uri = "bolt://localhost:7687"
username = "neo4j"
password = "Demo123!"

# Create a Neo4j session
driver = GraphDatabase.driver(uri, auth=(username, password))

# # Function to drop nodes in batches
# def drop_nodes_batch(tx, node_label):
#     tx.run(f"MATCH (n:{node_label}) WITH n LIMIT 500000 DETACH DELETE n")

# # Labels of nodes you want to delete
# node_labels = ["Song", "Album", "Artist", "Genre"]

# # Run the drop_nodes_batch function for each node label
# with driver.session() as session:
#     for label in node_labels:
#         session.write_transaction(drop_nodes_batch, label)
#         print(f"All nodes with label '{label}' deleted.")

# # Close the driver when done
# driver.close()


# Function to insert data from CSV files in batches
def insert_data_batch(tx, query, data):
    batch_size = 1000  # Adjust the batch size as needed
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        tx.run(query, data=batch)

# Define the data insertion queries for each CSV file
queries = {
    "songs": "UNWIND $data AS row CREATE (:Song {song_id: row.song_id, song_title: row.song_title, duration: row.duration, release_date: row.release_date, lyrics: row.lyrics})",
    "albums": "UNWIND $data AS row CREATE (:Album {album_id: row.album_id, album_name: row.album_name, release_date: row.release_date})",
    "artists": "UNWIND $data AS row CREATE (:Artist {artist_id: row.artist_id, artist_name: row.artist_name, biography: row.biography, origin: row.origin})",
    "genres": "UNWIND $data AS row CREATE (:Genre {genre_id: row.genre_id, genre_name: row.genre_name})"
}

# Function to read CSV files in chunks
def read_csv_chunked(filename, chunk_size=10000):
    with open(filename, 'r') as file:
        csv_data = csv.DictReader(file)
        while True:
            chunk = []
            try:
                for _ in range(chunk_size):
                    row = next(csv_data)
                    chunk.append(row)
                yield chunk
            except StopIteration:
                if chunk:
                    yield chunk
                break

# Iterate through each CSV file and insert data into Neo4j
with driver.session() as session:
    for filename, query in queries.items():
        for chunk in read_csv_chunked(f'csv100/{filename}.csv'):
            session.write_transaction(insert_data_batch, query, chunk)
        print(f"Data inserted into {filename}!")

# Close the driver when done
driver.close()
