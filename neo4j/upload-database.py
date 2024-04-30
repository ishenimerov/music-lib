from neo4j import GraphDatabase
import csv
# Define the Neo4j connection URI, username, and password
uri = "bolt://localhost:7687"
username = "neo4j"
password = "Demo123!"

# Create a Neo4j session
driver = GraphDatabase.driver(uri, auth=(username, password))

# Function to insert data from CSV files
def insert_data(tx, query, data):
    for row in data:
        tx.run(query, **row)

# Define the data insertion queries for each CSV file
queries = {
    "songs": "CREATE (:Song {song_id: $song_id, song_title: $song_title, duration: $duration, release_date: $release_date, lyrics: $lyrics})",
    "albums": "CREATE (:Album {album_id: $album_id, album_name: $album_name, release_date: $release_date})",
    "artists": "CREATE (:Artist {artist_id: $artist_id, artist_name: $artist_name, biography: $biography, origin: $origin})",
    "genres": "CREATE (:Genre {genre_id: $genre_id, genre_name: $genre_name})"
}

# Iterate through each CSV file and insert data into Neo4j
with driver.session() as session:
    for filename, query in queries.items():
        with open(f'csv/{filename}.csv', 'r') as file:
            csv_data = csv.DictReader(file)
            data = list(csv_data)
            session.write_transaction(insert_data, query, data)
            print(f"Data inserted into {filename}!")

# Close the driver when done
driver.close()