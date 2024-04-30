import os
import time
from cassandra.cluster import Cluster

# Connect to Cassandra cluster
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('music_library')

# Create a denormalized table to support the complex query
create_denormalized_table_query = """
    CREATE TABLE IF NOT EXISTS denormalized_songs (
        song_id INT PRIMARY KEY,
        song_title TEXT,
        artist_name TEXT,
        album_name TEXT,
        duration INT,
        release_date DATE,
        artist_id UUID,
        album_id UUID
    );
"""

# Execute create table query
session.execute(create_denormalized_table_query)

# Query songs with duration less than 300 seconds
select_songs_query = """
    SELECT song_id, song_title, duration, release_date, artist_id, album_id
    FROM songs
    WHERE duration < 300
    ALLOW FILTERING;
"""

# Execute query to get songs
songs_result = session.execute(select_songs_query)

# Iterate over selected songs
for song in songs_result:
    # Retrieve artist name using artist_id
    artist_query = f"SELECT artist_name FROM artists WHERE artist_id = {song.artist_id};"
    artist_name = session.execute(artist_query).one().artist_name

    # Retrieve album name using album_id
    album_query = f"SELECT album_name FROM albums WHERE album_id = {song.album_id};"
    album_name = session.execute(album_query).one().album_name

    # Insert data into denormalized table
    insert_query = """
        INSERT INTO denormalized_songs (song_id, song_title, artist_name, album_name, duration, release_date, artist_id, album_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    session.execute(insert_query, (song.song_id, song.song_title, artist_name, album_name, song.duration, song.release_date, song.artist_id, song.album_id))

print("Data inserted into denormalized_songs table.")

# File to store execution times
folder_name = "run-time-results-75"
file_path = os.path.join(folder_name, "complex-result.txt")

if not os.path.exists(folder_name):
    os.makedirs(folder_name)
    print(f"Created folder '{folder_name}'.")

# Record execution times in the file
with open(file_path, "a") as file:
    for i in range(31):
        start_time = time.time()
        session.execute(select_songs_query)
        end_time = time.time()
        runtime = end_time - start_time
        file.write(f"{i+1}. {runtime:.4f} sec\n")

print(f"Execution times saved in '{file_path}'.")

# Close the connection
session.shutdown()
cluster.shutdown()
print('Disconnected from the Cassandra cluster.')
