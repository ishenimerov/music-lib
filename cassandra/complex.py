import os
import time
from cassandra.cluster import Cluster

# Connect to Cassandra cluster
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('music_library')

print('Successfully connected to Cassandra.')

# Define the number of executions for the complex query
num_executions = 31

# Define the folder name
folder_name = os.path.join("cassandra", "run-time-results-25")

# File to store execution times
file_path = os.path.join(folder_name, "runtime_q3.txt")




# Record execution times in the file
with open(file_path, "w") as file:
    for i in range(num_executions):
        start_time = time.time()
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
        session.execute(create_denormalized_table_query)
        print('Successfully denormalized_songs created')
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
            artist_query = "SELECT artist_name FROM artists WHERE artist_id = %s;"
            artist_name_row = session.execute(artist_query, [song.artist_id]).one()
            artist_name = artist_name_row.artist_name if artist_name_row else None

            # Retrieve album name using album_id
            album_query = "SELECT album_name FROM albums WHERE album_id = %s;"
            album_name_row = session.execute(album_query, [song.album_id]).one()
            album_name = album_name_row.album_name if album_name_row else None

            # Insert data into denormalized table
            insert_query = """
            INSERT INTO denormalized_songs (song_id, song_title, artist_name, album_name, duration, release_date, artist_id, album_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            session.execute(insert_query, (song.song_id, song.song_title, artist_name, album_name, song.duration, song.release_date, song.artist_id, song.album_id))

        print(f"Iteration {i+1} - Data inserted into denormalized_songs table.")

        end_time = time.time()
        runtime = end_time - start_time
        file.write(f"{i+1}: {runtime:.4f} sec\n")
        # Drop the denormalized table after all iterations
        drop_table_query = "DROP TABLE IF EXISTS denormalized_songs;"
        session.execute(drop_table_query)
        print("Denormalized table dropped.")


print(f"Execution times saved in '{file_path}'.")

# Close the connection
session.shutdown()
cluster.shutdown()
print('Disconnected from the Cassandra cluster.')
