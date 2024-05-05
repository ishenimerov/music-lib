import os
import time
from cassandra.cluster import Cluster

# Connect to Cassandra cluster
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('music_library')

print('Successfully connected to Cassandra.')

# Record execution times in the file
num_executions = 31
folder_name = os.path.join("cassandra", "run-time-results-25")
file_path = os.path.join(folder_name, "runtime_q4.txt")

with open(file_path, "w") as file:
    for i in range(num_executions):
        start_time = time.time()

        # Drop the existing artist_song_counts table if it exists
        drop_table_query = "DROP TABLE IF EXISTS artist_song_counts;"
        session.execute(drop_table_query)
        print('Existing artist_song_counts table dropped, if it exists')

        # Create a denormalized table to store the count of songs for each artist
        create_artist_song_counts_table_query = """
            CREATE TABLE IF NOT EXISTS artist_song_counts (
                artist_id UUID PRIMARY KEY,
                song_count COUNTER
            );
        """
        session.execute(create_artist_song_counts_table_query)
        print('Successfully created artist_song_counts table')

        # Query songs with duration less than 300 seconds and update song counts for each artist
        select_songs_query = """
            SELECT artist_id
            FROM songs
            WHERE duration < 300
            ALLOW FILTERING;
        """
        songs_result = session.execute(select_songs_query)
        for song in songs_result:
            artist_id = song.artist_id

            # Increment song count for the artist
            update_count_query = f"""
                UPDATE artist_song_counts
                SET song_count = song_count + 1
                WHERE artist_id = {artist_id};
            """
            session.execute(update_count_query)

        print("Song counts updated for each artist.")

        end_time = time.time()
        runtime = end_time - start_time
        file.write(f"{i+1}: {runtime:.4f} sec\n")

print(f"Execution times saved in '{file_path}'.")

# Close the connection
session.shutdown()
cluster.shutdown()
print('Disconnected from the Cassandra cluster.')
