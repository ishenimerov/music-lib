import redis
import time
import os
import logging

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a folder named "run-time-results" if it doesn't exist
folder_name = "run-time-results"
folder_name = os.path.join("redis", "run-time-results")
if not os.path.exists(folder_name):
    os.makedirs(folder_name)
    logger.info("Created folder 'run-time-results' inside 'redis' folder")

# Define the number of times to execute the query
num_executions = 31

# Execute simple and record the runtime for each execution
with open(os.path.join(folder_name, "runtime_q1.txt"), "w") as file:
    for j in range(num_executions):
        start_time = time.time()
        # Iterate over all song keys and retrieve their values
        for song_key in r.smembers('songs'):
            song_data = r.hgetall(song_key)
        end_time = time.time()
        runtime = end_time - start_time
        # Write the runtime to the file
        file.write(f"{j+1}. {runtime:.4f} sec\n")

logger.info('Query 1 done!')

# Execute intermediate and record the runtime for each execution
with open(os.path.join(folder_name, "runtime_q2.txt"), "w") as file:
    for j in range(num_executions):
        start_time = time.time()
        # Retrieve songs with release date starting with '2021'
        for song_key in r.smembers('songs'):
            song_data = r.hgetall(song_key)
            release_date = song_data[b'release_date'].decode('utf-8')
            if release_date.startswith('2021'):
                song_title = song_data[b'song_title'].decode('utf-8')
                artist_id = song_data[b'artist_id'].decode('utf-8')
                release_date = song_data[b'release_date'].decode('utf-8')
        end_time = time.time()
        runtime = end_time - start_time
        # Write the runtime to the file
        file.write(f"{j+1}. {runtime:.4f} sec\n")

logger.info('Query 2 done!')

# Execute complex and record the runtime for each execution
with open(os.path.join(folder_name, "runtime_q3.txt"), "w") as file:
    for j in range(num_executions):
        start_time = time.time()
        # Retrieve songs released in 2023 and their associated artist and album information
        for song_key in r.smembers('songs'):
            song_data = r.hgetall(song_key)
            release_date = song_data[b'release_date'].decode('utf-8')
            if release_date.startswith('2023'):
                artist_id = song_data[b'artist_id'].decode('utf-8')
                album_id = song_data[b'album_id'].decode('utf-8')
                artist_name = r.hget(f'artist:{artist_id}', 'artist_name').decode('utf-8')
                album_name = r.hget(f'album:{album_id}', 'album_name').decode('utf-8')
                song_title = song_data[b'song_title'].decode('utf-8')
        end_time = time.time()
        runtime = end_time - start_time
        # Write the runtime to the file
        file.write(f"{j+1}. {runtime:.4f} sec\n")
logger.info('Query 3 done!')

# Execute adanced and record the runtime for each execution
with open(os.path.join(folder_name, "runtime_q4.txt"), "w") as file:
    for j in range(num_executions):
        start_time = time.time()
        # Retrieve all songs released in 2021 from Redis
        songs_2021 = []
        for song_key in r.smembers('songs'):
            song_data = r.hgetall(song_key)
            release_date = song_data[b'release_date'].decode('utf-8')
            if release_date.startswith('2021'):
                songs_2021.append(song_data)

        # Perform manual aggregation to calculate the average number of songs per artist
        artist_song_count = {}
        for song in songs_2021:
            artist_id = song[b'artist_id'].decode('utf-8')
            artist_song_count[artist_id] = artist_song_count.get(artist_id, 0) + 1
        avg_songs_per_artist = sum(artist_song_count.values()) / len(artist_song_count)
        # Retrieve songs where the count is greater than the average
        advanced_results = []
        for song in songs_2021:
            artist_id = song[b'artist_id'].decode('utf-8')
            if artist_song_count[artist_id] > avg_songs_per_artist:
                artist_name = r.hget(f'artist:{artist_id}', 'artist_name').decode('utf-8')
                song_title = song[b'song_title'].decode('utf-8')
                release_date = song[b'release_date'].decode('utf-8')
                advanced_results.append({'song_title': song_title, 'artist_name': artist_name, 'release_date': release_date})

        # Sort the results by release date in descending order
        print(advanced_results)
        advanced_results.sort(key=lambda x: x['release_date'], reverse=True)
        end_time = time.time()
        runtime = end_time - start_time
        # Write the runtime to the file
        file.write(f"{j+1}. {runtime:.4f} sec\n")

logger.info('Query 4 done!')

# Close the Redis connection
r.close()
logger.info('Redis connection closed.')