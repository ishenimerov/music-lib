import csv
import redis

# Connect to Redis container
r = redis.Redis(host='localhost', port=6379, db=1)
print('Connected to Redis container!')


# Insert data from songs.csv into Redis
with open('csv/songs.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        song_id = row['song_id']
        song_title = row['song_title']
        duration = row['duration']
        release_date = row['release_date']
        genre_id = row['genre_id']
        artist_id = row['artist_id']
        album_id = row['album_id']
        lyrics = row['lyrics']

        # Store song data in Redis hash
        song_key = f'song:{song_id}'
        r.hmset(song_key, {'song_id': song_id, 'song_title': song_title, 'duration': duration, 'release_date': release_date, 'genre_id': genre_id, 'artist_id': artist_id, 'album_id': album_id, 'lyrics': lyrics})
        r.sadd('songs', song_key)  # Add song key to the songs key set

print('Data inserted into Redis for songs!')

# Insert data from albums.csv into Redis
with open('csv/albums.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        album_id = row['album_id']
        album_name = row['album_name']
        release_date = row['release_date']

        # Store album data in Redis hash
        album_key = f'album:{album_id}'
        r.hmset(album_key, {'album_id': album_id, 'album_name': album_name, 'release_date': release_date})
        r.sadd('albums', album_key)  # Add album key to the albums key set

print('Data inserted into Redis for albums!')

# Insert data from artists.csv into Redis
with open('csv/artists.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        artist_id = row['artist_id']
        artist_name = row['artist_name']
        biography = row['biography']
        origin = row['origin']

        # Store artist data in Redis hash
        artist_key = f'artist:{artist_id}'
        r.hmset(artist_key, {'artist_id': artist_id, 'artist_name': artist_name, 'biography': biography, 'origin': origin})
        r.sadd('artists', artist_key)  # Add artist key to the artists key set

print('Data inserted into Redis for artists!')

# Insert data from genres.csv into Redis
with open('csv/genres.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        genre_id = row['genre_id']
        genre_name = row['genre_name']

        # Store genre data in Redis hash
        genre_key = f'genre:{genre_id}'
        r.hmset(genre_key, {'genre_id': genre_id, 'genre_name': genre_name})
        r.sadd('genres', genre_key)  # Add genre key to the genres key set

print('Data inserted into Redis for genres!')

# Close the Redis connection
r.close()
print('Redis connection closed.')
