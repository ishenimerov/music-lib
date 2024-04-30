import csv
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['music_library']

# Drop existing collections
db.songs.drop()
db.albums.drop()
db.artists.drop()
db.genres.drop()

# Insert data from songs.csv
with open('csv/songs.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        song_id, song_title, duration, release_date, genre_id, artist_id, album_id, lyrics = row
        db.songs.insert_one({
            '_id': song_id,
            'song_title': song_title,
            'duration': int(duration),
            'release_date': release_date,
            'genre_id': genre_id,
            'artist_id': artist_id,
            'album_id': album_id,
            'lyrics': lyrics
        })
print('Data inserted into songs collection!')

# Insert data from albums.csv
with open('csv/albums.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        album_id, album_name, release_date = row
        db.albums.insert_one({
            '_id': album_id,
            'album_name': album_name,
            'release_date': release_date
        })
print('Data inserted into albums collection!')

# Insert data from artists.csv
with open('csv/artists.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        artist_id, artist_name, biography, origin = row
        db.artists.insert_one({
            '_id': artist_id,
            'artist_name': artist_name,
            'biography': biography,
            'origin': origin
        })
print('Data inserted into artists collection!')

# Insert data from genres.csv
with open('csv/genres.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        genre_id, genre_name = row
        db.genres.insert_one({
            '_id': genre_id,
            'genre_name': genre_name
        })
print('Data inserted into genres collection!')

# Close the connection
client.close()
print('Connection closed!')
