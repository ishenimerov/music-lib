from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import csv
import uuid
# Connect to Cassandra
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()
print('Successfully connected to Cassandra.')

# Function to drop tables if they exist
# def drop_tables():
#     session.execute("USE music_library")
    # session.execute("DROP TABLE IF EXISTS songs")
#     session.execute("DROP TABLE IF EXISTS albums")
#     session.execute("DROP TABLE IF EXISTS artists")
#     session.execute("DROP TABLE IF EXISTS genres")

# # Drop tables if they exist
# drop_tables()

# Create keyspace
session.execute("CREATE KEYSPACE IF NOT EXISTS music_library WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.set_keyspace("music_library")

# Create songs table
session.execute('''CREATE TABLE IF NOT EXISTS songs (
                    song_id INT PRIMARY KEY,
                    song_title TEXT,
                    duration INT,
                    release_date DATE,
                    artist_id UUID,
                    album_id UUID,
                    genre_id UUID,
                    lyrics TEXT
                )''')

# Create albums table
session.execute('''CREATE TABLE IF NOT EXISTS albums (
                    album_id UUID PRIMARY KEY,
                    album_name TEXT,
                    release_date DATE
                )''')

# Create artists table
session.execute('''CREATE TABLE IF NOT EXISTS artists (
                    artist_id UUID PRIMARY KEY,
                    artist_name TEXT,
                    biography TEXT,
                    origin TEXT
                )''')

# Create genres table
session.execute('''CREATE TABLE IF NOT EXISTS genres (
                    genre_id UUID PRIMARY KEY,
                    genre_name TEXT
                )''')

# Insert data from songs.csv
with open('csv25/songs.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        song_id, song_title, duration, genre_id, release_date, artist_id, album_id, lyrics = row
        session.execute(
            """
            INSERT INTO songs ( song_id, song_title, duration, genre_id, release_date, artist_id, album_id, lyrics)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (int(song_id), song_title, int(duration), uuid.UUID(genre_id), release_date, uuid.UUID(artist_id), uuid.UUID(album_id), lyrics)
        )
print('Data inserted into songs table!')

# Insert data from albums.csv
with open('csv25/albums.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        album_id, album_name, release_date = row
        session.execute(
            """
            INSERT INTO albums (album_id, album_name, release_date)
            VALUES (%s, %s, %s)
            """,
            (uuid.UUID(album_id), album_name, release_date)
        )
print('Data inserted into albums table!')

# Insert data from artists.csv
with open('csv25/artists.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        artist_id, artist_name, biography, origin = row
        session.execute(
            """
            INSERT INTO artists (artist_id, artist_name, biography, origin)
            VALUES (%s, %s, %s, %s)
            """,
            (uuid.UUID(artist_id), artist_name, biography, origin)
        )
print('Data inserted into artists table!')

# Insert data from genres.csv
with open('csv25/genres.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        genre_id, genre_name = row
        session.execute(
            """
            INSERT INTO genres (genre_id, genre_name)
            VALUES (%s, %s)
            """,
            (uuid.UUID(genre_id), genre_name)
        )
print('Data inserted into genres table!')

# Close the connection
cluster.shutdown()
print('Successfully closed connection to Cassandra.')
