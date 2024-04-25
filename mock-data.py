import os
import csv
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid

fake = Faker()
predefined_genres = [
    "Pop",
    "Rock",
    "Hip hop",
    "Jazz",
    "Blues",
    "Country",
    "Electronic",
    "R&B (Rhythm and Blues)",
    "Reggae",
    "Classical",
    "Folk",
    "Indie",
    "Metal",
    "Punk",
    "Soul",
    "Funk",
    "Alternative",
    "Dance",
    "Gospel",
    "Latin"
]

# Function to generate a unique ID
def generate_uuid():
    return str(uuid.uuid4())

# Function to generate fake data for Song entity
def generate_song(song_id, artist_id, album_id):
    title = fake.catch_phrase()
    duration = random.randint(120, 600)  # Duration in seconds, between 2 to 10 minutes
    release_date = fake.date_between(start_date='-10y', end_date='today')
    composer = fake.name()
    lyrics = fake.paragraph(nb_sentences=10)
    return [song_id, title, duration, release_date, artist_id, album_id, composer, lyrics]

# Function to generate fake data for Album entity
def generate_album(album_id):
    title = fake.catch_phrase()
    release_date = fake.date_between(start_date='-10y', end_date='today')
    return [album_id, title, release_date]

# Function to generate fake data for Artist entity
def generate_artist(artist_id):
    name = fake.name()
    biography = fake.paragraph(nb_sentences=5)
    origin = fake.country()
    return [artist_id, name, biography, origin]

# Function to generate fake data for Genre entity
def generate_genre(genre_id):
    name = random.choice(predefined_genres)
    return [genre_id, name]

# Define the number of records
num_records = 10

# Generate unique IDs for albums, artists, and genres
album_ids = [generate_uuid() for _ in range(num_records)]
artist_ids = [generate_uuid() for _ in range(num_records)]
genre_ids = [generate_uuid() for _ in range(num_records)]

# Generate fake data for albums, artists, and genres
albums = [generate_album(album_id) for album_id in album_ids]
artists = [generate_artist(artist_id) for artist_id in artist_ids]
genres = [generate_genre(genre_id) for genre_id in genre_ids]

# Generate fake data for songs, associating them with specific artists and albums
songs = []
for song_id in range(num_records):
    # Randomly select an album ID and artist ID from the available lists
    album_id = random.choice(album_ids)
    artist_id = random.choice(artist_ids)
    songs.append(generate_song(song_id, artist_id, album_id))

# Create a folder named "csv" if it doesn't exist
folder_name = "csv"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

# Write data to CSV files in the "csv" folder
with open(os.path.join(folder_name, 'songs.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['song_id', 'title', 'duration', 'release_date', 'artist_id', 'album_id', 'composer', 'lyrics'])
    writer.writerows(songs)

with open(os.path.join(folder_name, 'albums.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['album_id', 'title', 'release_date'])
    writer.writerows(albums)

with open(os.path.join(folder_name, 'artists.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['artist_id', 'name', 'biography', 'origin'])
    writer.writerows(artists)

with open(os.path.join(folder_name, 'genres.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['genre_id', 'name'])
    writer.writerows(genres)
