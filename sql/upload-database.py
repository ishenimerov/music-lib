import csv
import pymysql
import uuid

# Connect to MySQL database
conn = pymysql.connect(host='localhost', user='root', password='', db='music_library')
cursor = conn.cursor()
print("Connected to MySQL database!")

# # Function to drop tables if they exist
# def drop_tables():
#     cursor.execute("DROP TABLE IF EXISTS songs")
#     cursor.execute("DROP TABLE IF EXISTS albums")
#     cursor.execute("DROP TABLE IF EXISTS artists")
#     cursor.execute("DROP TABLE IF EXISTS genres")

# # Drop tables if they exist
# drop_tables()

# Create temporary database
cursor.execute("CREATE DATABASE IF NOT EXISTS music_library")
cursor.execute("USE music_library")

# Create songs table
cursor.execute('''CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR(36) PRIMARY KEY,
                    song_title VARCHAR(255),
                    duration INT,
                    release_date DATE,
                    artist_id VARCHAR(36),
                    album_id VARCHAR(36),
                    genre_id VARCHAR(36),
                    lyrics TEXT
                )''')

# Create albums table
cursor.execute('''CREATE TABLE IF NOT EXISTS albums (
                    album_id VARCHAR(36) PRIMARY KEY,
                    album_name VARCHAR(255),
                    release_date DATE
                )''')

# Create artists table
cursor.execute('''CREATE TABLE IF NOT EXISTS artists (
                    artist_id VARCHAR(36) PRIMARY KEY,
                    artist_name VARCHAR(255),
                    biography TEXT,
                    origin VARCHAR(255)
                )''')

# Create genres table
cursor.execute('''CREATE TABLE IF NOT EXISTS genres (
                    genre_id VARCHAR(36) PRIMARY KEY,
                    genre_name VARCHAR(255)
                )''')

# Insert data from songs.csv
with open('csv100/songs.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        song_id, song_title, duration, release_date, genre_id, artist_id, album_id, lyrics = row
        query = f"INSERT INTO songs (song_id, song_title, duration, genre_id, release_date, artist_id, album_id, lyrics) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (song_id, song_title, int(duration), release_date, genre_id, artist_id, album_id, lyrics))
print('Data inserted into songs table!')

# Insert data from albums.csv
with open('csv100/albums.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        album_id, album_name, release_date = row
        query = f"INSERT INTO albums (album_id, album_name, release_date) VALUES (%s, %s, %s)"
        cursor.execute(query, (album_id, album_name, release_date))
print('Data inserted into albums table!')

# Insert data from artists.csv
with open('csv100/artists.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        artist_id, artist_name, biography, origin = row
        query = f"INSERT INTO artists (artist_id, artist_name, biography, origin) VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (artist_id, artist_name, biography, origin))
print('Data inserted into artists table!')

# Insert data from genres.csv
with open('csv100/genres.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        genre_id, genre_name = row
        query = f"INSERT INTO genres (genre_id, genre_name) VALUES (%s, %s)"
        cursor.execute(query, (genre_id, genre_name))
print('Data inserted into genres table!')

# Commit the changes and close the connection
conn.commit()
print('Changes committed!')
conn.close()
print('Connection closed!')
