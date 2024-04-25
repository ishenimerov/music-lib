import csv
import pymysql

# Connect to MySQL database
conn = pymysql.connect(host='localhost', user='root', password='', db='music_library')
cursor = conn.cursor()
print("Connected to MySQL database!")

# Create temporary database
cursor.execute("CREATE DATABASE IF NOT EXISTS music_library")
cursor.execute("USE music_library")

# Create songs table
cursor.execute('''CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR(36) PRIMARY KEY,
                    title VARCHAR(255),
                    duration INT,
                    release_date DATE,
                    artist_id VARCHAR(36),
                    album_id VARCHAR(36),
                    lyrics TEXT
                )''')

# Create albums table
cursor.execute('''CREATE TABLE IF NOT EXISTS albums (
                    album_id VARCHAR(36) PRIMARY KEY,
                    title VARCHAR(255),
                    release_date DATE
                )''')

# Create artists table
cursor.execute('''CREATE TABLE IF NOT EXISTS artists (
                    artist_id VARCHAR(36) PRIMARY KEY,
                    name VARCHAR(255),
                    biography TEXT,
                    origin VARCHAR(255)
                )''')

# Create genres table
cursor.execute('''CREATE TABLE IF NOT EXISTS genres (
                    genre_id VARCHAR(36) PRIMARY KEY,
                    name VARCHAR(255)
                )''')

# Insert data from songs.csv
with open('csv/songs.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        song_id, title, duration, release_date, artist_id, album_id, lyrics = row
        query = f"INSERT INTO songs (song_id, title, duration, release_date, artist_id, album_id, lyrics) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (song_id, title, int(duration), release_date, artist_id, album_id, lyrics))
print('Data inserted into songs table!')

# Insert data from albums.csv
with open('csv/albums.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        album_id, title, release_date = row
        query = f"INSERT INTO albums (album_id, title, release_date) VALUES (%s, %s, %s)"
        cursor.execute(query, (album_id, title, release_date))
print('Data inserted into albums table!')

# Insert data from artists.csv
with open('csv/artists.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        artist_id, name, biography, origin = row
        query = f"INSERT INTO artists (artist_id, name, biography, origin) VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (artist_id, name, biography, origin))
print('Data inserted into artists table!')

# Insert data from genres.csv
with open('csv/genres.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        genre_id, name = row
        query = f"INSERT INTO genres (genre_id, name) VALUES (%s, %s)"
        cursor.execute(query, (genre_id, name))
print('Data inserted into genres table!')

# Commit the changes and close the connection
conn.commit()
print('Changes committed!')
conn.close()
print('Connection closed!')
