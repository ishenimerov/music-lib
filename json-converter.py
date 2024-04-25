import os
import csv
import json

# Create a folder named "json" if it doesn't exist
folder_name = "json"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

# Convert songs CSV to JSON
with open('csv/songs.csv', 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    songs_data = [dict(row) for row in reader]

with open(os.path.join(folder_name, 'songs.json'), 'w', encoding='utf-8') as f:
    json.dump(songs_data, f, ensure_ascii=False, indent=4)

# Convert albums CSV to JSON
with open('csv/albums.csv', 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    albums_data = [dict(row) for row in reader]

with open(os.path.join(folder_name, 'albums.json'), 'w', encoding='utf-8') as f:
    json.dump(albums_data, f, ensure_ascii=False, indent=4)

# Convert artists CSV to JSON
with open('csv/artists.csv', 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    artists_data = [dict(row) for row in reader]

with open(os.path.join(folder_name, 'artists.json'), 'w', encoding='utf-8') as f:
    json.dump(artists_data, f, ensure_ascii=False, indent=4)

# Convert genres CSV to JSON
with open('csv/genres.csv', 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    genres_data = [dict(row) for row in reader]

with open(os.path.join(folder_name, 'genres.json'), 'w', encoding='utf-8') as f:
    json.dump(genres_data, f, ensure_ascii=False, indent=4)

print("All CSV files in the folder 'csv' have been converted to JSON format and saved in folder 'json'.")
print('JSON files created in ' + os.getcwd() + '/json')