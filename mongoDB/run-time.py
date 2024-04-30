import time
from pymongo import MongoClient
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Establish a connection to the MongoDB database
    client = MongoClient('localhost', 27017)
    db = client['music_library']
    logger.info("Connected to MongoDB database!")

     # Define the aggregation pipelines
    pipeline_simple = [
    {
        "$project": {
            "_id": 0,
            "song_id": 1,
            "song_title": 1,
            "duration": 1,
            "release_date": 1,
            "artist_id": 1,
            "album_id": 1,
            "genre_id": 1,
            "lyrics": 1
        }
    }
    ]

    pipeline_intermediate = [
    {
        "$match": {
            "release_date": {"$regex": "^2021"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "song_title": 1,
            "artist_id": 1,
            "release_date": 1
        }
    }
    ]

    pipeline_complex = [
    {
        "$lookup": {
            "from": "artists",
            "localField": "artist_id",
            "foreignField": "_id",
            "as": "artist"
        }
    },
    {
        "$unwind": "$artist"
    },
    {
        "$lookup": {
            "from": "albums",
            "localField": "album_id",
            "foreignField": "_id",
            "as": "album"
        }
    },
    {
        "$unwind": "$album"
    },
    {
        "$match": {
            "release_date": {"$regex": "^2023"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "song_title": 1,
            "artist_name": "$artist.artist_name",
            "album_name": "$album.album_name"
        }
    }
    ]

    pipeline_advanced = [
    {
        "$lookup": {
            "from": "artists",
            "localField": "artist_id",
            "foreignField": "_id",
            "as": "artist"
        }
    },
    {
        "$unwind": "$artist"
    },
    {
        "$match": {
            "release_date": {"$regex": "^2022"}
        }
    },
    {
        "$group": {
            "_id": {
                "song_title": "$song_title",
                "artist_name": "$artist.artist_name",
                "release_date": "$release_date"
            },
            "count": {"$sum": 1}
        }
    },
    {
        "$match": {
            "count": {"$gt": {"$avg": "$count"}}
        }
    },
    {
        "$sort": {"_id.release_date": -1}
    },
    {
        "$project": {
            "song_title": "$_id.song_title",
            "artist_name": "$_id.artist_name",
            "release_date": "$_id.release_date",
            "_id": 0
        }
    }
    ]

    # Create a folder named "run-time-results" if it doesn't exist
    folder_name = "run-time-results"
    folder_name = os.path.join("mongoDB", "run-time-results")
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        logger.info("Created folder 'run-time-results' inside 'mongo' folder")

    # Define the number of times to execute each query
    num_executions = 31

    # Iterate through the queries and execute each one
    for i, pipeline in enumerate([pipeline_simple, pipeline_intermediate, pipeline_complex, pipeline_advanced]):
        logger.info(f"Executing query {i+1}/{len(pipeline)}...")

        # Open a new file for writing with a different name for each query
        with open(os.path.join(folder_name, f"runtime_q{i+1}.txt"), "w") as file:
            # Execute the query and record the runtime for each execution
            for j in range(num_executions):
                start_time = time.time()
                result = db.songs.aggregate(pipeline)
                end_time = time.time()
                runtime = end_time - start_time
                # Write the runtime to the file
                file.write(f"{j+1}. {runtime:.4f} sec\n")

        logger.info(f"Query {i+1} done!")

except Exception as e:
    logger.error(f"Error: {e}")

finally:
    # Close the connection
    client.close()
    logger.info("Connection closed!")
