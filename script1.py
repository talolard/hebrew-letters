import os
import csv
import sqlite3
from typing import Optional

import requests
import json
from logging import getLogger

logger = getLogger(__name__)

# Pexels API setup
PEXELS_API_KEY = "A54Lfea4eJ00YlaxvcqcMyRAloBNAMf7osn4DJlNra9kIBoe8dPqGosg"
PEXELS_API_URL = "https://api.pexels.com/v1/search"

# Directory paths
MEDIA_DIR = "media"
DB_PATH = "db/database.db"

# Ensure media directory exists
os.makedirs(MEDIA_DIR, exist_ok=True)


def search_pexels(query, num_results=10):
    logger.info(f"Searching Pexels for: {query}")
    headers = {"Authorization": PEXELS_API_KEY}
    params = {"query": query, "per_page": num_results}
    response = requests.get(PEXELS_API_URL, headers=headers, params=params)
    if response.status_code == 200:
        logger.info("Successfully fetched data from Pexels")
        return response.json().get("photos", [])
    else:
        logger.error(f"Error fetching data from Pexels: {response.status_code}")
        return []


def save_images(photos, base_filename):
    file_paths = []
    logger.info(f"Saving {len(photos)} images")
    for i, photo in enumerate(photos):
        image_url = photo["src"]["original"]
        filename = f"{base_filename}_{i + 1}.jpg"
        filepath = os.path.join(MEDIA_DIR, filename)
        with requests.get(image_url, stream=True) as response:
            logger.info(f"Downloading image: {image_url}")
            if response.status_code == 200:
                logger.info(f"Successfully downloaded image: {image_url}")
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                logger.info(f"Saved image to: {filepath}")
                file_paths.append(filepath)
            else:
                logger.error(f"Error downloading image: {image_url}")
    return file_paths


# function should_skip_word checks the db if a word is already present in the db
def should_skip_word(cursor, word):
    cursor.execute(
        """
        SELECT COUNT(*) FROM media_mapping WHERE english_translation = ?
    """,
        (word,),
    )
    return cursor.fetchone()[0] > 0


def process_csv(file_path, conn, cursor, limit: Optional[int] = None):
    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for num, row in enumerate(reader):
            if limit and num >= limit:
                logger.info(f"Reached limit of {limit} rows")
                break
            elif should_skip_word(cursor, row["EnglishTranslation"]):
                logger.info(f"Skipping word: {row['EnglishTranslation']}")
                continue
            english_translation = row["EnglishTranslation"]
            photos = search_pexels(english_translation)
            logger.info(f"Found {len(photos)} images for: {english_translation}")
            file_paths = save_images(photos, english_translation.replace(" ", "_"))
            cursor.execute(
                """
                INSERT INTO media_mapping (
                    letter, hebrew_word, hebrew_word_with_nikud,
                    english_translation, german_translation, file_paths
                ) VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    row["letter"],
                    row["HebrewWord"],
                    row["HebrewWordwithNikud"],
                    english_translation,
                    row["GermanTranslation"],
                    json.dumps(file_paths),
                ),
            )
            logger.info("Inserted row into database")
            conn.commit()


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS media_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        letter TEXT,
        hebrew_word TEXT,
        hebrew_word_with_nikud TEXT,
        english_translation TEXT,
        german_translation TEXT,
        file_paths TEXT
    )
    """)
    conn.commit()

    process_csv("./hebrewLetterPlan.csv", conn, cursor)
    conn.close()


if __name__ == "__main__":
    main()
