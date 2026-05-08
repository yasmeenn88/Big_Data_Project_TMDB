import requests
import json
import os
import argparse
import subprocess
from datetime import datetime
from typing import List, Dict
import time
from pyspark.sql import SparkSession
import random

# Configuration
API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"
HDFS_RAW_PATH = os.getenv("HDFS_RAW_PATH", "hdfs://hadoop-namenode:9000/data/raw")
LOCAL_RAW_PATH = "/tmp/tmdb_data"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 2
RETRY_DELAY = 2

# Validation
if not API_KEY:
    raise ValueError("TMDB_API_KEY environment variable not set")

# Fetch Movies from TMDB API
def fetch_movies(pages: int = 10) -> List[Dict]:

    print(f" Fetching {pages} pages from TMDB API...")
    all_movies = []

    # كل الحطوات الجاية معمولة عشان أضمن قدر المستطاع أن كل مرة لما أعمل(API CALL) الأفلام متجيش متكررة
    sort_options = [
        "primary_release_date.desc",
        "vote_average.desc",
        "popularity.desc",
        "original_title.asc"
    ]
    selected_sort = random.choice(sort_options)


    day_of_month = datetime.now().day
    random_offset = random.randint(1, 30)
    start_page = (day_of_month * 10 + random_offset) % 490 + 1
    start_page = min(start_page, 500 - pages)

    print(f"  Sort by: {selected_sort}, starting from page: {start_page}")

    for page in range(start_page, start_page + pages):
        current_page = page if page <= 500 else page - 500

        url = f"{BASE_URL}/discover/movie"
        params = {
            "api_key": API_KEY,
            "page": current_page,
            "sort_by": selected_sort
        }

        for attempt in range(MAX_RETRIES):
            try:
                print(f"  Page {current_page}/{pages}... (Attempt {attempt + 1}/{MAX_RETRIES})")
                response = requests.get(
                    url,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()

                data = response.json()
                movies_in_page = data.get("results", [])
                all_movies.extend(movies_in_page)

                print(f" Fetched {len(movies_in_page)} movies from page {current_page}")
                break

            except requests.exceptions.RequestException as e:
                print(f" Attempt {attempt + 1} failed: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    print(f" Failed to fetch page {current_page} after {MAX_RETRIES} attempts")
                    continue

    print(f"\n  Total movies fetched: {len(all_movies)} across {pages} pages")
    print(f" Used sort: {selected_sort}")
    return all_movies

# Save Locally
def save_local(movies: List[Dict], filename: str = None) -> str:

    if filename is None:
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"tmdb_movies_{today}.json"

    os.makedirs(LOCAL_RAW_PATH, exist_ok=True)
    filepath = os.path.join(LOCAL_RAW_PATH, filename)

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(movies, f, indent=2, ensure_ascii=False)

        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"✓ Saved locally: {filepath} ({file_size_mb:.2f} MB)")
        return filepath

    except IOError as e:
        print(f" Error saving local file: {str(e)}")
        raise


def upload_to_hdfs(local_path: str, hdfs_filename: str = "tmdb_movies.json") -> bool:
    import requests as req

    print(f" Uploading to HDFS...")
    hdfs_full_path = f"{HDFS_RAW_PATH}/{hdfs_filename}"

    try:
        namenode_host = "hadoop-namenode"
        webhdfs_url = f"http://{namenode_host}:9870/webhdfs/v1/data/raw/{hdfs_filename}?op=CREATE&overwrite=true&user.name=root"

        # First request to get datanode URL
        response = req.put(webhdfs_url, allow_redirects=False)
        if response.status_code == 307:
            datanode_url = response.headers['Location']

            # Upload file
            with open(local_path, 'rb') as f:
                upload_response = req.put(datanode_url, data=f)

            if upload_response.status_code == 201:
                print(f"  ✓ Successfully uploaded to HDFS: {hdfs_full_path}")
                return True

        raise Exception(f"Failed to upload: {response.status_code}")

    except Exception as e:
        print(f" Error: {str(e)}")
        raise


# Main
def main():
    parser = argparse.ArgumentParser(description="Ingest TMDB data to HDFS")
    parser.add_argument("--pages", type=int, default=10, help="Number of pages to fetch")
    parser.add_argument("--hdfs-path", default=HDFS_RAW_PATH, help="HDFS output path")

    args = parser.parse_args()

    print(" TMDB DATA INGESTION PIPELINE ")
    print(f" Start time: {datetime.now().isoformat()}")
    print(f" Pages to fetch: {args.pages}")
    print(f" HDFS target: {args.hdfs_path}")

    try:
        # Step 1: Fetch from API
        movies = fetch_movies(pages=args.pages)

        if not movies:
            raise ValueError(" No movies fetched from API ")

        # Step 2: Save locally
        local_file = save_local(movies)

        # Step 3: Upload to HDFS using hdfs dfs -put (no Spark needed)
        upload_to_hdfs(local_file, "tmdb_movies.json")

        print("  INGESTION COMPLETED SUCCESSFULLY!")
        print(f" Total records: {len(movies)}")
        print(f" Local file: {local_file}")
        print(f" HDFS path: {args.hdfs_path}/tmdb_movies.json")
        print(f" End time: {datetime.now().isoformat()}")

    except Exception as e:
        print(f"\n INGESTION FAILED: {str(e)}")
        raise

if __name__ == "__main__":
    main()