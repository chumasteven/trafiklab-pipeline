import os
import zipfile
import tempfile

from dotenv import load_dotenv
import requests
from google.cloud import storage

load_dotenv()

api_key = os.getenv("TRAFIKLAB_REGIONAL_STATIC_API_KEY")

client = storage.Client.from_service_account_json(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
bucket = client.bucket("trafiklab-raw-data")

NEEDED_FILES = ["stops.txt", "trips.txt", "routes.txt"]

# Stream to a temp file on disk instead of buffering the whole zip in memory
with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
    tmp_path = tmp.name
    with requests.get(f"https://opendata.samtrafiken.se/gtfs/ul/ul.zip?key={api_key}", stream=True, timeout=60) as response:
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            tmp.write(chunk)

try:
    with zipfile.ZipFile(tmp_path) as archive:
        for filename in NEEDED_FILES:
            data = archive.read(filename)
            blob = bucket.blob(f"gtfs_static/latest/{filename}")
            blob.upload_from_string(data, content_type="text/csv")
            print(f"Uploaded {filename} to gtfs_static/latest/{filename}")
finally:
    os.remove(tmp_path)
