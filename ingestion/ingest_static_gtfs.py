import os

from dotenv import load_dotenv
import requests
from google.cloud import storage
from datetime import datetime

load_dotenv()

# Read the API key from the environment variable
api_key = os.getenv("TRAFIKLAB_REGIONAL_STATIC_API_KEY")

# configure the path to your Google Cloud credentials
google_credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# Fetch GTFS static data
response = requests.get(f"https://opendata.samtrafiken.se/gtfs/ul/ul.zip?key={api_key}", timeout=30)
response.raise_for_status()  # Check if the request was successful
file_name = f"gtfs_static_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.zip" # Create a unique file name based on the current timestamp
client = storage.Client.from_service_account_json(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
bucket = client.bucket("trafiklab-raw-data")
blob = bucket.blob(file_name)
blob.upload_from_string(response.content,content_type='application/zip')
print(f"GTFS static data fetched and stored as {file_name} in Google Cloud Storage.")
