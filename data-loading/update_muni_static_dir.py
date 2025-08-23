# This file will fetch the zip file from SFMTA's GTFS feed
# run a subprocess to remove the outdated (1 month) files
# place unzipped files into same location

import os
import json
import zipfile
import requests
import subprocess
from pathlib import Path
from google.transit import gtfs_realtime_pb2

static_output_dir = os.environ.get("STATIC_MUNI_DATA")
temp_zip_path = Path(static_output_dir) / "temp_gtfs.zip"
data_dir = Path(static_output_dir) / "data"

api_key = os.getenv('API_511_KEY')
if not api_key:
    raise ValueError("API_511_KEY environment variable not set")

url = f"http://api.511.org/transit/datafeeds?api_key={api_key}&operator_id=RG"

try:
    # Fetch zip file from MUNI API
    response = requests.get(url, timeout=30)

    with open(temp_zip_path, "wb") as binary_file:
        binary_file.write(response.content)
    print(f"Downloaded {len(response.content)} bytes")

    # clear out old data
    if data_dir.exists():
        subprocess.run(f"rm -rf {data_dir}/*", shell=True)

    # unzip new data into same place
    with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
        zip_ref.extractall(f"{static_output_dir}/data/")
    print(f"Extracted GTFS files to {data_dir}")

except Exception as e:
    print("Error:", e)