# Fetches and parses pb file from GTFS into df and then parquet 
# and stores in $RT_MUNI_PATH

from datetime import datetime, timezone
import pandas as pd
import requests
import json
import os

output_dir = '/mnt/ssd/weather/'

try:
    url = "https://api.openweathermap.org/data/2.5/weather?lat=37.7749&lon=-122.4194&appid=b238d81d1a09771d9a8fed72e8db6fd6"
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Weather API call failed: {response.status_code} - {response.text}")
    
    weather_dict = response.json()
    local_dt = datetime.fromtimestamp(weather_dict["dt"])
    weather_dict["timestamp_iso"] = local_dt.isoformat()
    with open(os.path.join(output_dir, "curr_weather.json"), 'w') as f:
        json.dump(weather_dict, f, indent=2)

except Exception as e:
    print("Error:", e)