import pandas as pd
import time
import os
import subprocess
import json
import requests
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
import pytz

hot_output_dir = "/mnt/ssd/hot/"
intermediate_output_dir = "/mnt/ssd/otwt_quentin/"

# to track every third pull (180s) and save to longterm storage
pull_count = 0

while True:
    try:
        # Fetch Protocol Buffer contents from MUNI API
        url = "http://api.511.org/transit/vehiclepositions?api_key=59a9ae05-3f9c-440e-8343-e3256be79b84&agency=SF"
        response = requests.get(url)

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        time_fetched = datetime.now().isoformat()

        # Iterate through MUNI vehicles in operation
        count = 0
        vehicles = []
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                v = entity.vehicle

                # collect time information
                dt = datetime.fromtimestamp(v.timestamp, tz=timezone.utc)
                local_tz = pytz.timezone("America/Los_Angeles")
                dt_local = dt.astimezone(local_tz)
                dt_local_12hr = dt_local.strftime("%I:%M:%S %p")

                # grab trip data, null if no current trip
                t = v.trip if v.HasField("trip") else None
                trip_id = t.trip_id if t else None
                route_id = t.route_id if t else None
                direction_id = t.direction_id if t else None
                start_date = t.start_date if t else None
                schedule_relationship = t.schedule_relationship if t else None

                vehicle = {
                    # time data
                    "timestamp_iso": dt_local.isoformat(),
                    "year": dt_local.year,
                    "month": dt_local.month,
                    "day": dt_local.day,
                    "hour": dt_local.hour,
                    "minute": dt_local.minute,
                    
                    # trip data
                    "trip_id": trip_id,
                    "route_id": route_id,
                    "direction_id": direction_id,
                    "start_date": start_date,
                    "schedule_relationship": schedule_relationship,

                    # vehicle data
                    "vehicle_id": v.vehicle.id,
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    #"bearing": v.position.bearing,
                    "speed_mps": v.position.speed,
                    "current_stop_sequence": v.current_stop_sequence,
                    "current_status": v.current_status,
                    "stop_id": v.stop_id,
                    "occupancy_status": v.occupancy_status
                }
                vehicles.append(vehicle)

        if vehicles:
            if pull_count % 3 == 0:
                latest_datetime = max(datetime.fromisoformat(v["timestamp_iso"]) for v in vehicles)
                timestamp_str = latest_datetime.strftime("%Y%m%d_%H%M%S")
                parquet_filename = f"vehicles_{timestamp_str}.parquet"
                df = pd.DataFrame(vehicles)
                # translate to parquet
                df.to_parquet(os.path.join(intermediate_output_dir, parquet_filename), engine="pyarrow")
                
                # Send to Quentin using rsync
                result = subprocess.run([
                    "rsync", "-av", "--remove-source-files",
                    intermediate_output_dir,
                    "charlie@quentin.local:/mnt/ssd/raw/vehicle/"
                ])

                if result.returncode == 0:
                    print(f"[{datetime.now().isoformat()}] Synced to Quentin and removed local file.")
                else:
                    print(f"[{datetime.now().isoformat()}] rsync failed with code {result.returncode}. File retained.")
                
                pull_count = 0

            # Write to disk
            with open(os.path.join(hot_output_dir, "map_data.json"), "w") as f:
                json.dump(vehicles, f, indent=2)
            print(f"[{datetime.now().isoformat()}] Saved {len(vehicles)} vehicles")

        # mark another successful pull
        pull_count += 1
    except Exception as e:
        print("Error:", e)
    time.sleep(60)  # wait 60 seconds before next fetch
