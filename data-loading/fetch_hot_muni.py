import pandas as pd
import os
from dotenv import load_dotenv
import json
import requests
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
import pytz

# load muni api key from local .env
load_dotenv()
MUNI_API_KEY = os.getenv('MUNI_API_KEY')
hot_output_dir = "/mnt/ssd/hot/"


try:
    # Fetch Protocol Buffer contents from MUNI API
    url = f"http://api.511.org/transit/vehiclepositions?api_key={MUNI_API_KEY}&agency=SF"
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
            active = True if trip_id else False
            
            # Handle stop_id - it might not be present
            stop_id = v.stop_id if v.HasField("stop_id") else None
            
            # You might also want to handle current_stop_sequence
            current_stop_sequence = v.current_stop_sequence if v.HasField("current_stop_sequence") else None
            
            # Handle current_status if it exists
            current_status = v.current_status if v.HasField("current_status") else None
            
            # Handle position data
            position = v.position if v.HasField("position") else None
            lat = position.latitude if position else None
            lon = position.longitude if position else None
            bearing = position.bearing if position and position.HasField("bearing") else None
            speed_mps = position.speed if position and position.HasField("speed") else None
            
            # Handle occupancy status if available
            occupancy = v.occupancy_status if v.HasField("occupancy_status") else None

            vehicle = {
                # time data
                "iso_timestamp": dt_local.isoformat(),
                "active": active,
                
                # trip data
                "trip_id": trip_id,
                "route_id": route_id,
                "direction_id": direction_id,

                # vehicle data
                "vehicle_id": v.vehicle.id if v.HasField("vehicle") else None,
                "lat": lat, 
                "lon": lon,
                "bearing": bearing,
                "speed_mps": speed_mps,
                "current_stop_sequence": current_stop_sequence,
                "current_status": current_status,
                "stop_id": stop_id,
                "occupancy": occupancy
            }
            vehicles.append(vehicle)

    if vehicles:
        # Write to disk
        with open(os.path.join(hot_output_dir, "map_data.json"), "w") as f:
            json.dump(vehicles, f, indent=2)
        print(f"[{datetime.now().isoformat()}] Saved {len(vehicles)} vehicles")

except Exception as e:
    print("Error:", e)
