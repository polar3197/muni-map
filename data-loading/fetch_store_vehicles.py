from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv
import psycopg2.extras
import pandas as pd
import psycopg2
import requests
import json
import pytz
import os

# load muni api key from local .env
load_dotenv()
MUNI_API_KEY = os.getenv('MUNI_API_KEY')
muni_data_path = os.environ.get('HOT_MUNI_DATA')
psql_db_name = os.environ.get('TRANSIT_DB_NAME')
psql_username = os.environ.get('PSQL_USERNAME')

hot_output_dir = "/mnt/ssd/hot/"



try:
    # Fetch Protocol Buffer contents from MUNI API
    url = f"http://api.511.org/transit/vehiclepositions?api_key={MUNI_API_KEY}&agency=SF"
    response = requests.get(url)

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    time_fetched = datetime.now().isoformat()

    # set up connection to postgres db
    conn = psycopg2.connect(
        dbname=psql_db_name,
        user=psql_username,
    )
    print("Connection to PostgreSQL successful!")
    cur = conn.cursor()

    # Iterate through MUNI vehicles in operation
    count = 0
    # list of dictionaries for json file
    vehicles_for_map = []
    # list of list of values for database
    vehicles_for_db = []
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
            vehicles_for_map.append(vehicle)
            vehicles_list = []

    # for vehicle in data:
    #     # add tuple representing vehicle to list for bulk insert into psql vehicles table
        vehicles_for_db.append((
            dt_local.isoformat(), # vehicle.get('iso_timestamp')
            active, # vehicle.get('active'),
            trip_id, # vehicle.get('trip_id'),
            route_id, # vehicle.get('route_id'),
            direction_id, # vehicle.get('direction_id'),
            v.vehicle.id if v.HasField("vehicle") else None, # vehicle.get('vehicle_id'),
            lat, # vehicle.get('lat'),
            lon, # vehicle.get('lon'),
            bearing, # vehicle.get('bearing'),
            speed_mps, # vehicle.get('speed_mps'),
            current_stop_sequence, # vehicle.get('current_stop_sequence'),
            current_status, # vehicle.get('current_status'),
            stop_id, # vehicle.get('stop_id'),
            occupancy # vehicle.get('occupancy')
        ))
        
        # execute insertion of vehicle records to db 
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO vehicles (iso_timestamp, active, trip_id, route_id, direction_id, vehicle_id, lat, lon, bearing, speed_mps, current_stop_sequence, current_status, stop_id, occupancy)
        VALUES %s
        """,
        vehicles_for_db,
        template=None,
        page_size=100  # Insert in batches of 100
    )
    conn.commit()
    print(f"Successfully inserted {len(vehicles_for_db)} vehicles")

    if vehicles_for_map:
        # Write to disk
        with open(os.path.join(hot_output_dir, "map_data.json"), "w") as f:
            json.dump(vehicles_for_map, f, indent=2)
        print(f"[{datetime.now().isoformat()}] Saved {len(vehicles_for_map)} vehicles")

except psycopg2.Error as e:
    print(f"Database error: {e}")
    if 'conn' in locals():  # Only rollback if connection exists
        conn.rollback()
except Exception as e:
    print("Error:", e)
finally:
    # Clean up connections
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
