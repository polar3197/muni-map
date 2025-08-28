from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
from pathlib import Path
from dotenv import load_dotenv
import psycopg2.extras
import pandas as pd
import psycopg2
import requests
import json
import pytz
import os

# Load environment variables
load_dotenv()
MUNI_API_KEY = os.getenv('MUNI_API_KEY')
muni_data_path = os.environ.get('HOT_MUNI_DATA')
psql_db_name = os.environ.get('TRANSIT_DB_NAME')
psql_username = os.environ.get('PSQL_USERNAME')
static_output_dir = os.environ.get("STATIC_MUNI_DATA")

# File paths
routes_json = Path(static_output_dir) / "route_info.json"
hot_output_dir = "/mnt/ssd/hot/"

# Load route data for lookups
with open(routes_json, 'r') as f:
    routes_data = json.load(f)

# Convert to lookup dictionary for better performance
routes_lookup = {route['route_short_name']: route for route in routes_data}
print(f"Loaded {len(routes_lookup)} routes for lookup")

def get_route_info(route_id):
    """Get route name and color with fallbacks"""
    route_info = routes_lookup.get(route_id, {})
    return {
        'name': route_info.get('route_long_name', route_id),
        'color': route_info.get('route_color', '999999')
    }

def extract_vehicle_data(entity):
    """Extract vehicle data from GTFS-RT entity"""
    if not entity.HasField("vehicle"):
        return None
        
    v = entity.vehicle
    
    # Time conversion
    dt = datetime.fromtimestamp(v.timestamp, tz=timezone.utc)
    dt_local = dt.astimezone(pytz.timezone("America/Los_Angeles"))
    
    # Trip data
    trip = v.trip if v.HasField("trip") else None
    trip_id = getattr(trip, 'trip_id', None) if trip else None
    route_id = getattr(trip, 'route_id', None) if trip else None
    direction_id = getattr(trip, 'direction_id', None) if trip else None
    
    # Position data
    pos = v.position if v.HasField("position") else None
    lat = getattr(pos, 'latitude', None) if pos else None
    lon = getattr(pos, 'longitude', None) if pos else None
    bearing = getattr(pos, 'bearing', None) if pos and pos.HasField("bearing") else None
    speed_mph = getattr(pos, 'speed', None) if pos and pos.HasField("speed") else None
    
    # Vehicle status
    vehicle_id = getattr(v.vehicle, 'id', None) if v.HasField("vehicle") else None
    stop_id = getattr(v, 'stop_id', None) if v.HasField("stop_id") else None
    current_stop_sequence = getattr(v, 'current_stop_sequence', None) if v.HasField("current_stop_sequence") else None
    current_status = getattr(v, 'current_status', None) if v.HasField("current_status") else None
    occupancy = getattr(v, 'occupancy_status', None) if v.HasField("occupancy_status") else None
    
    # Route metadata
    route_info = get_route_info(route_id) if route_id else {'name': None, 'color': None}
    
    return {
        'timestamp': dt_local.isoformat(),
        'active': bool(trip_id),
        'trip_id': trip_id,
        'route_id': route_id,
        'direction_id': direction_id,
        'vehicle_id': vehicle_id,
        'lat': lat,
        'lon': lon,
        'bearing': bearing,
        'speed_mph': speed_mph,
        'current_stop_sequence': current_stop_sequence,
        'current_status': current_status,
        'stop_id': stop_id,
        'occupancy': occupancy,
        'route_name': route_info['name'],
        'route_color': route_info['color']
    }

try:
    # Fetch Protocol Buffer from MUNI API
    url = f"http://api.511.org/transit/vehiclepositions?api_key={MUNI_API_KEY}&agency=SF"
    response = requests.get(url)
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    # Database connection
    conn = psycopg2.connect(dbname=psql_db_name, user=psql_username)
    print("Connection to PostgreSQL successful!")
    cur = conn.cursor()

    # Process vehicles
    vehicles_for_map = []
    vehicles_for_db = []
    
    for entity in feed.entity:
        vehicle_data = extract_vehicle_data(entity)
        if vehicle_data:
            # For JSON file (frontend)
            vehicles_for_map.append(vehicle_data)
            
            # For database (tuple format)
            vehicles_for_db.append((
                vehicle_data['timestamp'],
                vehicle_data['active'],
                vehicle_data['trip_id'],
                vehicle_data['route_id'],
                vehicle_data['direction_id'],
                vehicle_data['vehicle_id'],
                vehicle_data['lat'],
                vehicle_data['lon'],
                vehicle_data['bearing'],
                vehicle_data['speed_mph'],
                vehicle_data['current_stop_sequence'],
                vehicle_data['current_status'],
                vehicle_data['stop_id'],
                vehicle_data['occupancy']
            ))

    # Insert to database
    if vehicles_for_db:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO vehicles (iso_timestamp, active, trip_id, route_id, direction_id, 
                                vehicle_id, lat, lon, bearing, speed_mph, current_stop_sequence, 
                                current_status, stop_id, occupancy)
            VALUES %s
            """,
            vehicles_for_db,
            page_size=100
        )
        conn.commit()
        print(f"Successfully inserted {len(vehicles_for_db)} vehicles")

    # Write enriched JSON for frontend
    if vehicles_for_map:
        with open(os.path.join(hot_output_dir, "map_data.json"), "w") as f:
            json.dump(vehicles_for_map, f, indent=2)
        print(f"[{datetime.now().isoformat()}] Saved {len(vehicles_for_map)} vehicles with route info")

except requests.RequestException as e:
    print(f"API request error: {e}")
except psycopg2.Error as e:
    print(f"Database error: {e}")
    if 'conn' in locals():
        conn.rollback()
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    # Clean up connections
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()