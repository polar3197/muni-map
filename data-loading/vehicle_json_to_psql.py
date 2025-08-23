from dotenv import load_dotenv
import psycopg2.extras
import psycopg2
import json
import os

# load vars from dotenv
load_dotenv()

# want to read from PATH_TO_HOT_MUNI_DATA
muni_data_path = os.environ.get('HOT_MUNI_DATA')
psql_db_name = os.environ.get('TRANSIT_DB_NAME')
psql_username = os.environ.get('PSQL_USERNAME')

print(psql_username, psql_db_name)

with open(muni_data_path) as vehicle_json:
    data = json.load(vehicle_json)

# attempt to connect to psql database
try:
    conn = psycopg2.connect(
        dbname=psql_db_name,
        user=psql_username,
    )
    print("Connection to PostgreSQL successful!")

    cur = conn.cursor()
    vehicles_list = []

    for vehicle in data:
        # add tuple representing vehicle to list for bulk insert into psql vehicles table
        vehicles_list.append((
            vehicle.get('iso_timestamp'),
            vehicle.get('active'),
            vehicle.get('trip_id'),
            vehicle.get('route_id'),
            vehicle.get('direction_id'),
            vehicle.get('vehicle_id'),
            vehicle.get('lat'),
            vehicle.get('lon'),
            vehicle.get('bearing'),
            vehicle.get('speed_mps'),
            vehicle.get('current_stop_sequence'),
            vehicle.get('current_status'),
            vehicle.get('stop_id'),
            vehicle.get('occupancy')
        ))
        # if not vehicle["occupancy"]:
        #     print(vehicle["route_id"], vehicle["trip_id"])


    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO vehicles (iso_timestamp, active, trip_id, route_id, direction_id, vehicle_id, lat, lon, bearing, speed_mps, current_stop_sequence, current_status, stop_id, occupancy)
        VALUES %s
        """,
        vehicles_list,
        template=None,
        page_size=100  # Insert in batches of 100
    )
    conn.commit()
    print(f"Successfully inserted {len(vehicles_list)} vehicles")
except psycopg2.Error as e:
    print(f"Database error: {e}")
    if 'conn' in locals():  # Only rollback if connection exists
        conn.rollback()
except Exception as e:
    print(f"General error: {e}")
finally:
    # Clean up connections
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()

# row_one = data[0]
# print(json.dumps(row_one, indent=2))

# parse what is read into a python dict (with json dump?)

# format data according to columns of postgres vehicles table

# open connection to my postgres db

# insert bulk vehicle data into postgres 'transit' db into 'vehicles' tabl