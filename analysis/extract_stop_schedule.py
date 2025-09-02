import pandas as pd
import os
import typing
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
psql_db_name = os.environ.get("TRANSIT_DB_NAME")
psql_username = os.environ.get("PSQL_USERNAME")
static_output_dir = os.environ.get("STATIC_MUNI_DATA")
data_dir = Path(static_output_dir) / "data"


######### ROUTES ###########
'''

'''
def get_routes_sql():
    routes_csv = Path(data_dir) / "routes.txt"

    routes = pd.read_csv(routes_csv)
    trimmed_df = routes.loc[routes["agency_id"] == "SF", ["route_short_name", "route_long_name", "route_color"]]
    print(trimmed_df)
#################################

######### STOPS ###########
'''
['stop_id', 'stop_name', 'stop_code', 'stop_desc', 'stop_lat',
 'stop_lon', 'zone_id', 'stop_url', 'tts_stop_name', 'platform_code',
 'location_type', 'parent_station', 'stop_timezone',
 'wheelchair_boarding', 'level_id']
'''
def get_stops_sql():
    stops_csv = Path(data_dir) / "stops.txt"
#################################

######### STOP TIMES ###########
'''
['trip_id', 'stop_id', 'stop_sequence', 'stop_headsign', 'arrival_time',
 'departure_time', 'pickup_type', 'drop_off_type', 'continuous_pickup',
 'continuous_drop_off', 'shape_dist_traveled', 'timepoint']
'''
def get_stop_times_sql():
    es_csv = Path(data_dir) / "stop_times.txt"

    df_st = pd.read_csv(stop_times_csv)
    print(df_st.columns)
    trimmed_df = df_st.loc[df_st['trip_id'].str.startswith('SF'), ['trip_id', 'stop_id', 'stop_sequence', 'stop_headsign', 'arrival_time',
        'departure_time', 'pickup_type', 'drop_off_type', 'shape_dist_traveled', 'timepoint']]
#################################

######### TRIPS ###########
'''
 ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'trip_short_name',
  'direction_id', 'block_id', 'shape_id', 'wheelchair_accessible', 'bikes_allowed']
'''
def get_trips_sql():
    trips_csv = Path(data_dir) / "trips.txt"

    trips = pd.read_csv(trips_csv)
    trips = trips.loc[trips['trip_id'].str.startswith('SF'),  ['route_id', 'trip_id', 'trip_headsign',
    'direction_id', 'block_id', 'shape_id', 'wheelchair_accessible']]
    print(trips.head(50))

    try:
        conn = psycopg2.connect(dbname=psql_db_name, user=psql_username)
        print("Connection to PostgreSQL successful!")
        cur = conn.cursor()
        
        for i, r in trips.iterrows():
            trip_id = r['trip_id']
            route_id = r['route_id']
            shape_id = r['shape_id']
            trip_headsign = r['trip_headsign']
            
            cur.execute("""
                INSERT INTO trips (trip_id, route_id, shape_id, trip_headsign)
                VALUES (%s, %s, %s, %s)
            """, (trip_id, route_id, shape_id, trip_headsign))

        conn.commit()

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
    
#################################

######### SHAPES ###########
'''
['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence', 'shape_dist_traveled']
'''
def get_shape_sql():
    shapes_csv = Path(data_dir) / "shapes.txt"
    shapes = pd.read_csv(shapes_csv)
    shapes = shapes[shapes['shape_id'].str.startswith('SF')]
    values = []

    print(shapes.head(40))

    try:
        conn = psycopg2.connect(dbname=psql_db_name, user=psql_username)
        print("Connection to PostgreSQL successful!")
        cur = conn.cursor()
        
        for shape_id in shapes['shape_id'].unique():
            total_distance = shapes[shapes['shape_id'] == shape_id]['shape_dist_traveled'].max()
            pts = shapes[shapes['shape_id'] == shape_id].sort_values('shape_pt_sequence')
            linestring = f"LINESTRING({', '.join([f'{r.shape_pt_lon} {r.shape_pt_lat}' for r in pts.itertuples()])})"
            
            cur.execute("""
                INSERT INTO shapes (shape_id, route_line, total_distance)
                VALUES (%s, ST_GeomFromText(%s, 4326), %s)
            """, (shape_id, linestring, total_distance))

        conn.commit()

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
    

#################################
    

def main():
    get_trips_sql()
    return

if __name__ == "__main__":
    main()