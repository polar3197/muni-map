



funcs = {}

def avg_busses(route_id)
    '''
    WITH daily_counts AS (
        SELECT DATE(iso_timestamp) AS day,
        EXTRACT(hour FROM iso_timestamp) AS hour,
        FLOOR(EXTRACT(minute FROM iso_timestamp) / 10) * 10 AS minute_bin,
        COUNT(DISTINCT(vehicle_id)) AS num_buses
        FROM vehicles
        WHERE route_id = '33'
        GROUP BY day, hour, minute_bin
    )
    SELECT hour, 
            minute_bin, 
            ROUND(AVG(num_buses), 2) as avg_num_buses,
            ROUND(STDDEV(num_buses), 2) AS std_dev
        FROM daily_counts
        GROUP BY hour, minute_bin
        ORDER BY hour, minute_bin;
    '''

def main(query_name):
    sql = funcs[query_name]()
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