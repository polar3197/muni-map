
from dotenv import load_dotenv
import asyncpg
import asyncio
import os

load_dotenv()

async def main():
    psql_db_name = os.environ.get('TRANSIT_DB_NAME')
    psql_username = os.environ.get('PSQL_USERNAME')
    
    conn = await asyncpg.connect(
        database=psql_db_name,
        user=psql_username,
    )
    print("Connection to PostgreSQL successful!")
    
    # Do your queries
    rows = await conn.fetch("""
        SELECT route_id, speed_mph 
        FROM vehicles 
        ORDER BY speed_mph DESC 
        LIMIT 5
    """)
    for r in rows:
        print(f"route_id: {r['route_id']}, speed: {float(r['speed_mph'])}")
    await conn.close()

# Run the async function
if __name__ == "__main__":
    asyncio.run(main())