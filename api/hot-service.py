from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import os

app = FastAPI()

# this allows cross-origin requests from anywhere
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

HOT_DATA_PATH = os.environ.get("HOT_DATA_PATH", "/mnt/ssd/hot/map_data.json")
static_output_dir = os.environ.get("STATIC_MUNI_DATA")
routes_csv_path = Path(data_dir) / "routes.txt"

@app.get("/hot-data")
async def get_hot_data():
    if not os.path.exists(HOT_DATA_PATH):
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    try:
        with open(HOT_DATA_PATH, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/route-info")
def get_route_info():
    if not os.path.exists(routes_csv_path):
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    try:
        with open(routes_csv_path, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
