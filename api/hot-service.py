from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from pathlib import Path
from databases import Database
from dotenv import load_dotenv

load_dotenv()
HOT_MUNI_DATA = os.getenv("HOT_MUNI_DATA")

app = FastAPI()

# this allows cross-origin requests from anywhere
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "MUNI API is running", "hot_data_path": HOT_MUNI_DATA}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "hot_data_path": HOT_MUNI_DATA,
        "file_exists": os.path.exists(HOT_MUNI_DATA) if HOT_MUNI_DATA else False
    }

@app.get("/debug")
async def debug():
    return {
        "HOT_MUNI_DATA": HOT_MUNI_DATA,
        "file_exists": os.path.exists(HOT_MUNI_DATA) if HOT_MUNI_DATA else False,
        "cwd": os.getcwd(),
        "env_vars": dict(os.environ)
    }

@app.get("/hot-data")
async def get_hot_data():
    if not os.path.exists(HOT_MUNI_DATA):
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    try:
        with open(HOT_MUNI_DATA, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

