import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
static_output_dir = os.environ.get("STATIC_MUNI_DATA")
data_dir = Path(static_output_dir) / "data"
routes_csv = Path(data_dir) / "routes.txt"

df = pd.read_csv(routes_csv)
trimmed_df = df.loc[df["agency_id"] == "SF", ["route_short_name", "route_long_name", "route_color"]]
print(trimmed_df)
trimmed_df.to_json(f"{Path(static_output_dir) / 'route_info.json'}", orient='records', compression='infer', index=False)