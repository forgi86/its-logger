import requests
from datetime import datetime
import pytz
from pathlib import Path
import time
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# URLs
static_data_url = "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/data/ch.bfe.ladestellen-elektromobilitaet.json"
dynamic_data_url = "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/status/ch.bfe.ladestellen-elektromobilitaet.json"

# Paths
DATA_FOLDER = "data"
CHARGE_FOLDER = "charge"
BASE_SLEEP = 30


charge_path = Path(DATA_FOLDER) / CHARGE_FOLDER
charge_path.mkdir(parents=True, exist_ok=True)

# Persistent dictionary across runs
last_status = {}

# HTTP session for efficiency
session = requests.Session()

schema = pa.schema([
    ("STATION_ID", pa.string()),
    ("STATUS", pa.string()),
    ("TIME", pa.timestamp("s", tz="Europe/Zurich")),
    ("DATE", pa.date32())
])

while True:
    try:
        # Fetch dynamic status data
        response = session.get(dynamic_data_url)
        response.raise_for_status()
        dynamic_data = response.json()

        # Timezone-aware timestamp (Europe/Zurich)
        timestamp = pd.Timestamp.now(tz="Europe/Zurich").floor("s")
        print("Data fetched at:", timestamp.isoformat())

        # ---------------------------------------------------------
        # 1. Extract only changed statuses
        # ---------------------------------------------------------
        rows = []

        for operator in dynamic_data["EVSEStatuses"]:
            for record in operator["EVSEStatusRecord"]:
                sid = record["EvseID"]
                status = record["EVSEStatus"]

                if last_status.get(sid) != status:
                    rows.append({
                        "STATION_ID": sid,
                        "STATUS": status,
                        "TIME": timestamp
                    })
                    last_status[sid] = status

        print(len(rows), "status changes detected.")

        # ---------------------------------------------------------
        # 2. Write only if something changed
        # ---------------------------------------------------------
        if rows:
            df = pd.DataFrame(rows)

            df["STATUS"] = df["STATUS"].astype("category")
            df["TIME"] = timestamp
            df["DATE"] = df["TIME"].dt.date

            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

            pq.write_to_dataset(
                table,
                root_path=charge_path,
                partition_cols=["DATE"],
                compression="zstd"
            )

    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error during update:", e)
        
    # ---------------------------------------------------------
    # 3. Sleep with jitter to avoid synchronized polling
    # ---------------------------------------------------------

    jitter = random.uniform(-2, 2)  # +/- 2 seconds
    time.sleep(max(1, BASE_SLEEP + jitter))
