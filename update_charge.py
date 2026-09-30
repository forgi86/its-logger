import requests
from datetime import datetime
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
JITTER = False


charge_path = Path(DATA_FOLDER) / CHARGE_FOLDER
charge_path.mkdir(parents=True, exist_ok=True)

# Persistent dictionary across runs
last_status = {}
current_date = None

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

        today = timestamp.date()
        force_snapshot = current_date is None or today != current_date

        # ---------------------------------------------------------
        # 1. Collapse to one status per EVSE. The feed lists a few
        #    EvseIDs under more than one operator (e.g. swisscharge
        #    re-listing third-party points), often with conflicting
        #    statuses. Prefer the owner's copy (EvseID starts with its
        #    OperatorID), then a known status over "Unknown".
        # ---------------------------------------------------------
        current_status = {}  # sid -> (status, is_owner)
        listings = {}  # sid -> statuses from every operator listing it

        for operator in dynamic_data["EVSEStatuses"]:
            operator_id = operator.get("OperatorID") or ""
            for record in operator["EVSEStatusRecord"]:
                sid = record["EvseID"]
                status = record["EVSEStatus"]
                is_owner = bool(operator_id) and sid.startswith(operator_id)
                listings.setdefault(sid, []).append(status)

                prev = current_status.get(sid)
                if prev is None or (is_owner, status != "Unknown") > (prev[1], prev[0] != "Unknown"):
                    current_status[sid] = (status, is_owner)

        # Normally ~5; a jump signals a feed incident (e.g. March 2026: 4,163)
        n_dup = sum(len(s) > 1 for s in listings.values())
        n_conflict = sum(len(set(s)) > 1 for s in listings.values())
        print(n_dup, "EvseIDs listed more than once,", n_conflict, "with conflicting statuses.")

        # ---------------------------------------------------------
        # 2. Extract changed statuses (or every status on the first
        #    poll of the day / after a restart, as a full snapshot)
        # ---------------------------------------------------------
        rows = []

        for sid, (status, _) in current_status.items():
            if force_snapshot or last_status.get(sid) != status:
                rows.append({
                    "STATION_ID": sid,
                    "STATUS": status,
                    "TIME": timestamp
                })
                last_status[sid] = status

        # EVSEs seen before but absent from this poll: record them as
        # "Missing" once, so readers don't keep their last status forever.
        # Forgetting them makes a reappearance count as a change.
        # The feed should always be complete, so this is itself worth logging.
        missing = [sid for sid in last_status if sid not in current_status]
        for sid in missing:
            rows.append({
                "STATION_ID": sid,
                "STATUS": "Missing",
                "TIME": timestamp
            })
            del last_status[sid]
        if missing:
            print(len(missing), "EvseIDs dropped out of the feed (marked Missing):", ", ".join(missing[:5]) + (" ..." if len(missing) > 5 else ""))

        current_date = today

        print(len(rows), "status changes detected." if not force_snapshot else "rows written (full snapshot).")

        # ---------------------------------------------------------
        # 3. Write only if something changed
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
    # 4. Sleep with jitter to avoid synchronized polling
    # ---------------------------------------------------------

    jitter = random.uniform(-2, 2) if JITTER else 0 # +/- 2 seconds
    time.sleep(max(1, BASE_SLEEP + jitter))
