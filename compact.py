import duckdb
import os
from datetime import datetime, date
import pytz
from pathlib import Path


DATA_FOLDER = "data"
CHARGE_FOLDER = "charge"
DATA_PATH = Path(DATA_FOLDER) / CHARGE_FOLDER
DATA_PATH.mkdir(parents=True, exist_ok=True)



def compact_day(date_folder: str):
    folder_path = DATA_PATH / date_folder
    parquet_files = list(folder_path.glob("*.parquet"))

    if not parquet_files:
        return

    output_file = folder_path / "merged.parquet"
    if output_file.exists():
        return

    tmp_file = folder_path / "merged.tmp.parquet"

    with duckdb.connect() as con:
        # Explicitly cast columns
        con.execute(f"""
            COPY (
                SELECT 
                    CAST(STATION_ID AS VARCHAR) AS STATION_ID,
                    CAST(STATUS AS VARCHAR) AS STATUS,
                    CAST(TIME AS TIMESTAMP) AS TIME,
                FROM parquet_scan('{folder_path}/*.parquet')
                WHERE filename != '{output_file}'
                ORDER BY TIME, STATION_ID
            )
            TO '{tmp_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

    os.rename(tmp_file, output_file)

    for f in parquet_files:
        if f != output_file:
            f.unlink()


def main():
    today = datetime.now(pytz.timezone("Europe/Zurich")).date()
    
    for entry in os.listdir(DATA_PATH):
        if entry.startswith("DATE="):
            folder_date = date.fromisoformat(entry.split("=")[1])
            if folder_date < today:
                compact_day(entry)

if __name__ == "__main__":
    main()