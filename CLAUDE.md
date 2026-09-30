# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Data logger for the Swiss EV charging infrastructure (open data from the Swiss Federal Office of Energy, `data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet`). Three standalone Python scripts collect and compact data; Jupyter notebooks do the analysis. There is no package, build step, test suite, or linter.

## Commands

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python update_charge.py     # infinite loop: poll dynamic EVSE status every 30s
python update_stations.py   # infinite loop: dump static station inventory every 24h
python compact.py           # one-shot: merge shards of past days into merged.parquet
```

All scripts use paths relative to the CWD (`data/...`), so run them from the repo root.

Deployment runs inside Docker on a remote server: `./env_build.sh` builds the `logger` image, `./env_start.sh` starts the `logger-runner` container with the repo mounted at `/workspace`, `./env_shell.sh` opens a shell in it (collectors are typically run in tmux there). `download_data.sh` pulls `data/` from that server via scp.

## Architecture and data format

- **`update_charge.py`** — polls the dynamic status JSON (`EVSEStatuses[].EVSEStatusRecord[]` → `EvseID`, `EVSEStatus`) and appends to a Hive-partitioned Parquet dataset at `data/charge/DATE=YYYY-MM-DD/` via `pq.write_to_dataset` (one new shard file per poll that has rows). Schema: `STATION_ID` string, `STATUS` string, `TIME` timestamp[s, Europe/Zurich], `DATE` date32 (partition column). ZSTD compression.
  - **Snapshot + deltas semantics:** the first poll of each day (and the first poll after a process restart, since `last_status` lives only in memory) writes every station's status; later polls write only stations whose status changed. So each `DATE=` partition is self-contained: to reconstruct state at time t, sort by `TIME`, take each station's first row of the day as baseline, and forward-fill later rows. Any change to the writer must preserve this invariant.
  - All rows from one poll share the same `TIME`.
- **`update_stations.py`** — saves the raw static inventory JSON as `data/stations/stations_YYYYMMDDHHMMSS.json.gz`. Structure is `EVSEData[]` (per operator: `OperatorID`, `OperatorName`, `EVSEDataRecord[]`); station records join to charge data via `EvseID` ↔ `STATION_ID`.
- **`compact.py`** — for each `DATE=` folder strictly before today (Zurich time) that has no `merged.parquet` yet, uses DuckDB to rewrite all shards into a single `merged.parquet` sorted by `TIME, STATION_ID`, then deletes the shards. Note the merged file drops the `DATE` column (it is recovered from the partition directory) and casts `TIME` to a naive `TIMESTAMP`.
- **Notebooks** — `preproc.ipynb` shows the canonical loading pattern: `pd.read_parquet("data/charge/", filters=[("DATE", ">=", ...), ...])`, drop `DATE`, sort, then resample/forward-fill per station. `occupancy.ipynb` produces `fig/occupancy.png` (shown in README). `tmp/` is gitignored scratch.

`data/` is gitignored; a sample dataset is in the GitHub release `v0.1` (`data.zip`).
