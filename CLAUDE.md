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

Locally there is no `.venv`; the notebooks' kernel ("dev") is the conda env at `~/anaconda3/envs/dev` (`~/anaconda3/envs/dev/bin/python`, `.../bin/jupyter nbconvert --to notebook --execute --inplace X.ipynb`). The system `python3` lacks pyarrow.

Deployment runs inside Docker on a remote server: `./env_build.sh` builds the `logger` image, `./env_start.sh` starts the `logger-runner` container with the repo mounted at `/workspace`, `./env_shell.sh` opens a shell in it (collectors are typically run in tmux there). `download_data.sh` pulls `data/` from that server via scp. Logger changes only take effect after restarting it there.

## Architecture and data format

- **`update_charge.py`** — polls the dynamic status JSON (`EVSEStatuses[]` per operator → `EVSEStatusRecord[]` → `EvseID`, `EVSEStatus`) and appends to a Hive-partitioned Parquet dataset at `data/charge/DATE=YYYY-MM-DD/` via `pq.write_to_dataset` (one new shard file per poll that has rows). Schema: `STATION_ID` string, `STATUS` string, `TIME` timestamp[s, Europe/Zurich], `DATE` date32 (partition column). ZSTD compression.
  - **Snapshot + deltas semantics:** the first poll of each day (and the first poll after a process restart, since `last_status` lives only in memory) writes every station's status; later polls write only stations whose status changed. So each `DATE=` partition is self-contained: to reconstruct state at time t, sort by `TIME`, take each station's first row of the day as baseline, and forward-fill later rows. Any change to the writer must preserve this invariant.
  - All rows from one poll share the same `TIME`.
  - **Duplicate EvseIDs in the feed:** a few IDs are listed under two operators (e.g. swisscharge `CH*SUI` re-listing eCarUp/Saascharge points, usually with `Unknown`; 4,163 IDs shared by `CH*EWZ`/`CH*IWB` on 2026-03-04..11). The logger collapses them per poll: the owner's copy (the one whose `OperatorID` prefixes the EvseID) wins, then a non-`Unknown` status. Keying by (operator, EVSE) was considered and rejected as not worth the schema change. Before this fix, conflicting copies produced two rows per poll at the same `TIME`.
  - **`Missing` status:** a station seen before but absent from the current poll gets one `STATUS = "Missing"` row and is forgotten (so a reappearance is written as a change). The feed also emits `EvseNotFound`.
  - Each poll logs the number of duplicated / conflicting EvseIDs (normally ~5 / ~1) and any stations that dropped out.
- **`update_stations.py`** — saves the raw static inventory JSON as `data/stations/stations_YYYYMMDDHHMMSS.json.gz`. Structure is `EVSEData[]` (per operator: `OperatorID`, `OperatorName`, `EVSEDataRecord[]`); station records join to charge data via `EvseID` ↔ `STATION_ID`.
- **`compact.py`** — for each `DATE=` folder strictly before today (Zurich time) that has no `merged.parquet` yet, uses DuckDB to rewrite all shards into a single `merged.parquet` sorted by `TIME, STATION_ID`, then deletes the shards. The merged file drops the `DATE` column (it is recovered from the partition directory) and casts `TIME` to a naive `TIMESTAMP` in **UTC** — so compacted days are UTC-naive (a day starts at 22:00/23:00 of the previous date), while today's uncompacted shards are tz-aware Europe/Zurich. Any new column must be added to its explicit SELECT, and readers of a mixed dataset need an explicit `schema=` (pyarrow infers the schema from the oldest file and silently drops new columns).
- **Notebooks** — load with `pd.read_parquet("data/charge/", filters=[("DATE", ">=", start), ("DATE", "<=", end)])`, drop `DATE`, sort, then run the **cleanup cell** that follows the load cell (duplicated in `preproc.ipynb` and `occupancy.ipynb`): one status per `(STATION_ID, TIME)` preferring non-`Unknown`, drop rows that don't change a station's status, assert no duplicates. Historical data contains the duplicates, so per-station `resample`/`reindex`/`pivot` fail with "cannot reindex on an axis with duplicate labels" without it. `occupancy.ipynb`'s cleanup also infers `Missing` rows at full snapshots (polls with > 90% of stations) for data predating the logger's `Missing` rows.
  - Occupancy = cumulative sum of per-station changes in "is Occupied"; validated against snapshot ground truth (0 difference after the cleanup).

## Data caveats

- Daily midnight snapshots exist only from 2026-07-15; earlier ranges have no baseline, so a cumulative count ramps up at the start of the range.
- eCarUp (`CH*ECU`) statuses were frozen ~2026-05-22 → 2026-06-22 (945 stuck at `Occupied`); treat occupancy before 2026-06-23 as unreliable.
- Nightly feed outages show as short all-`Unknown` bursts (eCarUp ~02:01–02:06, `CH*SUI`/`CH*SHE` ~19:00–19:05), which appear as dips in occupancy since `Unknown` counts as not occupied.
- Per-operator analysis joins through the latest static dump, which counts re-listed IDs under both operators and misses stations no longer listed.

`data/` is gitignored; a sample dataset is in the GitHub release `v0.1` (`data.zip`). `fig/` is tracked (notebook outputs).
