# CitiBike Lakehouse

A lakehouse built on Databricks Free Edition over live Citi Bike (NYC) data.
One source enters through two paths: an incremental one using Structured Streaming
with checkpoints, and a batch one with SCD2 historization.

## Stack

Databricks (serverless) · PySpark Structured Streaming · Auto Loader · Delta Lake ·
Unity Catalog · dbt · Databricks Workflows

## Architecture

| Layer | Tool | What it does |
|---|---|---|
| Landing | Python + requests | Pulls raw JSON from the GBFS API into a Unity Catalog volume |
| Bronze | PySpark + Auto Loader | Reads incrementally with a checkpoint, explodes the station array |
| Silver | dbt | Incremental model with MERGE, typing and cleanup |
| Gold | dbt | Facts and dimensions, SCD2 snapshot |

## Source

GBFS feeds from Citi Bike NYC (`https://gbfs.lyft.com/gbfs/1.1/bkn/en`):

- `station_status.json` — 2,509 stations, refreshed every ~10s. Incremental.
- `station_information.json` — station catalog. Batch, historized as SCD2.
- `system_regions.json` — 7 regions. Static dimension.

## Layout

    databricks/notebooks/   Ingestion and bronze notebooks
    dbt/                    dbt project: silver and gold
    .env                    Databricks token (not versioned)

## Setup

1. Copy `.env.example` to `.env` and fill in `DBT_DATABRICKS_TOKEN`.
2. `pip install dbt-databricks`
3. `cd dbt && dbt debug --profiles-dir .`
