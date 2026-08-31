# Project state

Working notes for anyone (human or agent) picking this project up mid-flight.
Last updated: 2026-08-27.

## Goal

A portfolio-grade lakehouse on Databricks Free Edition that exercises Spark, dbt and a
public API, with one incremental path and one batch path. The point is not the data —
it is being able to defend every design decision in an interview.

## Status

The medallion is complete end to end. `dbt build` runs 5 models, 1 snapshot and 31
tests: 36 pass, 1 warns by design. Both ingestion jobs run on schedule.

Remaining: an hourly aggregate, a dbt task inside the Databricks job, a dashboard,
the README, and putting the whole thing on GitHub — the repo is not under version
control yet.

## Environment

| | |
|---|---|
| Databricks | Free Edition, serverless only, LinkedIn-verified (required for outbound internet) |
| Host | dbc-76d096b8-9aa0.cloud.databricks.com |
| SQL warehouse | Serverless Starter, 2X-Small, `/sql/1.0/warehouses/2d199cdfa88568b6` |
| Catalog | `citibike` — schemas `bronze`, `silver`, `gold` |
| Volumes | `bronze.landing`, `bronze.checkpoints` |
| dbt | 1.12.0, adapter 1.12.4, dbt_utils 1.4.1, Python 3.12.10, local `.venv` |
| Auth | PAT in `.env` as `DBT_DATABRICKS_TOKEN`, scopes `sql` + `pipelines` |

dbt runs from `dbt/` with `--profiles-dir .`, and the token must be loaded into the
shell first — dbt reads OS environment variables, not the `.env` file.

## Source

GBFS feeds from Citi Bike NYC, base `https://gbfs.lyft.com/gbfs/1.1/bkn/en`. No auth.

- `station_status` — ~2,509 stations, refreshed every ~10s. Incremental path.
- `station_information` — station catalog. Batch path, historised as SCD2.
- `system_regions` — 7 rows, joins on `region_id`.

## Pipeline

**Fast branch** — job `status_pipeline_30min`, every 30 minutes:
`01_ingest_station_status` (Python, lands raw JSON) -> `02_bronze_station_status`
(Auto Loader, `availableNow`, checkpoint) -> `bronze.station_status_raw`.

**Slow branch** — job `catalog_pipeline_daily`, 05:00 UTC:
`01_ingest_catalog` -> `02_bronze_catalog` (batch `spark.read`, full overwrite) ->
`bronze.station_information_raw`, `bronze.system_regions_raw`.

`_gbfs_common` holds the shared `fetch` / `land` helpers, pulled in with `%run`.

**dbt**

| Model | Materialisation | Why |
|---|---|---|
| `silver_station_status` | incremental, merge, 3h lookback | Converges no matter how often it runs |
| `silver_station_events` | streaming table | Events are immutable, so append-only fits |
| `snap_station_information` | snapshot, `check` strategy, `hard_deletes: invalidate` | SCD2 over the catalog |
| `dim_region` | table | 7 rows of reference data |
| `dim_station` | table | One row per station *version*, plus an unknown member |
| `fct_station_availability` | incremental, merge | Facts joined to the station as it was at capture time |

## Design decisions worth defending

- **Spark cannot pull from a REST API incrementally.** There is no
  `readStream.format("http")`. Extraction is plain Python on the driver.
- **Incremental is a property of the read, not of the data.** The API always returns
  all stations. Auto Loader is incremental because of the checkpoint; dbt because of
  `max(captured_at)` in the target.
- **`availableNow` over continuous streaming.** Real streaming semantics without
  compute running 24/7.
- **MERGE over APPEND in silver.** Bronze is append-only and can duplicate if a stream
  is reprocessed. MERGE makes the model converge. Idempotency.
- **Lookback window over a strict `>` cutoff.** A strict cutoff silently drops a
  late-arriving capture.
- **Streaming table for events, incremental merge for state.** Streaming tables cannot
  MERGE, so they suit immutable events and not corrected state.
- **`check` over `timestamp` snapshot strategy.** The feed has no reliable modified-at
  field, and `captured_at` moves every run — `timestamp` would open a new version for
  2,500 unchanged stations every day.
- **The temporal join is the whole point of SCD2**: `captured_at >= valid_from and
  (valid_to is null or captured_at < valid_to)`. An equality join would attach today's
  capacity to a fact from three months ago.
- **Back-dating the first version to 1970.** A snapshot only knows history from the day
  it started running; without back-dating, every fact predating it loses its dimension.
- **An unknown member instead of a null key.** The status feed reports stations the
  catalog does not list. An inner join would drop those measurements; a null key breaks
  downstream joins. The unknown member keeps the fact and makes the gap countable.

## The event-classification bug, worth writing up

The first version of `silver_station_events` evaluated availability before service
status. A switched-off station reports zero bikes, so every dead station was labelled
NO_BIKES — a maintenance problem counted as rider demand. **972 events were
mislabelled.** A "stations that run out of bikes" ranking built on that would have been
topped by broken hardware.

Found by noticing 545 rows with `is_renting = false` while the event log had zero
NOT_RENTING events. Fixed by reordering the CASE, splitting OUT_OF_SERVICE from
DISABLED, and adding `is_operational` so `is_empty` / `is_full` return NULL rather than
`true` where the question does not apply.

The numbers afterwards also separated two different problems: out-of-service stations
appear in ~8.8 captures each (chronic), stations that run out of bikes in ~3.2
(episodic). One is a maintenance ticket, the other is a redistribution truck.

## Gotchas already hit

- Auto Loader infers every column as STRING unless `cloudFiles.inferColumnTypes=true`.
  Changing it requires deleting the `schemaLocation`.
- The checkpoint and the target table are a pair. Resetting one without the other
  duplicates rows. Clean reset = `TRUNCATE TABLE` + `dbutils.fs.rm` of the checkpoint.
- A streaming table does not rewrite what it already processed. Changing the SQL only
  affects new rows; `--full-refresh` is required to reclassify history.
- **`relationships` ignores NULLs.** It passed on 5 orphaned facts while `not_null`
  failed on the same column. A `relationships` test without a `not_null` beside it is
  half a validation.
- A failing test is not automatically a data problem. `total_docks >= 1` failed on 924
  rows because switched-off stations report zero docks — the assumption was wrong, not
  the data. Scoping with `config: where:` is the fix; loosening the bound would have
  hidden it.
- A dbt PAT scoped to **BI Tools** cannot refresh a streaming table. Streaming tables
  are pipelines under the hood and need the `pipelines` scope alongside `sql`.
- `dbt_utils.expression_is_true` does not accept `group_by_columns`. A rule that needs
  a GROUP BY reads better as a singular test.
- Spark Connect returns an empty `lastProgress` after a query terminates. Verify by
  counting the table.
- Databricks normalises Gmail aliases, so `+tag` addresses cannot dodge an existing
  account.
- Free Edition allows one active pipeline per type. The streaming table uses it.

## Next up

1. `agg_system_hourly` in gold: occupancy by hour and region, operational stations only.
2. `git init`, first commit, push to GitHub. **Nothing is under version control yet.**
3. Add a dbt task to the Databricks job so the models refresh on schedule, and hang
   `dbt snapshot` off the daily catalog job — the snapshot is the one artefact that
   cannot be rebuilt after the fact.
4. AI/BI dashboard on gold.
5. README, drawing on this file.

## Known limitation

Captures are 30 minutes apart, so "average occupancy per hour" is really two
observations per hour, not a continuous average. Fine for day/night and peak patterns,
not for what happens between 08:05 and 08:55.
