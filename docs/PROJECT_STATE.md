# Project state

Working notes for anyone picking this up mid-flight. The README covers what the project
is; this file covers how it got there and what is still open.

Last updated: 2026-08-31.

## Status

Complete and running unattended. `dbt build` covers 7 models, 1 snapshot and 43 tests.
Both ingestion jobs and the dbt task run on schedule, CI runs on every pull request, and
two independent monitors watch for a stall.

## Environment

| | |
|---|---|
| Databricks | Free Edition, serverless only, LinkedIn-verified (required for outbound internet) |
| Host | dbc-76d096b8-9aa0.cloud.databricks.com |
| SQL warehouse | Serverless Starter, 2X-Small |
| Catalog | `citibike` — schemas `bronze`, `silver`, `gold`, plus `ci_silver` / `ci_gold` |
| Volumes | `bronze.landing`, `bronze.checkpoints` |
| dbt | 1.12.0, adapter 1.12.4, dbt_utils 1.4.1, Python 3.12 |
| Repo | github.com/mauriciorostagno/citibike-lakehouse, cloned into the workspace as a Git folder |

Three dbt targets: `dev` (local, token from `.env`), `databricks_job` (token injected by
the job at run time), `ci` (GitHub secret, prefixes schemas with `ci_`).

## Free Edition limits that shaped decisions

- Outbound internet is blocked until the account is LinkedIn-verified.
- One SQL warehouse, 2X-Small.
- One active pipeline per type. The streaming table uses it, which is why CI excludes it.
- Max 5 concurrent job tasks.
- No Scala or R.

## Monitoring

Two layers, on purpose. `dbt source freshness` runs inside the job before the build, so a
stale bronze stops the pipeline instead of feeding gold. A separate Databricks alert
queries `station_status_raw` hourly and fires above 90 minutes without a capture. The
second one exists because a monitor living inside the job cannot report that the job
stopped running.

## Gotchas already hit

- Auto Loader infers every column as STRING unless `cloudFiles.inferColumnTypes=true`.
  Changing it requires deleting the `schemaLocation`.
- The checkpoint and the target table are one unit. Resetting one without the other
  duplicates rows. Clean reset is `TRUNCATE TABLE` plus `dbutils.fs.rm` of the checkpoint.
- A streaming table does not rewrite what it already processed. Changing its SQL only
  affects new rows; `--full-refresh` is needed to reclassify history.
- `relationships` ignores NULLs. It passed on five orphaned facts while `not_null` failed
  on the same column.
- An unknown member applied halfway is worse than none. Coalescing `station_key` but not
  `region_id` and `region_name` split the hourly aggregate in two.
- A PAT scoped to BI Tools cannot refresh a streaming table. Needs `pipelines` alongside
  `sql`.
- `dbt_utils.expression_is_true` does not accept `group_by_columns`. A rule needing a
  GROUP BY reads better as a singular test.
- The dbt task's Profiles Directory is relative to the Project directory, not the repo
  root. Setting both to `dbt` produces `dbt/dbt`.
- Waiting on a streaming table inside the job killed the Spark Connect session at 638
  seconds. The fix was giving the table its own `schedule` config and excluding it from
  the build.
- Spark Connect returns an empty `lastProgress` after a query terminates. Count the table.
- Databricks normalises Gmail aliases, so `+tag` addresses cannot dodge an existing
  account.
- GitHub Actions workflow files cannot be written through the Databricks or remote-file
  tooling. They have to be created by hand.

## Known limitations

- Captures are 30 minutes apart, so "average occupancy per hour" is two observations per
  hour, not a continuous average.
- One station in the status feed is missing from the catalog feed. It lands on the unknown
  member and `assert_no_unknown_station_facts` warns about it every run, by design.
- CI schemas are never cleaned up. At this volume it does not matter; at a real one it
  would need a teardown step.

## Next

Historical trip data from citibikenyc.com/system-data: monthly CSVs with origin,
destination and duration per ride. That would add a real fact table, millions of rows,
and an origin-destination flow map the GBFS feeds cannot support. Own build, own writeup.
