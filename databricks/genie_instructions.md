# Genie space instructions

Paste into the Genie space's Instructions field. Kept here so the agent's behaviour is
reviewable like the rest of the project.

---

This workspace covers the Citi Bike NYC bike-share system. Data comes from the public
GBFS feeds every 30 minutes and is modelled into a star schema.

## What the tables are

- `mart_station_health` -- one row per station, scoring its behaviour across the whole
  observed window. Use it for questions about individual stations, rankings, or where
  problems are.
- `agg_system_hourly` -- one row per region per hour. Use it for time, trends, or hours
  of the day.
- `dim_station` -- one row per *version* of a station, not per station. A new row appears
  when name, capacity, coordinates or region change. Filter `is_current = true` unless
  the question is about history.
- `dim_region` -- seven boroughs and districts, plus an 'Unknown' member.
- `fct_station_availability` -- one row per station per capture, the raw grain. Only for
  point-in-time questions ("how many bikes at station X at 15:00 on 26 August"). For
  anything aggregated use the two tables above: they already apply the
  operational-stations rule, and recomputing from the fact is how that rule gets lost.

## Rules that matter for correct answers

- All rate columns are proportions between 0 and 1, not percentages. `empty_rate` of 0.43
  means 43% of the time. Multiply by 100 when presenting to a person.
- Availability metrics only count operational stations. A switched-off station reports
  zero bikes without anyone having taken one. `empty_rate` and `full_rate` are already
  measured against operational observations -- don't recompute them from raw counts.
- `health_status` separates two problems. `CHRONIC_*` means the station is like that most
  of the time, a maintenance issue. `EPISODICALLY_*` means it happens and recovers, a
  redistribution issue. If someone asks about "problem stations", ask which kind or
  report both.
- `is_chronic` is a boolean shortcut for the `CHRONIC_*` statuses.
- The 'Unknown' region and 'Unknown station' are records the status feed reports but the
  catalog feed doesn't list. Exclude them from rankings unless the question is about data
  quality.
- `captured_at` and `hour_start` are UTC. New York is UTC-4 in summer.

## Useful framings

- "How full is the system" -> `avg_fill_rate` in `agg_system_hourly`.
- "Which stations need attention" -> `mart_station_health`, ordered by `empty_rate` or
  `downtime_rate`, split by `is_chronic`.
- "When are the peaks" -> `capture_hour` in `agg_system_hourly`, averaged across days.
