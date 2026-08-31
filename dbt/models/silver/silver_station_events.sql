{{ config(
    materialized = 'streaming_table',
    schedule = {'every': '2 HOURS'}
) }}

-- Append-only log of stations in a notable state.
-- Streaming table works here because events are immutable: a station that was empty at
-- 14:35 was empty at 14:35, nothing later revises it. Streaming tables can't merge,
-- which is why silver_station_status isn't one.
-- Source is bronze, not silver: streaming reads need an append-only source.
--
-- The schedule above is why this model is excluded from the job's dbt build. A streaming
-- table is a pipeline, and it refreshes on its own cron. Making a 30-minute build wait
-- for it synchronously is what killed the Spark Connect session at 638 seconds.
-- Refresh cadence only affects how soon a new event shows up, not its timestamp:
-- captured_at comes from the data.

select
    station_id,
    captured_at,

    -- Order matters. An out-of-service station reports zero bikes, so checking
    -- availability first would file every dead station under NO_BIKES and turn a
    -- maintenance problem into a demand statistic.
    case
        when not cast(is_installed as boolean)                    then 'OUT_OF_SERVICE'
        when not cast(is_renting as boolean)
             and not cast(is_returning as boolean)                then 'DISABLED'
        when not cast(is_renting as boolean)                      then 'NOT_RENTING'
        when not cast(is_returning as boolean)                    then 'NOT_RETURNING'
        when num_bikes_available = 0                              then 'NO_BIKES'
        when num_docks_available = 0                              then 'NO_DOCKS'
    end as event_type,

    num_bikes_available,
    num_docks_available,
    _ingested_at

from stream({{ source('bronze', 'station_status_raw') }})

-- Filter only, no joins or aggregation: streaming tables handle stateless work well
-- and stateful work badly.
where num_bikes_available = 0
   or num_docks_available = 0
   or not cast(is_renting    as boolean)
   or not cast(is_returning  as boolean)
   or not cast(is_installed  as boolean)
