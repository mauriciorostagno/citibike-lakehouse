{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['station_id', 'captured_at'],
    on_schema_change = 'append_new_columns',
    liquid_clustered_by = 'captured_at'
) }}

-- One row per station per capture, typed and deduped.
-- Clustered on captured_at: the incremental filter and the MERGE both key off it, and
-- without clustering the merge scans more of the table every month.
-- Merge (not append) because bronze can duplicate if a stream is reprocessed --
-- this way the model lands on the same result however many times it runs.
-- Lookback window instead of a strict > cutoff so a late capture isn't dropped.

with source as (

    select *
    from {{ source('bronze', 'station_status_raw') }}

    {% if is_incremental() %}
    where captured_at > (
        select coalesce(max(captured_at) - interval 3 hours, timestamp'1970-01-01')
        from {{ this }}
    )
    {% endif %}

),

deduplicated as (

    -- Same (station, capture) can land twice in bronze. Keep the newest write.
    select
        *,
        row_number() over (
            partition by station_id, captured_at
            order by _ingested_at desc
        ) as _row_number
    from source

),

typed as (

    select
        station_id,
        captured_at,

        num_bikes_available,
        num_ebikes_available,
        num_bikes_disabled,
        num_docks_available,
        num_docks_disabled,

        cast(is_installed as boolean) as is_installed,
        cast(is_renting   as boolean) as is_renting,
        cast(is_returning as boolean) as is_returning,

        -- Feed sends this as a Unix epoch.
        to_timestamp(last_reported) as last_reported_at,

        -- How stale the station's own report was when we captured it. A big number
        -- means the station went quiet, not that the row is wrong.
        cast(unix_timestamp(captured_at) - last_reported as int) as report_lag_seconds,

        -- Derived here instead of taken from the catalog, so this model doesn't
        -- depend on station_information.
        num_bikes_available + num_bikes_disabled
            + num_docks_available + num_docks_disabled as total_docks,

        -- Availability only means something on a station that's actually working.
        cast(is_installed as boolean)
            and cast(is_renting as boolean)
            and cast(is_returning as boolean) as is_operational,

        -- NULL, not false, when the station is off: the question doesn't apply.
        case when cast(is_installed as boolean)
                  and cast(is_renting as boolean)
                  and cast(is_returning as boolean)
             then num_bikes_available = 0 end as is_empty,

        case when cast(is_installed as boolean)
                  and cast(is_renting as boolean)
                  and cast(is_returning as boolean)
             then num_docks_available = 0 end as is_full,

        _rescued_data,
        _source_file,
        _ingested_at

    from deduplicated
    where _row_number = 1

)

select * from typed
