{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['station_id', 'captured_at'],
    on_schema_change = 'append_new_columns',
    liquid_clustered_by = 'captured_at'
) }}

-- One row per station per capture, joined to the station as it was described then.
-- Clustered on captured_at, same reason as silver: it's what the MERGE and every
-- downstream query filter on.
-- The temporal join at the bottom is why the SCD2 dimension exists: an equality join on
-- station_id would label a fact from three months ago with today's name and region.

with status as (

    select *
    from {{ ref('silver_station_status') }}

    {% if is_incremental() %}
    where captured_at > (
        select coalesce(max(captured_at) - interval 3 hours, timestamp'1970-01-01')
        from {{ this }}
    )
    {% endif %}

)

select
    s.captured_at,
    date(s.captured_at) as capture_date,
    hour(s.captured_at) as capture_hour,

    s.station_id,

    -- Stations missing from the catalog fall back to the unknown member instead of a
    -- null key, so joins downstream stay total.
    coalesce(d.station_key, 'UNKNOWN')  as station_key,
    coalesce(d.region_id, '-1')         as region_id,
    coalesce(d.region_name, 'Unknown')  as region_name,

    s.num_bikes_available,
    s.num_ebikes_available,
    s.num_docks_available,
    s.total_docks,
    d.capacity,

    -- Divided by the docks the station is actually reporting, not the catalog capacity:
    -- capacity sits below the live dock count on 1828 of 2509 stations, so it is not a
    -- denominator you can trust. total_docks comes from the same feed and the same
    -- instant as the numerator, which keeps the ratio inside 0..1 by construction.
    -- NULL when the station was not operating -- a fill rate there means nothing.
    case
        when s.is_operational and s.total_docks > 0
        then round(s.num_bikes_available / s.total_docks, 4)
    end as bike_fill_rate,

    s.is_operational,
    s.is_empty,
    s.is_full,
    s.report_lag_seconds

from status s
left join {{ ref('dim_station') }} d
    on  s.station_id  = d.station_id
    and s.captured_at >= d.valid_from
    and (d.valid_to is null or s.captured_at < d.valid_to)
