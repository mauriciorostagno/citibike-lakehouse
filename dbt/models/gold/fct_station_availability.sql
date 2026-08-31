{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['station_id', 'captured_at'],
    on_schema_change = 'append_new_columns'
) }}

-- One row per station per capture, joined to the station as it was described then.
-- The temporal join at the bottom is why the SCD2 dimension exists: an equality join on
-- station_id would attach today's capacity to a fact from three months ago.

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

    -- Measured against the capacity on record at capture time. NULL when the station
    -- wasn't operating -- a fill rate on a switched-off station means nothing.
    case
        when s.is_operational and d.capacity > 0
        then round(s.num_bikes_available / d.capacity, 4)
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
