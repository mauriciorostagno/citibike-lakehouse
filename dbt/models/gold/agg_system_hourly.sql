-- System occupancy by region and hour. The table a time-series chart reads directly.
-- Rates are over observations, not distinct stations: a station empty in one of the two
-- captures in an hour counts as one empty observation out of two, which is what a
-- 30-minute sample can honestly claim.
-- Denominators use operational observations only, otherwise a maintenance backlog looks
-- like healthy supply.

with facts as (

    select *
    from {{ ref('fct_station_availability') }}

),

hourly as (

    select
        date_trunc('hour', captured_at)                 as hour_start,
        capture_date,
        capture_hour,
        region_id,
        region_name,

        count(*)                                        as observations,
        count(distinct station_id)                      as stations,
        sum(case when is_operational then 1 else 0 end) as operational_observations,

        sum(num_bikes_available)                        as bikes_available,
        sum(num_ebikes_available)                       as ebikes_available,
        sum(num_docks_available)                        as docks_available,

        avg(bike_fill_rate)                             as avg_fill_rate,
        sum(case when is_empty then 1 else 0 end)       as empty_observations,
        sum(case when is_full  then 1 else 0 end)       as full_observations,
        avg(report_lag_seconds)                         as avg_report_lag_seconds

    from facts
    group by 1, 2, 3, 4, 5

)

select
    hour_start,
    capture_date,
    capture_hour,
    region_id,
    region_name,

    stations,
    observations,
    operational_observations,

    bikes_available,
    ebikes_available,
    docks_available,

    round(avg_fill_rate, 4) as avg_fill_rate,

    round((observations - operational_observations) / nullif(observations, 0), 4)
                                                                       as downtime_rate,
    round(empty_observations / nullif(operational_observations, 0), 4) as empty_rate,
    round(full_observations  / nullif(operational_observations, 0), 4) as full_rate,
    round(avg_report_lag_seconds, 1)                                   as avg_report_lag_seconds

from hourly
