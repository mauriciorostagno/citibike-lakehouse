-- One row per station, scoring how it behaved across the observed window.
-- Out-of-service stations show up in almost every capture, stations that run out of
-- bikes in about a third. Different problems, different owners: one is a maintenance
-- ticket, the other a redistribution truck. health_status makes that a column so the
-- dashboard can rank by it and the reader knows what to do.

with facts as (

    select *
    from {{ ref('fct_station_availability') }}

),

per_station as (

    select
        station_id,

        count(*)                                       as observations,
        sum(case when is_operational then 1 else 0 end) as operational_observations,
        sum(case when is_empty       then 1 else 0 end) as empty_observations,
        sum(case when is_full        then 1 else 0 end) as full_observations,

        avg(bike_fill_rate) as avg_fill_rate,
        min(captured_at)    as first_seen_at,
        max(captured_at)    as last_seen_at

    from facts
    group by station_id

),

rated as (

    select
        *,
        round((observations - operational_observations) / nullif(observations, 0), 4) as downtime_rate,
        round(empty_observations / nullif(operational_observations, 0), 4)            as empty_rate,
        round(full_observations  / nullif(operational_observations, 0), 4)            as full_rate
    from per_station

),

classified as (

    select
        *,
        case
            -- Chronic: this is how the station usually is. Structural.
            when downtime_rate >= 0.5 then 'CHRONIC_OUTAGE'
            when empty_rate    >= 0.5 then 'CHRONICALLY_EMPTY'
            when full_rate     >= 0.5 then 'CHRONICALLY_FULL'

            -- Episodic: happens, then recovers. Operational.
            when empty_rate >= 0.15 then 'EPISODICALLY_EMPTY'
            when full_rate  >= 0.15 then 'EPISODICALLY_FULL'

            else 'HEALTHY'
        end as health_status
    from rated

)

select
    c.station_id,

    -- A station the catalog doesn't list still belongs here: it's measurably unhealthy
    -- and dropping it would hide that.
    coalesce(d.station_name, 'Unknown station') as station_name,
    d.short_name,
    d.latitude,
    d.longitude,
    d.capacity,
    coalesce(d.region_id, '-1')                 as region_id,
    coalesce(d.region_name, 'Unknown')          as region_name,

    c.health_status,

    -- Split from the status so a dashboard can filter on either without parsing strings.
    c.health_status like 'CHRONIC%' as is_chronic,

    c.observations,
    c.operational_observations,
    c.downtime_rate,
    c.empty_rate,
    c.full_rate,
    round(c.avg_fill_rate, 4) as avg_fill_rate,

    c.first_seen_at,
    c.last_seen_at

from classified c
left join {{ ref('dim_station') }} d
    on  c.station_id = d.station_id
    and d.is_current
