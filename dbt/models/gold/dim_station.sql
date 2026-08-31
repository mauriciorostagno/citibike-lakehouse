-- One row per version of a station, not per station.
-- station_key is a surrogate over (station_id, valid_from), so each version has its own
-- identity and a fact can point at the station as it was when the fact happened.

with versions as (

    select
        station_id,
        station_name,
        short_name,
        latitude,
        longitude,
        capacity,
        station_type,
        has_kiosk,
        region_id,
        dbt_valid_from,
        dbt_valid_to,
        row_number() over (partition by station_id order by dbt_valid_from) as version_number
    from {{ ref('snap_station_information') }}

),

bounded as (

    -- The snapshot only knows history from the day it first ran, so every station's
    -- first version starts then. Facts captured before that would match nothing.
    -- Back-dating to 1970 says: this is the oldest description we have, apply it back.
    select
        *,
        case when version_number = 1 then timestamp'1970-01-01' else dbt_valid_from end as valid_from,
        dbt_valid_to as valid_to
    from versions

),

real_stations as (

    select
        {{ dbt_utils.generate_surrogate_key(['b.station_id', 'b.dbt_valid_from']) }} as station_key,

        b.station_id,
        cast(b.version_number as int) as version_number,

        b.station_name,
        b.short_name,
        b.latitude,
        b.longitude,
        b.capacity,
        b.station_type,
        b.has_kiosk,

        -- Id and name are coalesced as a pair. Leaving one NULL while the other falls
        -- back splits one bucket into two groups and every aggregate downstream halves.
        coalesce(b.region_id, '-1')        as region_id,
        coalesce(r.region_name, 'Unknown') as region_name,

        b.valid_from,
        b.valid_to,
        b.valid_to is null as is_current

    from bounded b
    left join {{ ref('dim_region') }} r
        on b.region_id = r.region_id

),

unknown_member as (

    -- The status feed reports stations the catalog doesn't list. An inner join would
    -- drop those measurements and a null key would break downstream joins, so they
    -- land here instead. version_number 0 marks the row as synthetic.
    select
        'UNKNOWN'               as station_key,
        '-1'                    as station_id,
        cast(0 as int)          as version_number,
        'Unknown station'       as station_name,
        cast(null as string)    as short_name,
        cast(null as double)    as latitude,
        cast(null as double)    as longitude,
        cast(null as bigint)    as capacity,
        cast(null as string)    as station_type,
        cast(null as boolean)   as has_kiosk,
        '-1'                    as region_id,
        'Unknown'               as region_name,
        timestamp'1970-01-01'   as valid_from,
        cast(null as timestamp) as valid_to,
        false                   as is_current

)

select * from real_stations
union all
select * from unknown_member
