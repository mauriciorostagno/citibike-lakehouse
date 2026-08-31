-- Every station should have exactly one open version in the SCD2 dimension.
-- Two open versions make the temporal join match a capture twice and double that
-- station's numbers. No error, plausible output -- so it needs a test.

select
    station_id,
    count(*) as open_versions
from {{ ref('dim_station') }}
where is_current
group by station_id
having count(*) <> 1
