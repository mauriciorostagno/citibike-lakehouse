-- Auto Loader parks unexpected fields in _rescued_data instead of failing, which keeps
-- the pipeline alive but silent. Without this we'd notice a feed change months late.
-- Singular test: passes when it returns no rows.

select
    captured_at,
    station_id,
    _rescued_data
from {{ ref('silver_station_status') }}
where _rescued_data is not null
