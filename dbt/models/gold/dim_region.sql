-- Seven boroughs and districts, plus an unknown member.
-- The '-1' row keeps every join into this dimension total: some stations carry no
-- region_id, and a station missing from the catalog carries nothing at all.

select
    region_id,
    region_name,
    _ingested_at as loaded_at
from {{ source('bronze', 'system_regions_raw') }}

union all

select
    '-1'                    as region_id,
    'Unknown'               as region_name,
    cast(null as timestamp) as loaded_at
