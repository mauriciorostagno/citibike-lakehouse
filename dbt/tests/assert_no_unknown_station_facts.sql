{{ config(severity = 'warn') }}

-- Facts routed to the unknown member: stations the status feed reports but the catalog
-- feed doesn't list. Warn, not fail -- it's a property of the upstream feeds. But if the
-- count jumps, the catalog ingestion probably stopped running.

select
    station_id,
    count(*)         as orphan_facts,
    min(captured_at) as first_seen,
    max(captured_at) as last_seen
from {{ ref('fct_station_availability') }}
where station_key = 'UNKNOWN'
group by station_id
