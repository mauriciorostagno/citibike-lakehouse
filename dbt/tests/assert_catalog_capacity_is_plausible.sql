{{ config(severity = 'warn') }}

-- Catalog capacity below half the docks the station is actually reporting.
-- Mild disagreement is normal: capacity is a nominal figure the feed doesn't keep in
-- sync, and it sits under the live dock count on most of the system. This far off is
-- a broken catalog row, not staleness -- Grand Army Plaza went from 39 to 1 overnight.
-- Warns rather than fails: the source is wrong, and nothing downstream divides by it.

select
    station_id,
    max(capacity)    as catalog_capacity,
    max(total_docks) as docks_reported,
    count(*)         as affected_captures
from {{ ref('fct_station_availability') }}
where is_operational
  and capacity is not null
  and capacity < total_docks / 2
group by station_id
