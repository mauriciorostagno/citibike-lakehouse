-- Datasets behind the AI/BI dashboard.
--
-- Each block is one dataset in the dashboard's Data tab. They are deliberately thin:
-- the aggregation already happened in gold, so the dashboard does no work at query time.
--
-- Rates are returned as raw proportions, never pre-multiplied by 100. Percentage
-- formatting is a presentation decision and belongs in the widget: a column holding
-- 4.3 and named "pct" is ambiguous forever, a column holding 0.043 is not.

-- =====================================================================
-- ds_kpis · the counter row
-- Format downtime_rate and empty_rate as Percent, 1 decimal, in the widget.
-- =====================================================================
select
    count(*)                                     as stations_monitored,
    sum(case when is_chronic then 1 else 0 end)  as chronic_problem_stations,
    avg(downtime_rate)                           as downtime_rate,
    avg(empty_rate)                              as empty_rate
from citibike.gold.mart_station_health;

-- =====================================================================
-- ds_station_map · the map and the ranking table
-- The unknown member has no coordinates, so it drops out of the map here.
-- =====================================================================
select
    station_id,
    station_name,
    region_name,
    latitude,
    longitude,
    capacity,
    health_status,
    is_chronic,
    downtime_rate,
    empty_rate,
    full_rate,
    avg_fill_rate,
    observations
from citibike.gold.mart_station_health
where latitude is not null
  and longitude is not null;

-- =====================================================================
-- ds_hourly · the time series
-- 'Unknown' is excluded: it is a single orphan station, and one observation should
-- not draw a line as prominent as a district with two thousand.
-- =====================================================================
select
    hour_start,
    capture_hour,
    region_name,
    avg_fill_rate,
    empty_rate,
    full_rate,
    downtime_rate,
    bikes_available,
    docks_available,
    stations
from citibike.gold.agg_system_hourly
where region_name <> 'Unknown'
order by hour_start;

-- =====================================================================
-- ds_health_mix · how the fleet splits by status
-- =====================================================================
select
    health_status,
    count(*)                as stations,
    round(avg(capacity), 1) as avg_capacity,
    avg(empty_rate)         as empty_rate
from citibike.gold.mart_station_health
group by health_status
order by stations desc;

-- =====================================================================
-- ds_regions · the filter dimension
-- Unique on region_name, so it can sit on the "one" side of a relationship to both
-- ds_station_map and ds_hourly. One filter then drives the whole dashboard — the same
-- star schema the warehouse uses, restated at the presentation layer.
-- =====================================================================
select
    region_id,
    region_name
from citibike.gold.dim_region
order by region_name;
